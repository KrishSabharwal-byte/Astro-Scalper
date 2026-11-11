"""
Futures Slicing Scalper Engine — Symmetrical Long/Short Commodity Grid Scalper
Implements:
- Symmetrical 3-Step State Machine for both BUY (Long) and SELL (Short) commodity futures
- BUY Side: Dip-buying grid ladder (high->low), profit sell target (fill + profit_point), range-anchored stop loss (low - loss_point)
- SELL Side: Rally-shorting grid ladder (low->high), profit cover target (fill - profit_point), range-anchored stop loss (high + loss_point)
- Integer tick arithmetic precision and commodity-specific lot sizing (SILVERM, GOLDM, COPPER)
- Configurable gap-fill policies (all_crossed | single | skip_and_log)
- Contract locking on open positions & monthly expiry rollover support
- Feed staleness tracking & paused entries
- MCX session cutoff & automated EOD squareoff
- State persistence & crash recovery (to_dict / from_dict)
- Multi-slot isolation for 6 commodity slots (SILVERM-B/S, GOLDM-B/S, COPPER-B/S)
"""

import datetime
import json
import logging
import math
import os
import re
import uuid
from dataclasses import dataclass, field, asdict, fields
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger("FuturesEngine")

from angel_one_service import (
    COMMODITY_CONFIG,
    normalize_commodity_name,
    get_tick_size,
    get_lot_size,
)

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def is_market_open_ist(dt: Optional[datetime.datetime] = None) -> Tuple[bool, str]:
    """
    Checks if MCX Commodity futures market is currently open.
    Trading hours: Monday to Friday, 09:00 to 23:30 IST.
    Closed on:
    - Saturday and Sunday (weekends)
    - Before 09:00 AM IST
    - At or after 23:30 (11:30 PM IST)
    Returns: (is_open: bool, reason: str)
    """
    now = dt or datetime.datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)

    if now.weekday() in (5, 6):
        day_name = now.strftime("%A")
        return False, f"Market closed on weekend ({day_name})"

    t = now.time()
    open_t = datetime.time(9, 0)
    cutoff_t = datetime.time(23, 30)

    if t < open_t:
        return False, f"Market not open yet (opens at 09:00 IST, current: {t.strftime('%H:%M:%S')})"
    if t >= cutoff_t:
        return False, f"Market closed for EOD (closed at 23:30 IST, current: {t.strftime('%H:%M:%S')})"

    return True, "Market is open"


class SliceStatus(str, Enum):
    UNUSED = "UNUSED"
    USED = "USED"
    FILLED = "FILLED"
    EXITED = "EXITED"
    CANCELLED = "CANCELLED"
    PENDING = "UNUSED"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value.upper() == "PENDING":
            return cls.UNUSED
        return super()._missing_(value)


class SliceExitReason(str, Enum):
    PROFIT_TARGET = "PROFIT_TARGET"
    LOSS_EXIT = "LOSS_EXIT"
    EOD_SQUAREOFF = "EOD_SQUAREOFF"
    MANUAL = "MANUAL"


class GapFillMode(str, Enum):
    ALL_CROSSED = "all_crossed"
    SINGLE = "single"
    SKIP_AND_LOG = "skip_and_log"


def get_sequential_label(index: int) -> str:
    """
    Converts 0-based integer to spreadsheet-style alphabetical label:
    0 -> A, 1 -> B, ... 25 -> Z, 26 -> AA, 27 -> AB, etc.
    """
    label = ""
    index += 1
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        label = chr(65 + remainder) + label
    return label


SLOT_CANONICAL_MAP: Dict[str, str] = {
    "run01": "SILVERM-B",
    "run02": "SILVERM-S",
    "run03": "GOLDM-B",
    "run04": "GOLDM-S",
    "run05": "COPPER-B",
    "run06": "COPPER-S",
    "run07": "CRUDEOIL-B",
    "run08": "CRUDEOIL-S",
    "SILVERM-B": "SILVERM-B",
    "SILVERM-S": "SILVERM-S",
    "GOLDM-B": "GOLDM-B",
    "GOLDM-S": "GOLDM-S",
    "COPPER-B": "COPPER-B",
    "COPPER-S": "COPPER-S",
    "CRUDEOIL-B": "CRUDEOIL-B",
    "CRUDEOIL-S": "CRUDEOIL-S",
}

SLOT_LEGACY_MAP: Dict[str, str] = {
    "SILVERM-B": "run01",
    "SILVERM-S": "run02",
    "GOLDM-B": "run03",
    "GOLDM-S": "run04",
    "COPPER-B": "run05",
    "COPPER-S": "run06",
    "CRUDEOIL-B": "run07",
    "CRUDEOIL-S": "run08",
}


def calculate_level_for_price(
    price: float, range_high: float, range_low: float, step: float, trade_side: str = "BUY"
) -> Optional[float]:
    """
    Calculates the exact grid rung L for a given price based on price-range membership:
    - For BUY (long): L is the rung where L - step < price <= L
      L = range_high - floor((range_high - price) / step) * step
    - For SELL (short): L is the rung where L <= price < L + step
      L = range_low + floor((price - range_low) / step) * step
    """
    if step <= 0:
        return None

    side = trade_side.upper()
    if side == "SELL":
        if price < range_low - 1e-4 or price > range_high + step + 1e-4:
            return None
        k = math.floor((price - range_low + 1e-7) / step)
        level_price = round(range_low + (k * step), 4)
        if level_price > range_high + 1e-4 or level_price < range_low - 1e-4:
            return None
        return level_price
    else:
        # BUY side (descending)
        if price > range_high + 1e-4 or price < range_low - step - 1e-4:
            return None
        k = math.floor((range_high - price + 1e-7) / step)
        level_price = round(range_high - (k * step), 4)
        if level_price < range_low - 1e-4 or level_price > range_high + 1e-4:
            return None
        return level_price


def recenter_range(
    current_price: float,
    span: float,
    step: float,
    range_points: Optional[float] = None,
    slicer_count: Optional[int] = None,
    slice_interval: Optional[float] = None,
    original_range_high: Optional[float] = None,
    original_range_low: Optional[float] = None,
    trade_side: str = "BUY",
) -> Tuple[float, float]:
    """
    Recenters grid range anchored to current_price based on trade_side:
    - BUY (LONG): range_high is anchored at/just above current_price (rounded UP to step),
      and range_low = range_high - span. The entire ladder sits at or below current_price.
    - SELL (SHORT): range_low is anchored at/just below current_price (rounded DOWN to step),
      and range_high = range_low + span. The entire ladder sits at or above current_price.
    Aligns boundary levels to exact step/slice_interval ticks.
    """
    if span <= 0:
        span = 800.0
    if step <= 0:
        step = 100.0

    if (
        original_range_high is not None
        and original_range_low is not None
        and original_range_low <= current_price <= original_range_high
    ):
        return round(original_range_high, 2), round(original_range_low, 2)

    step_int = max(1, int(round(step * 100)))
    curr_int = int(round(current_price * 100))
    span_int = int(round(span * 100))

    side = str(trade_side).upper()
    if side == "SELL":
        # SELL: anchor bottom rung at or just below current_price (round down to nearest step)
        low_int = int(math.floor(curr_int / step_int)) * step_int
        if low_int <= 0:
            low_int = step_int
        high_int = low_int + span_int
    else:
        # BUY: anchor top rung at or just above current_price (round up to nearest step)
        high_int = int(math.ceil(curr_int / step_int)) * step_int
        low_int = high_int - span_int
        if low_int <= 0:
            low_int = step_int
            high_int = low_int + span_int

    new_range_high = round(high_int / 100.0, 2)
    new_range_low = round(low_int / 100.0, 2)

    return new_range_high, new_range_low


def validate_config(cfg: "ScalperRunConfig") -> None:
    """Strict configuration validator. Fails fast with clear ValueError."""
    if cfg.range_low < 0:
        raise ValueError(f"range_low ({cfg.range_low}) must be >= 0.")
    if cfg.range_high <= cfg.range_low:
        raise ValueError(f"range_low ({cfg.range_low}) must be strictly less than range_high ({cfg.range_high}).")
    if cfg.range_points is not None and cfg.range_points <= 0:
        raise ValueError(f"range_points ({cfg.range_points}) must be strictly greater than 0.")
    if cfg.slicer_count is not None and cfg.slicer_count < 1:
        raise ValueError(f"slicer_count ({cfg.slicer_count}) must be an integer >= 1.")
    if cfg.slice_interval <= 0:
        raise ValueError(f"slice_interval ({cfg.slice_interval}) must be greater than 0.")
    if cfg.profit_point <= 0:
        raise ValueError(f"profit_point ({cfg.profit_point}) must be greater than 0.")
    if cfg.loss_point <= 0:
        raise ValueError(f"loss_point ({cfg.loss_point}) must be greater than 0.")
    if cfg.qty_per_slice_lots <= 0:
        raise ValueError(f"qty_per_slice_lots ({cfg.qty_per_slice_lots}) must be at least 1.")

    span = round(cfg.range_high - cfg.range_low, 4)
    step = round(cfg.slice_interval, 4)
    remainder = round(span % step, 4)
    if remainder > 1e-4 and round(step - remainder, 4) > 1e-4:
        raise ValueError(
            f"Range span ({span}) must be evenly divisible by slice_interval ({step}). Remainder: {remainder}"
        )


@dataclass
class Slice:
    level_price: float
    status: SliceStatus = SliceStatus.UNUSED
    label: Optional[str] = None
    quantity: int = 5
    profit_point: float = 100.0
    profit_target: Optional[float] = None
    sl_price: Optional[float] = None
    loss_trigger: Optional[float] = None
    fill_price: Optional[float] = None
    filled_at: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[SliceExitReason] = None
    exited_at: Optional[str] = None
    order_id: Optional[str] = None
    pnl_points: Optional[float] = None
    pnl_rupees: Optional[float] = None
    trade_side: str = "BUY"

    def mark_used(self) -> None:
        """Marks grid level as USED after exit to enforce one-trade-per-level."""
        self.status = SliceStatus.USED
        self.order_id = None

    def reset_to_unused(self) -> None:
        """Resets slice state back to UNUSED on ladder re-centering or reset."""
        self.status = SliceStatus.UNUSED
        self.label = None
        self.fill_price = None
        self.filled_at = None
        self.profit_target = None
        self.exit_price = None
        self.exit_reason = None
        self.exited_at = None
        self.order_id = None
        self.pnl_points = None
        self.pnl_rupees = None

    def reset_to_pending(self) -> None:
        self.reset_to_unused()


@dataclass
class ScalperRunConfig:
    run_id: str = "run01"
    commodity: str = "SILVERM"
    trade_side: str = "BUY"  # "BUY" (Long) | "SELL" (Short)
    contract_symbol: Optional[str] = None
    contract_token: Optional[str] = None
    expiry: Optional[str] = None
    product_type: str = "INTRADAY"  # "INTRADAY" | "CARRYFORWARD"
    range_points: Optional[float] = None
    slicer_count: Optional[int] = None
    slice_interval: float = 100.0
    range_high: float = 90400.0
    range_low: float = 89600.0
    qty_per_slice_lots: int = 1
    profit_point: float = 100.0
    loss_point: float = 150.0
    poll_interval_seconds: float = 2.0
    last_ltp: Optional[float] = None
    lock_contract_on_entry: bool = True
    auto_reresolve: bool = False
    flicker_guard_ticks: int = 3
    gap_fill_mode: str = "all_crossed"
    cutoff_time_ist: str = "23:30"
    market_open_ist: str = "09:00"
    auto_eod_squareoff: bool = True
    trading_mode: str = "paper"  # "paper" | "live"

    # Backward compatibility aliases
    instrument_name: Optional[str] = None
    option_type: Optional[str] = None

    def __post_init__(self):
        self.commodity = normalize_commodity_name(self.commodity or self.instrument_name or "SILVERM")
        self.instrument_name = self.commodity
        self.trade_side = self.trade_side.upper() if self.trade_side else "BUY"
        self.option_type = self.trade_side

        if self.qty_per_slice_lots is None:
            self.qty_per_slice_lots = COMMODITY_CONFIG.get(self.commodity, {}).get("default_qty_per_slice_lots", 1)

        if self.contract_symbol is None:
            self.contract_symbol = f"{self.commodity} FUT"

        if self.range_points is not None and self.slicer_count is not None:
            if self.range_points <= 0:
                raise ValueError(f"range_points ({self.range_points}) must be strictly greater than 0.")
            if self.slicer_count < 1:
                raise ValueError(f"slicer_count ({self.slicer_count}) must be an integer >= 1.")
            self.slicer_count = int(self.slicer_count)
            self.slice_interval = round(self.range_points / self.slicer_count, 4)
            if self.range_high is not None:
                self.range_low = round(self.range_high - self.range_points, 4)
        else:
            self.range_points = round(self.range_high - self.range_low, 4)
            if self.slice_interval > 0:
                self.slicer_count = max(1, int(round((self.range_high - self.range_low) / self.slice_interval)))
            else:
                self.slicer_count = 1
        validate_config(self)

    @property
    def lot_size(self) -> int:
        return get_lot_size(self.commodity)

    @property
    def total_slice_quantity(self) -> int:
        return self.qty_per_slice_lots * self.lot_size

    @property
    def original_span(self) -> float:
        return round(self.range_high - self.range_low, 4)


RunConfig = ScalperRunConfig


@dataclass
class TradeRecord:
    trade_id: str
    run_id: str
    commodity: str
    trade_side: str
    action: str
    label: str
    level_price: float
    fill_price: float
    exit_price: float
    quantity: int
    pnl_points: float
    pnl_rupees: float
    exit_reason: str
    filled_at: str
    exited_at: str
    contract_symbol: str
    contract_token: Optional[str] = None
    expiry: Optional[str] = None
    entry_time: Optional[str] = None
    exit_time: Optional[str] = None
    symbol: Optional[str] = None
    instrument: Optional[str] = None
    run_number: int = 1
    mode: str = "paper"
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None

    def __post_init__(self):
        if self.entry_time is None:
            self.entry_time = self.filled_at
        if self.exit_time is None:
            self.exit_time = self.exited_at
        if self.symbol is None:
            self.symbol = self.commodity
        if self.instrument is None:
            self.instrument = self.commodity


class ScalperRun:
    """
    Independent Commodity Futures Scalper Run executing the symmetrical 3-step state machine.
    Fully isolated per run_id with zero shared mutable state.
    """

    def __init__(
        self,
        config: Any,
        trade_callback: Optional[Any] = None,
        order_callback: Optional[Any] = None,
    ):
        if isinstance(config, str):
            if isinstance(trade_callback, ScalperRunConfig):
                self.config = trade_callback
                trade_callback = order_callback
                order_callback = None
            else:
                self.config = ScalperRunConfig(run_id=config)
            self.run_id = config
        else:
            self.config = config
            self.run_id = self.config.run_id

        self.trade_callback = trade_callback
        self.order_callback = order_callback
        self.is_active = False
        self.run_number = 1
        self.cycle_count = 1
        self.label_counter = 0
        self.last_ltp: Optional[float] = None
        self.last_processed_seq: Optional[int] = None
        self.accumulated_realized_pnl: float = 0.0
        self.active_slices: List[Slice] = []
        self.grid_ladder: List[Slice] = []
        self.trade_history: List[TradeRecord] = []
        self.audit_events: List[Dict[str, Any]] = []

        self.locked_contract_symbol: Optional[str] = self.config.contract_symbol
        self.locked_contract_token: Optional[str] = self.config.contract_token
        self.contract_resolver: Optional[Any] = None

        self.feed_status: str = "LIVE"
        self.stale_polls_count: int = 0
        self.stale_threshold_polls: int = 3
        self.eod_squared_off: bool = False
        self.last_eod_date: Optional[datetime.date] = None
        self.last_stop_reason: Optional[str] = None
        self._slot_id: Optional[str] = None
        self.last_filled_level: Optional[float] = None

        self._generate_grid()

    @property
    def realized_pnl(self) -> float:
        return float(self.accumulated_realized_pnl or 0.0)

    def get_unrealized_pnl(self) -> float:
        total = 0.0
        ltp = self.last_ltp
        if ltp is None:
            return 0.0
        for s in self.active_slices:
            fill = s.fill_price if s.fill_price is not None else s.level_price
            if self.config.trade_side == "SELL":
                pnl_pts = fill - ltp
            else:
                pnl_pts = ltp - fill
            pnl_rup = pnl_pts * s.quantity
            total += pnl_rup
        return float(round(total, 2))

    @property
    def slot_id(self) -> str:
        if self._slot_id:
            return self._slot_id
        return SLOT_CANONICAL_MAP.get(self.run_id, self.run_id)

    @slot_id.setter
    def slot_id(self, val: str) -> None:
        self._slot_id = val

    @property
    def slot_code(self) -> str:
        return self.slot_id

    @property
    def slot_status(self) -> str:
        if self.is_active or len(self.active_slices) > 0:
            return "OPEN"
        elif len(self.trade_history) > 0:
            return "CLOSED"
        return "EMPTY"

    def _dispatch_order(
        self,
        action: str,
        quantity: int,
        price: float,
        order_type: str = "MARKET",
        product_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatches order to registered callback (ClientSession.place_order)."""
        symbol = self.locked_contract_symbol or self.config.contract_symbol
        token = self.locked_contract_token or self.config.contract_token
        trading_mode = getattr(self.config, "trading_mode", "paper") or "paper"
        prod_type = product_type or getattr(self.config, "product_type", "INTRADAY") or "INTRADAY"

        if self.order_callback:
            try:
                res = self.order_callback(
                    run_id=self.run_id,
                    action=action,
                    symbol=symbol,
                    token=token,
                    quantity=quantity,
                    price=price,
                    order_type=order_type,
                    product_type=prod_type,
                    trading_mode=trading_mode,
                )
                if isinstance(res, dict):
                    return res
            except Exception as e:
                return {"status": "error", "message": str(e), "mode": trading_mode}

        sim_id = f"sim_{uuid.uuid4().hex[:8]}"
        return {
            "status": "success",
            "order_id": sim_id,
            "mode": "paper",
            "fill_price": price,
        }

    def _get_timestamp(self) -> str:
        return datetime.datetime.now(IST).isoformat()

    def _generate_grid(self, custom_high: Optional[float] = None, custom_low: Optional[float] = None) -> None:
        """
        Generates ladder levels.
        - For BUY: descending ladder from range_high down to range_low. Stop loss = range_low - loss_point.
        - For SELL: ascending ladder from range_low up to range_high. Stop loss = range_high + loss_point.
        """
        self.grid_ladder.clear()
        high = custom_high if custom_high is not None else self.config.range_high
        low = custom_low if custom_low is not None else self.config.range_low
        step = self.config.slice_interval

        if high is None or low is None or step is None or step <= 0:
            return

        self.effective_range_high = high
        self.effective_range_low = low

        side = self.config.trade_side.upper()
        if side == "SELL":
            self.range_stop_loss = round(high + self.config.loss_point, 4)
            low_int = int(round(low * 100))
            high_int = int(round(high * 100))
            step_int = max(1, int(round(step * 100)))

            for lvl_int in range(low_int, high_int + 1, step_int):
                lvl_price = round(lvl_int / 100.0, 2)
                self.grid_ladder.append(
                    Slice(
                        level_price=lvl_price,
                        status=SliceStatus.UNUSED,
                        quantity=self.config.total_slice_quantity,
                        profit_point=self.config.profit_point,
                        sl_price=self.range_stop_loss,
                        trade_side="SELL",
                    )
                )
        else:
            # BUY side (descending)
            self.range_stop_loss = max(round(low - self.config.loss_point, 4), 0.0)
            high_int = int(round(high * 100))
            low_int = int(round(low * 100))
            step_int = max(1, int(round(step * 100)))

            for lvl_int in range(high_int, low_int - 1, -step_int):
                lvl_price = round(lvl_int / 100.0, 2)
                self.grid_ladder.append(
                    Slice(
                        level_price=lvl_price,
                        status=SliceStatus.UNUSED,
                        quantity=self.config.total_slice_quantity,
                        profit_point=self.config.profit_point,
                        sl_price=self.range_stop_loss,
                        trade_side="BUY",
                    )
                )

    def start(self) -> bool:
        # 1. Attempt to resolve contract if missing
        if not self.config.contract_symbol or not self.config.contract_token:
            if self.contract_resolver:
                try:
                    resolved = self.contract_resolver(self.config.commodity)
                    if resolved and resolved.get("symbol") and resolved.get("token"):
                        self.config.contract_symbol = resolved["symbol"]
                        self.config.contract_token = str(resolved["token"])
                        self.config.expiry = resolved.get("expiry_str")
                except Exception as e:
                    logger.warning(f"Error resolving contract on start for {self.run_id}: {e}")

        # 2. Defensive check for live trading mode
        mode = getattr(self.config, "trading_mode", "paper") or "paper"
        if mode == "live":
            tok = str(self.config.contract_token) if self.config.contract_token else ""
            if not tok or tok.startswith("token_"):
                logger.error(f"Cannot start {self.run_id} in LIVE mode without a resolved real MCX contract token.")
                return False

        # 3. Fallback contract symbol & token in paper mode if still None
        if not self.config.contract_symbol:
            today = datetime.date.today()
            sim_exp = (today + datetime.timedelta(days=30)).strftime("%d%b%Y").upper()
            self.config.contract_symbol = f"{self.config.commodity}{sim_exp}FUT"
            self.config.contract_token = f"token_{self.config.commodity.lower()}"

        self.is_active = True
        self.eod_squared_off = False
        self.locked_contract_symbol = self.config.contract_symbol
        self.locked_contract_token = str(self.config.contract_token) if self.config.contract_token else None
        self._record_event("RUN_STARTED", self.last_ltp or self.config.range_high, f"Commodity Scalper Run {self.run_id} started ({self.config.commodity} {self.config.trade_side}).")
        return True

    def reset(self) -> None:
        """Resets the slot run state cleanly, clearing active flags and unused slices."""
        self.is_active = False
        self.eod_squared_off = False
        self.last_stop_reason = None
        self.active_slices.clear()
        self._generate_grid()
        self._record_event("SLOT_RESET", self.last_ltp or self.config.range_high, f"Commodity Scalper Run {self.run_id} reset to idle state.")

    def stop(self) -> List[TradeRecord]:
        self.is_active = False
        self.last_stop_reason = "MANUAL_STOP"
        exited = []
        ltp = self.last_ltp if self.last_ltp is not None else self.config.range_high
        if self.active_slices:
            exited = self._exit_all_active_slices(exit_price=ltp, reason=SliceExitReason.MANUAL)
        self._record_event(
            "RUN_STOPPED",
            ltp,
            f"Commodity Scalper Run {self.run_id} stopped manually.",
            {"stop_reason": "MANUAL_STOP", "exited_count": len(exited)},
        )
        return exited

    def eod_squareoff(self, exit_price: Optional[float] = None) -> List[TradeRecord]:
        """Exits all active slices at market close cutoff and cuts/closes the grid."""
        self.is_active = False
        self.eod_squared_off = True
        self.last_stop_reason = "EOD_SQUAREOFF"
        self.last_eod_date = datetime.datetime.now(IST).date()
        price = exit_price if exit_price is not None else (self.last_ltp or self.config.range_high)
        exited = []
        if self.active_slices:
            exited = self._exit_all_active_slices(exit_price=price, reason=SliceExitReason.EOD_SQUAREOFF)
        self.active_slices.clear()
        self.grid_ladder.clear()
        self.last_filled_level = None
        self._record_event(
            "EOD_SQUAREOFF",
            price,
            f"MCX EOD Square-off executed at {price:.2f}. All positions squared off and grid closed at {self.config.cutoff_time_ist} IST.",
            {"exited_count": len(exited), "stop_reason": "EOD_SQUAREOFF"},
        )
        return exited

    def exit_slice(
        self,
        slice_identifier: Any,
        exit_price: Optional[float] = None,
        reason: SliceExitReason = SliceExitReason.MANUAL,
    ) -> Optional[TradeRecord]:
        """Manually exits a specific active slice."""
        if not self.active_slices:
            return None

        target_slice: Optional[Slice] = None
        ident_str = str(slice_identifier).strip() if slice_identifier is not None else ""
        ident_upper = ident_str.upper()

        for s in self.active_slices:
            if s.order_id and str(s.order_id).strip() == ident_str:
                target_slice = s
                break
            if s.label and str(s.label).strip().upper() == ident_upper:
                target_slice = s
                break

        if not target_slice and ident_upper in ("FIRST", "0", "LOWEST"):
            target_slice = self.active_slices[0]
        elif not target_slice:
            target_slice = self.active_slices[0]

        price = exit_price if exit_price is not None else (self.last_ltp or target_slice.fill_price or target_slice.level_price)
        fill = target_slice.fill_price if target_slice.fill_price is not None else target_slice.level_price

        side = self.config.trade_side.upper()
        # For BUY: profit = exit - fill, broker action = SELL
        # For SELL: profit = fill - exit, broker action = BUY (cover)
        if side == "SELL":
            pnl_pts = round(fill - price, 4)
            dispatch_action = "BUY"
        else:
            pnl_pts = round(price - fill, 4)
            dispatch_action = "SELL"

        pnl_rup = round(pnl_pts * target_slice.quantity, 4)
        timestamp = self._get_timestamp()

        exit_res = self._dispatch_order(
            action=dispatch_action,
            quantity=target_slice.quantity,
            price=price,
            order_type="MARKET",
        )
        exit_order_id = str(exit_res.get("order_id") or f"ORD-{dispatch_action}-{uuid.uuid4().hex[:8].upper()}")

        target_slice.status = SliceStatus.EXITED
        target_slice.exit_price = price
        target_slice.exit_reason = reason
        target_slice.exited_at = timestamp
        target_slice.pnl_points = pnl_pts
        target_slice.pnl_rupees = pnl_rup

        self.accumulated_realized_pnl = round(self.accumulated_realized_pnl + pnl_rup, 4)

        rec = TradeRecord(
            trade_id=f"TRD-{uuid.uuid4().hex[:8].upper()}",
            run_id=self.run_id,
            commodity=self.config.commodity,
            trade_side=side,
            action=dispatch_action,
            label=target_slice.label or "MANUAL",
            level_price=target_slice.level_price,
            fill_price=fill,
            exit_price=price,
            quantity=target_slice.quantity,
            pnl_points=pnl_pts,
            pnl_rupees=pnl_rup,
            exit_reason=reason.value,
            filled_at=target_slice.filled_at or timestamp,
            exited_at=timestamp,
            contract_symbol=self.locked_contract_symbol or self.config.contract_symbol,
            contract_token=self.locked_contract_token or self.config.contract_token,
            expiry=self.config.expiry,
            entry_time=target_slice.filled_at or timestamp,
            exit_time=timestamp,
            run_number=self.run_number,
            mode=getattr(self.config, "trading_mode", "paper") or "paper",
            entry_order_id=target_slice.order_id,
            exit_order_id=exit_order_id,
        )
        self._record_trade(rec)

        if target_slice in self.active_slices:
            self.active_slices.remove(target_slice)

        for grid_slice in self.grid_ladder:
            if abs(grid_slice.level_price - target_slice.level_price) < 1e-4:
                grid_slice.mark_used()
                break

        self._record_event(
            "MANUAL_SLICE_EXIT",
            price,
            f"Slice {target_slice.label or target_slice.order_id} exited at {price:.2f} (P&L: {'+' if pnl_pts >= 0 else ''}{pnl_pts:.2f} pts / {'+' if pnl_rup >= 0 else ''}₹{pnl_rup:.2f}).",
            {"trade": asdict(rec), "slice_identifier": slice_identifier},
        )
        return rec

    def check_eod_cutoff(
        self,
        current_time_ist: Optional[datetime.time] = None,
        current_date: Optional[datetime.date] = None,
    ) -> bool:
        """
        If auto_eod_squareoff is enabled:
        - At or after cutoff_time_ist (23:30 IST), before market_open_ist (09:00 IST),
          or on weekends (Saturday / Sunday), squares off all open slices and cuts the grid
          so stale grids are never carried forward into the next day.
        - During normal market hours (Monday to Friday, 09:00 <= time < 23:30 IST),
          resets eod_squared_off so new day trades and signals can open fresh grids.
        """
        ist_now = datetime.datetime.now(IST)
        now_t = current_time_ist if current_time_ist is not None else ist_now.time()
        now_d = current_date if current_date is not None else ist_now.date()

        try:
            cutoff_parts = [int(p) for p in self.config.cutoff_time_ist.split(":")[:2]]
            cutoff_t = datetime.time(cutoff_parts[0], cutoff_parts[1])
        except Exception:
            cutoff_t = datetime.time(23, 30)

        try:
            open_parts = [int(p) for p in self.config.market_open_ist.split(":")[:2]]
            open_t = datetime.time(open_parts[0], open_parts[1])
        except Exception:
            open_t = datetime.time(9, 0)

        is_weekend = (now_d.weekday() in (5, 6))
        is_market_hours = (not is_weekend) and (open_t <= now_t < cutoff_t)

        # 1. Trading hours: Monday to Friday 09:00 to 23:30 IST — safe window, never squareoff
        if is_market_hours:
            if self.eod_squared_off:
                self.eod_squared_off = False

            # Clean up any stale active slices filled prior to today's trading session
            if self.active_slices:
                stale_slices = []
                for s in self.active_slices:
                    if s.filled_at:
                        try:
                            fill_date_str = str(s.filled_at).split("T")[0].split(" ")[0]
                            fill_date = datetime.date.fromisoformat(fill_date_str)
                            if fill_date < now_d:
                                stale_slices.append(s)
                        except Exception:
                            pass
                if stale_slices:
                    logger.warning(
                        f"[EOD_CLEANUP] Slot {self.run_id}: Detected {len(stale_slices)} stale slices filled on "
                        f"previous date. Auto-squaring off stale slices."
                    )
                    self.eod_squareoff(exit_price=self.last_ltp)
                    return True
            return False

        # 2. Outside trading hours (at/after 23:30, before 09:00, or Saturday / Sunday)
        if not self.config.auto_eod_squareoff:
            return False

        # Fallback price if last_ltp is None or zero: use slice fill_price or range_high
        fallback_price = None
        if self.last_ltp and self.last_ltp > 0:
            fallback_price = self.last_ltp
        elif self.active_slices and self.active_slices[0].fill_price:
            fallback_price = self.active_slices[0].fill_price
        else:
            fallback_price = self.config.range_high

        if self.is_active or len(self.active_slices) > 0 or len(self.grid_ladder) > 0:
            logger.info(
                f"[EOD_SQUAREOFF] Slot {self.run_id}: MCX market closed cutoff reached "
                f"(time={now_t.strftime('%H:%M:%S') if hasattr(now_t, 'strftime') else now_t}, date={now_d}, weekend={is_weekend}). "
                f"Stopping scalper, squaring off {len(self.active_slices)} slices, and cutting all {len(self.grid_ladder)} ladder rungs."
            )
            self.eod_squareoff(exit_price=fallback_price)
            self.last_eod_date = now_d
            return True

        self.eod_squared_off = True
        self.last_eod_date = now_d
        if len(self.grid_ladder) > 0:
            self.grid_ladder.clear()
        return False

    def update_feed_health(self, is_valid: bool) -> None:
        """Tracks consecutive feed errors to flag STALE status."""
        if is_valid:
            if self.feed_status == "STALE":
                self._record_event("FEED_RECOVERED", self.last_ltp or 0.0, "Angel One live feed recovered to LIVE status.")
            self.feed_status = "LIVE"
            self.stale_polls_count = 0
        else:
            self.stale_polls_count += 1
            if self.stale_polls_count >= self.stale_threshold_polls and self.feed_status != "STALE":
                self.feed_status = "STALE"
                self._record_event(
                    "FEED_STALE",
                    self.last_ltp or 0.0,
                    f"Live feed stale after {self.stale_polls_count} missing polls. Pausing entries.",
                )

    def get_range_stop_loss(self) -> float:
        """Returns the range-anchored stop loss."""
        if not hasattr(self, "range_stop_loss") or self.range_stop_loss is None:
            side = self.config.trade_side.upper()
            if side == "SELL":
                high = getattr(self, "effective_range_high", self.config.range_high)
                self.range_stop_loss = round(high + self.config.loss_point, 4)
            else:
                low = max(getattr(self, "effective_range_low", self.config.range_low), 0.0)
                self.range_stop_loss = max(round(low - self.config.loss_point, 4), 0.0)
        return self.range_stop_loss

    def get_loss_trigger_price(self) -> Optional[float]:
        return self.get_range_stop_loss()

    def clear_history(self) -> None:
        self.trade_history.clear()
        self.accumulated_realized_pnl = 0.0
        self._record_event(
            "HISTORY_CLEARED",
            self.last_ltp or 0.0,
            f"Trade history cleared and realized P&L reset to ₹0.00 for Run {self.run_id}.",
        )

    def _record_event(self, event_type: str, price: float, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        self.audit_events.insert(0, {
            "timestamp": self._get_timestamp(),
            "run_id": self.run_id,
            "commodity": self.config.commodity,
            "trade_side": self.config.trade_side,
            "event_type": event_type,
            "price": round(price, 2) if price else 0.0,
            "message": message,
            "details": details or {},
        })
        if len(self.audit_events) > 150:
            self.audit_events = self.audit_events[:150]

    def _record_trade(self, trade: TradeRecord) -> None:
        self.trade_history.insert(0, trade)
        if self.trade_callback:
            try:
                self.trade_callback(asdict(trade), asdict(self.config))
            except Exception as e:
                logger.warning(f"Error in trade_callback: {e}")

    def _exit_all_active_slices(self, exit_price: float, reason: SliceExitReason) -> List[TradeRecord]:
        timestamp = self._get_timestamp()
        exited_trades = []
        side = self.config.trade_side.upper()
        dispatch_action = "BUY" if side == "SELL" else "SELL"

        for s in list(self.active_slices):
            fill = s.fill_price if s.fill_price is not None else s.level_price
            if side == "SELL":
                pnl_pts = round(fill - exit_price, 4)
            else:
                pnl_pts = round(exit_price - fill, 4)
            pnl_rup = round(pnl_pts * s.quantity, 4)

            exit_res = self._dispatch_order(
                action=dispatch_action,
                quantity=s.quantity,
                price=exit_price,
                order_type="MARKET",
            )
            exit_order_id = str(exit_res.get("order_id") or f"ORD-{dispatch_action}-{uuid.uuid4().hex[:8].upper()}")

            s.status = SliceStatus.EXITED
            s.exit_price = exit_price
            s.exit_reason = reason
            s.exited_at = timestamp
            s.pnl_points = pnl_pts
            s.pnl_rupees = pnl_rup

            self.accumulated_realized_pnl = round(self.accumulated_realized_pnl + pnl_rup, 4)

            rec = TradeRecord(
                trade_id=f"TRD-{uuid.uuid4().hex[:8].upper()}",
                run_id=self.run_id,
                commodity=self.config.commodity,
                trade_side=side,
                action=dispatch_action,
                label=s.label or "X",
                level_price=s.level_price,
                fill_price=fill,
                exit_price=exit_price,
                quantity=s.quantity,
                pnl_points=pnl_pts,
                pnl_rupees=pnl_rup,
                exit_reason=reason.value,
                filled_at=s.filled_at or timestamp,
                exited_at=timestamp,
                contract_symbol=self.locked_contract_symbol or self.config.contract_symbol,
                contract_token=self.locked_contract_token or self.config.contract_token,
                expiry=self.config.expiry,
                entry_time=s.filled_at or timestamp,
                exit_time=timestamp,
                run_number=self.run_number,
                mode=getattr(self.config, "trading_mode", "paper") or "paper",
                entry_order_id=s.order_id,
                exit_order_id=exit_order_id,
            )
            self._record_trade(rec)
            exited_trades.append(rec)

        self.active_slices.clear()
        return exited_trades

    def level_for_price(self, price: float) -> Optional[float]:
        """Calculates matching grid rung for given price."""
        if not self.grid_ladder:
            return None
        r_high = max(s.level_price for s in self.grid_ladder)
        r_low = min(s.level_price for s in self.grid_ladder)
        step = self.config.slice_interval

        calculated = calculate_level_for_price(price, r_high, r_low, step, trade_side=self.config.trade_side)
        if calculated is None:
            return None
        for s in self.grid_ladder:
            if abs(s.level_price - calculated) < 1e-3:
                return s.level_price
        return calculated

    def process_tick(
        self,
        current_price: float,
        seq_num: Optional[int] = None,
        current_time_ist: Optional[datetime.time] = None,
    ) -> Dict[str, Any]:
        """
        Symmetrical 3-Step State Machine execution on every market tick:
        Step 1: Loss Backstop (Stops tick if hit)
        Step 2: Profit Target Exit (Evaluates already-filled slices)
        Step 3: Grid Entry (BUY entry on dips for Long, SELL entry on rallies for Short)
        """
        if seq_num is not None:
            if self.last_processed_seq is not None and seq_num <= self.last_processed_seq:
                return {"status": "dedup_ignored", "seq_num": seq_num, "tick_price": current_price}
            self.last_processed_seq = seq_num

        current_price = round(current_price, 4)
        self.last_ltp = current_price
        side = self.config.trade_side.upper()

        actions = {
            "tick_price": current_price,
            "seq_num": seq_num,
            "buys": [],
            "sells": [],
            "loss_exit": False,
            "cycle_reset": False,
            "gap_skipped": [],
        }

        if not self.is_active:
            return actions

        # Check automated EOD cutoff
        if self.config.auto_eod_squareoff and not self.eod_squared_off:
            if self.check_eod_cutoff(current_time_ist=current_time_ist):
                actions["loss_exit"] = False
                actions["eod_squareoff"] = True
                return actions

        # -----------------------------------------------------------------
        # STEP 1: LOSS BACKSTOP TRIGGER (Range-Anchored)
        # -----------------------------------------------------------------
        loss_trigger = self.get_loss_trigger_price()
        hit_loss = False
        if loss_trigger is not None and len(self.active_slices) > 0:
            if side == "SELL":
                # For short: price rising above stop loss triggers exit
                if current_price >= loss_trigger - 1e-4:
                    hit_loss = True
            else:
                # For long: price falling below stop loss triggers exit
                if current_price <= loss_trigger + 1e-4:
                    hit_loss = True

        if hit_loss:
            exited = self._exit_all_active_slices(exit_price=current_price, reason=SliceExitReason.LOSS_EXIT)
            self.is_active = False
            self.last_stop_reason = "LOSS_BACKSTOP"
            actions["loss_exit"] = True
            actions["cycle_reset"] = False

            self._record_event(
                "LOSS_BACKSTOP_TRIGGERED",
                current_price,
                f"[LOSS_BACKSTOP] Stop loss triggered at ₹{current_price:.2f} (Stop: ₹{loss_trigger:.2f}). Exited {len(exited)} slices. Scalper run stopped.",
                {
                    "exited_count": len(exited),
                    "loss_trigger": loss_trigger,
                    "range_stop_loss": loss_trigger,
                    "stop_reason": "LOSS_BACKSTOP",
                },
            )
            return actions

        # -----------------------------------------------------------------
        # STEP 2: PROFIT TARGET EXIT
        # -----------------------------------------------------------------
        if self.active_slices:
            existing_active = list(self.active_slices)
            for s in existing_active:
                hit_profit = False
                if s.profit_target is not None:
                    if side == "SELL":
                        # Short target: price falls below target
                        if current_price <= s.profit_target + 1e-4:
                            hit_profit = True
                    else:
                        # Long target: price rises above target
                        if current_price >= s.profit_target - 1e-4:
                            hit_profit = True

                if hit_profit:
                    fill = s.fill_price if s.fill_price is not None else s.level_price
                    if side == "SELL":
                        pnl_pts = round(fill - current_price, 4)
                        exit_action = "BUY"  # Buy to cover
                    else:
                        pnl_pts = round(current_price - fill, 4)
                        exit_action = "SELL"  # Sell to close

                    pnl_rup = round(pnl_pts * s.quantity, 4)
                    timestamp = self._get_timestamp()

                    exit_res = self._dispatch_order(
                        action=exit_action,
                        quantity=s.quantity,
                        price=current_price,
                        order_type="MARKET",
                    )
                    exit_order_id = str(exit_res.get("order_id") or f"ORD-{exit_action}-{uuid.uuid4().hex[:8].upper()}")

                    self.accumulated_realized_pnl = round(self.accumulated_realized_pnl + pnl_rup, 4)

                    rec = TradeRecord(
                        trade_id=f"TRD-{uuid.uuid4().hex[:8].upper()}",
                        run_id=self.run_id,
                        commodity=self.config.commodity,
                        trade_side=side,
                        action=exit_action,
                        label=s.label or "A",
                        level_price=s.level_price,
                        fill_price=fill,
                        exit_price=current_price,
                        quantity=s.quantity,
                        pnl_points=pnl_pts,
                        pnl_rupees=pnl_rup,
                        exit_reason=SliceExitReason.PROFIT_TARGET.value,
                        filled_at=s.filled_at or timestamp,
                        exited_at=timestamp,
                        contract_symbol=self.locked_contract_symbol or self.config.contract_symbol,
                        contract_token=self.locked_contract_token or self.config.contract_token,
                        expiry=self.config.expiry,
                        entry_time=s.filled_at or timestamp,
                        exit_time=timestamp,
                        run_number=self.run_number,
                        mode=getattr(self.config, "trading_mode", "paper") or "paper",
                        entry_order_id=s.order_id,
                        exit_order_id=exit_order_id,
                    )
                    self._record_trade(rec)
                    if side == "SELL":
                        actions["buys"].append(asdict(rec))
                    else:
                        actions["sells"].append(asdict(rec))

                    self._record_event(
                        "SLICE_PROFIT_EXIT",
                        current_price,
                        f"Slice {s.label} (@ {fill:.2f}) closed at {current_price:.2f} (Target: {s.profit_target:.2f}, P&L: +{pnl_pts:.2f} pts / +₹{pnl_rup:.2f}). Level {s.level_price:.2f} marked USED.",
                        {"trade": asdict(rec)},
                    )

                    if s in self.active_slices:
                        self.active_slices.remove(s)

                    for grid_slice in self.grid_ladder:
                        if abs(grid_slice.level_price - s.level_price) < 1e-4:
                            grid_slice.mark_used()
                            break

        # -----------------------------------------------------------------
        # STEP 3: GRID ENTRY TRIGGER
        # -----------------------------------------------------------------
        if self.feed_status == "STALE" or self.eod_squared_off:
            return actions

        matched_slice: Optional[Slice] = None
        is_crossing_entry = False

        traded_levels = [s.level_price for s in self.grid_ladder if s.status in (SliceStatus.FILLED, SliceStatus.USED)]

        if side == "SELL":
            # SELL Side (Short): Entry on upward rally / level crossing
            lowest_traded = min(traded_levels) if traded_levels else None
            highest_active = max((s.level_price for s in self.active_slices), default=None)

            # Case 3A: Downward crossing entry (price drops below lowest traded)
            if lowest_traded is not None and current_price < lowest_traded - 1e-4:
                unused_lower = [
                    s for s in self.grid_ladder
                    if s.status in (SliceStatus.UNUSED, SliceStatus.PENDING) and s.level_price < lowest_traded - 1e-4
                ]
                if unused_lower:
                    matched_slice = max(unused_lower, key=lambda s: s.level_price)
                    is_crossing_entry = True

            # Case 3B: Step-up short entry when active positions exist
            elif highest_active is not None:
                if current_price >= (highest_active + self.config.slice_interval - 1e-4):
                    target_level_price = self.level_for_price(current_price)
                    if target_level_price is not None and target_level_price >= (highest_active + self.config.slice_interval - 1e-4):
                        candidate = next((s for s in self.grid_ladder if abs(s.level_price - target_level_price) < 1e-3), None)
                        if candidate and candidate.status in (SliceStatus.UNUSED, SliceStatus.PENDING):
                            matched_slice = candidate

            # Case 3C: Startup / Flat entry
            elif highest_active is None:
                target_level_price = self.level_for_price(current_price)
                if target_level_price is not None:
                    candidate = next((s for s in self.grid_ladder if abs(s.level_price - target_level_price) < 1e-3), None)
                    if candidate and candidate.status in (SliceStatus.UNUSED, SliceStatus.PENDING):
                        matched_slice = candidate

        else:
            # BUY Side (Long): Entry on downward dip / level crossing
            highest_traded = max(traded_levels) if traded_levels else None
            lowest_active = min((s.level_price for s in self.active_slices), default=None)

            # Case 3A: Upward crossing entry
            if highest_traded is not None and current_price > highest_traded + 1e-4:
                unused_upper = [
                    s for s in self.grid_ladder
                    if s.status in (SliceStatus.UNUSED, SliceStatus.PENDING) and s.level_price > highest_traded + 1e-4
                ]
                if unused_upper:
                    matched_slice = min(unused_upper, key=lambda s: s.level_price)
                    is_crossing_entry = True

            # Case 3B: Step-down buy entry
            elif lowest_active is not None:
                if current_price <= (lowest_active - self.config.slice_interval + 1e-4):
                    target_level_price = self.level_for_price(current_price)
                    if target_level_price is not None and target_level_price <= (lowest_active - self.config.slice_interval + 1e-4):
                        candidate = next((s for s in self.grid_ladder if abs(s.level_price - target_level_price) < 1e-3), None)
                        if candidate and candidate.status in (SliceStatus.UNUSED, SliceStatus.PENDING):
                            matched_slice = candidate

            # Case 3C: Startup / Flat entry
            elif lowest_active is None:
                target_level_price = self.level_for_price(current_price)
                if target_level_price is not None:
                    candidate = next((s for s in self.grid_ladder if abs(s.level_price - target_level_price) < 1e-3), None)
                    if candidate and candidate.status in (SliceStatus.UNUSED, SliceStatus.PENDING):
                        matched_slice = candidate

        # Execute Entry Fill
        if matched_slice and matched_slice.status in (SliceStatus.UNUSED, SliceStatus.PENDING):
            fill_price = current_price
            entry_action = "SELL" if side == "SELL" else "BUY"

            order_res = self._dispatch_order(
                action=entry_action,
                quantity=self.config.total_slice_quantity,
                price=fill_price,
                order_type="MARKET",
            )

            trading_mode = getattr(self.config, "trading_mode", "paper") or "paper"
            if trading_mode == "live" and order_res.get("status") != "success":
                err_msg = order_res.get("message") or f"Broker rejected live {entry_action} order"
                self._record_event(
                    "LIVE_ENTRY_REJECTED",
                    fill_price,
                    f"Live {entry_action} order rejected by Angel One: {err_msg}. Slice at level {matched_slice.level_price:.2f} was not filled.",
                    {"error": err_msg, "order_res": order_res, "level_price": matched_slice.level_price},
                )
                return actions

            label = get_sequential_label(self.label_counter)
            self.label_counter += 1
            order_id = str(order_res.get("order_id") or f"ORD-{entry_action}-{uuid.uuid4().hex[:8].upper()}")
            timestamp = self._get_timestamp()

            if side == "SELL":
                profit_target = round(fill_price - self.config.profit_point, 4)
            else:
                profit_target = round(fill_price + self.config.profit_point, 4)

            matched_slice.status = SliceStatus.FILLED
            matched_slice.label = label
            matched_slice.order_id = order_id
            matched_slice.fill_price = fill_price
            matched_slice.filled_at = timestamp
            matched_slice.profit_target = profit_target
            matched_slice.sl_price = self.get_range_stop_loss()
            matched_slice.quantity = self.config.total_slice_quantity

            self.active_slices.append(matched_slice)
            self.last_filled_level = matched_slice.level_price

            entry_info = {
                "order_id": order_id,
                "label": label,
                "action": entry_action,
                "trade_side": side,
                "level_price": matched_slice.level_price,
                "fill_price": fill_price,
                "profit_target": profit_target,
                "sl_price": self.get_range_stop_loss(),
                "quantity": matched_slice.quantity,
                "is_crossing_entry": is_crossing_entry,
            }
            if side == "SELL":
                actions["sells"].append(entry_info)
            else:
                actions["buys"].append(entry_info)

            self._record_event(
                "SLICE_ENTRY",
                fill_price,
                f"Slice {label} {entry_action} at {fill_price:.2f} [Level {matched_slice.level_price:.2f}] (Target: {profit_target:.2f}, Stop: {self.get_range_stop_loss():.2f}, Qty: {matched_slice.quantity})",
                entry_info,
            )

        return actions

    def get_status(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "slot_id": self.slot_id,
            "slot_code": self.slot_code,
            "slot_status": self.slot_status,
            "is_active": self.is_active,
            "commodity": self.config.commodity,
            "trade_side": self.config.trade_side,
            "contract_symbol": self.locked_contract_symbol or self.config.contract_symbol,
            "contract_token": self.locked_contract_token or self.config.contract_token,
            "expiry": self.config.expiry,
            "product_type": self.config.product_type,
            "last_ltp": self.last_ltp,
            "feed_status": self.feed_status,
            "accumulated_realized_pnl": self.accumulated_realized_pnl,
            "today_realized_pnl": getattr(self, "today_realized_pnl", 0.0),
            "today_trade_count": getattr(self, "today_trade_count", 0),
            "unrealized_pnl_rupees": self.get_unrealized_pnl(),
            "total_pnl": round(self.accumulated_realized_pnl + self.get_unrealized_pnl(), 2),
            "loss_trigger_price": self.get_loss_trigger_price(),
            "grid_ladder": [asdict(s) for s in self.grid_ladder],
            "active_slices": [asdict(s) for s in self.active_slices],
            "trade_history": [asdict(t) for t in self.trade_history],
            "config": asdict(self.config),
            "audit_events": self.audit_events[:50],
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "is_active": self.is_active,
            "run_number": self.run_number,
            "cycle_count": self.cycle_count,
            "label_counter": self.label_counter,
            "last_ltp": self.last_ltp,
            "last_processed_seq": self.last_processed_seq,
            "accumulated_realized_pnl": self.accumulated_realized_pnl,
            "locked_contract_symbol": self.locked_contract_symbol,
            "locked_contract_token": self.locked_contract_token,
            "feed_status": self.feed_status,
            "eod_squared_off": self.eod_squared_off,
            "last_eod_date": (
                self.last_eod_date.isoformat()
                if isinstance(getattr(self, "last_eod_date", None), datetime.date)
                else (str(self.last_eod_date) if getattr(self, "last_eod_date", None) else None)
            ),
            "last_stop_reason": self.last_stop_reason,
            "config": asdict(self.config),
            "grid_ladder": [asdict(s) for s in self.grid_ladder],
            "active_slices": [asdict(s) for s in self.active_slices],
            "trade_history": [asdict(t) for t in self.trade_history],
            "audit_events": self.audit_events[:50],
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        trade_callback: Optional[Any] = None,
        order_callback: Optional[Any] = None,
    ) -> "ScalperRun":
        cfg_data = data.get("config", {})
        valid_cfg_fields = {f.name for f in fields(ScalperRunConfig)}
        clean_cfg = {k: v for k, v in cfg_data.items() if k in valid_cfg_fields} if isinstance(cfg_data, dict) else {}
        cfg = ScalperRunConfig(**clean_cfg)
        instance = cls(config=cfg, trade_callback=trade_callback, order_callback=order_callback)

        instance.is_active = data.get("is_active", False)
        instance.run_number = data.get("run_number", 1)
        instance.cycle_count = data.get("cycle_count", 1)
        instance.label_counter = data.get("label_counter", 0)
        instance.last_ltp = data.get("last_ltp")
        instance.last_processed_seq = data.get("last_processed_seq")
        instance.accumulated_realized_pnl = data.get("accumulated_realized_pnl", 0.0)
        instance.locked_contract_symbol = data.get("locked_contract_symbol")
        instance.locked_contract_token = data.get("locked_contract_token")
        instance.feed_status = data.get("feed_status", "LIVE")
        instance.eod_squared_off = data.get("eod_squared_off", False)
        last_eod_raw = data.get("last_eod_date")
        if isinstance(last_eod_raw, str):
            try:
                instance.last_eod_date = datetime.date.fromisoformat(last_eod_raw)
            except Exception:
                instance.last_eod_date = None
        elif isinstance(last_eod_raw, datetime.date):
            instance.last_eod_date = last_eod_raw
        else:
            instance.last_eod_date = None
        instance.last_stop_reason = data.get("last_stop_reason")

        valid_slice_fields = {f.name for f in fields(Slice)}
        ladder_raw = data.get("grid_ladder", [])
        if ladder_raw:
            ladder = []
            for s in ladder_raw:
                if not isinstance(s, dict):
                    continue
                s_copy = {k: v for k, v in s.items() if k in valid_slice_fields}
                if isinstance(s_copy.get("status"), str):
                    try:
                        s_copy["status"] = SliceStatus(s_copy["status"])
                    except Exception:
                        pass
                if isinstance(s_copy.get("exit_reason"), str):
                    try:
                        s_copy["exit_reason"] = SliceExitReason(s_copy["exit_reason"])
                    except Exception:
                        pass
                ladder.append(Slice(**s_copy))
            instance.grid_ladder = ladder

        active_raw = data.get("active_slices", [])
        if active_raw:
            active = []
            for s in active_raw:
                if not isinstance(s, dict):
                    continue
                s_copy = {k: v for k, v in s.items() if k in valid_slice_fields}
                if isinstance(s_copy.get("status"), str):
                    try:
                        s_copy["status"] = SliceStatus(s_copy["status"])
                    except Exception:
                        pass
                if isinstance(s_copy.get("exit_reason"), str):
                    try:
                        s_copy["exit_reason"] = SliceExitReason(s_copy["exit_reason"])
                    except Exception:
                        pass
                active.append(Slice(**s_copy))
            instance.active_slices = active

        history_raw = data.get("trade_history", [])
        if history_raw:
            valid_trade_fields = {f.name for f in fields(TradeRecord)}
            instance.trade_history = [
                TradeRecord(**{k: v for k, v in t.items() if k in valid_trade_fields})
                for t in history_raw if isinstance(t, dict)
            ]

        instance.audit_events = data.get("audit_events", [])
        return instance


class RunManager:
    """
    Manages 6 fixed commodity slots (SILVERM-B, SILVERM-S, GOLDM-B, GOLDM-S, COPPER-B, COPPER-S)
    per client session.
    """

    def __init__(
        self,
        max_runs: int = 6,
        state_file: str = ":memory:",
        client_id: str = "admin",
        trade_callback: Optional[Any] = None,
        order_callback: Optional[Any] = None,
    ):
        self.max_runs = max_runs
        self.state_file = state_file
        self.client_id = client_id
        self.trade_callback = trade_callback
        self.order_callback = order_callback

        self.runs: Dict[str, ScalperRun] = {}
        self.active_run_id: str = "SILVERM-B"
        self.master_trade_history: List[Dict[str, Any]] = []
        self.slot_events: List[Dict[str, Any]] = []
        self.basket_risk_scope: str = "per_instrument"

        self._initialize_default_slots()

    def _initialize_default_slots(self):
        """Initializes the standard commodity slots."""
        slot_defs = [
            ("SILVERM-B", "SILVERM", "BUY", 91000.0, 89000.0, 250.0, 250.0, 375.0),
            ("SILVERM-S", "SILVERM", "SELL", 91000.0, 89000.0, 250.0, 250.0, 375.0),
            ("GOLDM-B", "GOLDM", "BUY", 75250.0, 74750.0, 100.0, 100.0, 150.0),
            ("GOLDM-S", "GOLDM", "SELL", 75250.0, 74750.0, 100.0, 100.0, 150.0),
            ("COPPER-B", "COPPER", "BUY", 880.0, 820.0, 10.0, 10.0, 15.0),
            ("COPPER-S", "COPPER", "SELL", 880.0, 820.0, 10.0, 10.0, 15.0),
            ("CRUDEOIL-B", "CRUDEOIL", "BUY", 6100.0, 5900.0, 25.0, 25.0, 37.5),
            ("CRUDEOIL-S", "CRUDEOIL", "SELL", 6100.0, 5900.0, 25.0, 25.0, 37.5),
        ]

        for s_id, comm, side, r_high, r_low, step, prof, loss in slot_defs:
            default_qty = COMMODITY_CONFIG.get(comm, {}).get("default_qty_per_slice_lots", 1)
            span_pts = round(r_high - r_low, 4)
            s_cnt = max(1, int(round(span_pts / step)))
            cfg = ScalperRunConfig(
                run_id=s_id,
                commodity=comm,
                trade_side=side,
                range_high=r_high,
                range_low=r_low,
                range_points=span_pts,
                slicer_count=s_cnt,
                slice_interval=step,
                profit_point=prof,
                loss_point=loss,
                qty_per_slice_lots=default_qty,
                auto_eod_squareoff=True,
                cutoff_time_ist="23:30",
                market_open_ist="09:00",
            )
            run = ScalperRun(
                config=cfg,
                trade_callback=self.trade_callback,
                order_callback=self.order_callback,
            )
            run.slot_id = s_id
            self.runs[s_id] = run

        self.active_run_id = "SILVERM-B"

    def get_run(self, run_id: str) -> Optional[ScalperRun]:
        canonical = SLOT_CANONICAL_MAP.get(run_id, run_id)
        return self.runs.get(canonical) or self.runs.get(run_id)

    def list_runs(self) -> List[Dict[str, Any]]:
        return [r.get_status() for r in self.runs.values()]

    def get_all_trade_history(self) -> List[Dict[str, Any]]:
        return list(self.master_trade_history)

    def sync_runs_from_history(self):
        """Re-attaches trade history to individual run objects from master strictly per slot/side."""
        today_str = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        for r in self.runs.values():
            canonical = SLOT_CANONICAL_MAP.get(r.run_id, r.run_id)
            s_trades = [
                t for t in self.master_trade_history
                if (
                    t.get("run_id") == r.run_id
                    or t.get("slot_id") == r.run_id
                    or (t.get("run_id") and SLOT_CANONICAL_MAP.get(t.get("run_id")) == canonical)
                    or (t.get("commodity") == r.config.commodity and t.get("trade_side") == r.config.trade_side)
                )
            ]
            r.accumulated_realized_pnl = round(sum(float(t.get("pnl_rupees") or 0.0) for t in s_trades), 2)
            today_trades = [
                t for t in s_trades
                if today_str in str(t.get("exit_time") or t.get("exited_at") or t.get("created_at") or t.get("timestamp") or "")
            ]
            r.today_realized_pnl = round(sum(float(t.get("pnl_rupees") or 0.0) for t in today_trades), 2)
            r.today_trade_count = len(today_trades)

    def record_slot_event(
        self,
        slot_id: str,
        event_type: str,
        signal: str,
        price: float,
        quantity: int,
        pnl_pts: Optional[float] = None,
        pnl_rupees: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        event = {
            "timestamp": datetime.datetime.now(IST).strftime("%H:%M:%S"),
            "slot_id": slot_id,
            "event_type": event_type,
            "signal": signal,
            "price": price,
            "quantity": quantity,
            "pnl_points": pnl_pts,
            "pnl_rupees": pnl_rupees,
            "details": details or {},
        }
        self.slot_events.append(event)
        if len(self.slot_events) > 100:
            self.slot_events = self.slot_events[-100:]

    def get_slots_matrix(self, eligible_slots: Optional[List[str]] = None) -> Dict[str, Any]:
        """Returns unified 6-slot status matrix and commodity P&L rollups."""
        matrix = {}
        comm_pnl: Dict[str, Dict[str, float]] = {
            "SILVERM": {"realized": 0.0, "today_realized": 0.0, "unrealized": 0.0, "total": 0.0},
            "GOLDM": {"realized": 0.0, "today_realized": 0.0, "unrealized": 0.0, "total": 0.0},
            "COPPER": {"realized": 0.0, "today_realized": 0.0, "unrealized": 0.0, "total": 0.0},
            "CRUDEOIL": {"realized": 0.0, "today_realized": 0.0, "unrealized": 0.0, "total": 0.0},
        }

        for s_id, r in self.runs.items():
            comm = r.config.commodity
            u_pnl = r.get_unrealized_pnl()
            r_pnl = r.realized_pnl
            tot_pnl = round(r_pnl + u_pnl, 2)
            today_r_pnl = getattr(r, "today_realized_pnl", 0.0)
            today_cnt = getattr(r, "today_trade_count", 0)

            matrix[s_id] = {
                "slot_id": s_id,
                "commodity": comm,
                "trade_side": r.config.trade_side,
                "is_active": r.is_active,
                "slot_status": r.slot_status,
                "active_slices_count": len(r.active_slices),
                "contract_symbol": r.locked_contract_symbol or r.config.contract_symbol,
                "last_ltp": r.last_ltp,
                "realized_pnl": r_pnl,
                "today_realized_pnl": today_r_pnl,
                "today_trade_count": today_cnt,
                "unrealized_pnl": u_pnl,
                "total_pnl": tot_pnl,
                "range_points": r.config.range_points,
                "slicer_count": r.config.slicer_count,
                "slice_interval": r.config.slice_interval,
                "profit_point": r.config.profit_point,
                "loss_point": r.config.loss_point,
                "qty_per_slice_lots": r.config.qty_per_slice_lots,
                "is_eligible": bool(eligible_slots and s_id in eligible_slots),
            }

            if comm in comm_pnl:
                comm_pnl[comm]["realized"] = round(comm_pnl[comm]["realized"] + r_pnl, 2)
                comm_pnl[comm]["today_realized"] = round(comm_pnl[comm].get("today_realized", 0.0) + today_r_pnl, 2)
                comm_pnl[comm]["unrealized"] = round(comm_pnl[comm]["unrealized"] + u_pnl, 2)
                comm_pnl[comm]["total"] = round(comm_pnl[comm]["total"] + tot_pnl, 2)

        return {
            "slots_matrix": matrix,
            "instrument_pnl": comm_pnl,
            "commodity_pnl": comm_pnl,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "client_id": self.client_id,
            "active_run_id": self.active_run_id,
            "runs": {s_id: r.to_dict() for s_id, r in self.runs.items()},
            "basket_risk_scope": self.basket_risk_scope,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        client_id: str = "admin",
        trade_callback: Optional[Any] = None,
        order_callback: Optional[Any] = None,
    ) -> "RunManager":
        manager = cls(client_id=client_id, trade_callback=trade_callback, order_callback=order_callback)
        manager.active_run_id = data.get("active_run_id", "SILVERM-B")
        manager.basket_risk_scope = data.get("basket_risk_scope", "per_instrument")

        runs_raw = data.get("runs", {})
        for s_id, r_data in runs_raw.items():
            canonical = SLOT_CANONICAL_MAP.get(s_id, s_id)
            manager.runs[canonical] = ScalperRun.from_dict(
                r_data,
                trade_callback=trade_callback,
                order_callback=order_callback,
            )
            manager.runs[canonical].slot_id = canonical

        return manager
