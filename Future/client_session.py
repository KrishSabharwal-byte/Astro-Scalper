"""
ClientSession Multi-Tenant Container for Commodity Futures Scalper.
Encapsulates each client's independent trading engine, broker feed,
rate-limit mutex, SSE queues, and background tick streaming workers.
Zero shared global state between clients.
"""

import asyncio
import datetime
import json
import logging
import os
import threading
import time
from typing import Dict, Any, Optional, List

from futures_slice_trading_engine import (
    RunManager,
    ScalperRunConfig,
    COMMODITY_CONFIG,
    normalize_commodity_name,
    recenter_range,
    IST,
    is_market_open_ist,
)
from angel_one_service import (
    AngelOneFeed,
    ANGEL_CLIENT_ID,
    ANGEL_API_KEY,
    ANGEL_TOTP_SECRET,
    ANGEL_MPIN,
    is_plausible_ltp,
)
from mongo_service import mongo_service
from credential_service import credential_service
from astro_signal_engine import astro_signal_engine, parse_astro_csv

logger = logging.getLogger("ClientSession")

ALL_COMMODITY_SLOTS = [
    "SILVERM-B",
    "SILVERM-S",
    "GOLDM-B",
    "GOLDM-S",
    "COPPER-B",
    "COPPER-S",
    "CRUDEOIL-B",
    "CRUDEOIL-S",
]


class ClientSession:
    """
    Encapsulated per-client session container.
    Guarantees strict isolation for trades, runs, rate locks, SSE streams, and workers.
    """

    def __init__(self, client_id: str, state_dir: Optional[str] = None):
        if not client_id or not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("CRITICAL SECURITY GUARD: client_id is required to create ClientSession.")

        self.client_id = client_id.strip()
        self.state_dir = state_dir or "."
        self.created_at = datetime.datetime.now(IST)
        self.last_activity = datetime.datetime.now(IST)
        self._lock = threading.Lock()

        # Authoritative Trading Mode from credential_service (default: "paper")
        self.trading_mode: str = credential_service.get_trading_mode(self.client_id)

        # 1. Dedicated In-Memory RunManager
        def client_trade_callback(trade_data: Dict[str, Any], run_config: Optional[Dict[str, Any]] = None):
            trade_data["mode"] = getattr(self, "trading_mode", "paper") or "paper"
            mongo_service.insert_trade_async(trade_data, client_id=self.client_id, run_config=run_config)
            if hasattr(self, "run_manager") and self.run_manager:
                self.run_manager.master_trade_history.insert(0, trade_data)
                self.run_manager.sync_runs_from_history()
            try:
                slot_id = (run_config or {}).get("run_id") or trade_data.get("run_id") or "SILVERM-B"
                self.run_manager.record_slot_event(
                    slot_id=slot_id,
                    event_type="EXIT",
                    signal=trade_data.get("trigger_signal") or trade_data.get("signal") or "EXIT",
                    price=trade_data.get("exit_price") or 0.0,
                    quantity=trade_data.get("quantity") or 0,
                    pnl_pts=trade_data.get("pnl_points"),
                    pnl_rupees=trade_data.get("pnl_rupees"),
                    details={
                        "reason": str(trade_data.get("exit_reason")),
                        "entry_price": trade_data.get("fill_price"),
                        "trade_id": trade_data.get("trade_id"),
                        "commodity": trade_data.get("commodity"),
                        "trade_side": trade_data.get("trade_side"),
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to record slot exit event: {e}")

            # Persist run state to MongoDB after every trade
            try:
                self.persist_state()
            except Exception as e:
                logger.warning(f"Failed to persist state after trade for client '{self.client_id}': {e}")

        def client_order_callback(
            run_id: str,
            action: str,
            symbol: str,
            token: str,
            quantity: int,
            price: float,
            order_type: str = "MARKET",
            product_type: str = "INTRADAY",
            trading_mode: str = "paper",
        ) -> Dict[str, Any]:
            res = self.place_order(
                run_id=run_id,
                symbol=symbol,
                token=token,
                quantity=quantity,
                price=price,
                transaction_type=action,
                order_type=order_type,
                product_type=product_type,
            )
            # Persist state after order placement (entry or exit) so active slices survive restarts
            try:
                self.persist_state()
            except Exception:
                pass
            return res

        self.run_manager = RunManager(
            max_runs=8,
            state_file=":memory:",
            client_id=self.client_id,
            trade_callback=client_trade_callback,
            order_callback=client_order_callback,
        )

        # Synchronize trading mode across all initialized runs
        for r in self.run_manager.runs.values():
            r.config.trading_mode = self.trading_mode
        self.sanitize_inactive_slots()

        # Load historical trades from MongoDB COMMODITY database (single fast attempt)
        if mongo_service.ensure_connection():
            try:
                db_trades = mongo_service.get_recent_trades(client_id=self.client_id, limit=500)
                if db_trades:
                    self.run_manager.master_trade_history = db_trades
                    self.run_manager.sync_runs_from_history()
                    logger.info(f"Loaded {len(db_trades)} trades from MongoDB COMMODITY for client '{self.client_id}'.")
                else:
                    logger.info(f"No historical trades found in MongoDB for client '{self.client_id}'.")
            except Exception as e:
                logger.warning(f"Note loading MongoDB history for client '{self.client_id}': {e}")

        # 1. Restore active run state (open positions, grid ladder) from local disk backup (zero disruption)
        local_state_file = os.path.join(os.path.dirname(__file__), "runtime_runs_state.json")
        if os.path.exists(local_state_file):
            try:
                with open(local_state_file, "r") as f:
                    local_backup = json.load(f)
                client_backup = local_backup.get(self.client_id)
                if client_backup and isinstance(client_backup, dict) and client_backup.get("runs"):
                    restored = RunManager.from_dict(
                        client_backup,
                        client_id=self.client_id,
                        trade_callback=client_trade_callback,
                        order_callback=client_order_callback,
                    )
                    for s_id, r in restored.runs.items():
                        if s_id in self.run_manager.runs:
                            r.trade_callback = client_trade_callback
                            r.order_callback = client_order_callback
                            r.config.trading_mode = self.trading_mode
                            if r.is_active or r.active_slices:
                                self.run_manager.runs[s_id] = r
                                logger.info(
                                    f"Restored active slot '{s_id}' from local disk backup for client '{self.client_id}' "
                                    f"(is_active={r.is_active}, active_slices={len(r.active_slices)})."
                                )
                            elif r.grid_ladder and not self.run_manager.runs[s_id].grid_ladder:
                                self.run_manager.runs[s_id] = r
            except Exception as e:
                logger.warning(f"Could not load local state backup for client '{self.client_id}': {e}")

        # Restore active run state (open positions, grid ladder) from MongoDB
        try:
            if mongo_service.ensure_connection():
                saved_state = mongo_service.load_client_state(self.client_id)
                if saved_state and isinstance(saved_state, dict) and saved_state.get("runs"):
                    restored = RunManager.from_dict(
                        saved_state,
                        client_id=self.client_id,
                        trade_callback=client_trade_callback,
                        order_callback=client_order_callback,
                    )
                    for s_id, r in restored.runs.items():
                        if s_id in self.run_manager.runs:
                            r.trade_callback = client_trade_callback
                            r.order_callback = client_order_callback
                            r.config.trading_mode = self.trading_mode
                            # Restore slots that were active or had open positions / grid
                            if r.is_active or r.active_slices:
                                self.run_manager.runs[s_id] = r
                                logger.info(
                                    f"Restored active slot '{s_id}' for client '{self.client_id}' "
                                    f"(is_active={r.is_active}, active_slices={len(r.active_slices)})."
                                )
                            elif r.grid_ladder and not self.run_manager.runs[s_id].grid_ladder:
                                self.run_manager.runs[s_id] = r
                    logger.info(f"Successfully checked and restored run state from MongoDB for client '{self.client_id}'.")
        except Exception as e:
            logger.warning(f"Could not restore run state from MongoDB for client '{self.client_id}': {e}")

        # Check market open status upon session initialization to avoid stale ladders/slices
        now_ist = datetime.datetime.now(IST)
        is_market_open, closed_reason = is_market_open_ist(now_ist)
        if not is_market_open:
            for s_id, r in list(self.run_manager.runs.items()):
                if r.is_active or r.active_slices or r.grid_ladder:
                    logger.info(
                        f"Market closed ({closed_reason}) during session init for '{self.client_id}'. "
                        f"Squaring off slot '{s_id}' and clearing ladder."
                    )
                    r.eod_squareoff()
        else:
            for s_id, r in list(self.run_manager.runs.items()):
                if not r.is_active and r.grid_ladder:
                    r.grid_ladder.clear()
                elif r.is_active and r.active_slices:
                    today_date = now_ist.date()
                    stale_slices = []
                    for s in r.active_slices:
                        if s.filled_at:
                            try:
                                fill_d = datetime.date.fromisoformat(str(s.filled_at).split("T")[0].split(" ")[0])
                                if fill_d < today_date:
                                    stale_slices.append(s)
                            except Exception:
                                pass
                    if stale_slices:
                        logger.warning(
                            f"Slot '{s_id}' for client '{self.client_id}' had {len(stale_slices)} stale slices from previous dates. "
                            f"Auto-squaring off stale slices."
                        )
                        r.eod_squareoff()

        # 2. In-Memory Decrypted Broker Feed (Angel One)
        self.angel_feed = self._init_broker_feed()
        for r in self.run_manager.runs.values():
            r.contract_resolver = self.angel_feed.resolve_future_contract

        # 3. Dedicated Per-Client Rate Limit Mutex
        self.angel_api_lock = asyncio.Lock()

        # 4. Dedicated Per-Client SSE Subscribers
        self.sse_subscribers: List[asyncio.Queue] = []

        # 5. Dedicated Per-Client Live Feed Worker
        self.live_feed_running = False
        self.live_feed_task: Optional[asyncio.Task] = None

        # 6. Dedicated Astro Signal Engine state per client (6 slots)
        self.astro_auto_trigger: Dict[str, bool] = {s: False for s in ALL_COMMODITY_SLOTS}
        self.astro_auto_trigger["all"] = False

        self.last_astro_direction: Dict[str, Optional[str]] = {
            "SILVERM": None,
            "GOLDM": None,
            "COPPER": None,
            "CRUDEOIL": None,
        }
        self.last_astro_preview: Optional[Dict[str, Any]] = None
        self._parsed_astro_cache: Optional[Dict[str, Any]] = None
        self.last_astro_consensus: Optional[str] = None
        self.slot_signal_fingerprints: Dict[str, Optional[str]] = {s: None for s in ALL_COMMODITY_SLOTS}

    def _init_broker_feed(self) -> AngelOneFeed:
        """Initializes in-memory AngelOneFeed from decrypted credentials, falling back to system broker config."""
        creds = credential_service.get_decrypted_broker_credentials(self.client_id, broker_name="angel_one")
        if creds and creds.get("client_id") and creds.get("api_key"):
            return AngelOneFeed(
                client_id=creds["client_id"],
                api_key=creds["api_key"],
                totp_secret=creds["totp_secret"],
                mpin=creds["mpin"],
                owner_client_id=self.client_id,
            )
        # Fallback to admin/system credentials so all sessions receive live feed
        admin_creds = credential_service.get_decrypted_broker_credentials("admin", broker_name="angel_one")
        if admin_creds and admin_creds.get("client_id") and admin_creds.get("api_key"):
            return AngelOneFeed(
                client_id=admin_creds["client_id"],
                api_key=admin_creds["api_key"],
                totp_secret=admin_creds["totp_secret"],
                mpin=admin_creds.get("mpin"),
                owner_client_id=self.client_id,
            )
        return AngelOneFeed(
            client_id=ANGEL_CLIENT_ID or "H202296",
            api_key=ANGEL_API_KEY or "ufOmdMdM",
            totp_secret=ANGEL_TOTP_SECRET or "USMXJQBFGE6LATUTYI4EVRTUMM",
            mpin=ANGEL_MPIN or "3794",
            owner_client_id=self.client_id,
        )

    def reload_broker_credentials(self) -> None:
        """Reloads and decrypts broker credentials."""
        with self._lock:
            self.angel_feed = self._init_broker_feed()
            logger.info(f"Broker feed reloaded for client '{self.client_id}'.")

    def reload_trading_mode(self) -> str:
        """Reloads trading mode from credential_service and synchronizes all run configurations."""
        with self._lock:
            self.trading_mode = credential_service.get_trading_mode(self.client_id)
            for r in self.run_manager.runs.values():
                r.config.trading_mode = self.trading_mode
            logger.info(f"Synchronized trading mode to '{self.trading_mode}' for client '{self.client_id}'.")
            return self.trading_mode

    def persist_state(self) -> bool:
        """Persists multi-slot run manager state to local disk backup and MongoDB."""
        try:
            if hasattr(self, "run_manager") and self.run_manager:
                manager_dict = self.run_manager.to_dict()
                # 1. Local disk persistence backup (atomic write)
                try:
                    state_file = os.path.join(os.path.dirname(__file__), "runtime_runs_state.json")
                    existing = {}
                    if os.path.exists(state_file):
                        try:
                            with open(state_file, "r") as f:
                                existing = json.load(f)
                        except Exception:
                            existing = {}
                    existing[self.client_id] = manager_dict
                    tmp_file = state_file + f".tmp_{os.getpid()}"
                    with open(tmp_file, "w") as f:
                        json.dump(existing, f, indent=2)
                    os.replace(tmp_file, state_file)
                except Exception as ex:
                    logger.warning(f"Error saving local state backup for client '{self.client_id}': {ex}")

                # 2. Remote MongoDB persistence (non-blocking fallback)
                if mongo_service.ensure_connection():
                    return mongo_service.save_client_state(self.client_id, manager_dict)
        except Exception as e:
            logger.warning(f"Failed to persist run state for client '{self.client_id}': {e}")
        return False

    def touch(self) -> None:
        self.last_activity = datetime.datetime.now(IST)

    def sanitize_inactive_slots(self) -> None:
        """Sanitizes any idle slots to ensure their ladder step and range match authoritative defaults."""
        for s_id, r in self.run_manager.runs.items():
            if not r.is_active and len(r.active_slices) == 0:
                comm = r.config.commodity
                cfg = COMMODITY_CONFIG.get(comm)
                if cfg:
                    expected_step = cfg.get("default_step")
                    if expected_step and (r.config.slice_interval != expected_step or not r.config.range_points):
                        logger.info(f"Sanitizing slot {s_id} ({comm}): resetting slice_interval from {r.config.slice_interval} to {expected_step}")
                        r.config.slice_interval = expected_step
                        r.config.range_points = cfg.get("default_range_points")
                        r.config.slicer_count = cfg.get("default_slicer_count")
                        r.config.profit_point = cfg.get("default_profit_point")
                        r.config.loss_point = cfg.get("default_loss_point")
                        r.config.range_high = cfg.get("range_high")
                        r.config.range_low = cfg.get("range_low")
                        r._generate_grid()

    def get_state_payload(self) -> Dict[str, Any]:
        """Constructs current client state payload for dashboard and API (fast in-memory path)."""
        self.sanitize_inactive_slots()
        active_run = self.run_manager.get_run(self.run_manager.active_run_id) or next(iter(self.run_manager.runs.values()), None)
        active_status = active_run.get_status() if active_run else {}

        commodity = (active_status.get("config", {}).get("commodity") if active_status else None) or (
            active_run.config.commodity if active_run else "SILVERM"
        )
        comm_cfg = COMMODITY_CONFIG.get(commodity, COMMODITY_CONFIG["SILVERM"])
        default_ltp = comm_cfg["default_ltp"]

        run_ltp = active_status.get("last_ltp")
        valid_run_ltp = run_ltp if (run_ltp and is_plausible_ltp(commodity, run_ltp)) else None
        feed_ltp = self.angel_feed.latest_ltp_by_comm.get(commodity) if self.angel_feed else None
        valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(commodity, feed_ltp)) else None

        ltp_val = valid_run_ltp or valid_feed_ltp or default_ltp
        logger.debug(f"[LTP_TRACE] client={self.client_id} get_state_payload commodity={commodity} valid_run_ltp={valid_run_ltp} valid_feed_ltp={valid_feed_ltp} ltp_val={ltp_val}")
        contract_sym = active_status.get("contract_symbol") or f"{commodity} FUT"

        # Pure in-memory trade history (0ms latency, zero blocking I/O in broadcast loop)
        combined_history = self.run_manager.get_all_trade_history()

        active_astro_doc = self.last_astro_preview
        if not active_astro_doc and mongo_service.is_connected:
            active_astro_doc = mongo_service.get_active_astro_file(self.client_id, use_cache=True)
        if not active_astro_doc:
            self.last_astro_preview = None
            self.last_astro_direction = {}

        active_preview = getattr(self, "last_astro_preview", {}) or {}
        sig_name = active_preview.get("signal_name") or "NEUTRAL"
        eligible_slots = active_preview.get("eligible_slots") or []

        matrix_data = self.run_manager.get_slots_matrix(eligible_slots=eligible_slots)
        slots_matrix = matrix_data["slots_matrix"]
        commodity_pnl = matrix_data["commodity_pnl"]

        feed_status_dict = self.angel_feed.get_status() if self.angel_feed else {}
        feed_err = feed_status_dict.get("feed_error")
        feed_escalated = feed_status_dict.get("feed_error_escalated", False)
        feed_persisted_sec = feed_status_dict.get("feed_error_persisted_sec", 0.0)

        if feed_escalated:
            overall_feed_status = "CRITICAL_ERROR"
        elif feed_err:
            overall_feed_status = "ERROR"
        elif self.angel_feed and self.angel_feed.is_authenticated:
            overall_feed_status = "LIVE"
        else:
            overall_feed_status = "UNAUTHENTICATED"

        return {
            "client_id": self.client_id,
            "trading_mode": getattr(self, "trading_mode", "paper"),
            "active_run_id": self.run_manager.active_run_id,
            "runs": self.run_manager.list_runs(),
            "active_run": active_status,
            "history": combined_history[:100],
            "angel_feed": feed_status_dict,
            "feed_error": feed_err,
            "feed_error_persisted_sec": feed_persisted_sec,
            "feed_error_escalated": feed_escalated,
            "feed_status": overall_feed_status,
            "broker_credential_status": credential_service.get_credential_status(self.client_id),
            "timestamp": datetime.datetime.now(IST).isoformat(),
            "latest_ltp_by_comm": dict(self.angel_feed.latest_ltp_by_comm) if self.angel_feed else {},
            "ltp_by_commodity": dict(self.angel_feed.latest_ltp_by_comm) if self.angel_feed else {},
            # HUD / Visualizer Fields
            "slicer_running": active_run.is_active if active_run else False,
            "commodity": commodity,
            "instrument": commodity,
            "trade_side": active_status.get("trade_side", "BUY"),
            "last_ltp": ltp_val,
            "contract_symbol": contract_sym,
            "contract_token": active_status.get("contract_token"),
            "expiry": active_status.get("expiry"),
            "ladder_levels": active_status.get("grid_ladder", []),
            "open_slices": active_status.get("active_slices", []),
            "closed_slices": active_status.get("trade_history", []),
            "total_realized_pnl": active_status.get("accumulated_realized_pnl", 0.0),
            "unrealized_pnl": active_status.get("unrealized_pnl_rupees", 0.0),
            "total_pnl": active_status.get("total_pnl", 0.0),
            "active_loss_trigger": active_status.get("loss_trigger_price"),
            "ladder_cycle_id": active_status.get("cycle_count", 1),
            "config": active_status.get("config", {}),
            "recent_audit_logs": active_status.get("audit_events", []),
            "slots_matrix": slots_matrix,
            "instrument_pnl": commodity_pnl,
            "commodity_pnl": commodity_pnl,
            "active_signal": sig_name,
            "basket_risk_scope": getattr(self.run_manager, "basket_risk_scope", "per_instrument"),
            "slot_events": getattr(self.run_manager, "slot_events", [])[-50:],
            "astro_state": {
                "has_active_file": bool(active_astro_doc),
                "filename": active_astro_doc.get("filename") if active_astro_doc else None,
                "file_id": str(active_astro_doc.get("_id", "")) if active_astro_doc else None,
                "row_count": active_astro_doc.get("row_count", 0) if active_astro_doc else 0,
                "uploaded_at": active_astro_doc.get("uploaded_at") if active_astro_doc else None,
                "auto_trigger_by_slot": dict(self.astro_auto_trigger),
                "last_astro_direction": dict(self.last_astro_direction),
                "cluster_preview": self.last_astro_preview,
                "active_signal": sig_name,
                "eligible_slots": eligible_slots,
                "basket_risk_scope": getattr(self.run_manager, "basket_risk_scope", "per_instrument"),
                "slots_matrix": slots_matrix,
                "commodity_pnl": commodity_pnl,
            },
        }

    async def broadcast_state(self) -> None:
        """Broadcasts state strictly to this client's open browser tabs."""
        self.touch()
        if not self.sse_subscribers:
            return

        payload = self.get_state_payload()
        message = f"data: {json.dumps(payload)}\n\n"
        dead_queues = []
        for queue in list(self.sse_subscribers):
            if queue.qsize() > 8:
                dead_queues.append(queue)
                continue
            try:
                queue.put_nowait(message)
            except Exception:
                dead_queues.append(queue)

        for dq in dead_queues:
            if dq in self.sse_subscribers:
                self.sse_subscribers.remove(dq)

    async def run_live_feed_cycle(self) -> None:
        """Executes one tick cycle for this client's active MCX futures runs."""
        loop = asyncio.get_running_loop()

        if (
            not self.angel_feed.is_authenticated
            and self.angel_feed.client_id
            and self.angel_feed.api_key
            and not AngelOneFeed.is_in_cooldown()
        ):
            try:
                await loop.run_in_executor(None, self.angel_feed.login)
            except Exception as e:
                logger.warning(f"Error logging into Angel One for client '{self.client_id}': {e}")

        # Check MCX EOD cutoff across all slots (squares off open trades and cuts grid at 23:30 IST)
        cutoff_triggered = False
        for r in self.run_manager.runs.values():
            if r.check_eod_cutoff():
                cutoff_triggered = True
                logger.info(f"[EOD Squareoff] Slot {r.run_id} auto-squared off open trades and cut grid at {r.config.cutoff_time_ist} IST for client '{self.client_id}'.")
        if cutoff_triggered:
            self.slot_signal_fingerprints = {s: None for s in ALL_COMMODITY_SLOTS}

        async with self.angel_api_lock:
            # 1. Resolve active futures contracts for all 4 commodities
            comm_contracts: Dict[str, Dict[str, Any]] = {}
            mcx_tokens: List[str] = []

            for comm in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
                contract = self.angel_feed.resolve_future_contract(comm)
                if contract:
                    comm_contracts[comm] = contract
                    tok = str(contract["token"])
                    if tok not in mcx_tokens:
                        mcx_tokens.append(tok)

            # 2. Batch fetch MCX futures market data in a single API call
            batch_ltps: Dict[str, float] = {}
            if mcx_tokens:
                batch_ltps = await loop.run_in_executor(
                    None, self.angel_feed.fetch_market_data_batch, {"MCX": mcx_tokens}
                )

            # 3. Update live futures LTP across all 6 slots and process ticks
            for s_id, r in list(self.run_manager.runs.items()):
                comm = r.config.commodity
                contract = comm_contracts.get(comm)
                if contract:
                    tok = str(contract["token"])
                    r.config.contract_symbol = contract["symbol"]
                    r.config.contract_token = tok
                    r.config.expiry = contract.get("expiry_str")
                    r.locked_contract_symbol = contract["symbol"]
                    r.locked_contract_token = tok

                    fut_ltp = batch_ltps.get(tok)
                    if fut_ltp is not None and fut_ltp > 0:
                        if is_plausible_ltp(comm, fut_ltp):
                            r.last_ltp = fut_ltp
                            self.angel_feed.latest_ltp_by_comm[comm] = fut_ltp
                            r.update_feed_health(True)
                            logger.info(f"[LTP_TRACE] client={self.client_id} slot={s_id} commodity={comm} token={tok} fut_ltp={fut_ltp}")
                            if r.is_active:
                                r.process_tick(fut_ltp)
                        else:
                            logger.error(f"[LTP_TRACE] [SANITY_GUARD_REJECT] client={self.client_id} slot={s_id} commodity={comm} token={tok} rejected_fut_ltp={fut_ltp}")
                    else:
                        if r.last_ltp and r.last_ltp > 0 and is_plausible_ltp(comm, r.last_ltp):
                            r.update_feed_health(True)
                        else:
                            r.update_feed_health(False)

            # 4. Evaluate Astro CSV Auto-Trigger into eligible commodity slots
            now_ist = datetime.datetime.now(IST)
            await self.evaluate_fixed_slots(
                current_dt=now_ist,
                batch_ltps=batch_ltps,
            )

        await self.broadcast_state()

    async def evaluate_fixed_slots(
        self,
        current_dt: Optional[datetime.datetime] = None,
        batch_ltps: Optional[Dict[str, float]] = None,
        active_astro_content: Optional[str] = None,
        enforce_weekends: bool = True,
    ) -> Dict[str, Any]:
        """
        Consumes directional signal from astro_signal_engine (BUY/SELL)
        and executes into designated commodity slots.
        """
        if batch_ltps is None:
            batch_ltps = {}
        now_ist = current_dt or datetime.datetime.now(IST)
        if now_ist.tzinfo is None:
            now_ist = now_ist.replace(tzinfo=IST)

        # 1. Weekend check: Saturday (5) & Sunday (6) -> MCX Market Closed
        if enforce_weekends and now_ist.weekday() in (5, 6):
            logger.info(f"[Astro Signal] Slicer blocked on weekend ({now_ist.strftime('%A')}). Slicer will not start.")
            return {
                "signal": "NEUTRAL",
                "status": "BLOCKED",
                "reason": f"MCX Market closed on weekend ({now_ist.strftime('%A')}). Slicer will not start.",
                "triggered_slots": [],
                "skipped": [{"slot": s, "reason": "weekend_closed"} for s in ALL_COMMODITY_SLOTS],
            }

        # 2. Trading hours check: 09:00 to 23:30 IST
        cutoff_t = datetime.time(23, 30)
        open_t = datetime.time(9, 0)
        if not (open_t <= now_ist.time() < cutoff_t):
            logger.info(f"[Astro Signal] Outside trading hours ({now_ist.strftime('%H:%M:%S')} IST). Slicer will not start.")
            return {
                "signal": "NEUTRAL",
                "status": "BLOCKED",
                "reason": f"Outside MCX trading hours (09:00 - 23:30 IST, current: {now_ist.strftime('%H:%M:%S')}). Slicer will not start.",
                "triggered_slots": [],
                "skipped": [{"slot": s, "reason": "outside_trading_hours"} for s in ALL_COMMODITY_SLOTS],
            }

        content = active_astro_content
        if not content:
            active_file = mongo_service.get_active_astro_file(self.client_id)
            if active_file:
                content = active_file.get("content")

        if not content:
            self.last_astro_preview = None
            self.last_astro_direction = {}
            return {"signal": "NEUTRAL", "status": "NO_FILE", "triggered_slots": [], "skipped": []}

        try:
            df, meta = parse_astro_csv(content)
        except Exception as e:
            logger.warning(f"Error parsing Astro CSV for client '{self.client_id}': {e}")
            return {"signal": "NEUTRAL", "status": "PARSE_ERROR", "error": str(e), "triggered_slots": [], "skipped": []}

        eval_res = astro_signal_engine.evaluate_cluster(
            df=df,
            current_dt=now_ist,
            cutoff_time_ist="23:30",
            market_open_ist="09:00",
            commodity="SILVERM",
            enforce_weekends=enforce_weekends,
        )

        sig_status = eval_res.get("status")
        sig_dir = eval_res.get("trade_side") or eval_res.get("signal")  # "BUY" or "SELL"
        fingerprint = eval_res.get("fingerprint")

        # Determine eligible slots based on signal direction
        eligible_slots: List[str] = []
        if sig_status == "APPROVED" and sig_dir in ("BUY", "SELL"):
            if sig_dir == "BUY":
                eligible_slots = ["SILVERM-B", "GOLDM-B", "COPPER-B", "CRUDEOIL-B"]
            elif sig_dir == "SELL":
                eligible_slots = ["SILVERM-S", "GOLDM-S", "COPPER-S", "CRUDEOIL-S"]

        triggered: List[str] = []
        skipped: List[Dict[str, str]] = []

        if sig_status == "APPROVED" and sig_dir in ("BUY", "SELL"):
            logger.info(
                f"[Astro Signal] SIGNAL_APPROVED for {sig_dir} (fingerprint: {fingerprint}). "
                f"Evaluating {len(eligible_slots)} eligible slots: {eligible_slots} for client '{self.client_id}'"
            )
            for s_id in eligible_slots:
                is_auto = bool(self.astro_auto_trigger.get(s_id, False) or self.astro_auto_trigger.get("all", False))
                if not is_auto:
                    logger.info(f"[Astro Signal] Slot '{s_id}' skipped: auto_trigger_disabled (signal={sig_dir}, fingerprint={fingerprint})")
                    skipped.append({"slot": s_id, "reason": "auto_trigger_disabled"})
                    continue

                slot_run = self.run_manager.get_run(s_id)
                if not slot_run:
                    logger.warning(f"[Astro Signal] Slot '{s_id}' skipped: slot_missing")
                    skipped.append({"slot": s_id, "reason": "slot_missing"})
                    continue

                # Dedup by fingerprint per slot
                last_fp = self.slot_signal_fingerprints.get(s_id)
                if last_fp == fingerprint:
                    logger.info(f"[Astro Signal] Slot '{s_id}' skipped: duplicate_fingerprint ({fingerprint})")
                    skipped.append({"slot": s_id, "reason": "duplicate_fingerprint"})
                    continue

                # Check if slot is idle / flat
                if slot_run.is_active or len(slot_run.active_slices) > 0:
                    logger.info(
                        f"[Astro Signal] Slot '{s_id}' skipped: slot_already_active "
                        f"(is_active={slot_run.is_active}, active_slices={len(slot_run.active_slices)})"
                    )
                    skipped.append({"slot": s_id, "reason": "slot_already_active"})
                    continue

                comm = slot_run.config.commodity

                # Check broker feed auth in live mode
                if getattr(self, "trading_mode", "paper") == "live" and not self.angel_feed.is_authenticated:
                    logger.warning(f"[Astro Signal] Slot '{s_id}' skipped: broker_unauthenticated (live mode requires authenticated feed)")
                    skipped.append({"slot": s_id, "reason": "broker_unauthenticated"})
                    continue

                # Ensure contract is resolved if missing
                if not slot_run.config.contract_token or not slot_run.config.contract_symbol:
                    if hasattr(self.angel_feed, "resolve_future_contract"):
                        contract = self.angel_feed.resolve_future_contract(comm)
                        if contract:
                            tok = str(contract["token"])
                            slot_run.config.contract_symbol = contract["symbol"]
                            slot_run.config.contract_token = tok
                            slot_run.config.expiry = contract.get("expiry_str")
                            slot_run.locked_contract_symbol = contract["symbol"]
                            slot_run.locked_contract_token = tok

                valid_slot_ltp = slot_run.last_ltp if (slot_run.last_ltp and is_plausible_ltp(comm, slot_run.last_ltp)) else None
                feed_ltp = self.angel_feed.latest_ltp_by_comm.get(comm) if self.angel_feed else None
                valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(comm, feed_ltp)) else None
                comm_cfg = COMMODITY_CONFIG.get(comm, COMMODITY_CONFIG["SILVERM"])
                ltp = valid_slot_ltp or valid_feed_ltp or comm_cfg.get("default_ltp", 90000.0)
                logger.info(f"[LTP_TRACE] client={self.client_id} slot={s_id} commodity={comm} source=evaluate_fixed_slots chosen_ltp={ltp}")

                # Dynamic recentering anchored to current futures LTP
                new_high, new_low = recenter_range(
                    current_price=ltp,
                    span=slot_run.config.original_span,
                    step=slot_run.config.slice_interval,
                    trade_side=slot_run.config.trade_side,
                )
                slot_run.config.range_high = new_high
                slot_run.config.range_low = new_low
                slot_run._generate_grid()

                started = slot_run.start()
                if not started:
                    logger.warning(f"[Astro Signal] Slot '{s_id}' skipped: start_refused")
                    skipped.append({"slot": s_id, "reason": "start_refused"})
                    continue

                self.slot_signal_fingerprints[s_id] = fingerprint
                triggered.append(s_id)

                logger.info(f"[Astro Signal] AUTO_TRIGGER successful for slot '{s_id}' ({comm} {slot_run.config.trade_side}) @ ₹{ltp:.2f} (fp={fingerprint})")
                self.run_manager.record_slot_event(
                    slot_id=s_id,
                    event_type="AUTO_TRIGGER",
                    signal=sig_dir,
                    price=ltp,
                    quantity=slot_run.config.total_slice_quantity,
                    details={
                        "fingerprint": fingerprint,
                        "commodity": comm,
                        "range_high": new_high,
                        "range_low": new_low,
                    },
                )

        self.last_astro_preview = {
            "signal_status": sig_status,
            "signal_name": sig_dir or "NEUTRAL",
            "fingerprint": fingerprint,
            "timestamp": now_ist.strftime("%H:%M:%S"),
            "eligible_slots": eligible_slots,
            "triggered_slots": triggered,
            "skipped": skipped,
            "window": eval_res.get("window", []),
            "reason": eval_res.get("reason"),
            "filter": eval_res.get("filter"),
        }

        return {
            "signal": sig_dir or "NEUTRAL",
            "status": sig_status,
            "eligible_slots": eligible_slots,
            "triggered_slots": triggered,
            "skipped": skipped,
            "fingerprint": fingerprint,
        }

    def place_order(
        self,
        run_id: str,
        symbol: str,
        token: str,
        quantity: int,
        price: float,
        transaction_type: str = "BUY",
        order_type: str = "LIMIT",
        product_type: str = "INTRADAY",
    ) -> Dict[str, Any]:
        """Dispatches order via decrypted AngelOneFeed."""
        return self.angel_feed.place_order(
            client_id=self.client_id,
            run_id=run_id,
            run_owner_client_id=self.client_id,
            symbol=symbol,
            token=token,
            quantity=quantity,
            price=price,
            transaction_type=transaction_type,
            order_type=order_type,
            product_type=product_type,
            trading_mode=self.trading_mode,
        )

    def start_live_feed(self) -> None:
        """Starts background tick worker for this client session."""
        with self._lock:
            if self.live_feed_running:
                return
            self.live_feed_running = True
            self.live_feed_task = asyncio.create_task(self._live_feed_worker())
            logger.info(f"Started live commodity feed worker for client '{self.client_id}'.")

    def stop_live_feed(self) -> None:
        """Stops background tick worker for this client session."""
        with self._lock:
            self.live_feed_running = False
            if self.live_feed_task and not self.live_feed_task.done():
                self.live_feed_task.cancel()
            logger.info(f"Stopped live commodity feed worker for client '{self.client_id}'.")

    async def _live_feed_worker(self) -> None:
        while self.live_feed_running:
            try:
                await self.run_live_feed_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in live commodity feed worker for client '{self.client_id}': {e}")
            await asyncio.sleep(1.5)

    def close(self) -> None:
        """Closes session and releases worker tasks."""
        self.persist_state()
        self.stop_live_feed()
        self.sse_subscribers.clear()
        logger.info(f"ClientSession for '{self.client_id}' closed.")
