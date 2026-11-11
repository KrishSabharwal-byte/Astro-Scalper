"""
Tests for Symmetric Long/Short Futures Slicing Scalper Engine for Index Futures
Verifies:
1. BUY (Long) Ladder:
   - Levels generated descending from range_high down to range_low.
   - Entry triggered when price drops to unused rung.
   - Profit target = fill_price + profit_point (sold above fill).
   - Range stop loss = range_low - loss_point (backstop below lowest rung).
   - Realized P&L = (exit_price - fill_price) * quantity.
2. SELL (Short) Ladder:
   - Levels generated ascending from range_low up to range_high.
   - Entry triggered when price rallies up to unused rung.
   - Profit target = fill_price - profit_point (bought to cover below fill).
   - Range stop loss = range_high + loss_point (backstop above highest rung).
   - Realized P&L = (fill_price - exit_price) * quantity.
3. Grid symmetry for identical range/step/profit/loss configs.
4. Lot size calculation for NIFTY (65), BANKNIFTY (30), SENSEX (20).
"""

import pytest
from futures_slice_trading_engine import (
    ScalperRun,
    ScalperRunConfig,
    SliceStatus,
    SliceExitReason,
    recenter_range,
    calculate_level_for_price,
    INDEX_CONFIG,
    get_lot_size,
    get_tick_size,
)


def test_index_lot_sizes_and_ticks():
    assert get_lot_size("NIFTY") == 65
    assert get_lot_size("BANKNIFTY") == 30
    assert get_lot_size("SENSEX") == 20

    assert get_tick_size("NIFTY") == 0.05
    assert get_tick_size("BANKNIFTY") == 0.05
    assert get_tick_size("SENSEX") == 0.05


def test_buy_side_grid_generation():
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,
    )
    run = ScalperRun(config=cfg)

    # 9 levels: 25400 down to 24600
    assert len(run.grid_ladder) == 9
    assert run.grid_ladder[0].level_price == 25400.0
    assert run.grid_ladder[-1].level_price == 24600.0

    # Range stop loss for BUY is range_low - loss_point = 24600 - 100 = 24500
    assert run.get_range_stop_loss() == 24500.0
    assert run.config.total_slice_quantity == 65  # 1 lot * 65


def test_sell_side_grid_generation():
    cfg = ScalperRunConfig(
        run_id="NIFTY-S",
        commodity="NIFTY",
        trade_side="SELL",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,
    )
    run = ScalperRun(config=cfg)

    # 9 levels ascending: 24600 up to 25400
    assert len(run.grid_ladder) == 9
    assert run.grid_ladder[0].level_price == 24600.0
    assert run.grid_ladder[-1].level_price == 25400.0

    # Range stop loss for SELL is range_high + loss_point = 25400 + 100 = 25500
    assert run.get_range_stop_loss() == 25500.0
    assert run.config.total_slice_quantity == 65


def test_buy_long_execution_and_profit_exit():
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,  # 1 lot * 65 = 65 qty
        auto_eod_squareoff=False,
    )
    run = ScalperRun(config=cfg)
    run.start()

    # Tick 1: Price at 25000 -> matches level 25000, fills BUY
    act1 = run.process_tick(25000.0)
    assert len(act1["buys"]) == 1
    assert len(run.active_slices) == 1

    s = run.active_slices[0]
    assert s.fill_price == 25000.0
    assert s.profit_target == 25050.0  # fill + 50
    assert s.quantity == 65

    # Tick 2: Price rises to profit target 25050.0 -> exits SELL for rung 25000
    act2 = run.process_tick(25050.0)
    assert len(act2["sells"]) == 1  # exited 25000 slice

    assert run.realized_pnl == 3250.0  # +50 pts * 65 qty = +₹3250
    assert len(run.trade_history) == 1
    tr = run.trade_history[0]
    assert tr.exit_reason == SliceExitReason.PROFIT_TARGET.value
    assert tr.pnl_points == 50.0
    assert tr.pnl_rupees == 3250.0


def test_sell_short_execution_and_profit_cover():
    cfg = ScalperRunConfig(
        run_id="NIFTY-S",
        commodity="NIFTY",
        trade_side="SELL",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,  # 1 lot * 65 = 65 qty
        auto_eod_squareoff=False,
    )
    run = ScalperRun(config=cfg)
    run.start()

    # Tick 1: Price at 25000 -> matches level 25000, fills SELL (short)
    act1 = run.process_tick(25000.0)
    assert len(act1["sells"]) == 1
    assert len(run.active_slices) == 1

    s = run.active_slices[0]
    assert s.fill_price == 25000.0
    assert s.profit_target == 24950.0  # fill - 50 (cover target below)
    assert s.quantity == 65

    # Tick 2: Price drops to cover target 24950.0 -> exits BUY (cover) for rung 25000
    act2 = run.process_tick(24950.0)
    assert len(act2["buys"]) == 1   # exited 25000 slice

    assert run.realized_pnl == 3250.0  # (25000 - 24950) * 65 = +₹3250
    assert len(run.trade_history) == 1
    tr = run.trade_history[0]
    assert tr.exit_reason == SliceExitReason.PROFIT_TARGET.value
    assert tr.pnl_points == 50.0
    assert tr.pnl_rupees == 3250.0


def test_buy_long_loss_backstop():
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,
        auto_eod_squareoff=False,
    )
    run = ScalperRun(config=cfg)
    run.start()

    # Fill at lowest level 24600
    run.process_tick(24600.0)
    assert len(run.active_slices) == 1

    # Price drops to stop loss 24500 (24600 - 100)
    act = run.process_tick(24500.0)
    assert act["loss_exit"] is True
    assert run.is_active is False
    assert len(run.active_slices) == 0

    # Loss = (24500 - 24600) * 65 = -100 pts * 65 = -₹6500
    assert run.realized_pnl == -6500.0


def test_sell_short_loss_backstop():
    cfg = ScalperRunConfig(
        run_id="NIFTY-S",
        commodity="NIFTY",
        trade_side="SELL",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        profit_point=50.0,
        loss_point=100.0,
        qty_per_slice_lots=1,
        auto_eod_squareoff=False,
    )
    run = ScalperRun(config=cfg)
    run.start()

    # Fill at highest level 25400
    run.process_tick(25400.0)
    assert len(run.active_slices) == 1

    # Price rallies up to stop loss 25500 (25400 + 100)
    act = run.process_tick(25500.0)
    assert act["loss_exit"] is True
    assert run.is_active is False
    assert len(run.active_slices) == 0

    # Loss = (25400 - 25500) * 65 = -100 pts * 65 = -₹6500
    assert run.realized_pnl == -6500.0


def test_recenter_range_buy_direction_anchoring():
    live_ltp = 25234.50
    span = 800.0
    step = 100.0

    high, low = recenter_range(
        current_price=live_ltp,
        span=span,
        step=step,
        trade_side="BUY",
    )
    # ceil(25234.50 / 100) * 100 = 25300.0
    assert high == 25300.0
    assert low == 24500.0
    assert high - low == 800.0

    cfg = ScalperRunConfig(
        run_id="TEST-BUY",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=high,
        range_low=low,
        slice_interval=step,
    )
    run = ScalperRun(config=cfg)
    assert len(run.grid_ladder) == 9

    # Top rung is at high (25300.0)
    assert run.grid_ladder[0].level_price == 25300.0
    # Bottom rung is at low (24500.0)
    assert run.grid_ladder[-1].level_price == 24500.0

    # Every generated rung price is <= range_high and tick aligned
    for rung in run.grid_ladder:
        assert rung.level_price <= high
        assert (rung.level_price % step) == 0.0


def test_recenter_range_sell_direction_anchoring():
    live_ltp = 25234.50
    span = 800.0
    step = 100.0

    high, low = recenter_range(
        current_price=live_ltp,
        span=span,
        step=step,
        trade_side="SELL",
    )
    # floor(25234.50 / 100) * 100 = 25200.0
    assert low == 25200.0
    assert high == 26000.0
    assert high - low == 800.0

    cfg = ScalperRunConfig(
        run_id="TEST-SELL",
        commodity="NIFTY",
        trade_side="SELL",
        range_high=high,
        range_low=low,
        slice_interval=step,
    )
    run = ScalperRun(config=cfg)
    assert len(run.grid_ladder) == 9

    # Bottom rung is at low (25200.0)
    assert run.grid_ladder[0].level_price == 25200.0
    # Top rung is at high (26000.0)
    assert run.grid_ladder[-1].level_price == 26000.0

    # Every generated rung price is >= range_low and tick aligned
    for rung in run.grid_ladder:
        assert rung.level_price >= low
        assert (rung.level_price % step) == 0.0
