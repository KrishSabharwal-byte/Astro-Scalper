import datetime
import pytest
import zoneinfo
from futures_slice_trading_engine import (
    ScalperRunConfig,
    ScalperRun,
    Slice,
    SliceStatus,
    SliceExitReason,
    RunManager,
)
from client_session import ClientSession

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

BUY_CLUSTER_CSV = """Date,Time,U/D Logic
2026-09-22,09:15,Upside
2026-09-22,09:30,Upside
2026-09-22,09:45,Upside
"""

SELL_CLUSTER_CSV = """Date,Time,U/D Logic
2026-09-22,09:15,Downside
2026-09-22,09:30,Downside
2026-09-22,09:45,Downside
"""


def test_eod_cutoff_at_1530_squares_off_and_cuts_grid():
    """
    Verifies that at 15:30 IST (3:30 PM):
    - All open positions in the slicer are squared off.
    - The grid ladder is cut/cleared completely so yesterday's grid is not carried forward.
    - The slot status becomes inactive/idle.
    """
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25400.0,
        range_low=24600.0,
        slice_interval=100.0,
        qty_per_slice_lots=1,
        auto_eod_squareoff=True,
        cutoff_time_ist="15:30",
        market_open_ist="09:15",
    )
    run = ScalperRun(config=cfg)
    run.start()

    # Verify grid ladder is initially populated
    assert len(run.grid_ladder) == 9
    assert run.is_active is True

    # Simulate an active position filled during the day
    test_slice = run.grid_ladder[2]
    test_slice.status = SliceStatus.FILLED
    test_slice.fill_price = 25200.0
    test_slice.order_id = "ORD-TEST-01"
    run.active_slices.append(test_slice)
    run.last_ltp = 25150.0

    assert len(run.active_slices) == 1

    # Time at 15:29 IST: Before cutoff -> should NOT square off
    t_before = datetime.time(15, 29, 0)
    assert run.check_eod_cutoff(current_time_ist=t_before) is False
    assert run.is_active is True
    assert len(run.active_slices) == 1
    assert len(run.grid_ladder) == 9

    # Time at 15:30 IST: 3:30 PM Cutoff hit -> MUST square off and CUT grid
    t_cutoff = datetime.time(15, 30, 0)
    triggered = run.check_eod_cutoff(current_time_ist=t_cutoff)
    assert triggered is True

    # 1. Run must be stopped
    assert run.is_active is False
    assert run.eod_squared_off is True
    assert run.last_stop_reason == "EOD_SQUAREOFF"

    # 2. All open positions must be exited / cleared
    assert len(run.active_slices) == 0

    # 3. Previous grid MUST be cut/closed completely (no rungs carried forward)
    assert len(run.grid_ladder) == 0
    assert run.last_filled_level is None

    # 4. Exited trade recorded with EOD_SQUAREOFF
    assert len(run.trade_history) == 1
    tr = run.trade_history[0]
    assert tr.exit_reason == SliceExitReason.EOD_SQUAREOFF.value
    assert tr.exit_price == 25150.0


def test_eod_cutoff_resets_on_next_trading_day():
    """
    Verifies that on the next trading day (e.g. 09:20 AM),
    eod_squared_off is automatically reset to False so new day trades can execute.
    """
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        auto_eod_squareoff=True,
        cutoff_time_ist="15:30",
        market_open_ist="09:15",
    )
    run = ScalperRun(config=cfg)
    run.eod_squared_off = True

    # During trading hours on the next day (09:20 AM IST)
    t_next_day = datetime.time(9, 20, 0)
    res = run.check_eod_cutoff(current_time_ist=t_next_day)

    assert res is False
    assert run.eod_squared_off is False


@pytest.mark.asyncio
async def test_grid_auto_opens_on_upside_signal():
    """
    Verifies that after closing at 15:30, on the next day an UPSIDE signal
    automatically opens fresh grids for BUY slots dynamically anchored to spot price.
    """
    session = ClientSession(client_id="test_eod_auto_open_buy")
    eval_time = datetime.datetime(2026, 9, 22, 9, 20, tzinfo=IST)

    # Simulate slot was squared off and grid cut at 15:30 last night
    slot_b = session.run_manager.get_run("NIFTY-B")
    slot_b.eod_squareoff(exit_price=24900.0)
    assert len(slot_b.grid_ladder) == 0
    assert slot_b.is_active is False
    assert len(slot_b.active_slices) == 0

    # Arm auto trigger
    session.astro_auto_trigger["NIFTY-B"] = True

    # Simulate spot LTP available
    batch_ltps = {"token_nifty": 25000.0}
    session.angel_feed.latest_ltp_by_comm["NIFTY"] = 25000.0

    # Next morning upside signal arrives
    res = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        batch_ltps=batch_ltps,
        active_astro_content=BUY_CLUSTER_CSV,
    )

    assert res["status"] == "APPROVED"
    assert res["signal"] == "BUY"
    assert "NIFTY-B" in res["triggered_slots"]

    # Slot must now be active with fresh grid generated
    assert slot_b.is_active is True
    assert len(slot_b.grid_ladder) > 0
    # Spot price was 25000, ladder must be anchored around it
    assert slot_b.config.range_high >= 25000.0


@pytest.mark.asyncio
async def test_grid_auto_opens_on_downside_signal():
    """
    Verifies that after closing at 15:30, on the next day a DOWNSIDE signal
    automatically opens fresh grids for SELL slots dynamically anchored to spot price.
    """
    session = ClientSession(client_id="test_eod_auto_open_sell")
    eval_time = datetime.datetime(2026, 9, 22, 9, 20, tzinfo=IST)

    # Simulate slot was squared off and grid cut at 15:30 last night
    slot_s = session.run_manager.get_run("NIFTY-S")
    slot_s.eod_squareoff(exit_price=24900.0)
    assert len(slot_s.grid_ladder) == 0
    assert slot_s.is_active is False
    assert len(slot_s.active_slices) == 0

    # Arm auto trigger
    session.astro_auto_trigger["NIFTY-S"] = True

    # Simulate spot LTP available
    session.angel_feed.latest_ltp_by_comm["NIFTY"] = 25000.0

    # Next morning downside signal arrives
    res = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=SELL_CLUSTER_CSV,
    )

    assert res["status"] == "APPROVED"
    assert res["signal"] == "SELL"
    assert "NIFTY-S" in res["triggered_slots"]

    # Slot must now be active with fresh grid generated
    assert slot_s.is_active is True
    assert len(slot_s.grid_ladder) > 0
    assert slot_s.config.range_low <= 25000.0
