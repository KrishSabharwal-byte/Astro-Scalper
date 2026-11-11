"""
Unit tests specifically verifying SENSEX configuration, BFO resolution, and slot execution.
"""

import pytest
from angel_one_service import (
    AngelOneFeed,
    INDEX_CONFIG,
    normalize_index_name,
    get_lot_size,
    get_tick_size,
    is_plausible_ltp,
)
from futures_slice_trading_engine import (
    ScalperRun,
    ScalperRunConfig,
    RunManager,
    recenter_range,
)


def test_sensex_config_and_normalization():
    assert normalize_index_name("SENSEX") == "SENSEX"
    assert normalize_index_name("BSX") == "SENSEX"
    assert normalize_index_name("sensex") == "SENSEX"
    assert normalize_index_name("BSE SENSEX") == "SENSEX"

    assert get_lot_size("SENSEX") == 20
    assert get_tick_size("SENSEX") == 0.05

    assert is_plausible_ltp("SENSEX", 82000.0) is True
    assert is_plausible_ltp("SENSEX", 5000.0) is False
    assert is_plausible_ltp("SENSEX", 300000.0) is False
    assert is_plausible_ltp("SENSEX", -10.0) is False


def test_sensex_slot_initialization():
    rm = RunManager(max_runs=6)
    assert "SENSEX-B" in rm.runs
    assert "SENSEX-S" in rm.runs

    buy_run = rm.get_run("SENSEX-B")
    sell_run = rm.get_run("SENSEX-S")

    assert buy_run.config.commodity == "SENSEX"
    assert buy_run.config.trade_side == "BUY"
    assert buy_run.config.qty_per_slice_lots == 1
    assert buy_run.config.total_slice_quantity == 20

    assert sell_run.config.commodity == "SENSEX"
    assert sell_run.config.trade_side == "SELL"
    assert sell_run.config.qty_per_slice_lots == 1
    assert sell_run.config.total_slice_quantity == 20


def test_sensex_ladder_anchoring():
    live_ltp = 82143.5
    span = 400.0
    step = 50.0

    # BUY: range_high = ceil(82143.5 / 50) * 50 = 82150.0, range_low = 81750.0
    high_b, low_b = recenter_range(live_ltp, span, step, trade_side="BUY")
    assert high_b == 82150.0
    assert low_b == 81750.0

    cfg_b = ScalperRunConfig(
        run_id="SENSEX-B",
        commodity="SENSEX",
        trade_side="BUY",
        range_high=high_b,
        range_low=low_b,
        slice_interval=step,
    )
    run_b = ScalperRun(config=cfg_b)
    assert len(run_b.grid_ladder) == 9
    assert run_b.grid_ladder[0].level_price == 82150.0
    assert run_b.grid_ladder[-1].level_price == 81750.0
    for rung in run_b.grid_ladder:
        assert rung.level_price <= high_b

    # SELL: range_low = floor(82143.5 / 50) * 50 = 82100.0, range_high = 82500.0
    high_s, low_s = recenter_range(live_ltp, span, step, trade_side="SELL")
    assert low_s == 82100.0
    assert high_s == 82500.0

    cfg_s = ScalperRunConfig(
        run_id="SENSEX-S",
        commodity="SENSEX",
        trade_side="SELL",
        range_high=high_s,
        range_low=low_s,
        slice_interval=step,
    )
    run_s = ScalperRun(config=cfg_s)
    assert len(run_s.grid_ladder) == 9
    assert run_s.grid_ladder[0].level_price == 82100.0
    assert run_s.grid_ladder[-1].level_price == 82500.0
    for rung in run_s.grid_ladder:
        assert rung.level_price >= low_s


def test_sensex_bfo_scrip_resolution():
    feed = AngelOneFeed()
    feed._load_instruments()

    contract = feed.resolve_future_contract("SENSEX")
    assert contract is not None
    assert "SENSEX" in contract["symbol"]
    assert contract["lotsize"] == 20
    assert contract["exch_seg"] == "BFO"
