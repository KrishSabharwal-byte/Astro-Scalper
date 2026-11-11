import datetime
import pytest
from angel_one_service import AngelOneFeed, INDEX_CONFIG, get_lot_size, get_tick_size
from futures_slice_trading_engine import ScalperRunConfig, ScalperRun, RunManager


def test_target_ladder_gaps_and_ratios():
    """
    Verifies target ladder gaps (slice_interval / default_step) for all 3 indices:
    - Nifty: 60.0 points
    - Bank Nifty: 80.0 points
    - Sensex: 150.0 points
    And that default_range_points / default_slicer_count == default_step exactly.
    """
    targets = {
        "NIFTY": 60.0,
        "BANKNIFTY": 80.0,
        "SENSEX": 150.0,
    }

    for inst, expected_gap in targets.items():
        cfg = INDEX_CONFIG[inst]
        assert cfg["default_step"] == expected_gap, f"{inst} default_step != {expected_gap}"
        ratio = cfg["default_range_points"] / cfg["default_slicer_count"]
        assert ratio == expected_gap, f"{inst} ratio ({ratio}) != {expected_gap}"


def test_index_names_and_lot_sizes():
    """
    Verifies display names and lot sizes:
    - NIFTY: display_name="Nifty 50 Futures", symbol_prefix="NIFTY", lot_size=65
    - BANKNIFTY: display_name="Bank Nifty Futures", symbol_prefix="BANKNIFTY", lot_size=30
    - SENSEX: display_name="BSE Sensex Futures", symbol_prefix="SENSEX", lot_size=20
    """
    nifty = INDEX_CONFIG["NIFTY"]
    assert nifty["display_name"] == "Nifty 50 Futures"
    assert nifty["symbol_prefix"] == "NIFTY"
    assert nifty["lot_size"] == 65
    assert get_lot_size("NIFTY") == 65

    bank = INDEX_CONFIG["BANKNIFTY"]
    assert bank["display_name"] == "Bank Nifty Futures"
    assert bank["symbol_prefix"] == "BANKNIFTY"
    assert bank["lot_size"] == 30
    assert get_lot_size("BANKNIFTY") == 30

    sensex = INDEX_CONFIG["SENSEX"]
    assert sensex["display_name"] == "BSE Sensex Futures"
    assert sensex["symbol_prefix"] == "SENSEX"
    assert sensex["lot_size"] == 20
    assert get_lot_size("SENSEX") == 20


def test_computed_grid_slice_interval():
    """
    Verifies that starting a ScalperRun for each index computes the exact ladder gap:
    - Nifty: 60.0
    - Bank Nifty: 80.0
    - Sensex: 150.0
    """
    targets = {
        "NIFTY": 60.0,
        "BANKNIFTY": 80.0,
        "SENSEX": 150.0,
    }

    for inst, expected_step in targets.items():
        c_cfg = INDEX_CONFIG[inst]
        run_cfg = ScalperRunConfig(
            commodity=inst,
            range_points=c_cfg["default_range_points"],
            slicer_count=c_cfg["default_slicer_count"],
            slice_interval=c_cfg["default_step"],
            range_high=c_cfg["default_range_high"],
            range_low=c_cfg["default_range_low"],
        )
        assert run_cfg.slice_interval == expected_step

        run = ScalperRun(config=run_cfg)
        assert len(run.grid_ladder) > 0
        diffs = [
            round(abs(run.grid_ladder[i].level_price - run.grid_ladder[i + 1].level_price), 4)
            for i in range(len(run.grid_ladder) - 1)
        ]
        assert all(d == expected_step for d in diffs), f"{inst} ladder rung diffs != {expected_step}: {diffs}"


def test_contract_resolution_end_to_end():
    """
    Verifies that AngelOneFeed resolves contracts for NIFTY, BANKNIFTY, and SENSEX
    from the local scrips_index_futures.json cache, extracting correct lot sizes and exchanges.
    """
    feed = AngelOneFeed()
    feed._load_instruments()

    # NIFTY resolution -> NFO
    nifty_c = feed.resolve_future_contract("NIFTY")
    assert nifty_c is not None
    assert "NIFTY" in nifty_c["symbol"]
    assert nifty_c["lotsize"] == 65
    assert nifty_c["exch_seg"] == "NFO"

    # BANKNIFTY resolution -> NFO
    bank_c = feed.resolve_future_contract("BANKNIFTY")
    assert bank_c is not None
    assert "BANKNIFTY" in bank_c["symbol"]
    assert bank_c["lotsize"] == 30
    assert bank_c["exch_seg"] == "NFO"

    # SENSEX resolution -> BFO
    sensex_c = feed.resolve_future_contract("SENSEX")
    assert sensex_c is not None
    assert "SENSEX" in sensex_c["symbol"]
    assert sensex_c["lotsize"] == 20
    assert sensex_c["exch_seg"] == "BFO"
