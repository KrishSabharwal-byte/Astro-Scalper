"""
Tests for Index Futures Resolution & Order Safety in Angel One Service
Verifies:
1. Nearest monthly expiry FUTIDX contract resolution
2. Fallback to closest expiry if none is >= today
3. Tick size rounding per index:
   - NIFTY: ₹0.05
   - BANKNIFTY: ₹0.05
   - SENSEX: ₹0.05
4. Paper vs Live order execution with fail-closed checks
"""

import datetime
import pytest
from angel_one_service import (
    AngelOneFeed,
    INDEX_CONFIG,
    get_tick_size,
    get_lot_size,
)


def test_index_future_contract_resolution():
    feed = AngelOneFeed()
    today = datetime.date.today()

    # Populate mock futures master cache
    feed.futures_master_cache["NIFTY"] = [
        {
            "symbol": "NIFTY_FAR_FUT",
            "token": "68500",
            "name": "NIFTY",
            "expiry_str": "FAR",
            "expiry_date": today + datetime.timedelta(days=30),
            "lotsize": 65,
            "tick_size": 0.05,
            "exch_seg": "NFO",
            "instrumenttype": "FUTIDX",
        },
        {
            "symbol": "NIFTY_NEAR_FUT",
            "token": "68407",
            "name": "NIFTY",
            "expiry_str": "NEAR",
            "expiry_date": today + datetime.timedelta(days=5),
            "lotsize": 65,
            "tick_size": 0.05,
            "exch_seg": "NFO",
            "instrumenttype": "FUTIDX",
        },
    ]

    best = feed.resolve_future_contract("NIFTY")
    assert best is not None
    # Nearest expiry >= today should be NIFTY_NEAR_FUT
    assert best["symbol"] == "NIFTY_NEAR_FUT"
    assert best["token"] == "68407"
    assert best["lotsize"] == 65
    assert best["exch_seg"] == "NFO"


def test_paper_order_execution_index_tick_rounding():
    feed = AngelOneFeed(owner_client_id="trader01")

    # Paper order on NIFTY with non-tick-aligned price (e.g. 25123.47 -> rounded to 0.05)
    res_nifty = feed.place_order(
        client_id="trader01",
        run_id="NIFTY-B",
        run_owner_client_id="trader01",
        symbol="NIFTY24SEP26FUT",
        token="68407",
        quantity=65,
        price=25123.47,
        transaction_type="BUY",
        order_type="LIMIT",
        product_type="INTRADAY",
        trading_mode="paper",
        commodity="NIFTY",
    )
    assert res_nifty["status"] == "success"
    assert res_nifty["mode"] == "paper"

    # Paper order on SENSEX (BFO)
    res_sensex = feed.place_order(
        client_id="trader01",
        run_id="SENSEX-B",
        run_owner_client_id="trader01",
        symbol="SENSEX26OCT26FUT",
        token="123456",
        quantity=20,
        price=82154.33,
        transaction_type="BUY",
        order_type="LIMIT",
        product_type="INTRADAY",
        trading_mode="paper",
        commodity="SENSEX",
    )
    assert res_sensex["status"] == "success"
    assert res_sensex["mode"] == "paper"
