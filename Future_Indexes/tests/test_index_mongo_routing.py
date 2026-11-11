"""
Tests for FUTURE_INDEXES MongoDB Service & Per-User Collection Routing
Verifies:
1. Primary database is FUTURE_INDEXES
2. Normalization of index names (NIFTY, BANKNIFTY, SENSEX)
3. Trades insert and query correctly per user and per instrument
4. Querying recent trades aggregates across instruments or filters by instrument
5. Clearing trades deletes from specific or all instruments
"""

import pytest
from mongo_service import MongoService
from angel_one_service import normalize_index_name


def test_index_normalization():
    assert normalize_index_name("NIFTY") == "NIFTY"
    assert normalize_index_name("NIFTY 50") == "NIFTY"
    assert normalize_index_name("NIFTY50") == "NIFTY"
    assert normalize_index_name("BANKNIFTY") == "BANKNIFTY"
    assert normalize_index_name("BANK NIFTY") == "BANKNIFTY"
    assert normalize_index_name("NIFTYBANK") == "BANKNIFTY"
    assert normalize_index_name("SENSEX") == "SENSEX"
    assert normalize_index_name("BSX") == "SENSEX"
    assert normalize_index_name("BSE SENSEX") == "SENSEX"


def test_in_memory_index_trade_routing():
    svc = MongoService(uri=None, db_name="Future_Indexes")
    assert svc.db_name == "Future_Indexes"

    client_id = "User1"

    # Insert NIFTY trade
    t1 = {
        "trade_id": "TRD_NIFTY_01",
        "commodity": "NIFTY",
        "instrument": "NIFTY",
        "trade_side": "BUY",
        "fill_price": 25000.0,
        "exit_price": 25050.0,
        "quantity": 65,
        "pnl_points": 50.0,
        "pnl_rupees": 3250.0,
        "exit_reason": "PROFIT_TARGET",
    }
    id1 = svc.insert_trade(t1, client_id=client_id, run_config={"commodity": "NIFTY", "trade_side": "BUY"})
    assert id1 is not None

    # Insert BANKNIFTY trade
    t2 = {
        "trade_id": "TRD_BANKNIFTY_01",
        "commodity": "BANKNIFTY",
        "instrument": "BANKNIFTY",
        "trade_side": "SELL",
        "fill_price": 52000.0,
        "exit_price": 51900.0,
        "quantity": 30,
        "pnl_points": 100.0,
        "pnl_rupees": 3000.0,
        "exit_reason": "PROFIT_TARGET",
    }
    id2 = svc.insert_trade(t2, client_id=client_id, run_config={"commodity": "BANKNIFTY", "trade_side": "SELL"})
    assert id2 is not None

    # Insert SENSEX trade
    t3 = {
        "trade_id": "TRD_SENSEX_01",
        "commodity": "SENSEX",
        "instrument": "SENSEX",
        "trade_side": "BUY",
        "fill_price": 82000.0,
        "exit_price": 82100.0,
        "quantity": 20,
        "pnl_points": 100.0,
        "pnl_rupees": 2000.0,
        "exit_reason": "PROFIT_TARGET",
    }
    id3 = svc.insert_trade(t3, client_id=client_id, run_config={"commodity": "SENSEX", "trade_side": "BUY"})
    assert id3 is not None

    # Verify querying by instrument
    nifty_trades = svc.get_recent_trades(client_id=client_id, commodity="NIFTY")
    assert len(nifty_trades) == 1
    assert nifty_trades[0]["trade_id"] == "TRD_NIFTY_01"

    bank_trades = svc.get_recent_trades(client_id=client_id, commodity="BANKNIFTY")
    assert len(bank_trades) == 1
    assert bank_trades[0]["trade_id"] == "TRD_BANKNIFTY_01"

    sensex_trades = svc.get_recent_trades(client_id=client_id, commodity="SENSEX")
    assert len(sensex_trades) == 1
    assert sensex_trades[0]["trade_id"] == "TRD_SENSEX_01"

    # Verify aggregated querying (all indices)
    all_trades = svc.get_recent_trades(client_id=client_id)
    assert len(all_trades) == 3

    # Verify single trade deletion
    del_ok = svc.delete_trade(client_id=client_id, trade_id="TRD_BANKNIFTY_01", commodity="BANKNIFTY")
    assert del_ok is True
    assert len(svc.get_recent_trades(client_id=client_id, commodity="BANKNIFTY")) == 0
    assert len(svc.get_recent_trades(client_id=client_id)) == 2

    # Clear specific instrument
    svc.clear_trades(client_id=client_id, commodity="NIFTY")
    assert len(svc.get_recent_trades(client_id=client_id, commodity="NIFTY")) == 0
    assert len(svc.get_recent_trades(client_id=client_id, commodity="SENSEX")) == 1

    # Clear all remaining
    svc.clear_trades(client_id=client_id)
    assert len(svc.get_recent_trades(client_id=client_id)) == 0
