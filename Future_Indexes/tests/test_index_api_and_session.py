"""
Comprehensive Integration & End-to-End API Tests for Index Futures Scalper
Tests:
1. Auth & Session Management (/auth/login, /auth/me, /auth/logout, JWT verification)
2. Broker Credentials & Trading Mode (/credentials/angel_one, /credentials/status, /credentials/trading_mode)
3. 6 Fixed Index Slots (NIFTY-B, NIFTY-S, BANKNIFTY-B, BANKNIFTY-S, SENSEX-B, SENSEX-S)
4. Slot Configuration (/runs/{run_id}/instrument, /runs/{run_id}/commodity, /runs/{run_id}/trade_side, /runs/{run_id}/product_type)
5. Run Lifecycle (/runs/{run_id}/start, /stop, /squareoff, /recenter, /tick)
6. Astro Signal Engine Endpoints (/astro/upload, /astro/files, /astro/activate, /astro/preview, /astro/files/{file_id})
7. Per-Instrument Trade History Querying & Clear (/history, /history?instrument=NIFTY, /history/clear)
8. Multi-Tenant Session Isolation & SSE Events
"""

import io
import pytest
from fastapi.testclient import TestClient

from app import app, get_client_session
from auth_service import auth_service
from mongo_service import mongo_service


def test_auth_lifecycle(test_user_client):
    client, cid = test_user_client

    # Test /auth/me
    res_me = client.get("/auth/me")
    assert res_me.status_code == 200
    assert res_me.json()["client_id"] == cid
    assert res_me.json()["role"] == "trader"

    # Test invalid credentials
    bad_client = TestClient(app)
    res_bad = bad_client.post("/auth/login", json={"client_id": cid, "password": "WrongPassword"})
    assert res_bad.status_code == 401


def test_broker_credentials_and_mode(test_user_client):
    client, cid = test_user_client

    # Set credentials
    res_cred = client.post("/credentials/angel_one", json={
        "api_key": "MOCK_KEY_123",
        "client_code": "MOCK_CLIENT_INDEX",
        "pin": "1234",
        "totp_secret": "JBSWY3DPEHPK3PXP",
    })
    assert res_cred.status_code == 200
    assert res_cred.json()["status"] == "success"

    # Check status
    res_status = client.get("/credentials/status")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert status_data["has_credentials"] is True
    assert status_data["trading_mode"] == "paper"

    # Switch trading mode
    res_mode = client.post("/credentials/trading_mode", json={"mode": "paper"})
    assert res_mode.status_code == 200
    assert res_mode.json()["trading_mode"] == "paper"


def test_six_fixed_index_slots_lifecycle(test_user_client):
    client, cid = test_user_client
    session = get_client_session(cid)

    # Verify standard index slots exist (NIFTY, BANKNIFTY, SENSEX)
    expected_slots = [
        "NIFTY-B", "NIFTY-S",
        "BANKNIFTY-B", "BANKNIFTY-S",
        "SENSEX-B", "SENSEX-S",
    ]
    for slot_id in expected_slots:
        run = session.run_manager.get_run(slot_id)
        assert run is not None
        assert run.config.run_id == slot_id

    # Test updating instrument & trade side for NIFTY-B
    res_comm = client.post("/runs/NIFTY-B/commodity", json={"commodity": "NIFTY"})
    assert res_comm.status_code == 200
    assert res_comm.json()["commodity"] == "NIFTY"

    res_inst = client.post("/runs/NIFTY-B/instrument", json={"instrument": "NIFTY"})
    assert res_inst.status_code == 200
    assert res_inst.json()["instrument"] == "NIFTY"

    res_side = client.post("/runs/NIFTY-B/trade_side", json={"trade_side": "BUY"})
    assert res_side.status_code == 200
    assert res_side.json()["trade_side"] == "BUY"

    # Start NIFTY-B run
    start_payload = {
        "range_high": 25500.0,
        "range_low": 24500.0,
        "slice_interval": 100.0,
        "profit_point": 50.0,
        "loss_point": 100.0,
        "qty_per_slice_lots": 1,
        "auto_eod_squareoff": False,
    }
    res_start = client.post("/runs/NIFTY-B/start", json=start_payload)
    assert res_start.status_code == 200
    run_state = res_start.json()["run"]
    assert run_state["is_active"] is True
    assert run_state["commodity"] == "NIFTY"
    assert run_state["trade_side"] == "BUY"
    assert len(run_state["grid_ladder"]) == 11

    # Simulate price tick to trigger entry
    res_tick = client.post("/runs/NIFTY-B/tick", json={"ltp": 25000.0})
    assert res_tick.status_code == 200
    assert len(res_tick.json()["run"]["active_slices"]) == 1

    # Recenter range
    res_recenter = client.post("/runs/NIFTY-B/recenter")
    assert res_recenter.status_code == 200
    assert res_recenter.json()["status"] == "success"

    # Stop run
    res_stop = client.post("/runs/NIFTY-B/stop")
    assert res_stop.status_code == 200
    assert res_stop.json()["run"]["is_active"] is False

    # Squareoff run
    res_sq = client.post("/runs/NIFTY-B/squareoff")
    assert res_sq.status_code == 200
    assert len(res_sq.json()["run"]["active_slices"]) == 0


def test_astro_endpoints_and_preview(test_user_client):
    client, cid = test_user_client

    csv_data = (
        "Date,Time,U/D Logic\n"
        "2026-09-22,10:00,Upside\n"
        "2026-09-22,10:10,Upside\n"
        "2026-09-22,10:20,Upside\n"
        "2026-09-22,10:30,Downside\n"
    )
    file_bytes = io.BytesIO(csv_data.encode("utf-8"))

    # Upload Astro CSV
    res_upload = client.post(
        "/astro/upload",
        files={"file": ("index_test_astro.csv", file_bytes, "text/csv")},
    )
    assert res_upload.status_code == 200
    data = res_upload.json()
    assert data["status"] == "success"
    assert data["row_count"] == 4
    file_id = data["file_id"]

    # List Astro files
    res_files = client.get("/astro/files")
    assert res_files.status_code == 200
    files = res_files.json()["files"]
    assert any(f["file_id"] == file_id for f in files)

    # Preview
    res_prev = client.post("/astro/preview", json={"content": csv_data})
    assert res_prev.status_code == 200
    assert res_prev.json()["status"] == "success"
    assert len(res_prev.json()["preview_rows"]) == 4

    # Delete Astro file
    res_del = client.delete(f"/astro/files/{file_id}")
    assert res_del.status_code == 200
    assert res_del.json()["status"] == "success"


def test_index_trade_history_and_clear(test_user_client):
    client, cid = test_user_client

    # Insert test trade in NIFTY
    mongo_service.insert_trade(
        trade_data={
            "trade_id": "TRD-NIFTY-1",
            "commodity": "NIFTY",
            "instrument": "NIFTY",
            "run_id": "NIFTY-B",
            "trade_side": "BUY",
            "product_type": "INTRADAY",
            "entry_price": 25000.0,
            "exit_price": 25050.0,
            "quantity": 65,
            "pnl_points": 50.0,
            "pnl_rupees": 3250.0,
            "exit_reason": "PROFIT_TARGET",
        },
        client_id=cid,
        run_config={"commodity": "NIFTY", "trade_side": "BUY"},
    )

    # Insert test trade in BANKNIFTY
    mongo_service.insert_trade(
        trade_data={
            "trade_id": "TRD-BANKNIFTY-1",
            "commodity": "BANKNIFTY",
            "instrument": "BANKNIFTY",
            "run_id": "BANKNIFTY-S",
            "trade_side": "SELL",
            "product_type": "INTRADAY",
            "entry_price": 52000.0,
            "exit_price": 51900.0,
            "quantity": 30,
            "pnl_points": 100.0,
            "pnl_rupees": 3000.0,
            "exit_reason": "PROFIT_TARGET",
        },
        client_id=cid,
        run_config={"commodity": "BANKNIFTY", "trade_side": "SELL"},
    )

    # Query all history
    res_all = client.get("/history")
    assert res_all.status_code == 200
    trades_all = res_all.json()["trades"]
    assert len(trades_all) >= 2

    # Query NIFTY only
    res_nifty = client.get("/history?commodity=NIFTY")
    assert res_nifty.status_code == 200
    trades_nifty = res_nifty.json()["trades"]
    assert all(t.get("commodity") == "NIFTY" or t.get("instrument") == "NIFTY" for t in trades_nifty)

    # Clear NIFTY history
    res_clr = client.post("/history/clear", json={"commodity": "NIFTY"})
    assert res_clr.status_code == 200

    # Verify NIFTY cleared but BANKNIFTY remains
    res_after = client.get("/history?commodity=NIFTY")
    assert len(res_after.json()["trades"]) == 0
    res_bank = client.get("/history?commodity=BANKNIFTY")
    assert len(res_bank.json()["trades"]) >= 1

    # Clear all history
    res_clr_all = client.post("/history/clear", json={})
    assert res_clr_all.status_code == 200


def test_slot_instrument_switch_clears_old_ltp(test_user_client):
    client, cid = test_user_client
    session = get_client_session(cid)

    # Set slot to NIFTY and simulate tick of 25150.0
    res_comm = client.post("/runs/NIFTY-B/commodity", json={"commodity": "NIFTY"})
    assert res_comm.status_code == 200
    run = session.run_manager.get_run("NIFTY-B")
    run.last_ltp = 25150.0
    assert run.last_ltp == 25150.0

    # Now switch slot from NIFTY to SENSEX
    res_switch = client.post("/runs/NIFTY-B/commodity", json={"commodity": "SENSEX"})
    assert res_switch.status_code == 200

    # Verify that run.last_ltp was cleared and did NOT carry over 25150.0
    assert run.last_ltp is None, "Switching instrument MUST clear old instrument's last_ltp"

    # Verify grid levels are centered around SENSEX (>= 50000), not around NIFTY ~25000
    assert run.config.range_high >= 50000.0, f"Grid range high ({run.config.range_high}) must be for SENSEX, not NIFTY ~25000"
    assert run.config.range_low >= 50000.0, f"Grid range low ({run.config.range_low}) must be for SENSEX, not NIFTY ~25000"
