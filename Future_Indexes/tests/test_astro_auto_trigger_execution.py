"""
Unit & Integration Tests for Astro CSV Auto-Trigger Execution Pipeline in Future_Indexes
Verifies:
1. Signal APPROVED with Auto-Trigger Disabled -> Explicit skip-reason logging and tracking ("auto_trigger_disabled").
2. Signal APPROVED with Auto-Trigger Enabled -> Successful AUTO_TRIGGER execution, slot activated, event recorded.
3. Slot Already Active -> Skip-reason tracking ("slot_already_active").
4. Duplicate Fingerprint -> Skip-reason tracking ("duplicate_fingerprint").
5. Defensive ScalperRun.start() contract & mode validation.
6. API Endpoints /runs/{run_id}/auto_trigger & /astro/upload state round-trip.
"""

import datetime
import io
import uuid
import pytest
from fastapi.testclient import TestClient

from app import app, get_client_session
from auth_service import auth_service
from client_session import ClientSession
from futures_slice_trading_engine import ScalperRun, ScalperRunConfig, IST


BUY_CLUSTER_CSV = """Date,Time,U/D Logic
2026-02-01,10:00,Upside
2026-02-01,10:10,Upside
2026-02-01,10:20,Upside
2026-02-01,10:30,Neutral
"""

SELL_CLUSTER_CSV = """Date,Time,U/D Logic
2026-02-01,11:00,Downside
2026-02-01,11:10,Downside
2026-02-01,11:20,Downside
2026-02-01,11:30,Neutral
"""


@pytest.mark.asyncio
async def test_signal_approved_auto_trigger_disabled_skip_reason():
    """When a signal is APPROVED but auto-trigger is OFF, slots must be recorded as skipped with 'auto_trigger_disabled'."""
    client_id = f"test_astro_skip_{uuid.uuid4().hex[:6]}"
    session = ClientSession(client_id=client_id)
    eval_time = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)

    # By default, all auto_trigger flags are False
    res = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )

    assert res["status"] == "APPROVED"
    assert res["signal"] == "BUY"
    assert res["eligible_slots"] == ["NIFTY-B", "BANKNIFTY-B", "SENSEX-B"]
    assert res["triggered_slots"] == []
    assert len(res["skipped"]) == 3

    skip_map = {item["slot"]: item["reason"] for item in res["skipped"]}
    assert skip_map["NIFTY-B"] == "auto_trigger_disabled"
    assert skip_map["BANKNIFTY-B"] == "auto_trigger_disabled"
    assert skip_map["SENSEX-B"] == "auto_trigger_disabled"

    # Verify slots remain inactive
    for s_id in ["NIFTY-B", "BANKNIFTY-B", "SENSEX-B"]:
        slot_run = session.run_manager.get_run(s_id)
        assert not slot_run.is_active


@pytest.mark.asyncio
async def test_signal_approved_auto_trigger_enabled_execution():
    """When auto-trigger is enabled for a slot, it must trigger the trade, start the run, and record the event."""
    client_id = f"test_astro_trig_{uuid.uuid4().hex[:6]}"
    session = ClientSession(client_id=client_id)
    eval_time = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)

    # Enable auto-trigger for NIFTY-B only
    session.astro_auto_trigger["NIFTY-B"] = True

    res = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )

    assert res["status"] == "APPROVED"
    assert res["signal"] == "BUY"
    assert res["triggered_slots"] == ["NIFTY-B"]

    # BANKNIFTY-B and SENSEX-B should still be recorded in skipped
    skip_map = {item["slot"]: item["reason"] for item in res["skipped"]}
    assert skip_map.get("BANKNIFTY-B") == "auto_trigger_disabled"
    assert skip_map.get("SENSEX-B") == "auto_trigger_disabled"
    assert "NIFTY-B" not in skip_map

    # Check slot run state
    slot_run = session.run_manager.get_run("NIFTY-B")
    assert slot_run.is_active
    assert slot_run.config.commodity == "NIFTY"
    assert slot_run.config.trade_side == "BUY"

    # Verify AUTO_TRIGGER event recorded
    events = session.run_manager.slot_events
    auto_events = [e for e in events if e.get("event_type") == "AUTO_TRIGGER" and e.get("slot_id") == "NIFTY-B"]
    assert len(auto_events) == 1
    assert auto_events[0]["signal"] == "BUY"


@pytest.mark.asyncio
async def test_signal_approved_sensex_auto_trigger_execution():
    """When auto-trigger is enabled for SENSEX, it must trigger SENSEX-B for BUY and SENSEX-S for SELL."""
    client_id = f"test_astro_sensex_{uuid.uuid4().hex[:6]}"
    session = ClientSession(client_id=client_id)
    eval_time = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)

    # 1. Test BUY signal triggers SENSEX-B
    session.astro_auto_trigger["SENSEX-B"] = True

    res_buy = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )

    assert res_buy["status"] == "APPROVED"
    assert res_buy["signal"] == "BUY"
    assert "SENSEX-B" in res_buy["eligible_slots"]
    assert res_buy["triggered_slots"] == ["SENSEX-B"]

    slot_buy = session.run_manager.get_run("SENSEX-B")
    assert slot_buy.is_active
    assert slot_buy.config.commodity == "SENSEX"
    assert slot_buy.config.trade_side == "BUY"

    # Verify AUTO_TRIGGER event for SENSEX-B
    sensex_buy_events = [
        e for e in session.run_manager.slot_events
        if e.get("event_type") == "AUTO_TRIGGER" and e.get("slot_id") == "SENSEX-B"
    ]
    assert len(sensex_buy_events) == 1
    assert sensex_buy_events[0]["details"]["commodity"] == "SENSEX"

    # 2. Test SELL signal triggers SENSEX-S
    eval_sell_time = datetime.datetime(2026, 2, 1, 11, 5, tzinfo=IST)
    session.astro_auto_trigger["SENSEX-S"] = True

    res_sell = await session.evaluate_fixed_slots(
        current_dt=eval_sell_time,
        active_astro_content=SELL_CLUSTER_CSV,
    )

    assert res_sell["status"] == "APPROVED"
    assert res_sell["signal"] == "SELL"
    assert "SENSEX-S" in res_sell["eligible_slots"]
    assert res_sell["triggered_slots"] == ["SENSEX-S"]

    slot_sell = session.run_manager.get_run("SENSEX-S")
    assert slot_sell.is_active
    assert slot_sell.config.commodity == "SENSEX"
    assert slot_sell.config.trade_side == "SELL"

    sensex_sell_events = [
        e for e in session.run_manager.slot_events
        if e.get("event_type") == "AUTO_TRIGGER" and e.get("slot_id") == "SENSEX-S"
    ]
    assert len(sensex_sell_events) == 1
    assert sensex_sell_events[0]["details"]["commodity"] == "SENSEX"


@pytest.mark.asyncio
async def test_signal_skipped_when_slot_already_active():
    """When a slot is already running, an approved signal must skip it with 'slot_already_active'."""
    client_id = f"test_astro_active_{uuid.uuid4().hex[:6]}"
    session = ClientSession(client_id=client_id)
    eval_time = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)

    # Enable auto-trigger for NIFTY-B
    session.astro_auto_trigger["NIFTY-B"] = True
    slot_run = session.run_manager.get_run("NIFTY-B")
    slot_run.start()
    assert slot_run.is_active

    res = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )

    assert res["status"] == "APPROVED"
    assert "NIFTY-B" not in res["triggered_slots"]
    skip_map = {item["slot"]: item["reason"] for item in res["skipped"]}
    assert skip_map.get("NIFTY-B") == "slot_already_active"


@pytest.mark.asyncio
async def test_signal_skipped_on_duplicate_fingerprint():
    """When a slot previously executed for the current signal fingerprint, it must not re-trigger."""
    client_id = f"test_astro_dedup_{uuid.uuid4().hex[:6]}"
    session = ClientSession(client_id=client_id)
    eval_time = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)

    session.astro_auto_trigger["NIFTY-B"] = True

    # 1. First evaluation triggers slot
    res1 = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )
    assert res1["triggered_slots"] == ["NIFTY-B"]
    fp = res1["fingerprint"]
    assert session.slot_signal_fingerprints["NIFTY-B"] == fp

    # Manually stop the slot to make it idle again
    slot_run = session.run_manager.get_run("NIFTY-B")
    slot_run.stop()
    assert not slot_run.is_active

    # 2. Second evaluation with identical fingerprint must skip due to duplicate_fingerprint
    res2 = await session.evaluate_fixed_slots(
        current_dt=eval_time,
        active_astro_content=BUY_CLUSTER_CSV,
    )
    assert res2["triggered_slots"] == []
    skip_map = {item["slot"]: item["reason"] for item in res2["skipped"]}
    assert skip_map.get("NIFTY-B") == "duplicate_fingerprint"


def test_defensive_scalper_run_start_and_reset():
    """ScalperRun.start() handles fallback resolution and reset() restores idle state."""
    cfg = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25250.0,
        range_low=24750.0,
        slice_interval=50.0,
        profit_point=50.0,
        loss_point=75.0,
        trading_mode="paper",
    )
    run = ScalperRun(config=cfg)

    # In paper mode, start() succeeds and resolves symbol/token
    assert run.start() is True
    assert run.is_active is True
    assert run.locked_contract_symbol is not None

    # Test reset()
    run.reset()
    assert run.is_active is False
    assert len(run.active_slices) == 0

    # In live mode without real token, start() refuses
    cfg_live = ScalperRunConfig(
        run_id="NIFTY-B",
        commodity="NIFTY",
        trade_side="BUY",
        range_high=25250.0,
        range_low=24750.0,
        slice_interval=50.0,
        profit_point=50.0,
        loss_point=75.0,
        trading_mode="live",
        contract_token=None,
    )
    run_live = ScalperRun(config=cfg_live)
    assert run_live.start() is False
    assert run_live.is_active is False


def test_auto_trigger_api_endpoint_and_upload_flow():
    """Verifies /runs/{run_id}/auto_trigger toggle API and /astro/upload response structure."""
    client = TestClient(app)
    cid = f"trader_astro_api_{uuid.uuid4().hex[:6]}"
    auth_service.create_user(cid, "Password123!", is_active=True, role="trader")

    res_login = client.post("/auth/login", json={"client_id": cid, "password": "Password123!"})
    assert res_login.status_code == 200
    token = res_login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Enable auto-trigger for NIFTY-B via API
    res_toggle = client.post(
        "/runs/NIFTY-B/auto_trigger",
        json={"enabled": True, "slot_id": "NIFTY-B"},
        headers=headers,
    )
    assert res_toggle.status_code == 200
    data = res_toggle.json()
    assert data["status"] == "success"
    assert data["auto_trigger"]["NIFTY-B"] is True
    assert data["auto_trigger"]["BANKNIFTY-B"] is False

    # 2. Upload Astro CSV via API
    csv_bytes = BUY_CLUSTER_CSV.encode("utf-8")
    files = {"file": ("test_astro_report.csv", io.BytesIO(csv_bytes), "text/csv")}
    res_upload = client.post("/astro/upload", files=files, headers=headers)
    assert res_upload.status_code == 200
    upload_data = res_upload.json()
    assert upload_data["status"] == "success"
    assert upload_data["row_count"] == 4
    assert "auto_trigger_by_slot" in upload_data
    assert upload_data["auto_trigger_by_slot"]["NIFTY-B"] is True
    assert "evaluation" in upload_data
