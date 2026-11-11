import pytest
from app import app, INDEX_CONFIG, get_client_session
from futures_slice_trading_engine import RunManager, ScalperRunConfig


def test_index_config_defaults():
    """Verify backend INDEX_CONFIG contains correct lot_size and default_qty_per_slice_lots = 1."""
    assert INDEX_CONFIG["NIFTY"]["lot_size"] == 65
    assert INDEX_CONFIG["NIFTY"]["default_qty_per_slice_lots"] == 1
    assert INDEX_CONFIG["NIFTY"]["display_name"] == "Nifty 50 Futures"

    assert INDEX_CONFIG["BANKNIFTY"]["lot_size"] == 30
    assert INDEX_CONFIG["BANKNIFTY"]["default_qty_per_slice_lots"] == 1
    assert INDEX_CONFIG["BANKNIFTY"]["display_name"] == "Bank Nifty Futures"

    assert INDEX_CONFIG["SENSEX"]["lot_size"] == 20
    assert INDEX_CONFIG["SENSEX"]["default_qty_per_slice_lots"] == 1
    assert INDEX_CONFIG["SENSEX"]["display_name"] == "BSE Sensex Futures"


def test_scalper_run_config_defaults():
    """Verify ScalperRunConfig has qty_per_slice_lots=1, giving quantities: Nifty 65, Bank Nifty 30, Sensex 20."""
    cfg_nifty = ScalperRunConfig(commodity="NIFTY")
    assert cfg_nifty.qty_per_slice_lots == 1
    assert cfg_nifty.total_slice_quantity == 65  # 1 lot * 65

    cfg_bank = ScalperRunConfig(commodity="BANKNIFTY")
    assert cfg_bank.qty_per_slice_lots == 1
    assert cfg_bank.total_slice_quantity == 30  # 1 lot * 30

    cfg_sensex = ScalperRunConfig(commodity="SENSEX")
    assert cfg_sensex.qty_per_slice_lots == 1
    assert cfg_sensex.total_slice_quantity == 20  # 1 lot * 20


def test_run_manager_slot_defaults():
    """Verify RunManager._initialize_default_slots populates correct default quantities for 1 lot."""
    rm = RunManager(max_runs=6, client_id="test_rm_defaults")

    expected = {
        "NIFTY-B": (1, 65),
        "NIFTY-S": (1, 65),
        "BANKNIFTY-B": (1, 30),
        "BANKNIFTY-S": (1, 30),
        "SENSEX-B": (1, 20),
        "SENSEX-S": (1, 20),
    }

    for slot_id, (expected_lots, expected_total_qty) in expected.items():
        run = rm.get_run(slot_id)
        assert run is not None, f"Slot {slot_id} missing"
        assert run.config.qty_per_slice_lots == expected_lots, (
            f"Slot {slot_id} expected {expected_lots} lots, got {run.config.qty_per_slice_lots}"
        )
        assert run.config.total_slice_quantity == expected_total_qty, (
            f"Slot {slot_id} expected {expected_total_qty} total quantity, got {run.config.total_slice_quantity}"
        )


def test_start_fresh_run_defaults_all_indices(test_user_client):
    """Confirm starting a fresh run for each of the 3 indices gives 1 lot and exact quantities: Nifty 65, BankNifty 30, Sensex 20."""
    client, cid = test_user_client
    session = get_client_session(cid)

    indices = [
        ("NIFTY-B", "NIFTY", 1, 65),
        ("BANKNIFTY-B", "BANKNIFTY", 1, 30),
        ("SENSEX-B", "SENSEX", 1, 20),
    ]

    for slot_id, inst, exp_lots, exp_total_qty in indices:
        # Start without specifying qty_per_slice_lots
        res = client.post(f"/runs/{slot_id}/start", json={"commodity": inst})
        assert res.status_code == 200, f"Failed starting {slot_id}: {res.text}"

        run = session.run_manager.get_run(slot_id)
        assert run.config.qty_per_slice_lots == exp_lots, (
            f"{inst} expected {exp_lots} lots, got {run.config.qty_per_slice_lots}"
        )
        assert run.config.total_slice_quantity == exp_total_qty, (
            f"{inst} expected {exp_total_qty} total slice qty, got {run.config.total_slice_quantity}"
        )

        # Stop run for clean state
        client.post(f"/runs/{slot_id}/stop")


def test_start_run_manual_override_respected(test_user_client):
    """Confirm user manual override is respected when starting a run."""
    client, cid = test_user_client
    session = get_client_session(cid)

    # Start NIFTY with explicit 2 lots: 2 * 65 = 130 qty
    res = client.post("/runs/NIFTY-B/start", json={"commodity": "NIFTY", "qty_per_slice_lots": 2})
    assert res.status_code == 200
    run = session.run_manager.get_run("NIFTY-B")
    assert run.config.qty_per_slice_lots == 2
    assert run.config.total_slice_quantity == 130  # 2 lots * 65
    client.post("/runs/NIFTY-B/stop")

    # Start SENSEX with explicit 2 lots: 2 * 20 = 40 qty
    res_sx = client.post("/runs/SENSEX-B/start", json={"commodity": "SENSEX", "qty_per_slice_lots": 2})
    assert res_sx.status_code == 200
    run_sx = session.run_manager.get_run("SENSEX-B")
    assert run_sx.config.qty_per_slice_lots == 2
    assert run_sx.config.total_slice_quantity == 40  # 2 lots * 20
    client.post("/runs/SENSEX-B/stop")


def test_switch_instrument_quantities(test_user_client):
    """Confirm switching instrument via /runs/{run_id}/commodity yields correct lot sizes and quantities."""
    client, cid = test_user_client
    session = get_client_session(cid)

    slot_id = "NIFTY-B"

    # Switch to BANKNIFTY
    res = client.post(f"/runs/{slot_id}/commodity", json={"commodity": "BANKNIFTY"})
    assert res.status_code == 200
    run = session.run_manager.get_run(slot_id)
    assert run.config.commodity == "BANKNIFTY"
    assert run.config.qty_per_slice_lots == 1
    assert run.config.total_slice_quantity == 30

    # Switch to SENSEX
    res = client.post(f"/runs/{slot_id}/commodity", json={"commodity": "SENSEX"})
    assert res.status_code == 200
    run = session.run_manager.get_run(slot_id)
    assert run.config.commodity == "SENSEX"
    assert run.config.qty_per_slice_lots == 1
    assert run.config.total_slice_quantity == 20

    # Switch back to NIFTY
    res = client.post(f"/runs/{slot_id}/commodity", json={"commodity": "NIFTY"})
    assert res.status_code == 200
    run = session.run_manager.get_run(slot_id)
    assert run.config.commodity == "NIFTY"
    assert run.config.qty_per_slice_lots == 1
    assert run.config.total_slice_quantity == 65


def test_frontend_static_defaults():
    """Verify static/index.html and static/app.js have expected lot sizes and quantity 1."""
    with open("static/index.html", "r", encoding="utf-8") as f:
        html = f.read()
    assert 'id="qtyPerSlice" min="1" step="1" required value="1"' in html

    with open("static/app.js", "r", encoding="utf-8") as f:
        js = f.read()
    assert "lotSize: 65" in js
    assert "lotSize: 30" in js
    assert "lotSize: 20" in js
    assert "defaultQtyPerSliceLots: 1" in js
