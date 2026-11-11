import pytest
import datetime
from fastapi.testclient import TestClient
from app import app
from mongo_service import MongoService, IST
from auth_service import auth_service

client = TestClient(app)


def test_pnl_summary_mongo_service_day_calculation():
    ms = MongoService(uri="", db_name="TestDB")  # Offline / in-memory mode
    cid = "test_user_pnl_day"
    ms.clear_trades(cid)

    today = datetime.datetime.now(IST).date()
    today_str = today.strftime("%Y-%m-%d")

    # Insert winning NIFTY trade today
    ms.insert_trade({
        "trade_id": "T1",
        "instrument": "NIFTY",
        "pnl_rupees": 1500.0,
        "exit_time": f"{today_str} 10:30:00",
        "mode": "paper",
    }, client_id=cid)

    # Insert losing NIFTY trade today
    ms.insert_trade({
        "trade_id": "T2",
        "instrument": "NIFTY",
        "pnl_rupees": -500.0,
        "exit_time": f"{today_str} 11:30:00",
        "mode": "paper",
    }, client_id=cid)

    # Insert winning BANKNIFTY trade today
    ms.insert_trade({
        "trade_id": "T3",
        "instrument": "BANKNIFTY",
        "pnl_rupees": 2000.0,
        "exit_time": f"{today_str} 12:00:00",
        "mode": "paper",
    }, client_id=cid)

    # Insert SENSEX trade on a different date (yesterday)
    yesterday = today - datetime.timedelta(days=1)
    ms.insert_trade({
        "trade_id": "T4",
        "instrument": "SENSEX",
        "pnl_rupees": 3000.0,
        "exit_time": f"{yesterday.strftime('%Y-%m-%d')} 14:00:00",
        "mode": "paper",
    }, client_id=cid)

    # Fetch summary for "day"
    summary = ms.get_pnl_summary(client_id=cid, period="day", ref_date=today)
    assert summary["range"]["from"] == today_str
    assert summary["range"]["to"] == today_str

    by_inst = {row["instrument"]: row for row in summary["by_instrument"]}

    # NIFTY: 1 win (+1500), 1 loss (-500) -> gross profit 1500, gross loss -500, net 1000
    assert by_inst["NIFTY"]["gross_profit"] == 1500.0
    assert by_inst["NIFTY"]["gross_loss"] == -500.0
    assert by_inst["NIFTY"]["net_pnl"] == 1000.0
    assert by_inst["NIFTY"]["trade_count"] == 2
    assert by_inst["NIFTY"]["win_count"] == 1
    assert by_inst["NIFTY"]["loss_count"] == 1

    # BANKNIFTY: 1 win (+2000)
    assert by_inst["BANKNIFTY"]["gross_profit"] == 2000.0
    assert by_inst["BANKNIFTY"]["gross_loss"] == 0.0
    assert by_inst["BANKNIFTY"]["net_pnl"] == 2000.0
    assert by_inst["BANKNIFTY"]["trade_count"] == 1
    assert by_inst["BANKNIFTY"]["win_count"] == 1
    assert by_inst["BANKNIFTY"]["loss_count"] == 0

    # SENSEX: 0 trades today
    assert by_inst["SENSEX"]["trade_count"] == 0

    # ALL total
    total = summary["total"]
    assert total["instrument"] == "ALL"
    assert total["gross_profit"] == 3500.0
    assert total["gross_loss"] == -500.0
    assert total["net_pnl"] == 3000.0
    assert total["trade_count"] == 3
    assert total["win_count"] == 2
    assert total["loss_count"] == 1


def test_pnl_summary_week_and_month_and_mode_filtering():
    ms = MongoService(uri="", db_name="TestDB")
    cid = "test_user_pnl_week_month"
    ms.clear_trades(cid)

    # Base date: Wednesday of some week
    ref_date = datetime.date(2026, 9, 16) # Wed
    mon_date = "2026-09-14"
    sun_date = "2026-09-20"

    # Trade in current week (paper)
    ms.insert_trade({
        "trade_id": "TW1",
        "instrument": "NIFTY",
        "pnl_rupees": 1000.0,
        "exit_time": "2026-09-15 10:00:00",
        "mode": "paper",
    }, client_id=cid)

    # Trade in current week (live)
    ms.insert_trade({
        "trade_id": "TW2",
        "instrument": "NIFTY",
        "pnl_rupees": 2500.0,
        "exit_time": "2026-09-16 11:00:00",
        "mode": "live",
    }, client_id=cid)

    # Trade in same month but previous week (paper)
    ms.insert_trade({
        "trade_id": "TM1",
        "instrument": "SENSEX",
        "pnl_rupees": -800.0,
        "exit_time": "2026-09-05 12:00:00",
        "mode": "paper",
    }, client_id=cid)

    # Trade in previous month (August)
    ms.insert_trade({
        "trade_id": "TP1",
        "instrument": "BANKNIFTY",
        "pnl_rupees": 5000.0,
        "exit_time": "2026-08-25 10:00:00",
        "mode": "paper",
    }, client_id=cid)

    # 1. Test week summary - all modes
    summary_week = ms.get_pnl_summary(client_id=cid, period="week", ref_date=ref_date)
    assert summary_week["range"]["from"] == mon_date
    assert summary_week["range"]["to"] == sun_date
    assert summary_week["total"]["trade_count"] == 2
    assert summary_week["total"]["net_pnl"] == 3500.0

    # 2. Test week summary - filtered by live mode
    summary_week_live = ms.get_pnl_summary(client_id=cid, period="week", ref_date=ref_date, mode="live")
    assert summary_week_live["total"]["trade_count"] == 1
    assert summary_week_live["total"]["net_pnl"] == 2500.0

    # 3. Test month summary - all modes
    summary_month = ms.get_pnl_summary(client_id=cid, period="month", ref_date=ref_date)
    assert summary_month["range"]["from"] == "2026-09-01"
    assert summary_month["range"]["to"] == "2026-09-30"
    assert summary_month["total"]["trade_count"] == 3
    assert summary_month["total"]["net_pnl"] == 2700.0 # 1000 + 2500 - 800


def test_api_history_summary_endpoint():
    token = auth_service.create_session_token("admin", "admin")
    headers = {"Authorization": f"Bearer {token}"}

    # Test default request (period=day, mode=None)
    res = client.get("/history/summary", headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "success"
    assert data["period"] == "day"
    assert "range" in data
    assert "by_instrument" in data
    assert len(data["by_instrument"]) == 3
    assert "total" in data

    # Test invalid period
    res_bad = client.get("/history/summary?period=invalid", headers=headers)
    assert res_bad.status_code == 400

    # Test custom date
    res_custom = client.get("/history/summary?period=custom&date=2026-09-20&mode=paper", headers=headers)
    assert res_custom.status_code == 200
    custom_data = res_custom.json()
    assert custom_data["period"] == "custom"
    assert custom_data["range"]["from"] == "2026-09-20"
    assert custom_data["range"]["to"] == "2026-09-20"
