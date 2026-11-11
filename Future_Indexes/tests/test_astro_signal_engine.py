"""
Comprehensive Unit Tests for Index Futures Astro Signal Engine
Covers:
1. CSV Header Detection with leading metadata/preamble lines.
2. Column normalization (U/D Logic, Direction, Signal, etc.).
3. Timestamp parsing and chronological sorting.
4. Direction normalization and aliasing (upside/downside/neutral).
5. 3-Row cluster window signal rule (BUY / SELL / Mixed Neutral / End of DataFrame).
6. Index Trading hours entry filter (09:15 - 15:30 IST).
7. Active open position filter.
8. Astro file storage, listing, activation, and deletion in FUTURE_INDEXES database.
"""

import datetime
import pytest
from astro_signal_engine import (
    AstroSignalEngine,
    detect_header_row,
    normalize_direction,
    parse_astro_csv,
    IST,
)
from mongo_service import mongo_service


def test_header_detection_with_preamble():
    lines = [
        "# Weekly Astro Report v2.1",
        "# Author: Astro Team",
        "# Generated for Index Futures",
        "Date,Time,U/D Logic",
        "2026-02-01,10:35,Upside",
        "2026-02-01,10:45,Upside",
    ]
    header_idx = detect_header_row(lines, max_scan_lines=15)
    assert header_idx == 3


def test_column_normalization_and_parsing():
    csv_content = """Report Metadata: Index Weekly
Created: 2026-02-01
Date,Time,U/D Logic
2026-02-01,10:35,Upside
2026-02-01,10:45,1.0
2026-02-01,10:55,Bullish
2026-02-01,11:03,Neutral
2026-02-01,11:45,Downside
2026-02-01,11:57,-1
2026-02-01,12:07,Bearish
"""
    df, meta = parse_astro_csv(csv_content)
    assert meta["row_count"] == 7
    assert "parsed_dt" in df.columns
    assert "direction" in df.columns
    assert df["parsed_dt"].is_monotonic_increasing


def test_direction_normalization_aliases():
    assert normalize_direction("Upside") == "upside"
    assert normalize_direction("BUY") == "upside"
    assert normalize_direction("Bullish") == "upside"
    assert normalize_direction("1") == "upside"
    assert normalize_direction("1.0") == "upside"
    assert normalize_direction("long") == "upside"

    assert normalize_direction("Downside") == "downside"
    assert normalize_direction("SELL") == "downside"
    assert normalize_direction("Bearish") == "downside"
    assert normalize_direction("-1") == "downside"
    assert normalize_direction("-1.0") == "downside"
    assert normalize_direction("short") == "downside"

    assert normalize_direction("Neutral") == "neutral"
    assert normalize_direction("Hold") == "neutral"
    assert normalize_direction("") == "neutral"
    assert normalize_direction(None) == "neutral"


def test_3_row_cluster_index_signals():
    csv_content = """Date,Time,U/D Logic
2026-02-01,10:00,Upside
2026-02-01,10:10,Upside
2026-02-01,10:20,Upside
2026-02-01,10:30,Neutral
2026-02-01,10:40,Downside
2026-02-01,10:50,Downside
2026-02-01,11:00,Downside
2026-02-01,11:10,Upside
"""
    df, _ = parse_astro_csv(csv_content)
    engine = AstroSignalEngine()

    # 10:05 (window 10:00, 10:10, 10:20: all Upside -> BUY)
    t1 = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)
    res1 = engine.evaluate_cluster(df, current_dt=t1, commodity="NIFTY")
    assert res1["status"] == "APPROVED"
    assert res1["signal"] == "BUY"
    assert res1["trade_side"] == "BUY"
    assert "0_2026-02-01T10:00:00+05:30_BUY" in res1["fingerprint"]
    assert res1["cluster_index"] == 0

    # 10:25 (window 10:20 [Up], 10:30 [Neutral], 10:40 [Down] -> Mixed)
    t2 = datetime.datetime(2026, 2, 1, 10, 25, tzinfo=IST)
    res2 = engine.evaluate_cluster(df, current_dt=t2, commodity="NIFTY")
    assert res2["status"] == "NO_SIGNAL"
    assert res2["reason"] == "cluster_mixed_or_neutral"

    # 10:45 (window 10:40, 10:50, 11:00: all Downside -> SELL)
    t3 = datetime.datetime(2026, 2, 1, 10, 45, tzinfo=IST)
    res3 = engine.evaluate_cluster(df, current_dt=t3, commodity="NIFTY")
    assert res3["status"] == "APPROVED"
    assert res3["signal"] == "SELL"
    assert res3["trade_side"] == "SELL"
    assert "4_2026-02-01T10:40:00+05:30_SELL" in res3["fingerprint"]
    assert res3["cluster_index"] == 4


def test_index_trading_hours_filter():
    csv_content = """Date,Time,U/D Logic
2026-02-01,09:00,Upside
2026-02-01,09:05,Upside
2026-02-01,09:10,Upside
2026-02-01,09:15,Upside
2026-02-01,09:20,Upside
2026-02-01,09:25,Upside
2026-02-01,15:30,Downside
2026-02-01,15:35,Downside
2026-02-01,15:40,Downside
2026-02-01,15:45,Downside
2026-02-01,15:50,Downside
"""
    df, _ = parse_astro_csv(csv_content)
    engine = AstroSignalEngine()

    # Pre-market: 09:10 (Index market open is 09:15) -> BLOCKED
    t_pre = datetime.datetime(2026, 2, 1, 9, 10, tzinfo=IST)
    res_pre = engine.evaluate_cluster(df, current_dt=t_pre, market_open_ist="09:15", cutoff_time_ist="15:30")
    assert res_pre["status"] == "BLOCKED"
    assert res_pre["filter"] == "trading_hours"

    # Post-cutoff: 15:35 (cutoff is 15:30) -> BLOCKED
    t_post = datetime.datetime(2026, 2, 1, 15, 35, tzinfo=IST)
    res_post = engine.evaluate_cluster(df, current_dt=t_post, market_open_ist="09:15", cutoff_time_ist="15:30")
    assert res_post["status"] == "BLOCKED"
    assert res_post["filter"] == "trading_hours"


def test_active_trade_filter():
    csv_content = """Date,Time,U/D Logic
2026-02-01,10:00,Upside
2026-02-01,10:10,Upside
2026-02-01,10:20,Upside
"""
    df, _ = parse_astro_csv(csv_content)
    engine = AstroSignalEngine()

    t = datetime.datetime(2026, 2, 1, 10, 5, tzinfo=IST)
    res = engine.evaluate_cluster(df, current_dt=t, has_active_trade=True)
    assert res["status"] == "BLOCKED"
    assert res["filter"] == "active_trade_open"


def test_mongo_astro_file_storage_and_activation():
    test_client = "test_trader_astro"
    csv_sample = "Date,Time,U/D Logic\n2026-02-01,10:00,Upside\n2026-02-01,10:10,Upside\n2026-02-01,10:20,Upside\n"

    fid1 = mongo_service.save_astro_file(
        client_id=test_client,
        filename="index_astro_week1.csv",
        content=csv_sample,
        row_count=3,
        is_active=True,
    )
    assert fid1 is not None

    active = mongo_service.get_active_astro_file(client_id=test_client)
    assert active is not None
    assert active["filename"] == "index_astro_week1.csv"

    fid2 = mongo_service.save_astro_file(
        client_id=test_client,
        filename="index_astro_week2.csv",
        content=csv_sample,
        row_count=3,
        is_active=True,
    )
    active2 = mongo_service.get_active_astro_file(client_id=test_client)
    assert active2["filename"] == "index_astro_week2.csv"

    mongo_service.set_active_astro_file(client_id=test_client, file_id=fid1)
    active3 = mongo_service.get_active_astro_file(client_id=test_client)
    assert active3["filename"] == "index_astro_week1.csv"

    mongo_service.delete_astro_file(client_id=test_client, file_id=fid1)
    active_after = mongo_service.get_active_astro_file(client_id=test_client)
    assert active_after is None

    assert mongo_service.set_active_astro_file(client_id=test_client, file_id=fid2) is True
    active_reactivated = mongo_service.get_active_astro_file(client_id=test_client)
    assert active_reactivated is not None
    assert active_reactivated["filename"] == "index_astro_week2.csv"
