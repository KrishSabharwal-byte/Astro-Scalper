"""
Unit Tests for Angel One Feed Resilience & Self-Healing in Future_Indexes:
1. Response key mismatch & AG8003 Token Missing handling
2. Single-flight concurrent login lock and cooldown behavior
3. Temporary simulated fallback contracts are never cached in resolve_future_contract
4. Feed error recording, clearing on valid ticks, and 60s escalation tracking
5. Sanity guard plausible LTP validation for NIFTY, BANKNIFTY, SENSEX
"""

import datetime
import time
from unittest.mock import MagicMock, patch
import pytest

from angel_one_service import (
    AngelOneFeed,
    is_smartapi_success,
    extract_smartapi_error,
    INDEX_CONFIG,
    is_plausible_ltp,
    INDEX_PLAUSIBLE_RANGES,
)


def test_smartapi_response_key_helpers():
    # Schema 1: standard status boolean with message/errorcode
    res1 = {"status": True, "message": "SUCCESS", "errorcode": "", "data": {"ltp": 25000}}
    assert is_smartapi_success(res1) is True
    assert extract_smartapi_error(res1) == ("SUCCESS", "")

    # Schema 2: quote endpoint failure with success/errorCode
    res2 = {"success": False, "message": "Token missing", "errorCode": "AG8003", "data": ""}
    assert is_smartapi_success(res2) is False
    assert extract_smartapi_error(res2) == ("Token missing", "AG8003")

    # Schema 3: string truthy
    res3 = {"status": "true", "data": {}}
    assert is_smartapi_success(res3) is True

    # Schema 4: None or invalid dict
    assert is_smartapi_success(None) is False
    assert is_smartapi_success("invalid") is False


def test_ag8003_token_missing_records_error():
    feed = AngelOneFeed(
        client_id="TEST_CID",
        api_key="TEST_KEY",
        totp_secret="JBSWY3DPEHPK3PXP",
        mpin="1234",
    )
    feed.is_authenticated = True
    feed.smart_api = MagicMock()

    feed.smart_api.getMarketData.return_value = {
        "success": False,
        "message": "Token missing",
        "errorCode": "AG8003",
        "data": "",
    }

    res = feed.fetch_market_data_batch({"NFO": ["68407", "68390"], "BFO": ["844615"]})
    assert feed.feed_error is not None
    assert "AG8003" in feed.feed_error or "Token missing" in feed.feed_error


def test_simulated_fallback_contract_is_not_cached():
    feed = AngelOneFeed()
    feed.futures_master_cache["NIFTY"] = []
    feed.latest_contract_by_comm.clear()

    with patch.object(feed, "_load_instruments"):
        # Resolve when master is offline
        sim_contract = feed.resolve_future_contract("NIFTY")
        assert sim_contract is not None
        assert sim_contract.get("_simulated") is True

        # MUST NOT be cached in latest_contract_by_comm
        assert "NIFTY" not in feed.latest_contract_by_comm

        # Now mock master becoming available
        feed.futures_master_cache["NIFTY"] = [
            {
                "symbol": "NIFTY29SEP26FUT",
                "token": "68407",
                "name": "NIFTY",
                "expiry_str": "29SEP2026",
                "expiry_date": datetime.date(2026, 9, 29),
                "lotsize": 65,
                "tick_size": 0.05,
                "exch_seg": "NFO",
                "instrumenttype": "FUTIDX",
            }
        ]

        # Next call should resolve real contract immediately without being poisoned by simulated fallback
        real_contract = feed.resolve_future_contract("NIFTY")
        assert real_contract["symbol"] == "NIFTY29SEP26FUT"
        assert real_contract["token"] == "68407"
        assert feed.latest_contract_by_comm.get("NIFTY") == real_contract


def test_concurrent_login_cooldown():
    feed = AngelOneFeed(
        client_id="TEST_CID",
        api_key="TEST_KEY",
        totp_secret="JBSWY3DPEHPK3PXP",
        mpin="1234",
    )

    call_count = 0

    def mock_generate_session(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return {"status": True, "data": {"jwtToken": "token123"}}

    with patch("angel_one_service.SmartConnect") as MockSC, patch.object(feed, "_load_instruments"):
        instance = MockSC.return_value
        instance.generateSession.side_effect = mock_generate_session

        # First login call succeeds
        res1 = feed.login()
        assert res1 is True
        assert call_count == 1

        # Second rapid login call within cooldown window should reuse existing state without firing generateSession
        res2 = feed.login()
        assert res2 is True
        assert call_count == 1, "Rapid subsequent login within cooldown should NOT trigger duplicate generateSession"

        # Force login should bypass cooldown
        res3 = feed.login(force=True)
        assert res3 is True
        assert call_count == 2


def test_feed_error_escalation_tracking():
    feed = AngelOneFeed()
    assert feed.feed_error is None

    # Record error
    feed._record_feed_error("Token missing (AG8003)")
    status = feed.get_status()
    assert status["feed_error"] == "Token missing (AG8003)"
    assert status["feed_error_escalated"] is False

    # Simulate error persisting for > 60 seconds
    feed.feed_error_first_ts = time.time() - 65.0
    status_escalated = feed.get_status()
    assert status_escalated["feed_error_escalated"] is True
    assert status_escalated["feed_error_persisted_sec"] >= 60.0

    # Clear error on successful tick
    feed._clear_feed_error()
    status_cleared = feed.get_status()
    assert status_cleared["feed_error"] is None
    assert status_cleared["feed_error_escalated"] is False


def test_sanity_guard_plausible_ltp_bounds():
    # Valid live & default prices for index futures
    assert is_plausible_ltp("NIFTY", 25000.0) is True
    assert is_plausible_ltp("NIFTY", 25500.0) is True
    assert is_plausible_ltp("BANKNIFTY", 54000.0) is True
    assert is_plausible_ltp("BANKNIFTY", 52000.0) is True
    assert is_plausible_ltp("SENSEX", 82000.0) is True
    assert is_plausible_ltp("SENSEX", 83500.0) is True

    # Cross-index contamination strictly rejected:
    # 1. NIFTY price (~25000) rejected for SENSEX (range [30000, 150000])
    assert is_plausible_ltp("SENSEX", 25000.0) is False, "Nifty price 25000 must NOT be accepted for SENSEX"
    # 2. SENSEX price (~82000) rejected for NIFTY (range [10000, 50000])
    assert is_plausible_ltp("NIFTY", 82000.0) is False, "Sensex price 82000 must NOT be accepted for NIFTY"

    # 3. Invalid/None/Negative values
    assert is_plausible_ltp("NIFTY", None) is False
    assert is_plausible_ltp("NIFTY", 0.0) is False
    assert is_plausible_ltp("NIFTY", -500.0) is False
    assert is_plausible_ltp("NIFTY", float("nan")) is False


def test_batch_fetch_rejects_corrupted_cross_index_ltp():
    feed = AngelOneFeed(
        client_id="TEST_CID",
        api_key="TEST_KEY",
        totp_secret="JBSWY3DPEHPK3PXP",
        mpin="1234",
    )
    feed.is_authenticated = True
    feed.smart_api = MagicMock()

    # Populate master cache so tokens map to index names
    feed.futures_master_cache["NIFTY"] = [{"token": "68407", "name": "NIFTY"}]
    feed.futures_master_cache["SENSEX"] = [{"token": "844615", "name": "SENSEX"}]

    # Broker returns corrupted 25000.0 price under SENSEX symbol (below 30000 SENSEX bound)
    feed.smart_api.getMarketData.return_value = {
        "status": True,
        "data": {
            "fetched": [
                {
                    "symbolToken": "844615",
                    "tradingSymbol": "SENSEX26SEPFUT",
                    "ltp": 25000.0,  # Corrupted: NIFTY price on Sensex contract!
                },
                {
                    "symbolToken": "68407",
                    "tradingSymbol": "NIFTY29SEP26FUT",
                    "ltp": 25150.0,  # Valid Nifty price
                },
            ]
        },
    }

    res = feed.fetch_market_data_batch({"NFO": ["68407"], "BFO": ["844615"]})

    # SENSEX corrupted price must be rejected from results and latest_ltp_by_comm
    assert "844615" not in res, "Sanity guard must reject corrupted 25000.0 for SENSEX"
    assert "SENSEX" not in feed.latest_ltp_by_comm, "Corrupted price must not enter latest_ltp_by_comm for SENSEX"

    # NIFTY valid price is accepted
    assert res.get("68407") == 25150.0
    assert feed.latest_ltp_by_comm.get("NIFTY") == 25150.0


def test_forced_reauth_resets_stale_session_and_bypasses_cooldown():
    feed = AngelOneFeed(
        client_id="TEST_CID",
        api_key="TEST_KEY",
        totp_secret="JBSWY3DPEHPK3PXP",
        mpin="1234",
    )
    feed.is_authenticated = True
    feed._last_login_attempt_ts = time.monotonic()

    with patch("angel_one_service.SmartConnect") as MockSC, patch.object(feed, "_load_instruments"):
        mock_instance = MockSC.return_value
        mock_instance.generateSession.return_value = {
            "status": True,
            "data": {
                "jwtToken": "fresh_jwt_token",
                "feedToken": "fresh_feed_token",
            },
        }

        res_normal = feed.login(force=False)
        assert res_normal is True
        assert mock_instance.generateSession.call_count == 0

        res_forced = feed.login(force=True)
        assert res_forced is True
        assert mock_instance.generateSession.call_count == 1
        assert feed.session_data.get("jwtToken") == "fresh_jwt_token"
        assert feed.session_data.get("feedToken") == "fresh_feed_token"
