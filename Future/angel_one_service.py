"""
Angel One SmartAPI Live Feed Integration for MCX Commodity Futures
Handles authentication, TOTP generation, MCX FUTCOM instrument lookup,
nearest monthly expiry resolution, live batch polling of Futures LTP,
and safe execution on MCX exchange.
"""

import asyncio
import datetime
import json
import logging
import math
import os
import re
import threading
import time
import urllib.request
from typing import Dict, Any, Optional, List
import pyotp
from SmartApi import SmartConnect

logger = logging.getLogger("AngelOneService")
logging.basicConfig(level=logging.INFO)

ANGEL_CLIENT_ID = os.environ.get("ANGEL_CLIENT_ID", "H202296")
ANGEL_API_KEY = os.environ.get("ANGEL_API_KEY", "ufOmdMdM")
ANGEL_TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET", "USMXJQBFGE6LATUTYI4EVRTUMM")
ANGEL_MPIN = os.environ.get("ANGEL_MPIN", "3794")

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

COMMODITY_CONFIG: Dict[str, Dict[str, Any]] = {
    "SILVERM": {
        "name": "SILVERM",
        "display_name": "Silver Micro",
        "symbol_prefix": "SILVERMIC",
        "exchange": "MCX",
        "instrumenttype": "FUTCOM",
        "lot_size": 1,
        "tick_size": 1.0,
        "default_ltp": 90000.0,
        "default_range_points": 2000.0,
        "default_slicer_count": 8,
        "default_range_high": 91000.0,
        "default_range_low": 89000.0,
        "default_step": 250.0,
        "default_profit_point": 250.0,
        "default_loss_point": 375.0,
        "default_qty_per_slice_lots": 1,
    },
    "GOLDM": {
        "name": "GOLDM",
        "display_name": "Gold Mini",
        "symbol_prefix": "GOLDM",
        "exchange": "MCX",
        "instrumenttype": "FUTCOM",
        "lot_size": 10,
        "tick_size": 1.0,
        "default_ltp": 75000.0,
        "default_range_points": 500.0,
        "default_slicer_count": 5,
        "default_range_high": 75250.0,
        "default_range_low": 74750.0,
        "default_step": 100.0,
        "default_profit_point": 100.0,
        "default_loss_point": 150.0,
        "default_qty_per_slice_lots": 1,
    },
    "COPPER": {
        "name": "COPPER",
        "display_name": "Copper",
        "symbol_prefix": "COPPER",
        "exchange": "MCX",
        "instrumenttype": "FUTCOM",
        "lot_size": 250,
        "tick_size": 0.05,
        "default_ltp": 850.0,
        "default_range_points": 60.0,
        "default_slicer_count": 6,
        "default_range_high": 880.0,
        "default_range_low": 820.0,
        "default_step": 10.0,
        "default_profit_point": 10.0,
        "default_loss_point": 15.0,
        "default_qty_per_slice_lots": 1,
    },
    "CRUDEOIL": {
        "name": "CRUDEOIL",
        "display_name": "Crude Oil Mini",
        "symbol_prefix": "CRUDEOILM",
        "exchange": "MCX",
        "instrumenttype": "FUTCOM",
        "lot_size": 10,
        "tick_size": 1.0,
        "default_ltp": 6000.0,
        "default_range_points": 200.0,
        "default_slicer_count": 8,
        "default_range_high": 6100.0,
        "default_range_low": 5900.0,
        "default_step": 25.0,
        "default_profit_point": 25.0,
        "default_loss_point": 37.5,
        "default_qty_per_slice_lots": 1,
    },
}

# Backward compatibility alias
INSTRUMENT_CONFIG = COMMODITY_CONFIG


def normalize_commodity_name(commodity: Optional[str]) -> str:
    """Normalizes input to one of SILVERM, GOLDM, COPPER, CRUDEOIL."""
    if not commodity:
        return "SILVERM"
    c = str(commodity).strip().upper()
    if "SILVER" in c:
        return "SILVERM"
    elif "GOLD" in c:
        return "GOLDM"
    elif "COPPER" in c:
        return "COPPER"
    elif "CRUDE" in c:
        return "CRUDEOIL"
    return c


def get_tick_size(commodity: str) -> float:
    """Returns official MCX tick size for commodity."""
    key = normalize_commodity_name(commodity)
    return COMMODITY_CONFIG.get(key, {}).get("tick_size", 0.05)


def get_lot_size(commodity: str) -> int:
    """Returns official MCX lot size for commodity."""
    key = normalize_commodity_name(commodity)
    return COMMODITY_CONFIG.get(key, {}).get("lot_size", 1)


COMMODITY_PLAUSIBLE_RANGES: Dict[str, tuple[float, float]] = {
    "SILVERM": (40000.0, 500000.0),
    "GOLDM": (40000.0, 300000.0),
    "COPPER": (400.0, 3500.0),
    "CRUDEOIL": (1000.0, 25000.0),
}


def is_plausible_ltp(commodity: Optional[str], price: Optional[float]) -> bool:
    """
    Defensive Sanity Guard: Validates that an LTP falls within realistic MCX price bounds
    for the specified commodity. Rejects corrupted, cross-contaminated, or zero/negative prices.
    """
    if price is None or not isinstance(price, (int, float)) or math.isnan(price) or price <= 0:
        return False
    comm = normalize_commodity_name(commodity)
    rng = COMMODITY_PLAUSIBLE_RANGES.get(comm)
    if not rng:
        return price > 0
    return rng[0] <= float(price) <= rng[1]



def is_smartapi_success(resp: Any) -> bool:
    """Robustly checks if SmartAPI response is successful across all response schemas."""
    if not isinstance(resp, dict):
        return False
    st = resp.get("status")
    if st is not None:
        if isinstance(st, bool):
            return st
        if str(st).strip().lower() in ("true", "success", "1"):
            return True
        if str(st).strip().lower() in ("false", "error", "0"):
            return False
    sc = resp.get("success")
    if sc is not None:
        if isinstance(sc, bool):
            return sc
        if str(sc).strip().lower() in ("true", "success", "1"):
            return True
        if str(sc).strip().lower() in ("false", "error", "0"):
            return False
    return False


def extract_smartapi_error(resp: Any) -> tuple[str, str]:
    """Robustly extracts (message, error_code) from SmartAPI response dictionary."""
    if not isinstance(resp, dict):
        return (str(resp) if resp is not None else "", "")
    msg = str(resp.get("message") or resp.get("error_description") or resp.get("errorMsg") or "").strip()
    code = str(resp.get("errorcode") or resp.get("errorCode") or resp.get("code") or "").strip()
    return (msg, code)


class AngelOneFeed:
    # Class-level rate limiter: Angel One enforces a maximum of 3 requests/sec per API key.
    # Enforcing a minimum interval of 1.0s guarantees <= 1.0 req/s across all concurrent threads.
    _pacer_lock = threading.Lock()
    _last_api_call_ts: Dict[str, float] = {}
    _min_call_interval: float = 1.0
    _rate_limit_cooldown_until: float = 0.0

    # Cross-session shared market cache for MCX commodity contracts (shared across all user sessions)
    _shared_market_lock = threading.Lock()
    _shared_market_cache: Dict[str, Dict[str, Any]] = {}
    _shared_ltp_by_comm: Dict[str, float] = {}

    @classmethod
    def set_rate_limit_cooldown(cls, seconds: float = 3.0) -> None:
        """Sets a global silent cooldown when a rate limit is detected."""
        with cls._pacer_lock:
            cls._rate_limit_cooldown_until = max(cls._rate_limit_cooldown_until, time.monotonic() + seconds)
            logger.warning(f"Angel One rate limit detected. Enforcing {seconds:.1f}s cooldown across all calls.")

    @classmethod
    def is_in_cooldown(cls) -> bool:
        """Checks if calls are currently suppressed due to active rate-limit cooldown."""
        with cls._pacer_lock:
            return time.monotonic() < cls._rate_limit_cooldown_until

    @classmethod
    def update_shared_cache(cls, token: str, entry: Dict[str, Any], comm: Optional[str] = None) -> None:
        """Updates cross-session shared market cache thread-safely."""
        with cls._shared_market_lock:
            cls._shared_market_cache[token] = entry
            sym = entry.get("symbol")
            if sym:
                cls._shared_market_cache[f"{sym}:{token}"] = entry
            if comm:
                cls._shared_market_cache[f"{comm}:{token}"] = entry
                cls._shared_ltp_by_comm[comm] = entry["ltp"]

    @classmethod
    def get_shared_cache(cls, token_or_key: str, max_age: float = 2.0) -> Optional[Dict[str, Any]]:
        """Retrieves entry from cross-session shared market cache if fresher than max_age."""
        with cls._shared_market_lock:
            entry = cls._shared_market_cache.get(str(token_or_key))
            if entry and (time.monotonic() - entry.get("time", 0.0)) <= max_age:
                return entry
            return None

    @classmethod
    def get_shared_ltp_by_comm(cls) -> Dict[str, float]:
        """Returns snapshot of shared commodity LTPs."""
        with cls._shared_market_lock:
            return dict(cls._shared_ltp_by_comm)

    def __init__(
        self,
        client_id: Optional[str] = None,
        api_key: Optional[str] = None,
        totp_secret: Optional[str] = None,
        mpin: Optional[str] = None,
        owner_client_id: Optional[str] = None,
    ):
        self.client_id = client_id or ANGEL_CLIENT_ID
        self.api_key = api_key or ANGEL_API_KEY
        self.totp_secret = totp_secret or ANGEL_TOTP_SECRET
        self.mpin = mpin or ANGEL_MPIN
        self.owner_client_id = (owner_client_id or "").strip()

        self.smart_api: Optional[SmartConnect] = None
        self.is_authenticated = False
        self.session_data: Dict[str, Any] = {}
        self.last_auth_time: Optional[datetime.datetime] = None

        # Re-entrant lock and cooldown for authentication
        self._login_lock = threading.RLock()
        self._last_login_attempt_ts: float = 0.0
        self._min_login_cooldown_sec: float = 3.0

        # In-memory fast caches
        self._future_cache: Dict[str, Dict[str, Any]] = {}
        self.futures_master_cache: Dict[str, List[Dict[str, Any]]] = {
            "SILVERM": [],
            "GOLDM": [],
            "COPPER": [],
            "CRUDEOIL": [],
        }
        self.last_cache_time: Optional[datetime.datetime] = None

        # Live Feed Status & Error Telemetry
        self.is_streaming = False
        self.stream_task: Optional[asyncio.Task] = None
        self.poll_interval_sec = 2.0

        self.latest_ltp_by_comm: Dict[str, float] = {}
        self.latest_contract_by_comm: Dict[str, Dict[str, Any]] = {}
        self._contract_resolved_at: Dict[str, float] = {}
        self._contract_cache_ttl_sec: float = 300.0  # Periodic 5-minute contract re-resolution

        self.last_tick_time: Optional[str] = None
        self.error_message: Optional[str] = None

        self.feed_error: Optional[str] = None
        self.feed_error_first_ts: Optional[float] = None
        self.feed_error_count: int = 0

    def _record_feed_error(self, err_str: str) -> None:
        """Records a live feed error and starts the persistence timer if not already active."""
        if not self.feed_error:
            self.feed_error_first_ts = time.time()
        self.feed_error = err_str
        self.feed_error_count += 1

    def _clear_feed_error(self) -> None:
        """Clears feed error state when valid live ticks are received."""
        self.feed_error = None
        self.feed_error_first_ts = None
        self.feed_error_count = 0

    def _pace_api_call(self) -> None:
        """Thread-safe pacer enforcing minimum interval between calls."""
        key = self.api_key or self.client_id or "default"
        with AngelOneFeed._pacer_lock:
            now = time.monotonic()
            if now < AngelOneFeed._rate_limit_cooldown_until:
                wait_cd = AngelOneFeed._rate_limit_cooldown_until - now
                if wait_cd > 0:
                    time.sleep(wait_cd)
                now = time.monotonic()

            last_ts = AngelOneFeed._last_api_call_ts.get(key, 0.0)
            elapsed = now - last_ts
            wait_time = AngelOneFeed._min_call_interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            AngelOneFeed._last_api_call_ts[key] = time.monotonic()

    def login(self, force: bool = False) -> bool:
        """Authenticates with Angel One using SmartAPI and TOTP, thread-safe with cooldown."""
        if not (self.client_id and self.api_key and self.totp_secret and self.mpin):
            self.error_message = "Angel One credentials missing or incomplete. Please submit broker credentials."
            self.is_authenticated = False
            return False

        with self._login_lock:
            now = time.monotonic()
            if force:
                logger.info(f"[LTP_TRACE] Forced re-authentication requested for {self.client_id}. Bypassing cooldown and clearing stale session.")
                self.is_authenticated = False
            else:
                # If already logged in recently and authenticated, and not forced, reuse session
                if self.is_authenticated and (now - self._last_login_attempt_ts < self._min_login_cooldown_sec):
                    return True

                # If a failed attempt happened very recently, observe minimum cooldown
                if (now - self._last_login_attempt_ts < self._min_login_cooldown_sec):
                    logger.info(f"Login cooldown active for {self.client_id} ({self._min_login_cooldown_sec:.1f}s), reusing current state (auth={self.is_authenticated})")
                    return self.is_authenticated

            self._last_login_attempt_ts = now

            for attempt in range(2):
                try:
                    self._pace_api_call()
                    totp = pyotp.TOTP(self.totp_secret).now()
                    self.smart_api = SmartConnect(api_key=self.api_key)
                    session = self.smart_api.generateSession(self.client_id, self.mpin, totp)

                    if session and is_smartapi_success(session):
                        self.session_data = session.get("data", {})
                        feed_tok = self.session_data.get("feedToken")
                        if feed_tok and hasattr(self.smart_api, "setFeedToken"):
                            self.smart_api.setFeedToken(feed_tok)
                        self.is_authenticated = True
                        self.last_auth_time = datetime.datetime.now()
                        self.error_message = None
                        self._clear_feed_error()
                        logger.info(f"Angel One Login Successful for Client: {self.client_id} (feedToken_present={bool(feed_tok)})")
                        self._load_instruments()
                        return True
                    else:
                        msg, errcode = extract_smartapi_error(session)
                        err_msg = f"{msg} ({errcode})" if errcode else (msg or "Login failed")
                        self.error_message = err_msg
                        self.is_authenticated = False
                        logger.error(f"Angel One Login failed: {err_msg}")
                        return False
                except Exception as e:
                    err_str = str(e)
                    if "exceeding access rate" in err_str or "Access denied" in err_str:
                        logger.warning(f"Rate limited on Angel One login (attempt {attempt+1}), backing off 0.5s...")
                        time.sleep(0.5)
                        continue
                    self.error_message = str(e)
                    self.is_authenticated = False
                    logger.error(f"Angel One Login Exception: {e}", exc_info=True)
                    return False
            return False

    def _load_instruments(self):
        """
        Loads and caches MCX FUTCOM instruments (SILVERM, GOLDM, COPPER) from Angel One OpenAPI Scrip Master.
        Persists to a local scrips_mcx_futures.json cache file.
        """
        cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrips_mcx_futures.json")
        today_date = datetime.date.today()

        # 1. Check if local MCX futures cache exists - load instantly (<10ms)
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                for k in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
                    self.futures_master_cache[k] = self._parse_scrip_items(cached_data.get(k, []))
                self.last_cache_time = datetime.datetime.now()
                logger.info(
                    f"Instantly loaded MCX futures from local cache: "
                    f"SILVERM ({len(self.futures_master_cache['SILVERM'])}), "
                    f"GOLDM ({len(self.futures_master_cache['GOLDM'])}), "
                    f"COPPER ({len(self.futures_master_cache['COPPER'])}), "
                    f"CRUDEOIL ({len(self.futures_master_cache['CRUDEOIL'])})"
                )
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
                if mtime == today_date:
                    return
                # If cached from an earlier date, refresh in background without blocking login or slot loading
                if not getattr(self, "_downloading_scrips", False):
                    self._downloading_scrips = True
                    threading.Thread(target=self._background_refresh_mcx_scrips, args=(cache_file,), daemon=True).start()
                return
            except Exception as e:
                logger.warning(f"Error reading local MCX scrips cache: {e}")

        # 2. Download fresh OpenAPI Scrip Master if no local cache exists
        self._download_scrip_master(cache_file)

    def _background_refresh_mcx_scrips(self, cache_file: str):
        try:
            logger.info("Background thread refreshing Angel One OpenAPI Scrip Master for MCX...")
            self._download_scrip_master(cache_file)
        finally:
            self._downloading_scrips = False

    def _download_scrip_master(self, cache_file: str):
        try:
            logger.info("Downloading official Angel One OpenAPI Scrip Master for MCX Futures...")
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                raw = json.loads(resp.read().decode("utf-8"))

                prefix_map = {cfg.get("symbol_prefix", k): k for k, cfg in COMMODITY_CONFIG.items()}
                name_map = {k: k for k in COMMODITY_CONFIG.keys()}
                name_map.update(prefix_map)

                parsed_dict: Dict[str, List[Dict[str, Any]]] = {k: [] for k in COMMODITY_CONFIG.keys()}
                for item in raw:
                    if item.get("exch_seg") == "MCX" and item.get("instrumenttype") == "FUTCOM":
                        nm = item.get("name", "")
                        comm_key = name_map.get(nm)
                        if comm_key and comm_key in parsed_dict:
                            parsed_dict[comm_key].append(item)

                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(parsed_dict, f)

                for k in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
                    self.futures_master_cache[k] = self._parse_scrip_items(parsed_dict.get(k, []))

                self.last_cache_time = datetime.datetime.now()
                logger.info(
                    f"Cached MCX futures from Scrip Master: "
                    f"SILVERM ({len(self.futures_master_cache['SILVERM'])}), "
                    f"GOLDM ({len(self.futures_master_cache['GOLDM'])}), "
                    f"COPPER ({len(self.futures_master_cache['COPPER'])}), "
                    f"CRUDEOIL ({len(self.futures_master_cache['CRUDEOIL'])})"
                )
        except Exception as e:
            logger.error(f"Error downloading OpenAPIScripMaster for MCX: {e}")

    def _parse_scrip_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        parsed = []
        for i in items:
            sym = i.get("symbol", "")
            token = str(i.get("token", ""))
            name = i.get("name", "")
            exp_str = i.get("expiry", "")
            try:
                lotsize = int(i.get("lotsize", 1))
            except Exception:
                lotsize = 1

            try:
                tick_size = float(i.get("tick_size", 100)) / 100.0  # tick_size in scrip master is in paise (100 = 1.0 pt)
            except Exception:
                tick_size = 1.0

            try:
                if len(exp_str) == 9:
                    exp_date = datetime.datetime.strptime(exp_str, "%d%b%Y").date()
                else:
                    exp_date = datetime.datetime.strptime(exp_str, "%d%b%y").date()
            except Exception:
                continue

            parsed.append({
                "symbol": sym,
                "token": token,
                "name": name,
                "expiry_str": exp_str,
                "expiry_date": exp_date,
                "lotsize": lotsize,
                "tick_size": tick_size,
                "exch_seg": "MCX",
                "instrumenttype": "FUTCOM",
            })
        return parsed

    def resolve_future_contract(self, commodity: str) -> Optional[Dict[str, Any]]:
        """
        Finds the nearest active monthly expiry FUTCOM contract for the commodity.
        Sorts candidate contracts ascending by expiry date and selects nearest where expiry_date >= today.
        Re-resolves periodically if cached contract expires or TTL elapses.
        Never caches simulated fallback contracts.
        """
        comm_key = normalize_commodity_name(commodity)
        today = datetime.date.today()
        now_ts = time.monotonic()

        cached_contract = self.latest_contract_by_comm.get(comm_key)
        cached_ts = self._contract_resolved_at.get(comm_key, 0.0)

        # Check if valid real contract is cached, not expired, and within TTL
        if cached_contract and (now_ts - cached_ts < self._contract_cache_ttl_sec):
            exp = cached_contract.get("expiry_date")
            if exp and exp >= today:
                return cached_contract

        cache = self.futures_master_cache.get(comm_key, [])
        if not cache:
            self._load_instruments()
            cache = self.futures_master_cache.get(comm_key, [])

        if not cache:
            # Fallback simulated contract if master is offline (DO NOT CACHE)
            cfg = COMMODITY_CONFIG.get(comm_key, COMMODITY_CONFIG["SILVERM"])
            prefix = cfg.get("symbol_prefix", comm_key)
            sim_exp = (today + datetime.timedelta(days=30)).strftime("%d%b%Y").upper()
            contract = {
                "symbol": f"{prefix}{sim_exp}FUT",
                "token": f"token_{prefix.lower()}",
                "name": prefix,
                "expiry_str": sim_exp,
                "expiry_date": today + datetime.timedelta(days=30),
                "lotsize": cfg["lot_size"],
                "tick_size": cfg["tick_size"],
                "exch_seg": "MCX",
                "instrumenttype": "FUTCOM",
            }
            logger.warning(
                f"[OFFLINE FALLBACK] MCX instrument master unavailable for {comm_key}. "
                f"Generated temporary simulated contract {contract['symbol']} (token: {contract['token']}). "
                f"NOT cached — will retry live resolution on next cycle."
            )
            # Intentionally NOT stored in self.latest_contract_by_comm
            return contract

        matched = [c for c in cache if c["expiry_date"] >= today]
        if matched:
            matched.sort(key=lambda x: x["expiry_date"])
            best = matched[0]
            self.latest_contract_by_comm[comm_key] = best
            self._contract_resolved_at[comm_key] = now_ts
            logger.info(f"[LTP_TRACE] commodity={comm_key} source=resolve_future_contract symbol={best['symbol']} token={best['token']} expiry={best['expiry_str']}")
            return best

        # Fallback to closest expiry in master cache
        cache.sort(key=lambda x: abs((x["expiry_date"] - today).days))
        best = cache[0]
        self.latest_contract_by_comm[comm_key] = best
        self._contract_resolved_at[comm_key] = now_ts
        logger.info(f"[LTP_TRACE] commodity={comm_key} source=resolve_future_contract closest_fallback symbol={best['symbol']} token={best['token']} expiry={best['expiry_str']}")
        return best

    def resolve_contract(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        """Compatibility alias for resolve_future_contract."""
        commodity = "SILVERM"
        if args:
            commodity = str(args[-1] if isinstance(args[-1], str) else args[0])
        if "commodity" in kwargs:
            commodity = kwargs["commodity"]
        elif "instrument" in kwargs:
            commodity = kwargs["instrument"]
        return self.resolve_future_contract(commodity)

    def fetch_market_data_batch(
        self,
        exchange_tokens: Dict[str, List[str]],
        max_cache_age: float = 2.0,
    ) -> Dict[str, float]:
        """
        Fetches live LTP for MCX commodity futures in a single unified API call via getMarketData.
        Checks cross-session shared market cache first to avoid duplicate API calls across user sessions.
        Handles dual status/success schemas, rate limits, and feeds error telemetry.
        """
        results: Dict[str, float] = {}
        tokens_needed: Dict[str, List[str]] = {}

        now = time.monotonic()
        with AngelOneFeed._shared_market_lock:
            for exch, tokens in exchange_tokens.items():
                for t in tokens:
                    t_str = str(t)
                    # 1. Check instance cache
                    cached = self._future_cache.get(t_str)
                    if not (cached and (now - cached.get("time", 0.0)) < max_cache_age):
                        # 2. Check cross-session shared market cache
                        cached = AngelOneFeed._shared_market_cache.get(t_str)
                        if cached:
                            self._future_cache[t_str] = cached

                    if cached and (now - cached.get("time", 0.0)) < max_cache_age:
                        results[t_str] = cached["ltp"]
                        cached_comm = cached.get("commodity")
                        if cached_comm and is_plausible_ltp(cached_comm, cached["ltp"]):
                            self.latest_ltp_by_comm[cached_comm] = cached["ltp"]
                        logger.debug(f"[LTP_TRACE] commodity={cached_comm or 'UNKNOWN'} source=batch_cache_hit token={t_str} value={cached['ltp']}")
                    else:
                        tokens_needed.setdefault(exch, []).append(t_str)

        if not tokens_needed:
            return results

        if AngelOneFeed.is_in_cooldown():
            with AngelOneFeed._shared_market_lock:
                for exch, tokens in tokens_needed.items():
                    for t in tokens:
                        t_str = str(t)
                        cached = self._future_cache.get(t_str) or AngelOneFeed._shared_market_cache.get(t_str)
                        if cached:
                            results[t_str] = cached["ltp"]
                            c_comm = cached.get("commodity")
                            if c_comm and is_plausible_ltp(c_comm, cached["ltp"]):
                                self.latest_ltp_by_comm[c_comm] = cached["ltp"]
            if results:
                self._clear_feed_error()
            return results

        if not self.smart_api or not self.is_authenticated:
            if not self.login():
                with AngelOneFeed._shared_market_lock:
                    for exch, tokens in tokens_needed.items():
                        for t in tokens:
                            t_str = str(t)
                            cached = self._future_cache.get(t_str) or AngelOneFeed._shared_market_cache.get(t_str)
                            if cached:
                                results[t_str] = cached["ltp"]
                return results

        is_rate_limit = False
        try:
            self._pace_api_call()
            # Re-check shared cache after pacing wait in case another concurrent session fetched in the interim
            now = time.monotonic()
            still_needed: Dict[str, List[str]] = {}
            with AngelOneFeed._shared_market_lock:
                for exch, tokens in tokens_needed.items():
                    for t in tokens:
                        t_str = str(t)
                        cached = AngelOneFeed._shared_market_cache.get(t_str)
                        if cached and (now - cached.get("time", 0.0)) < max_cache_age:
                            results[t_str] = cached["ltp"]
                            self._future_cache[t_str] = cached
                            cached_comm = cached.get("commodity")
                            if cached_comm and is_plausible_ltp(cached_comm, cached["ltp"]):
                                self.latest_ltp_by_comm[cached_comm] = cached["ltp"]
                        else:
                            still_needed.setdefault(exch, []).append(t_str)

            if not still_needed:
                if results:
                    self._clear_feed_error()
                return results

            resp = self.smart_api.getMarketData("LTP", still_needed)
            if is_smartapi_success(resp) and resp.get("data"):
                fetched = resp["data"].get("fetched", [])
                now_ts = time.monotonic()
                with AngelOneFeed._shared_market_lock:
                    for item in fetched:
                        t_str = str(item.get("symbolToken", ""))
                        ltp_val = float(item.get("ltp", 0.0))
                        sym = item.get("tradingSymbol", "")
                        if ltp_val > 0:
                            # Identify matching commodity from trading symbol
                            comm_match = None
                            for comm in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
                                if sym.startswith(comm):
                                    comm_match = comm
                                    break

                            # Defensive Sanity Guard: prevent corrupted/cross-commodity prices
                            if comm_match and not is_plausible_ltp(comm_match, ltp_val):
                                logger.error(
                                    f"[LTP_TRACE] [SANITY_GUARD_REJECT] commodity={comm_match} symbol={sym} token={t_str} "
                                    f"rejected_value={ltp_val} (outside plausible range {COMMODITY_PLAUSIBLE_RANGES[comm_match]})"
                                )
                                continue

                            results[t_str] = ltp_val
                            cache_entry = {"ltp": ltp_val, "time": now_ts, "commodity": comm_match, "symbol": sym}
                            self._future_cache[t_str] = cache_entry
                            if sym:
                                self._future_cache[f"{sym}:{t_str}"] = cache_entry
                            if comm_match:
                                self._future_cache[f"{comm_match}:{t_str}"] = cache_entry
                                self.latest_ltp_by_comm[comm_match] = ltp_val
                                AngelOneFeed._shared_ltp_by_comm[comm_match] = ltp_val
                                logger.info(f"[LTP_TRACE] commodity={comm_match} source=batch_fetch symbol={sym} token={t_str} value={ltp_val}")
                            else:
                                logger.info(f"[LTP_TRACE] commodity=UNKNOWN source=batch_fetch symbol={sym} token={t_str} value={ltp_val}")

                            AngelOneFeed._shared_market_cache[t_str] = cache_entry
                            if sym:
                                AngelOneFeed._shared_market_cache[f"{sym}:{t_str}"] = cache_entry
                            if comm_match:
                                AngelOneFeed._shared_market_cache[f"{comm_match}:{t_str}"] = cache_entry

                    self.last_tick_time = datetime.datetime.now(IST).strftime("%H:%M:%S")

                if results:
                    self._clear_feed_error()
                return results
            elif resp and not is_smartapi_success(resp):
                msg, errcode = extract_smartapi_error(resp)
                err_display = f"{msg} ({errcode})" if errcode else (msg or "Market data request rejected")
                logger.warning(f"Angel One market data error: {err_display}")

                is_rate_limit = (
                    "exceeding access rate" in msg.lower()
                    or "rate" in msg.lower()
                    or "access denied" in msg.lower()
                    or "too many requests" in msg.lower()
                )
                if is_rate_limit:
                    AngelOneFeed.set_rate_limit_cooldown(3.0)
                elif "token" in msg.lower() or "session" in msg.lower() or "invalid" in msg.lower() or errcode.upper() in ("AG8001", "AG8003", "AB1010", "AB1007", "AB1004"):
                    self._record_feed_error(err_display)
                    logger.warning(f"Angel One auth invalidation detected ({errcode}: {msg}). Re-authenticating with force=True...")
                    self.login(force=True)
                else:
                    self._record_feed_error(err_display)
        except Exception as e:
            err_str = str(e)
            is_rate_limit = (
                "exceeding access rate" in err_str.lower()
                or "access denied" in err_str.lower()
                or "too many requests" in err_str.lower()
            )
            if is_rate_limit:
                AngelOneFeed.set_rate_limit_cooldown(3.0)
                logger.warning(f"Angel One rate limit encountered in fetch_market_data_batch: {err_str}. Enforcing 3.0s cooldown and serving cached data.")
            else:
                self._record_feed_error(err_str)
                logger.error(f"Error in fetch_market_data_batch: {e}")

        # Fallback to last known cache with sanity check
        with AngelOneFeed._shared_market_lock:
            for exch, tokens in tokens_needed.items():
                for t in tokens:
                    t_str = str(t)
                    if t_str not in results:
                        c_entry = self._future_cache.get(t_str) or AngelOneFeed._shared_market_cache.get(t_str)
                        if c_entry:
                            c_comm = c_entry.get("commodity")
                            c_ltp = c_entry["ltp"]
                            if not c_comm or is_plausible_ltp(c_comm, c_ltp):
                                results[t_str] = c_ltp
                                if c_comm:
                                    self.latest_ltp_by_comm[c_comm] = c_ltp
                                logger.info(f"[LTP_TRACE] commodity={c_comm or 'UNKNOWN'} source=batch_cache_fallback token={t_str} value={c_ltp}")

        if results:
            if is_rate_limit or not self.feed_error:
                self._clear_feed_error()
        else:
            if is_rate_limit:
                self._record_feed_error("Angel One rate limit reached — waiting for cooldown")

        return results

    def fetch_future_ltp(
        self,
        symbol: str,
        token: str,
        exchange: str = "MCX",
        max_cache_age: float = 2.0,
    ) -> Optional[float]:
        """Fetches live Future LTP for a given MCX contract."""
        cache_key = f"{symbol}:{token}"
        comm = None
        for c in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
            if symbol.startswith(c):
                comm = c
                break

        now = time.monotonic()
        with AngelOneFeed._shared_market_lock:
            cached = (
                self._future_cache.get(cache_key)
                or self._future_cache.get(str(token))
                or AngelOneFeed._shared_market_cache.get(cache_key)
                or AngelOneFeed._shared_market_cache.get(str(token))
            )
            if max_cache_age > 0 and cached and (now - cached.get("time", 0.0)) < max_cache_age:
                cached_ltp = cached.get("ltp")
                if not comm or is_plausible_ltp(comm, cached_ltp):
                    logger.debug(f"[LTP_TRACE] commodity={comm or 'UNKNOWN'} source=fetch_future_ltp_cache symbol={symbol} value={cached_ltp}")
                    return cached_ltp

        if AngelOneFeed.is_in_cooldown():
            if cached and max_cache_age > 0:
                cached_ltp = cached.get("ltp")
                if not comm or is_plausible_ltp(comm, cached_ltp):
                    self._clear_feed_error()
                    return cached_ltp
            return None

        if not self.smart_api or not self.is_authenticated:
            if not self.login():
                if cached and max_cache_age > 0:
                    cached_ltp = cached.get("ltp")
                    if not comm or is_plausible_ltp(comm, cached_ltp):
                        return cached_ltp
                return None

        is_rate_limit = False
        try:
            self._pace_api_call()
            resp = self.smart_api.ltpData(exchange, symbol, token)
            if is_smartapi_success(resp) and resp.get("data"):
                ltp = float(resp["data"]["ltp"])
                if comm and not is_plausible_ltp(comm, ltp):
                    logger.error(
                        f"[LTP_TRACE] [SANITY_GUARD_REJECT] commodity={comm} symbol={symbol} token={token} "
                        f"source=fetch_future_ltp rejected_value={ltp} (outside plausible range {COMMODITY_PLAUSIBLE_RANGES.get(comm)})"
                    )
                    return None

                cache_entry = {"ltp": ltp, "time": time.monotonic(), "commodity": comm, "symbol": symbol}
                self._future_cache[cache_key] = cache_entry
                self._future_cache[str(token)] = cache_entry
                if comm:
                    self._future_cache[f"{comm}:{token}"] = cache_entry
                    self.latest_ltp_by_comm[comm] = ltp
                    logger.info(f"[LTP_TRACE] commodity={comm} source=fetch_future_ltp symbol={symbol} token={token} value={ltp}")

                with AngelOneFeed._shared_market_lock:
                    AngelOneFeed._shared_market_cache[str(token)] = cache_entry
                    AngelOneFeed._shared_market_cache[cache_key] = cache_entry
                    if comm:
                        AngelOneFeed._shared_market_cache[f"{comm}:{token}"] = cache_entry
                        AngelOneFeed._shared_ltp_by_comm[comm] = ltp

                self._clear_feed_error()
                return ltp
            elif resp and not is_smartapi_success(resp):
                msg, errcode = extract_smartapi_error(resp)
                err_display = f"{msg} ({errcode})" if errcode else (msg or "LTP request rejected")
                logger.warning(f"Angel One ltpData error: {err_display}")

                is_rate_limit = (
                    "exceeding access rate" in msg.lower()
                    or "rate" in msg.lower()
                    or "access denied" in msg.lower()
                    or "too many requests" in msg.lower()
                )
                if is_rate_limit:
                    AngelOneFeed.set_rate_limit_cooldown(3.0)
                elif "token" in msg.lower() or "session" in msg.lower() or "invalid" in msg.lower() or errcode.upper() in ("AG8001", "AG8003", "AB1010", "AB1007", "AB1004"):
                    self._record_feed_error(err_display)
                    logger.warning(f"Angel One auth invalidation in fetch_future_ltp ({errcode}: {msg}). Re-authenticating with force=True...")
                    self.login(force=True)
                else:
                    self._record_feed_error(err_display)
        except Exception as e:
            err_str = str(e)
            is_rate_limit = (
                "exceeding access rate" in err_str.lower()
                or "access denied" in err_str.lower()
                or "too many requests" in err_str.lower()
            )
            if is_rate_limit:
                AngelOneFeed.set_rate_limit_cooldown(3.0)
                logger.warning(f"Angel One rate limit in fetch_future_ltp: {err_str}")
            else:
                self._record_feed_error(err_str)
                logger.error(f"Error fetching future LTP for {symbol}: {e}")

        if cached and max_cache_age > 0:
            cached_ltp = cached.get("ltp")
            if not comm or is_plausible_ltp(comm, cached_ltp):
                if is_rate_limit or not self.feed_error:
                    self._clear_feed_error()
                return cached_ltp

        if is_rate_limit and not cached:
            self._record_feed_error("Angel One rate limit reached — waiting for cooldown")

        return None


    def place_order(
        self,
        client_id: str,
        run_id: str,
        run_owner_client_id: str,
        symbol: str,
        token: str,
        quantity: int,
        price: float,
        transaction_type: str = "BUY",
        order_type: str = "LIMIT",
        product_type: str = "INTRADAY",
        trading_mode: str = "paper",
        commodity: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes an order on MCX exchange with strict fail-closed safety validation.
        Applies commodity-specific tick size rounding (e.g. ₹1.00 for SILVERM/GOLDM, ₹0.05 for COPPER).
        """
        import uuid
        from order_safety_service import validate_order_safety

        mode = (trading_mode or "paper").strip().lower()
        if mode not in ("paper", "live"):
            mode = "paper"

        comm_name = normalize_commodity_name(commodity or symbol)
        tick = get_tick_size(comm_name)

        order_params = {
            "symbol": symbol,
            "token": token,
            "quantity": quantity,
            "price": price,
            "transaction_type": transaction_type,
            "order_type": order_type,
            "product_type": product_type,
            "trading_mode": mode,
            "exchange": "MCX",
        }

        validate_order_safety(
            client_id=client_id,
            run_id=run_id,
            run_owner_client_id=run_owner_client_id,
            credential_owner_client_id=self.owner_client_id or client_id,
            order_params=order_params,
        )

        logger.info(
            f"Order safety validation passed: Client '{client_id}', Run '{run_id}', Mode '{mode}', "
            f"{transaction_type} {quantity}x {symbol} @ ₹{price} on MCX"
        )

        if mode == "live":
            if (not self.smart_api or not self.is_authenticated) and self.client_id and self.api_key and self.totp_secret and self.mpin:
                try:
                    logger.info(f"Attempting live broker login for client '{client_id}' prior to order placement with force=True...")
                    self.login(force=True)
                except Exception as login_err:
                    logger.warning(f"Auto-login attempt failed: {login_err}")

            if not self.smart_api or not self.is_authenticated:
                err_msg = "Live mode requires an authenticated AngelOne session. Order rejected."
                logger.error(f"LIVE ORDER REJECTED for client '{client_id}': {err_msg}")
                return {
                    "status": "error",
                    "message": err_msg,
                    "mode": "live",
                    "order_params": order_params,
                }

            try:
                clean_ordertype = order_type.upper()
                if clean_ordertype == "MARKET":
                    price_str = "0"
                else:
                    aligned_price = round(round(float(price) / tick) * tick, 2)
                    price_str = f"{aligned_price:.2f}"

                order_payload = {
                    "variety": "NORMAL",
                    "tradingsymbol": str(symbol),
                    "symboltoken": str(token),
                    "transactiontype": transaction_type.upper(),
                    "exchange": "MCX",
                    "ordertype": clean_ordertype,
                    "producttype": product_type.upper(),
                    "duration": "DAY",
                    "price": price_str,
                    "squareoff": "0",
                    "stoploss": "0",
                    "quantity": str(int(quantity)),
                }

                self._pace_api_call()
                order_id_str = None
                api_err_msg = None

                if hasattr(self.smart_api, "_postRequest"):
                    try:
                        api_res = self.smart_api._postRequest("api.order.place", order_payload)
                        if api_res and isinstance(api_res, dict) and is_smartapi_success(api_res):
                            data = api_res.get("data") or {}
                            order_id_str = str(data.get("orderid", "")) if isinstance(data, dict) else str(data)
                        elif api_res and isinstance(api_res, dict):
                            msg, errcode = extract_smartapi_error(api_res)
                            api_err_msg = f"{msg} ({errcode})" if errcode else msg
                    except Exception as req_err:
                        logger.warning(f"Note calling SmartAPI _postRequest: {req_err}")

                if not order_id_str and self.smart_api:
                    err_lower = str(api_err_msg or "").lower()
                    is_auth_err = (
                        "token" in err_lower
                        or "session" in err_lower
                        or "invalid" in err_lower
                        or "ab1007" in err_lower
                        or "ab1010" in err_lower
                        or "ag8001" in err_lower
                        or "ag8003" in err_lower
                    )
                    if is_auth_err:
                        logger.info(f"Angel One session expired or invalid token for client '{client_id}'. Auto-refreshing session...")
                        if self.login(force=True) and self.smart_api:
                            try:
                                api_res = self.smart_api._postRequest("api.order.place", order_payload)
                                if api_res and isinstance(api_res, dict) and is_smartapi_success(api_res):
                                    data = api_res.get("data") or {}
                                    order_id_str = str(data.get("orderid", "")) if isinstance(data, dict) else str(data)
                                elif api_res and isinstance(api_res, dict):
                                    msg, errcode = extract_smartapi_error(api_res)
                                    api_err_msg = f"{msg} ({errcode})" if errcode else msg
                            except Exception as retry_err:
                                logger.warning(f"Error during retried order place: {retry_err}")

                    if not order_id_str:
                        try:
                            res = self.smart_api.placeOrder(order_payload)
                            if res:
                                order_id_str = str(res)
                        except Exception as pe_err:
                            logger.warning(f"Note calling SmartAPI placeOrder: {pe_err}")

                if not order_id_str:
                    err_msg = api_err_msg or f"Angel One rejected {transaction_type} order for {symbol} on MCX ({quantity} qty @ {price_str}). Check margin, market hours, or contract."
                    logger.error(f"LIVE ORDER FAILED for client '{client_id}': {err_msg}")
                    return {
                        "status": "error",
                        "message": err_msg,
                        "mode": "live",
                        "order_params": order_params,
                    }

                logger.info(f"LIVE MCX ORDER CONFIRMED: Client '{client_id}', Order ID: {order_id_str}, {transaction_type} {quantity}x {symbol} @ ₹{price_str}")
                return {
                    "status": "success",
                    "order_id": order_id_str,
                    "mode": "live",
                    "order_params": order_params,
                }
            except Exception as e:
                logger.error(f"Error placing live MCX order with SmartAPI: {e}", exc_info=True)
                return {
                    "status": "error",
                    "message": str(e),
                    "mode": "live",
                    "order_params": order_params,
                }

        # Paper Mode: return simulated fill
        fill_price = price if price and price > 0 else (self.latest_ltp_by_comm.get(comm_name) or 0.0)
        return {
            "status": "success",
            "order_id": f"sim_{uuid.uuid4().hex[:8]}",
            "mode": "paper",
            "fill_price": fill_price,
            "order_params": order_params,
        }

    def get_status(self) -> Dict[str, Any]:
        persisted_sec = (time.time() - self.feed_error_first_ts) if (self.feed_error and self.feed_error_first_ts) else 0.0
        merged_ltp = dict(AngelOneFeed.get_shared_ltp_by_comm())
        merged_ltp.update(self.latest_ltp_by_comm)
        return {
            "authenticated": self.is_authenticated,
            "streaming": self.is_streaming,
            "client_id": self.client_id,
            "owner_client_id": self.owner_client_id,
            "ltp_by_commodity": merged_ltp,
            "latest_ltp_by_comm": merged_ltp,
            "contracts_by_commodity": {k: v.get("symbol") for k, v in self.latest_contract_by_comm.items()},
            "last_tick_time": self.last_tick_time,
            "error": self.error_message,
            "feed_error": self.feed_error,
            "feed_error_persisted_sec": round(persisted_sec, 1),
            "feed_error_escalated": persisted_sec >= 60.0,
            "feed_error_count": self.feed_error_count,
        }
