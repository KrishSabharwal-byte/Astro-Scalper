"""
Angel One SmartAPI Live Feed Integration for Index Futures (NIFTY, BANKNIFTY, SENSEX)
Handles authentication, TOTP generation, NFO/BFO FUTIDX instrument lookup,
nearest monthly expiry resolution, live batch polling of Futures LTP,
and safe execution on NFO/BFO exchanges.
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

ANGEL_CLIENT_ID = os.environ.get("ANGEL_CLIENT_ID", "")
ANGEL_API_KEY = os.environ.get("ANGEL_API_KEY", "")
ANGEL_TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET", "")
ANGEL_MPIN = os.environ.get("ANGEL_MPIN", "")

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# =============================================================================
# INDEX FUTURES CONFIGURATION
# NOTE: Lot sizes are exchange-revised periodically (NSE/BSE periodically revise
# derivative lot sizes, including Jan 2026 revisions). These values serve as initial
# sensible defaults and must be verified/updated from Angel One's live OpenAPIScripMaster
# before deploying live capital.
# =============================================================================
INDEX_CONFIG: Dict[str, Dict[str, Any]] = {
    "NIFTY": {
        "name": "NIFTY",
        "display_name": "Nifty 50 Futures",
        "symbol_prefix": "NIFTY",
        "exchange": "NFO",
        "instrumenttype": "FUTIDX",
        "lot_size": 65,  # Exchange-revised lot size placeholder (Verify live before real capital)
        "tick_size": 0.05,
        "default_ltp": 25000.0,
        "default_range_points": 600.0,
        "default_slicer_count": 10,
        "default_range_high": 25300.0,
        "default_range_low": 24700.0,
        "default_step": 60.0,
        "default_profit_point": 60.0,
        "default_loss_point": 90.0,
        "default_qty_per_slice_lots": 1,
    },
    "BANKNIFTY": {
        "name": "BANKNIFTY",
        "display_name": "Bank Nifty Futures",
        "symbol_prefix": "BANKNIFTY",
        "exchange": "NFO",
        "instrumenttype": "FUTIDX",
        "lot_size": 30,  # Exchange-revised lot size placeholder (Verify live before real capital)
        "tick_size": 0.05,
        "default_ltp": 54000.0,
        "default_range_points": 800.0,
        "default_slicer_count": 10,
        "default_range_high": 54400.0,
        "default_range_low": 53600.0,
        "default_step": 80.0,
        "default_profit_point": 80.0,
        "default_loss_point": 120.0,
        "default_qty_per_slice_lots": 1,
    },
    "SENSEX": {
        "name": "SENSEX",
        "display_name": "BSE Sensex Futures",
        "symbol_prefix": "SENSEX",
        "exchange": "BFO",
        "instrumenttype": "FUTIDX",
        "lot_size": 20,  # Exchange-revised lot size placeholder (Verify live before real capital)
        "tick_size": 0.05,
        "default_ltp": 82000.0,
        "default_range_points": 1200.0,
        "default_slicer_count": 8,
        "default_range_high": 82600.0,
        "default_range_low": 81400.0,
        "default_step": 150.0,
        "default_profit_point": 150.0,
        "default_loss_point": 225.0,
        "default_qty_per_slice_lots": 1,
    },
}

# Compatibility aliases
INSTRUMENT_CONFIG = INDEX_CONFIG
COMMODITY_CONFIG = INDEX_CONFIG


def normalize_index_name(instrument: Optional[str]) -> str:
    """Normalizes input string to one of NIFTY, BANKNIFTY, SENSEX."""
    if not instrument:
        return "NIFTY"
    s = str(instrument).strip().upper()
    if "BANK" in s:
        return "BANKNIFTY"
    elif "NIFTY" in s:
        return "NIFTY"
    elif "SENSEX" in s or "BSX" in s:
        return "SENSEX"
    return s


# Compatibility alias
normalize_commodity_name = normalize_index_name


def get_tick_size(instrument: str) -> float:
    """Returns official tick size for index."""
    key = normalize_index_name(instrument)
    return INDEX_CONFIG.get(key, {}).get("tick_size", 0.05)


def get_lot_size(instrument: str) -> int:
    """Returns lot size for index."""
    key = normalize_index_name(instrument)
    return INDEX_CONFIG.get(key, {}).get("lot_size", 65)


INDEX_PLAUSIBLE_RANGES: Dict[str, tuple[float, float]] = {
    "NIFTY": (10000.0, 50000.0),
    "BANKNIFTY": (20000.0, 100000.0),
    "SENSEX": (30000.0, 150000.0),
}

# Compatibility alias
COMMODITY_PLAUSIBLE_RANGES = INDEX_PLAUSIBLE_RANGES


def is_plausible_ltp(instrument: Optional[str], price: Optional[float]) -> bool:
    """
    Defensive Sanity Guard: Validates that an LTP falls within realistic index bounds
    for the specified index. Rejects corrupted, cross-contaminated, or zero/negative prices.
    """
    if price is None or not isinstance(price, (int, float)) or math.isnan(price) or price <= 0:
        return False
    inst = normalize_index_name(instrument)
    rng = INDEX_PLAUSIBLE_RANGES.get(inst)
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
    # Enforcing a minimum interval of 0.65s guarantees <= 1.54 req/s across all concurrent threads.
    _pacer_lock = threading.Lock()
    _last_api_call_ts: Dict[str, float] = {}
    _min_call_interval: float = 0.65
    _rate_limit_cooldown_until: float = 0.0

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
            "NIFTY": [],
            "BANKNIFTY": [],
            "SENSEX": [],
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
                if self.is_authenticated and (now - self._last_login_attempt_ts < self._min_login_cooldown_sec):
                    return True

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
        Loads and caches index FUTIDX instruments (NIFTY, BANKNIFTY on NFO, SENSEX on BFO)
        from local cache scrips_index_futures.json or official Angel One OpenAPI Scrip Master.
        """
        cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrips_index_futures.json")
        today_date = datetime.date.today()

        if os.path.exists(cache_file):
            try:
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
                if mtime == today_date or True:  # Accept local cache if populated
                    with open(cache_file, "r", encoding="utf-8") as f:
                        cached_data = json.load(f)
                    has_data = False
                    for k in ["NIFTY", "BANKNIFTY", "SENSEX"]:
                        items = cached_data.get(k, [])
                        if items:
                            has_data = True
                        self.futures_master_cache[k] = self._parse_scrip_items(items)
                    if has_data:
                        self.last_cache_time = datetime.datetime.now()
                        logger.info(
                            f"Loaded Index futures from local cache: "
                            f"NIFTY ({len(self.futures_master_cache['NIFTY'])}), "
                            f"BANKNIFTY ({len(self.futures_master_cache['BANKNIFTY'])}), "
                            f"SENSEX ({len(self.futures_master_cache['SENSEX'])})"
                        )
                        return
            except Exception as e:
                logger.warning(f"Error reading local index scrips cache: {e}")

        try:
            logger.info("Downloading official Angel One OpenAPI Scrip Master for Index Futures...")
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                raw = json.loads(resp.read().decode("utf-8"))

                parsed_dict: Dict[str, List[Dict[str, Any]]] = {
                    "NIFTY": [],
                    "BANKNIFTY": [],
                    "SENSEX": [],
                }

                for item in raw:
                    exch = item.get("exch_seg")
                    itype = item.get("instrumenttype")
                    name = item.get("name")

                    if itype == "FUTIDX":
                        if exch == "NFO" and name in ("NIFTY", "BANKNIFTY"):
                            parsed_dict[name].append(item)
                        elif exch == "BFO" and name == "SENSEX":
                            parsed_dict[name].append(item)

                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(parsed_dict, f, indent=2)

                for k in ["NIFTY", "BANKNIFTY", "SENSEX"]:
                    self.futures_master_cache[k] = self._parse_scrip_items(parsed_dict.get(k, []))

                self.last_cache_time = datetime.datetime.now()
                logger.info(
                    f"Cached Index futures: "
                    f"NIFTY ({len(self.futures_master_cache['NIFTY'])}), "
                    f"BANKNIFTY ({len(self.futures_master_cache['BANKNIFTY'])}), "
                    f"SENSEX ({len(self.futures_master_cache['SENSEX'])})"
                )
                return
        except Exception as e:
            logger.error(f"Error downloading OpenAPIScripMaster for Index Futures: {e}")

    def _parse_scrip_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        parsed = []
        for i in items:
            sym = i.get("symbol", "")
            token = str(i.get("token", ""))
            name = i.get("name", "")
            exp_str = i.get("expiry", "")
            exch_seg = i.get("exch_seg", "NFO")
            if name == "SENSEX":
                exch_seg = "BFO"

            try:
                lotsize = int(i.get("lotsize", INDEX_CONFIG.get(name, {}).get("lot_size", 1)))
            except Exception:
                lotsize = INDEX_CONFIG.get(name, {}).get("lot_size", 1)

            try:
                raw_ts = float(i.get("tick_size", 5))
                tick_size = (raw_ts / 100.0) if raw_ts >= 1.0 else raw_ts
                if tick_size <= 0:
                    tick_size = 0.05
            except Exception:
                tick_size = 0.05

            exp_date = None
            for fmt in ("%d%b%Y", "%d%b%y"):
                try:
                    exp_date = datetime.datetime.strptime(exp_str, fmt).date()
                    break
                except Exception:
                    pass

            if not exp_date:
                continue

            parsed.append({
                "symbol": sym,
                "token": token,
                "name": name,
                "expiry_str": exp_str,
                "expiry_date": exp_date,
                "lotsize": lotsize,
                "tick_size": tick_size,
                "exch_seg": exch_seg,
                "instrumenttype": "FUTIDX",
            })
        return parsed

    def resolve_future_contract(self, instrument: str) -> Optional[Dict[str, Any]]:
        """
        Finds the nearest active monthly expiry FUTIDX contract for the index.
        Sorts candidate contracts ascending by expiry date and selects nearest where expiry_date >= today.
        Re-resolves periodically if cached contract expires or TTL elapses.
        Never caches simulated fallback contracts.
        """
        inst_key = normalize_index_name(instrument)
        today = datetime.date.today()
        now_ts = time.monotonic()

        cached_contract = self.latest_contract_by_comm.get(inst_key)
        cached_ts = self._contract_resolved_at.get(inst_key, 0.0)

        # Check if valid real contract is cached, not expired, and within TTL
        if cached_contract and (now_ts - cached_ts < self._contract_cache_ttl_sec):
            exp = cached_contract.get("expiry_date")
            if exp and exp >= today:
                return cached_contract

        cache = self.futures_master_cache.get(inst_key, [])
        if not cache:
            self._load_instruments()
            cache = self.futures_master_cache.get(inst_key, [])

        if not cache:
            # Fallback simulated contract if master is offline (DO NOT CACHE)
            cfg = INDEX_CONFIG.get(inst_key, INDEX_CONFIG["NIFTY"])
            prefix = cfg.get("symbol_prefix", inst_key)
            target_year = today.year + 1
            simulated_expiry = datetime.date(target_year, 12, 31)
            return {
                "symbol": f"{prefix}{simulated_expiry.strftime('%d%b%y').upper()}FUT",
                "token": "999999",
                "name": inst_key,
                "expiry_str": simulated_expiry.strftime("%d%b%Y").upper(),
                "expiry_date": simulated_expiry,
                "lotsize": cfg.get("lot_size", 1),
                "tick_size": cfg.get("tick_size", 0.05),
                "exch_seg": cfg.get("exchange", "NFO"),
                "instrumenttype": "FUTIDX",
                "_simulated": True,
            }

        valid_contracts = [c for c in cache if c["expiry_date"] >= today]
        if not valid_contracts:
            valid_contracts = sorted(cache, key=lambda x: abs((x["expiry_date"] - today).days))
        else:
            valid_contracts.sort(key=lambda x: x["expiry_date"])

        chosen = valid_contracts[0]
        self.latest_contract_by_comm[inst_key] = chosen
        self._contract_resolved_at[inst_key] = now_ts
        logger.info(
            f"Resolved near-month Index Future for {inst_key}: {chosen['symbol']} "
            f"(Token: {chosen['token']}, Expiry: {chosen['expiry_str']}, LotSize: {chosen['lotsize']})"
        )
        return chosen

    def get_market_data(self, mode: str, exchange_tokens: Dict[str, List[str]]) -> Optional[Dict[str, Any]]:
        """Invokes SmartAPI getMarketData API endpoint safely with pacer and cooldown check."""
        if not self.is_authenticated or not self.smart_api:
            self.error_message = "Angel One session not authenticated"
            return None

        if AngelOneFeed.is_in_cooldown():
            return None

        try:
            self._pace_api_call()
            res = self.smart_api.getMarketData(mode=mode, exchangeTokens=exchange_tokens)
            if res and is_smartapi_success(res):
                self._clear_feed_error()
                return res.get("data", {})
            else:
                msg, errcode = extract_smartapi_error(res)
                err_str = f"{msg} ({errcode})" if errcode else (msg or "Feed fetch failure")
                if "AG8003" in err_str or "token" in err_str.lower():
                    AngelOneFeed.set_rate_limit_cooldown(3.0)
                self._record_feed_error(err_str)
                return None
        except Exception as e:
            err_str = str(e)
            if "exceeding access rate" in err_str or "Access denied" in err_str:
                AngelOneFeed.set_rate_limit_cooldown(4.0)
            self._record_feed_error(err_str)
            logger.error(f"Error fetching market data from Angel One: {e}")
            return None

    def fetch_market_data_batch(
        self,
        exchange_tokens: Dict[str, List[str]],
        max_cache_age: float = 1.8,
    ) -> Dict[str, float]:
        """
        Fetches live LTP for Index futures across exchanges (NFO, BFO) in a single unified API call.
        """
        results: Dict[str, float] = {}
        tokens_needed: Dict[str, List[str]] = {}

        now = time.monotonic()
        for exch, tokens in exchange_tokens.items():
            for t in tokens:
                t_str = str(t)
                cached = self._future_cache.get(t_str)
                if cached and (now - cached.get("time", 0.0)) < max_cache_age:
                    results[t_str] = cached["ltp"]
                    cached_inst = cached.get("commodity") or cached.get("instrument")
                    if cached_inst and is_plausible_ltp(cached_inst, cached["ltp"]):
                        self.latest_ltp_by_comm[cached_inst] = cached["ltp"]
                    logger.debug(f"[LTP_TRACE] instrument={cached_inst or 'UNKNOWN'} source=batch_cache_hit token={t_str} value={cached['ltp']}")
                else:
                    tokens_needed.setdefault(exch, []).append(t_str)

        if not tokens_needed:
            return results

        if AngelOneFeed.is_in_cooldown():
            for exch, tokens in tokens_needed.items():
                for t in tokens:
                    c = self._future_cache.get(t)
                    if c:
                        results[t] = c["ltp"]
            return results

        # Invert token lookup to identify instrument
        token_to_inst: Dict[str, str] = {}
        for inst_key, contracts in self.futures_master_cache.items():
            for c in contracts:
                tok_str = str(c.get("token"))
                token_to_inst[tok_str] = inst_key
        # Check active contract tokens
        for inst_key, contract in self.latest_contract_by_comm.items():
            if contract and contract.get("token"):
                token_to_inst[str(contract.get("token"))] = inst_key

        try:
            data = self.get_market_data("FULL", tokens_needed)
            if data and "fetched" in data:
                fetched_list = data.get("fetched", [])
                for item in fetched_list:
                    tok = str(item.get("symbolToken") or item.get("tradingSymbol", ""))
                    sym = str(item.get("tradingSymbol", ""))
                    ltp = item.get("ltp")
                    if ltp is not None:
                        try:
                            flt_ltp = float(ltp)
                            inst_name = token_to_inst.get(tok)
                            if not inst_name and sym:
                                for candidate in ["BANKNIFTY", "NIFTY", "SENSEX"]:
                                    if sym.startswith(candidate):
                                        inst_name = candidate
                                        break

                            if inst_name and not is_plausible_ltp(inst_name, flt_ltp):
                                logger.error(
                                    f"[LTP_TRACE] [SANITY_GUARD_REJECT] batch_fetch rejected invalid LTP={flt_ltp} "
                                    f"for instrument={inst_name} token={tok}"
                                )
                                continue

                            results[tok] = flt_ltp
                            self._future_cache[tok] = {
                                "ltp": flt_ltp,
                                "time": now,
                                "commodity": inst_name,
                                "instrument": inst_name,
                            }
                            if inst_name:
                                self.latest_ltp_by_comm[inst_name] = flt_ltp
                                logger.debug(f"[LTP_TRACE] instrument={inst_name} source=batch_api_fetched token={tok} value={flt_ltp}")
                        except Exception:
                            pass
                self.last_tick_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
        except Exception as e:
            logger.error(f"Batch market data fetch exception: {e}")

        # Return cached value for any tokens that failed to fetch
        for exch, tokens in tokens_needed.items():
            for t in tokens:
                if t not in results:
                    c = self._future_cache.get(t)
                    if c:
                        results[t] = c["ltp"]
        return results

    def fetch_future_ltp(self, instrument: str) -> Optional[float]:
        """Fetches the latest LTP for the nearest active Index future contract."""
        inst_key = normalize_index_name(instrument)
        contract = self.resolve_future_contract(inst_key)
        if not contract:
            return None

        token = str(contract.get("token"))
        exch_seg = contract.get("exch_seg") or INDEX_CONFIG.get(inst_key, {}).get("exchange", "NFO")
        batch_resp = self.fetch_market_data_batch({exch_seg: [token]})
        val = batch_resp.get(token)

        if val is not None and not is_plausible_ltp(inst_key, val):
            logger.error(f"[LTP_TRACE] [SANITY_GUARD_REJECT] fetch_future_ltp rejected LTP={val} for instrument={inst_key}")
            return None

        if val is not None:
            self.latest_ltp_by_comm[inst_key] = val
        return val

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
        instrument: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatches an order to Angel One or logs simulated paper execution."""
        from order_safety_service import validate_order_safety

        inst_val = instrument or commodity or "NIFTY"
        inst_key = normalize_index_name(inst_val)
        cfg = INDEX_CONFIG.get(inst_key, INDEX_CONFIG["NIFTY"])
        exch = cfg.get("exchange", "NFO")
        ts = cfg.get("tick_size", 0.05)

        # Enforce tick rounding
        rounded_price = round(round(price / ts) * ts, 4)

        order_params = {
            "symbol": symbol,
            "token": token,
            "quantity": quantity,
            "price": rounded_price,
            "transaction_type": transaction_type,
            "order_type": order_type,
            "product_type": product_type,
            "exchange": exch,
            "trading_mode": trading_mode,
            "commodity": inst_key,
            "instrument": inst_key,
        }

        # Cross-client ownership security guard
        validate_order_safety(
            client_id=client_id,
            run_id=run_id,
            run_owner_client_id=run_owner_client_id,
            credential_owner_client_id=self.owner_client_id or client_id,
            order_params=order_params,
        )

        mode = trading_mode.strip().lower()
        if mode == "paper":
            now_iso = datetime.datetime.now(IST).isoformat()
            logger.info(
                f"[PAPER ORDER] Client={client_id} Run={run_id} Inst={inst_key} "
                f"{transaction_type} {quantity} @ {rounded_price:.2f} ({symbol})"
            )
            return {
                "status": "success",
                "mode": "paper",
                "order_id": f"sim_{int(time.time()*1000)}",
                "symbol": symbol,
                "token": token,
                "price": rounded_price,
                "quantity": quantity,
                "transaction_type": transaction_type,
                "timestamp": now_iso,
            }

        # LIVE ORDER EXECUTION
        if not self.is_authenticated or not self.smart_api:
            return {"status": "error", "message": "Angel One session not authenticated for live orders"}

        try:
            self._pace_api_call()
            payload = {
                "variety": "NORMAL",
                "tradingsymbol": symbol,
                "symboltoken": str(token),
                "transactiontype": transaction_type.upper(),
                "exchange": exch,
                "ordertype": order_type.upper(),
                "producttype": product_type.upper(),
                "duration": "DAY",
                "price": str(rounded_price),
                "quantity": str(quantity),
            }
            res = self.smart_api.placeOrder(payload)
            if res and is_smartapi_success(res):
                order_id = res.get("data", {}).get("orderid", "")
                logger.info(f"[LIVE ORDER PLACED] Client={client_id} Run={run_id} OrderId={order_id}")
                return {
                    "status": "success",
                    "mode": "live",
                    "order_id": order_id,
                    "symbol": symbol,
                    "token": token,
                    "price": rounded_price,
                    "quantity": quantity,
                    "transaction_type": transaction_type,
                    "raw_response": res,
                }
            else:
                msg, code = extract_smartapi_error(res)
                logger.error(f"[LIVE ORDER REJECTED] {msg} ({code})")
                return {"status": "error", "message": msg, "code": code, "raw_response": res}
        except Exception as e:
            logger.error(f"Live order placement exception: {e}")
            return {"status": "error", "message": str(e)}

    def get_status(self) -> Dict[str, Any]:
        persisted_sec = (time.time() - self.feed_error_first_ts) if (self.feed_error and self.feed_error_first_ts) else 0.0
        return {
            "authenticated": self.is_authenticated,
            "streaming": self.is_streaming,
            "client_id": self.client_id,
            "owner_client_id": self.owner_client_id,
            "ltp_by_commodity": dict(self.latest_ltp_by_comm),
            "latest_ltp_by_comm": dict(self.latest_ltp_by_comm),
            "contracts_by_commodity": {k: v.get("symbol") for k, v in self.latest_contract_by_comm.items()},
            "last_tick_time": self.last_tick_time,
            "error": self.error_message,
            "feed_error": self.feed_error,
            "feed_error_persisted_sec": round(persisted_sec, 1),
            "feed_error_escalated": persisted_sec >= 60.0,
            "feed_error_count": self.feed_error_count,
        }


# Global default feed
angel_one_feed = AngelOneFeed()
