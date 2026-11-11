"""
Multi-Tenant Scoped MongoDB Service for COMMODITY Database.
Routes each user's complete data (trades, run states, broker credentials, astro signals, audit logs)
into dedicated per-user collections in the COMMODITY database (e.g. COMMODITY.admin, COMMODITY.user1, COMMODITY.user2, etc.).
All read/write operations strictly assert a resolved client_id.
"""

import datetime
import logging
import os
import threading
import time
from typing import Dict, Any, Optional, List
from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError
from bson import ObjectId

logger = logging.getLogger("MongoService")

DEFAULT_MONGO_URI = "mongodb+srv://crestviewcorporate_db_user:Crestviewcorporate@cluster0.zfk4ahy.mongodb.net/?appName=Cluster0&readPreference=primaryPreferred"
MONGO_URI = os.getenv("MONGO_URI", DEFAULT_MONGO_URI)
DEFAULT_DB_NAME = os.getenv("MONGO_DB_NAME", "COMMODITY")
COMMODITY_COLLECTIONS = ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def normalize_commodity_name(commodity: Optional[str]) -> str:
    """Normalizes commodity symbol to standard key (SILVERM, GOLDM, COPPER, CRUDEOIL)."""
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


class MongoService:
    """
    Multi-Tenant Scoped MongoDB Service for COMMODITY Database.
    Routes user data into dedicated collections named per client_id (e.g., admin, user1, user2, user3, user4).
    """

    def __init__(
        self,
        uri: Optional[str] = MONGO_URI,
        db_name: str = DEFAULT_DB_NAME,
    ):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None
        self.is_connected = False
        self._circuit_breaker_until: float = 0.0
        self._lock = threading.Lock()
        self._in_memory_trades: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self._in_memory_astro_files: Dict[str, List[Dict[str, Any]]] = {}
        self._active_file_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._trade_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._trade_cache_time: Dict[str, float] = {}
        self._pnl_summary_cache: Dict[str, Dict[str, Any]] = {}
        self._pnl_summary_cache_time: Dict[str, float] = {}
        self._cache_ttl: float = 3.0
        self._connect()

    def _trip_circuit_breaker(self, cooldown_sec: float = 10.0) -> None:
        """Trips the circuit breaker to prevent repeated blocking timeouts."""
        self.is_connected = False
        self._circuit_breaker_until = time.monotonic() + cooldown_sec

    def _connect(self) -> bool:
        with self._lock:
            now = time.monotonic()
            if now < self._circuit_breaker_until:
                return False
            if not self.uri:
                logger.info("No 'MONGO_URI' provided in environment. Operating in resilient offline fallback mode.")
                self.is_connected = False
                return False

            candidate_uris = [
                "mongodb://crestviewcorporate_db_user:Crestviewcorporate@ac-kjmgfzp-shard-00-00.zfk4ahy.mongodb.net:27017,ac-kjmgfzp-shard-00-01.zfk4ahy.mongodb.net:27017/COMMODITY?ssl=true&authSource=admin",
                self.uri,
                "mongodb://crestviewcorporate_db_user:Crestviewcorporate@ac-kjmgfzp-shard-00-00.zfk4ahy.mongodb.net:27017/COMMODITY?ssl=true&authSource=admin&directConnection=true",
                "mongodb://crestviewcorporate_db_user:Crestviewcorporate@ac-kjmgfzp-shard-00-01.zfk4ahy.mongodb.net:27017/COMMODITY?ssl=true&authSource=admin&directConnection=true",
            ]

            for u in candidate_uris:
                if not u:
                    continue
                try:
                    self.client = MongoClient(
                        u,
                        serverSelectionTimeoutMS=1500,
                        connectTimeoutMS=1500,
                        socketTimeoutMS=2000,
                        readPreference="primaryPreferred",
                    )
                    try:
                        self.client.admin.command('ping', read_preference=ReadPreference.PRIMARY_PREFERRED)
                    except Exception as ping_err:
                        logger.warning(f"Admin ping warning: {ping_err}, proceeding with connection.")
                    self.is_connected = True
                    self._circuit_breaker_until = 0.0
                    logger.info(f"MongoDB connected successfully to cluster. Database: {self.db_name}")
                    return True
                except Exception as e:
                    self.is_connected = False
                    logger.debug(f"MongoDB candidate connection note: {e}")

            self._trip_circuit_breaker(10.0)
            logger.warning("All MongoDB connection candidates failed. Running in offline fallback mode for 10s.")
            return False

    def ensure_connection(self) -> bool:
        if self.is_connected and self.client is not None:
            return True
        now = time.monotonic()
        if now < self._circuit_breaker_until:
            return False
        return self._connect()

    def _require_client_id(self, client_id: Optional[str]) -> str:
        """Strict guard assertion ensuring client_id is resolved and non-empty."""
        if not client_id or not isinstance(client_id, str) or not client_id.strip():
            raise ValueError(
                "CRITICAL SECURITY GUARD: Operation rejected. A valid, non-empty 'client_id' "
                "is strictly required for per-client database operations."
            )
        return client_id.strip()

    def get_database(self):
        """Returns the primary COMMODITY MongoDB database object."""
        if not self.ensure_connection() or self.client is None:
            return None
        return self.client.get_database(self.db_name, read_preference=ReadPreference.PRIMARY_PREFERRED)

    def get_user_collection(self, client_id: str):
        """Returns MongoDB collection object for a specific client (e.g. COMMODITY.admin, COMMODITY.user1)."""
        db = self.get_database()
        if db is None:
            return None
        cid = self._require_client_id(client_id)
        return db.get_collection(cid, read_preference=ReadPreference.PRIMARY_PREFERRED)

    # -------------------------------------------------------------------
    # Trade Persistence (Stored in user's dedicated collection)
    # -------------------------------------------------------------------
    def insert_trade(
        self,
        trade_data: Dict[str, Any],
        client_id: str,
        run_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Inserts a closed trade into the user's dedicated collection in COMMODITY database
        (e.g., COMMODITY['admin'], COMMODITY['user1'], COMMODITY['user2']).
        """
        cid = self._require_client_id(client_id)
        cfg = run_config or {}
        raw_comm = cfg.get("commodity") or cfg.get("instrument_name") or trade_data.get("commodity") or trade_data.get("symbol") or "SILVERM"
        col_name = normalize_commodity_name(raw_comm)

        now_ist = datetime.datetime.now(IST)
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        trade_side = cfg.get("trade_side") or trade_data.get("trade_side") or ("BUY" if trade_data.get("action") == "BUY" else "SELL")
        mode = (
            trade_data.get("mode")
            or (cfg.get("trading_mode") if cfg else None)
            or "paper"
        ).strip().lower()
        if mode not in ("paper", "live"):
            mode = "paper"

        doc = {
            "type": "trade",
            "client_id": cid,
            "commodity": col_name,
            "trade_id": trade_data.get("trade_id"),
            "run_id": trade_data.get("run_id"),
            "run_number": trade_data.get("run_number") or cfg.get("run_number", 1),
            "symbol": col_name,
            "instrument": col_name,
            "trade_side": trade_side,
            "contract_symbol": trade_data.get("contract_symbol") or cfg.get("contract_symbol"),
            "contract_token": trade_data.get("contract_token") or cfg.get("contract_token"),
            "expiry": trade_data.get("expiry") or cfg.get("expiry"),
            "label": trade_data.get("label"),
            "level_price": trade_data.get("level_price"),
            "entry_price": trade_data.get("fill_price") or trade_data.get("entry_price"),
            "fill_price": trade_data.get("fill_price"),
            "exit_price": trade_data.get("exit_price"),
            "quantity": trade_data.get("quantity"),
            "pnl_points": trade_data.get("pnl_points"),
            "pnl_rupees": trade_data.get("pnl_rupees"),
            "exit_reason": trade_data.get("exit_reason"),
            "entry_time": trade_data.get("entry_time") or trade_data.get("filled_at"),
            "exit_time": trade_data.get("exit_time") or trade_data.get("exited_at"),
            "mode": mode,
            "created_at": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_utc": now_utc.isoformat(),
            "status": "CLOSED",
        }

        inserted_id = None
        try:
            col = self.get_user_collection(cid)
            if col is not None:
                res = col.insert_one(doc)
                inserted_id = str(res.inserted_id)
                logger.info(f"Trade {trade_data.get('trade_id')} [{mode.upper()}] saved to MongoDB [{self.db_name}.{cid}] [ID: {inserted_id}]")

            # Always mirror to admin if inserted for a sub-user, or mirror to user collections if inserted for admin
            db = self.get_database()
            if db is not None:
                mirror_targets = ["admin", "User 1", "user1"] if cid == "admin" else ["admin"]
                for target_cid in mirror_targets:
                    if target_cid != cid:
                        try:
                            mirror_doc = dict(doc)
                            mirror_doc.pop("_id", None)
                            mirror_doc["client_id"] = target_cid
                            db[target_cid].update_one(
                                {"type": "trade", "trade_id": trade_data.get("trade_id")},
                                {"$set": mirror_doc},
                                upsert=True,
                            )
                        except Exception as me:
                            logger.debug(f"Mirror trade note for '{target_cid}': {me}")
        except Exception as e:
            logger.error(f"Failed to insert trade into collection '{cid}': {e}")

        # In-memory backup
        if not inserted_id:
            import uuid
            inserted_id = f"mem_trd_{uuid.uuid4().hex[:8]}"
            doc["_id"] = inserted_id

        client_trades = self._in_memory_trades.setdefault(cid, {})
        comm_trades = client_trades.setdefault(col_name, [])
        comm_trades.insert(0, dict(doc))
        self._trade_cache.clear()
        self._trade_cache_time.clear()
        return inserted_id

    def insert_trade_async(
        self,
        trade_data: Dict[str, Any],
        client_id: str,
        run_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Non-blocking background insert scoped to client_id."""
        self._require_client_id(client_id)
        threading.Thread(target=self.insert_trade, args=(trade_data, client_id, run_config), daemon=True).start()

    def get_recent_trades(
        self,
        client_id: str,
        limit: int = 200,
        mode: Optional[str] = None,
        commodity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetches recent trades from client's dedicated collection in COMMODITY database.
        Supports filtering by commodity (SILVERM, GOLDM, COPPER) and mode (paper/live).
        Seamlessly merges master trades from admin collection so traders always see complete history.
        Uses a short 3.0s cache to avoid blocking the asyncio event loop on rapid polling.
        """
        import time as _t
        cid = self._require_client_id(client_id)
        cache_key = f"{cid}:{limit}:{mode}:{commodity}"
        now_ts = _t.time()
        if cache_key in self._trade_cache and (now_ts - self._trade_cache_time.get(cache_key, 0)) < self._cache_ttl:
            return list(self._trade_cache[cache_key])

        all_trades: List[Dict[str, Any]] = []
        seen_tids = set()

        query_parts: List[Dict[str, Any]] = [{"type": "trade"}]
        if commodity:
            norm_comm = normalize_commodity_name(commodity)
            query_parts.append({"$or": [{"commodity": norm_comm}, {"symbol": norm_comm}, {"instrument": norm_comm}]})
        if mode and mode.strip().lower() in ("paper", "live"):
            query_parts.append({"mode": mode.strip().lower()})

        query = {"$and": query_parts} if len(query_parts) > 1 else query_parts[0]

        if self.ensure_connection():
            try:
                # 1. Primary client collection
                col = self.get_user_collection(cid)
                if col is not None:
                    cursor = col.find(query).sort("_id", -1).limit(limit)
                    for doc in cursor:
                        doc["_id"] = str(doc["_id"])
                        tid = doc.get("trade_id") or doc["_id"]
                        if tid not in seen_tids:
                            seen_tids.add(tid)
                            all_trades.append(doc)

                # 2. Master fallback: also pull from 'admin' and aliases ('User 1', 'user1') if needed
                fallback_cids = ["admin"]
                if cid.lower().replace(" ", "") in ("user1", "user01"):
                    fallback_cids.extend(["admin", "User 1", "user1"])
                for f_cid in fallback_cids:
                    if f_cid != cid:
                        f_col = self.get_user_collection(f_cid)
                        if f_col is not None:
                            f_cursor = f_col.find(query).sort("_id", -1).limit(limit)
                            for doc in f_cursor:
                                doc["_id"] = str(doc["_id"])
                                tid = doc.get("trade_id") or doc["_id"]
                                if tid not in seen_tids:
                                    seen_tids.add(tid)
                                    all_trades.append(doc)
            except Exception as e:
                logger.error(f"Failed to fetch trades from MongoDB collection '{cid}': {e}")
                self._trip_circuit_breaker(10.0)

        # Merge in-memory fallback
        client_mem = self._in_memory_trades.get(cid, {})
        target_comms = [normalize_commodity_name(commodity)] if commodity else COMMODITY_COLLECTIONS
        for c in target_comms:
            mem_list = client_mem.get(c, [])
            for t in mem_list:
                if mode and t.get("mode") != mode.strip().lower():
                    continue
                tid = t.get("trade_id") or str(t.get("_id"))
                if tid not in seen_tids:
                    seen_tids.add(tid)
                    all_trades.append(dict(t))

        all_trades.sort(
            key=lambda x: str(x.get("exit_time") or x.get("exited_at") or x.get("entry_time") or x.get("created_at") or ""),
            reverse=True,
        )
        res = all_trades[:limit]
        self._trade_cache[cache_key] = res
        self._trade_cache_time[cache_key] = now_ts
        return res

    def clear_trades(self, client_id: str, commodity: Optional[str] = None) -> bool:
        """Clears trade history for client_id from their dedicated collection."""
        cid = self._require_client_id(client_id)
        success = True

        query: Dict[str, Any] = {"type": "trade"}
        if commodity:
            norm_comm = normalize_commodity_name(commodity)
            query = {"type": "trade", "$or": [{"commodity": norm_comm}, {"symbol": norm_comm}]}

        if self.ensure_connection():
            try:
                col = self.get_user_collection(cid)
                if col is not None:
                    col.delete_many(query)
            except Exception as e:
                logger.error(f"Failed to clear trades for client '{cid}': {e}")
                success = False

        client_mem = self._in_memory_trades.get(cid, {})
        if commodity:
            client_mem.pop(normalize_commodity_name(commodity), None)
        else:
            self._in_memory_trades.pop(cid, None)

        self._trade_cache.clear()
        self._trade_cache_time.clear()
        return success

    def delete_trade(self, client_id: str, trade_id: str, commodity: Optional[str] = None) -> bool:
        """Deletes a single trade by trade_id from client's dedicated collection."""
        cid = self._require_client_id(client_id)
        if not trade_id:
            return False

        deleted = False
        if self.ensure_connection():
            try:
                col = self.get_user_collection(cid)
                if col is not None:
                    or_clauses: List[Dict[str, Any]] = [
                        {"trade_id": str(trade_id)},
                        {"_id": str(trade_id)},
                    ]
                    try:
                        or_clauses.append({"_id": ObjectId(str(trade_id))})
                    except Exception:
                        pass

                    query = {"$and": [{"type": "trade"}, {"$or": or_clauses}]}
                    res = col.delete_one(query)
                    if res.deleted_count > 0:
                        deleted = True
                        logger.info(f"Deleted trade '{trade_id}' from collection '{cid}'.")
            except Exception as e:
                logger.error(f"Failed to delete trade '{trade_id}' for client '{cid}': {e}")

        # In-memory delete
        client_mem = self._in_memory_trades.get(cid, {})
        for comm_key, mem_list in client_mem.items():
            idx = next((i for i, t in enumerate(mem_list) if t.get("trade_id") == trade_id or str(t.get("_id")) == trade_id), None)
            if idx is not None:
                mem_list.pop(idx)
                deleted = True

        self._trade_cache.clear()
        self._trade_cache_time.clear()
        return deleted

    def get_pnl_summary(
        self,
        client_id: str,
        period: str = "day",
        ref_date: Optional[Any] = None,
        mode: Optional[str] = None,
        in_memory_trades: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Computes P&L summary grouped by instrument (SILVERM, GOLDM, COPPER, CRUDEOIL)
        for a specified IST time period ('day', 'week', 'month', 'custom').
        - day: given/current IST date
        - week: Mon-Sun of that date's week
        - month: calendar month of that date
        - custom: the given IST date (or ref_date)
        Calculates gross_profit, gross_loss, net_pnl, trade_count, win_count, loss_count per instrument,
        plus an 'ALL' total row.
        Respects paper/live mode filter and falls back to in-memory trades gracefully.
        """
        import calendar
        import re

        cid = self._require_client_id(client_id)
        cache_key = f"{cid}:{period}:{ref_date}:{mode}"
        now_ts = time.time()
        if cache_key in self._pnl_summary_cache and (now_ts - self._pnl_summary_cache_time.get(cache_key, 0)) < 3.0:
            return dict(self._pnl_summary_cache[cache_key])

        now_ist = datetime.datetime.now(IST)

        # 1. Resolve reference date in IST
        if ref_date is None or ref_date == "":
            ref_dt = now_ist.date()
        elif isinstance(ref_date, datetime.datetime):
            if ref_date.tzinfo is not None:
                ref_dt = ref_date.astimezone(IST).date()
            else:
                ref_dt = ref_date.date()
        elif isinstance(ref_date, datetime.date):
            ref_dt = ref_date
        elif isinstance(ref_date, str):
            ref_str = ref_date.strip()
            if "T" in ref_str:
                ref_str = ref_str.split("T")[0]
            elif " " in ref_str:
                ref_str = ref_str.split(" ")[0]
            try:
                ref_dt = datetime.date.fromisoformat(ref_str)
            except Exception:
                ref_dt = now_ist.date()
        else:
            ref_dt = now_ist.date()

        # 2. Compute start_date and end_date in IST
        p = str(period or "day").strip().lower()
        if p in ("day", "today"):
            p = "day"
            start_date = ref_dt
            end_date = ref_dt
        elif p in ("week", "this_week"):
            p = "week"
            start_date = ref_dt - datetime.timedelta(days=ref_dt.weekday())
            end_date = start_date + datetime.timedelta(days=6)
        elif p in ("month", "this_month"):
            p = "month"
            start_date = ref_dt.replace(day=1)
            _, last_day = calendar.monthrange(ref_dt.year, ref_dt.month)
            end_date = ref_dt.replace(day=last_day)
        elif p in ("all", "all_time", "total", "overall"):
            p = "all"
            start_date = None
            end_date = None
        elif p == "custom":
            start_date = ref_dt
            end_date = ref_dt
        else:
            p = "month"
            start_date = ref_dt.replace(day=1)
            _, last_day = calendar.monthrange(ref_dt.year, ref_dt.month)
            end_date = ref_dt.replace(day=last_day)

        # 3. Query client collection with same scoping and fallback as get_recent_trades
        all_trades: List[Dict[str, Any]] = []
        seen_tids = set()

        query_parts: List[Dict[str, Any]] = [{"type": "trade"}]
        if mode and mode.strip().lower() in ("paper", "live"):
            query_parts.append({"mode": mode.strip().lower()})

        query = {"$and": query_parts} if len(query_parts) > 1 else query_parts[0]

        if self.ensure_connection():
            try:
                # Primary client collection
                col = self.get_user_collection(cid)
                if col is not None:
                    cursor = col.find(query).sort("_id", -1)
                    for doc in cursor:
                        doc["_id"] = str(doc["_id"])
                        tid = doc.get("trade_id") or doc["_id"]
                        if tid not in seen_tids:
                            seen_tids.add(tid)
                            all_trades.append(doc)

                # Master fallback: also pull from 'admin' and aliases ('User 1', 'user1') if needed
                fallback_cids = ["admin"]
                if cid.lower().replace(" ", "") in ("user1", "user01"):
                    fallback_cids.extend(["admin", "User 1", "user1"])
                for f_cid in fallback_cids:
                    if f_cid != cid:
                        f_col = self.get_user_collection(f_cid)
                        if f_col is not None:
                            f_cursor = f_col.find(query).sort("_id", -1)
                            for doc in f_cursor:
                                doc["_id"] = str(doc["_id"])
                                tid = doc.get("trade_id") or doc["_id"]
                                if tid not in seen_tids:
                                    seen_tids.add(tid)
                                    all_trades.append(doc)
            except Exception as e:
                logger.error(f"Failed to fetch trades for P&L summary from collection '{cid}': {e}")
                self._trip_circuit_breaker(10.0)

        # Merge explicit in-memory trades (e.g. from active RunManager master history)
        if in_memory_trades:
            for t in in_memory_trades:
                if mode and str(t.get("mode") or "paper").strip().lower() != mode.strip().lower():
                    continue
                tid = t.get("trade_id") or str(t.get("_id"))
                if tid not in seen_tids:
                    seen_tids.add(tid)
                    all_trades.append(dict(t))

        # Merge in-memory fallback
        client_mem = self._in_memory_trades.get(cid, {})
        for c in COMMODITY_COLLECTIONS:
            mem_list = client_mem.get(c, [])
            for t in mem_list:
                if mode and t.get("mode") != mode.strip().lower():
                    continue
                tid = t.get("trade_id") or str(t.get("_id"))
                if tid not in seen_tids:
                    seen_tids.add(tid)
                    all_trades.append(dict(t))

        # 4. Extract trade date and filter by range
        def _get_trade_date(t: Dict[str, Any]) -> Optional[datetime.date]:
            # Priority: exit_time (fallback to entry_time / created_at / timestamp)
            for k in ("exit_time", "exited_at", "entry_time", "filled_at", "created_at", "timestamp_utc", "timestamp", "time", "date"):
                val = t.get(k)
                if not val:
                    continue
                if isinstance(val, (int, float)):
                    try:
                        sec = val / 1000.0 if val > 1e11 else float(val)
                        return datetime.datetime.fromtimestamp(sec, tz=IST).date()
                    except Exception:
                        pass
                if isinstance(val, datetime.datetime):
                    if val.tzinfo is not None:
                        return val.astimezone(IST).date()
                    return val.date()
                if isinstance(val, datetime.date):
                    return val
                if isinstance(val, str):
                    val_clean = val.strip()
                    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", val_clean)
                    if m:
                        try:
                            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                        except ValueError:
                            pass
                    try:
                        dt = datetime.datetime.fromisoformat(val_clean.replace("Z", "+00:00"))
                        if dt.tzinfo is not None:
                            return dt.astimezone(IST).date()
                        return dt.date()
                    except Exception:
                        pass
            return None

        # Filter trades within the IST date range
        filtered_trades = []
        for t in all_trades:
            t_dt = _get_trade_date(t)
            if start_date is None or end_date is None:
                filtered_trades.append(t)
            elif t_dt and start_date <= t_dt <= end_date:
                filtered_trades.append(t)

        # 5. Group by commodity (SILVERM, GOLDM, COPPER, CRUDEOIL)
        stats: Dict[str, Dict[str, Any]] = {
            comm: {
                "commodity": comm,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "net_pnl": 0.0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
            }
            for comm in COMMODITY_COLLECTIONS
        }

        for t in filtered_trades:
            raw_comm = t.get("commodity") or t.get("symbol") or t.get("instrument") or ""
            norm_comm = normalize_commodity_name(raw_comm)
            if norm_comm not in stats:
                stats[norm_comm] = {
                    "commodity": norm_comm,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                    "net_pnl": 0.0,
                    "trade_count": 0,
                    "win_count": 0,
                    "loss_count": 0,
                }

            try:
                pnl = float(t.get("pnl_rupees") or 0.0)
            except (ValueError, TypeError):
                pnl = 0.0

            s = stats[norm_comm]
            s["trade_count"] += 1
            if pnl > 0:
                s["gross_profit"] += pnl
                s["win_count"] += 1
            elif pnl < 0:
                s["gross_loss"] += pnl
                s["loss_count"] += 1

        # 6. Aggregate by_instrument and ALL total row
        by_instrument: List[Dict[str, Any]] = []
        total_stats: Dict[str, Any] = {
            "commodity": "ALL",
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "net_pnl": 0.0,
            "trade_count": 0,
            "win_count": 0,
            "loss_count": 0,
        }

        # Maintain consistent order: SILVERM, GOLDM, COPPER, CRUDEOIL, then others
        ordered_comms = list(COMMODITY_COLLECTIONS)
        for c in stats.keys():
            if c not in ordered_comms:
                ordered_comms.append(c)

        for c in ordered_comms:
            s = stats[c]
            gp = round(s["gross_profit"], 2)
            gl = round(s["gross_loss"], 2)
            np = round(gp + gl, 2)
            item = {
                "commodity": c,
                "gross_profit": gp,
                "gross_loss": gl,
                "net_pnl": np,
                "trade_count": s["trade_count"],
                "win_count": s["win_count"],
                "loss_count": s["loss_count"],
            }
            by_instrument.append(item)

            total_stats["gross_profit"] += gp
            total_stats["gross_loss"] += gl
            total_stats["trade_count"] += s["trade_count"]
            total_stats["win_count"] += s["win_count"]
            total_stats["loss_count"] += s["loss_count"]

        total_stats["gross_profit"] = round(total_stats["gross_profit"], 2)
        total_stats["gross_loss"] = round(total_stats["gross_loss"], 2)
        total_stats["net_pnl"] = round(total_stats["gross_profit"] + total_stats["gross_loss"], 2)

        # 7. Aggregate by_slot (all 8 commodity slots)
        slot_order = [
            "SILVERM-B", "SILVERM-S",
            "GOLDM-B", "GOLDM-S",
            "COPPER-B", "COPPER-S",
            "CRUDEOIL-B", "CRUDEOIL-S"
        ]
        slot_stats: Dict[str, Dict[str, Any]] = {
            sid: {
                "slot_id": sid,
                "commodity": sid.split("-")[0],
                "side": "BUY" if sid.endswith("-B") else "SELL",
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "net_pnl": 0.0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
            }
            for sid in slot_order
        }

        for t in filtered_trades:
            raw_slot = str(t.get("run_id") or t.get("slot_id") or "").strip()
            canonical_slot = raw_slot if raw_slot in slot_stats else None
            if not canonical_slot:
                raw_comm = normalize_commodity_name(t.get("commodity") or "")
                raw_side = str(t.get("trade_side") or "BUY").strip().upper()
                canonical_slot = f"{raw_comm}-{'B' if raw_side == 'BUY' else 'S'}"
            if canonical_slot in slot_stats:
                ss = slot_stats[canonical_slot]
                try:
                    pnl = float(t.get("pnl_rupees") or 0.0)
                except (ValueError, TypeError):
                    pnl = 0.0
                ss["trade_count"] += 1
                if pnl > 0:
                    ss["gross_profit"] += pnl
                    ss["win_count"] += 1
                elif pnl < 0:
                    ss["gross_loss"] += pnl
                    ss["loss_count"] += 1

        by_slot: List[Dict[str, Any]] = []
        for sid in slot_order:
            ss = slot_stats[sid]
            gp = round(ss["gross_profit"], 2)
            gl = round(ss["gross_loss"], 2)
            by_slot.append({
                "slot_id": sid,
                "commodity": ss["commodity"],
                "side": ss["side"],
                "gross_profit": gp,
                "gross_loss": gl,
                "net_pnl": round(gp + gl, 2),
                "trade_count": ss["trade_count"],
                "win_count": ss["win_count"],
                "loss_count": ss["loss_count"],
            })

        res = {
            "period": p,
            "range": {
                "from": start_date.strftime("%Y-%m-%d") if start_date else "ALL",
                "to": end_date.strftime("%Y-%m-%d") if end_date else "ALL",
            },
            "by_instrument": by_instrument,
            "by_slot": by_slot,
            "total": total_stats,
        }
        self._pnl_summary_cache[cache_key] = dict(res)
        self._pnl_summary_cache_time[cache_key] = time.time()
        return res

    # -------------------------------------------------------------------
    # Run State Storage (Stored in user's dedicated collection)
    # -------------------------------------------------------------------
    def save_client_state(self, client_id: str, state_data: Dict[str, Any]) -> bool:
        """Persists multi-slot run state document in user's dedicated collection (COMMODITY[cid])."""
        cid = self._require_client_id(client_id)
        try:
            col = self.get_user_collection(cid)
            if col is None:
                return False
            now_ist = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
            state_doc = {
                "_id": "runs_state",
                "type": "runs_state",
                "client_id": cid,
                "updated_at": now_ist,
                "state": state_data,
            }
            col.replace_one({"$or": [{"_id": "runs_state"}, {"type": "runs_state"}]}, state_doc, upsert=True)
            return True
        except Exception as e:
            logger.error(f"Failed to save state in DB for client '{cid}': {e}")
            self._trip_circuit_breaker(60.0)
            return False

    def load_client_state(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Loads multi-slot run state document from user's dedicated collection."""
        cid = self._require_client_id(client_id)
        try:
            col = self.get_user_collection(cid)
            if col is None:
                return None
            doc = col.find_one({"$or": [{"_id": "runs_state"}, {"type": "runs_state"}]})
            if doc and "state" in doc:
                return doc["state"]
            return None
        except Exception as e:
            logger.error(f"Failed to load state from DB for client '{cid}': {e}")
            self._trip_circuit_breaker(60.0)
            return None

    # -------------------------------------------------------------------
    # Astro Report File Storage (Stored in dedicated COMMODITY.astro_files)
    # -------------------------------------------------------------------
    def get_astro_collection(self):
        """Returns dedicated MongoDB collection for Astro CSV reports: COMMODITY.astro_files."""
        db = self.get_database()
        if db is None:
            return None
        return db["astro_files"]

    def save_astro_file(
        self,
        client_id: str,
        filename: str,
        content: str,
        row_count: int,
        is_active: bool = True,
    ) -> str:
        """Stores an uploaded Astro CSV in dedicated COMMODITY.astro_files collection with active flag."""
        cid = self._require_client_id(client_id)
        now_ist = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

        doc = {
            "type": "astro_file",
            "client_id": cid,
            "filename": filename,
            "content": content,
            "row_count": row_count,
            "uploaded_at": now_ist,
            "active": is_active,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
        }

        file_id = None
        try:
            col = self.get_astro_collection()
            if col is not None:
                if is_active:
                    col.update_many({"client_id": cid}, {"$set": {"active": False}})
                res = col.insert_one(doc)
                file_id = str(res.inserted_id)
                doc["_id"] = file_id
                doc["file_id"] = file_id
        except Exception as e:
            logger.warning(f"Failed to insert astro file into MongoDB collection 'astro_files': {e}")

        if not file_id:
            import uuid
            file_id = f"astro-{uuid.uuid4().hex[:12]}"
            doc["_id"] = file_id
            doc["file_id"] = file_id

        user_files = self._in_memory_astro_files.setdefault(cid, [])
        if is_active:
            for f in user_files:
                f["active"] = False
            self._active_file_cache[cid] = dict(doc)
        user_files.insert(0, doc)
        return file_id

    def list_astro_files(self, client_id: str) -> List[Dict[str, Any]]:
        """Returns list of uploaded Astro reports for client from COMMODITY.astro_files, sorted newest first."""
        cid = self._require_client_id(client_id)
        try:
            col = self.get_astro_collection()
            if col is not None:
                cursor = col.find({"client_id": cid}, {"content": 0}).sort("uploaded_at", -1)
                results = []
                for doc in cursor:
                    doc["_id"] = str(doc["_id"])
                    doc["file_id"] = doc["_id"]
                    results.append(doc)
                return results
        except Exception as e:
            logger.warning(f"Note listing astro files from DB collection 'astro_files': {e}")

        mem_files = self._in_memory_astro_files.get(cid, [])
        res_mem = []
        for f in mem_files:
            item = {k: v for k, v in f.items() if k != "content"}
            item["file_id"] = str(item.get("_id", ""))
            res_mem.append(item)
        return res_mem

    def get_active_astro_file(self, client_id: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """Returns the active Astro file document from COMMODITY.astro_files."""
        cid = self._require_client_id(client_id)
        if not hasattr(self, "_active_file_cache"):
            self._active_file_cache = {}

        if use_cache and cid in self._active_file_cache:
            cached = self._active_file_cache[cid]
            return dict(cached) if cached is not None else None

        try:
            col = self.get_astro_collection()
            if col is not None:
                doc = col.find_one({"client_id": cid, "active": True})
                # Fallback check on user collection if migrating
                if not doc:
                    user_col = self.get_user_collection(cid)
                    if user_col is not None:
                        doc = user_col.find_one({"type": "astro_file", "active": True})
                if doc:
                    doc["_id"] = str(doc["_id"])
                    doc["file_id"] = doc["_id"]
                    self._active_file_cache[cid] = doc
                    return doc
                # Mongo connection succeeded and explicitly has NO active file
                self._active_file_cache[cid] = None
                return None
        except Exception as e:
            logger.warning(f"Note querying active astro file from DB: {e}")
            self._trip_circuit_breaker(60.0)

        # Fallback to in-memory store ONLY if DB query failed
        mem_files = self._in_memory_astro_files.get(cid, [])
        active_f = next((f for f in mem_files if f.get("active")), None)
        if active_f:
            active_f["file_id"] = str(active_f.get("_id", ""))
            self._active_file_cache[cid] = dict(active_f)
            return active_f

        self._active_file_cache[cid] = None
        return None

    def get_astro_file_by_id(self, client_id: str, file_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a specific Astro CSV document by ID from COMMODITY.astro_files."""
        cid = self._require_client_id(client_id)
        try:
            col = self.get_astro_collection()
            if col is not None:
                try:
                    obj_id = ObjectId(file_id)
                    doc = col.find_one({"_id": obj_id, "client_id": cid})
                except Exception:
                    doc = col.find_one({"_id": file_id, "client_id": cid})

                if doc:
                    doc["_id"] = str(doc["_id"])
                    doc["file_id"] = doc["_id"]
                    return doc
                return None
        except Exception as e:
            logger.warning(f"Note querying astro file by ID from DB: {e}")

        mem_files = self._in_memory_astro_files.get(cid, [])
        for f in mem_files:
            if str(f.get("_id")) == str(file_id):
                res = dict(f)
                res["file_id"] = str(res.get("_id", ""))
                return res
        return None

    def set_active_astro_file(self, client_id: str, file_id: str) -> bool:
        """Sets a specific Astro report as active in COMMODITY.astro_files."""
        cid = self._require_client_id(client_id)
        if not hasattr(self, "_active_file_cache"):
            self._active_file_cache = {}
        self._active_file_cache.pop(cid, None)

        matched = False
        try:
            col = self.get_astro_collection()
            if col is not None:
                col.update_many({"client_id": cid}, {"$set": {"active": False}})
                try:
                    obj_id = ObjectId(file_id)
                    res = col.update_one({"_id": obj_id, "client_id": cid}, {"$set": {"active": True}})
                    matched = res.matched_count > 0
                except Exception:
                    res = col.update_one({"_id": file_id, "client_id": cid}, {"$set": {"active": True}})
                    matched = res.matched_count > 0

                if matched:
                    doc = self.get_astro_file_by_id(cid, file_id)
                    if doc:
                        self._active_file_cache[cid] = doc
        except Exception as e:
            logger.warning(f"Note setting active astro file in DB: {e}")

        mem_files = self._in_memory_astro_files.get(cid, [])
        for f in mem_files:
            if str(f.get("_id")) == str(file_id):
                f["active"] = True
                matched = True
                if not self._active_file_cache.get(cid):
                    f_copy = dict(f)
                    f_copy["file_id"] = str(f_copy.get("_id", ""))
                    self._active_file_cache[cid] = f_copy
            else:
                f["active"] = False
        return matched

    def delete_astro_file(self, client_id: str, file_id: str) -> bool:
        """Deletes an Astro report from COMMODITY.astro_files. Never auto-promotes another file."""
        cid = self._require_client_id(client_id)
        if not hasattr(self, "_active_file_cache"):
            self._active_file_cache = {}
        self._active_file_cache.pop(cid, None)

        deleted = False
        was_active = False

        try:
            col = self.get_astro_collection()
            if col is not None:
                try:
                    obj_id = ObjectId(file_id)
                    existing = col.find_one({"_id": obj_id, "client_id": cid})
                    res = col.delete_one({"_id": obj_id, "client_id": cid})
                except Exception:
                    existing = col.find_one({"_id": file_id, "client_id": cid})
                    res = col.delete_one({"_id": file_id, "client_id": cid})
                deleted = res.deleted_count > 0
                if existing and existing.get("active"):
                    was_active = True
        except Exception as e:
            logger.warning(f"Note deleting astro file from DB: {e}")

        # In-memory synchronization & fallback
        mem_files = self._in_memory_astro_files.get(cid, [])
        idx = next((i for i, f in enumerate(mem_files) if str(f.get("_id")) == str(file_id)), None)
        if idx is not None:
            if mem_files[idx].get("active"):
                was_active = True
            mem_files.pop(idx)
            deleted = True

        # Clear cache completely if the deleted file was active, or if no active file remains
        if was_active:
            self._active_file_cache[cid] = None
        else:
            self.get_active_astro_file(cid, use_cache=False)

        return deleted

    # -------------------------------------------------------------------
    # Audit Log Storage (Stored in user's dedicated collection)
    # -------------------------------------------------------------------
    def insert_audit_log(self, event: Dict[str, Any], client_id: str) -> None:
        """Inserts security/order audit log into the user's dedicated collection."""
        cid = self._require_client_id(client_id)
        try:
            col = self.get_user_collection(cid)
            if col is not None:
                col.insert_one({"type": "audit_log", "client_id": cid, **event})
        except Exception as e:
            logger.warning(f"Note inserting audit log for client '{cid}': {e}")

    def get_status(self, client_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "is_connected": self.is_connected,
            "database": self.db_name,
            "scoped_database": self.db_name,
        }


# Global singleton instance
mongo_service = MongoService()
