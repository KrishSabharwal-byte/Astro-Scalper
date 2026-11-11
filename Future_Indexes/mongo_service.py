"""
Multi-Tenant Scoped MongoDB Service for FUTURE_INDEXES Database.
Routes each user's complete data (trades, run states, broker credentials, astro signals, audit logs)
into dedicated per-user collections in the FUTURE_INDEXES database (e.g. FUTURE_INDEXES.admin, FUTURE_INDEXES.User1).
All read/write operations strictly assert a resolved client_id.
"""

import datetime
import logging
import os
import threading
from typing import Dict, Any, Optional, List
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId

from angel_one_service import normalize_index_name

logger = logging.getLogger("MongoService")

DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_URI = os.getenv("MONGO_URI", DEFAULT_MONGO_URI)
DEFAULT_DB_NAME = os.getenv("MONGO_DB_NAME", "FUTURE_INDEXES")
INDEX_COLLECTIONS = ["NIFTY", "BANKNIFTY", "SENSEX"]
COMMODITY_COLLECTIONS = INDEX_COLLECTIONS

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


class MongoService:
    """
    Multi-Tenant Scoped MongoDB Service for FUTURE_INDEXES Database.
    Routes user data into dedicated collections named per client_id (e.g., admin, User1).
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
        self._lock = threading.Lock()
        self._in_memory_trades: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self._in_memory_astro_files: Dict[str, List[Dict[str, Any]]] = {}
        self._active_file_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._connect()

    def _connect(self) -> bool:
        with self._lock:
            if not self.uri:
                logger.info("No 'MONGO_URI' provided in environment. Operating in resilient offline fallback mode.")
                self.is_connected = False
                return False
            try:
                self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
                self.client.admin.command('ping')
                try:
                    existing_dbs = self.client.list_database_names()
                    target_lower = self.db_name.lower()
                    for db_n in existing_dbs:
                        if db_n.lower() == target_lower:
                            self.db_name = db_n
                            break
                except Exception:
                    pass
                self.is_connected = True
                logger.info(f"MongoDB connected successfully to cluster. Database: {self.db_name}")
                return True
            except Exception as e:
                self.is_connected = False
                logger.warning(f"MongoDB connection failed: {e}. Running in offline fallback mode.")
                return False

    def ensure_connection(self) -> bool:
        if self.is_connected and self.client is not None:
            return True
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
        """Returns the primary FUTURE_INDEXES MongoDB database object."""
        if not self.ensure_connection() or self.client is None:
            return None
        return self.client[self.db_name]

    def get_user_collection(self, client_id: str):
        """Returns MongoDB collection object for a specific client (e.g. FUTURE_INDEXES.admin, FUTURE_INDEXES.User1)."""
        db = self.get_database()
        if db is None:
            return None
        cid = self._require_client_id(client_id)
        return db[cid]

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
        Inserts a closed trade into the user's dedicated collection in FUTURE_INDEXES database
        (e.g., FUTURE_INDEXES['admin'], FUTURE_INDEXES['User1']).
        """
        cid = self._require_client_id(client_id)
        cfg = run_config or {}
        raw_inst = (
            cfg.get("instrument")
            or cfg.get("commodity")
            or cfg.get("instrument_name")
            or trade_data.get("instrument")
            or trade_data.get("commodity")
            or trade_data.get("symbol")
            or "NIFTY"
        )
        col_name = normalize_index_name(raw_inst)

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
            "instrument": col_name,
            "commodity": col_name,
            "trade_id": trade_data.get("trade_id"),
            "run_id": trade_data.get("run_id"),
            "run_number": trade_data.get("run_number") or cfg.get("run_number", 1),
            "symbol": col_name,
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
        except Exception as e:
            logger.error(f"Failed to insert trade into collection '{cid}': {e}")

        # In-memory backup
        if not inserted_id:
            import uuid
            inserted_id = f"mem_trd_{uuid.uuid4().hex[:8]}"
            doc["_id"] = inserted_id

        client_trades = self._in_memory_trades.setdefault(cid, {})
        inst_trades = client_trades.setdefault(col_name, [])
        inst_trades.insert(0, dict(doc))
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
        instrument: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetches recent trades from client's dedicated collection in FUTURE_INDEXES database.
        Supports filtering by instrument (NIFTY, BANKNIFTY, SENSEX) and mode (paper/live).
        """
        cid = self._require_client_id(client_id)
        all_trades: List[Dict[str, Any]] = []

        target_inst = instrument or commodity
        query_parts: List[Dict[str, Any]] = [{"type": "trade"}]
        if target_inst:
            norm_inst = normalize_index_name(target_inst)
            query_parts.append({"$or": [{"instrument": norm_inst}, {"commodity": norm_inst}, {"symbol": norm_inst}]})
        if mode and mode.strip().lower() in ("paper", "live"):
            query_parts.append({"mode": mode.strip().lower()})

        query = {"$and": query_parts} if len(query_parts) > 1 else query_parts[0]

        if self.ensure_connection():
            try:
                col = self.get_user_collection(cid)
                if col is not None:
                    cursor = col.find(query).sort("_id", -1).limit(limit)
                    for doc in cursor:
                        doc["_id"] = str(doc["_id"])
                        all_trades.append(doc)
            except Exception as e:
                logger.error(f"Failed to fetch trades from MongoDB collection '{cid}': {e}")

        # Merge in-memory fallback
        client_mem = self._in_memory_trades.get(cid, {})
        targets = [normalize_index_name(target_inst)] if target_inst else INDEX_COLLECTIONS
        for c in targets:
            mem_list = client_mem.get(c, [])
            for t in mem_list:
                if mode and t.get("mode") != mode.strip().lower():
                    continue
                if not any(x.get("trade_id") == t.get("trade_id") for x in all_trades):
                    all_trades.append(dict(t))

        all_trades.sort(
            key=lambda x: str(x.get("exit_time") or x.get("exited_at") or x.get("created_at") or ""),
            reverse=True,
        )
        return all_trades[:limit]

    def clear_trades(self, client_id: str, commodity: Optional[str] = None, instrument: Optional[str] = None) -> bool:
        """Clears trade history for client_id from their dedicated collection."""
        cid = self._require_client_id(client_id)
        success = True
        target_inst = instrument or commodity

        query: Dict[str, Any] = {"type": "trade"}
        if target_inst:
            norm_inst = normalize_index_name(target_inst)
            query = {"type": "trade", "$or": [{"instrument": norm_inst}, {"commodity": norm_inst}, {"symbol": norm_inst}]}

        if self.ensure_connection():
            try:
                col = self.get_user_collection(cid)
                if col is not None:
                    col.delete_many(query)
            except Exception as e:
                logger.error(f"Failed to clear trades for client '{cid}': {e}")
                success = False

        client_mem = self._in_memory_trades.get(cid, {})
        if target_inst:
            client_mem.pop(normalize_index_name(target_inst), None)
        else:
            self._in_memory_trades.pop(cid, None)

        return success

    def delete_trade(self, client_id: str, trade_id: str, commodity: Optional[str] = None, instrument: Optional[str] = None) -> bool:
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
        for inst_key, mem_list in client_mem.items():
            idx = next((i for i, t in enumerate(mem_list) if t.get("trade_id") == trade_id or str(t.get("_id")) == trade_id), None)
            if idx is not None:
                mem_list.pop(idx)
                deleted = True

        return deleted

    # -------------------------------------------------------------------
    # Run State Storage (Stored in user's dedicated collection)
    # -------------------------------------------------------------------
    def save_client_state(self, client_id: str, state_data: Dict[str, Any]) -> bool:
        """Persists multi-slot run state document in user's dedicated collection (FUTURE_INDEXES[cid])."""
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
            return None

    # -------------------------------------------------------------------
    # Astro Report File Storage (Stored in dedicated FUTURE_INDEXES.astro_files)
    # -------------------------------------------------------------------
    def get_astro_collection(self):
        """Returns dedicated MongoDB collection for Astro CSV reports: FUTURE_INDEXES.astro_files."""
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
        """Stores an uploaded Astro CSV in dedicated FUTURE_INDEXES.astro_files collection with active flag."""
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
        """Returns list of uploaded Astro reports for client from FUTURE_INDEXES.astro_files, sorted newest first."""
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
        """Returns the active Astro file document from FUTURE_INDEXES.astro_files."""
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
                if not doc:
                    user_col = self.get_user_collection(cid)
                    if user_col is not None:
                        doc = user_col.find_one({"type": "astro_file", "active": True})
                if doc:
                    doc["_id"] = str(doc["_id"])
                    doc["file_id"] = doc["_id"]
                    self._active_file_cache[cid] = doc
                    return doc
                self._active_file_cache[cid] = None
                return None
        except Exception as e:
            logger.warning(f"Note querying active astro file from DB: {e}")

        mem_files = self._in_memory_astro_files.get(cid, [])
        active_f = next((f for f in mem_files if f.get("active")), None)
        if active_f:
            active_f["file_id"] = str(active_f.get("_id", ""))
            self._active_file_cache[cid] = dict(active_f)
            return active_f

        self._active_file_cache[cid] = None
        return None

    def get_astro_file_by_id(self, client_id: str, file_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a specific Astro CSV document by ID from FUTURE_INDEXES.astro_files."""
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
        """Sets a specific Astro report as active in FUTURE_INDEXES.astro_files."""
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
        """Deletes an Astro report from FUTURE_INDEXES.astro_files. Never auto-promotes another file."""
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

    def get_pnl_summary(
        self,
        client_id: str,
        period: str,
        ref_date: Optional[datetime.date] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Computes gross P&L summary per instrument (NIFTY, BANKNIFTY, SENSEX) for a calendar period.

        period: "day" | "week" | "month" | "custom"
        ref_date: the IST date to anchor the range (defaults to today IST)
        mode: optional "paper" | "live" filter (same as get_recent_trades)

        Returns a dict with:
          - range: {"from": str, "to": str}  (ISO date strings, IST)
          - by_instrument: list of per-instrument rows
          - total: summed row across all instruments
        """
        cid = self._require_client_id(client_id)

        # --- Resolve IST date range ---
        today_ist = datetime.datetime.now(IST).date()
        if ref_date is None:
            anchor = today_ist
        elif isinstance(ref_date, str):
            try:
                anchor = datetime.date.fromisoformat(ref_date.strip()[:10])
            except Exception:
                anchor = today_ist
        elif isinstance(ref_date, datetime.datetime):
            anchor = ref_date.date()
        else:
            anchor = ref_date

        if period == "day":
            range_start = anchor
            range_end = anchor
        elif period == "week":
            # Monday of the week containing anchor
            range_start = anchor - datetime.timedelta(days=anchor.weekday())
            range_end = range_start + datetime.timedelta(days=6)
        elif period == "month":
            range_start = anchor.replace(day=1)
            # last day of month: first day of next month - 1 day
            if anchor.month == 12:
                range_end = anchor.replace(day=31)
            else:
                range_end = (anchor.replace(month=anchor.month + 1, day=1) - datetime.timedelta(days=1))
        else:  # "custom" treated as single day
            range_start = anchor
            range_end = anchor

        # Build datetime boundaries (IST, inclusive)
        dt_from = datetime.datetime(range_start.year, range_start.month, range_start.day, 0, 0, 0, tzinfo=IST)
        dt_to = datetime.datetime(range_end.year, range_end.month, range_end.day, 23, 59, 59, tzinfo=IST)

        start_date_str = range_start.strftime("%Y-%m-%d")
        end_date_str = range_end.strftime("%Y-%m-%d")

        # --- Normalised mode filter ---
        mode_filter = mode.strip().lower() if mode and mode.strip().lower() in ("paper", "live") else None

        # --- Helper: extract date part (YYYY-MM-DD) from a trade doc ---
        def _trade_date(t: Dict[str, Any]) -> str:
            raw = str(t.get("exit_time") or t.get("entry_time") or t.get("created_at") or "").strip()
            if len(raw) >= 10:
                return raw[:10]
            return ""

        # --- Initialise accumulators ---
        AccRow = Dict[str, Any]

        def _empty_row(instrument: str) -> AccRow:
            return {
                "instrument": instrument,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "net_pnl": 0.0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
            }

        accumulators: Dict[str, AccRow] = {inst: _empty_row(inst) for inst in INDEX_COLLECTIONS}
        seen_trade_ids: set = set()

        def _accumulate(t: Dict[str, Any]) -> None:
            tdate = _trade_date(t)
            if not tdate or tdate < start_date_str or tdate > end_date_str:
                return
            if mode_filter and (t.get("mode") or "paper") != mode_filter:
                return
            raw_inst = (
                t.get("instrument") or t.get("commodity") or t.get("symbol") or "NIFTY"
            )
            inst = normalize_index_name(raw_inst)
            if inst not in accumulators:
                return
            pnl = float(t.get("pnl_rupees") or 0)
            acc = accumulators[inst]
            acc["trade_count"] += 1
            if pnl > 0:
                acc["gross_profit"] += pnl
                acc["win_count"] += 1
            elif pnl < 0:
                acc["gross_loss"] += pnl
                acc["loss_count"] += 1
            acc["net_pnl"] += pnl

        # --- Fetch from MongoDB ---
        if self.ensure_connection():
            try:
                col = self.get_user_collection(cid)
                if col is not None:
                    query_parts: List[Dict[str, Any]] = [{"type": "trade"}]
                    if mode_filter:
                        query_parts.append({"mode": mode_filter})
                    query = {"$and": query_parts} if len(query_parts) > 1 else query_parts[0]
                    cursor = col.find(query).sort("_id", -1).limit(5000)
                    for doc in cursor:
                        trade_id = str(doc.get("trade_id") or doc.get("_id") or "")
                        if trade_id:
                            seen_trade_ids.add(trade_id)
                        doc["_id"] = str(doc["_id"])
                        _accumulate(doc)
            except Exception as e:
                logger.error(f"[PnL Summary] Failed to fetch from MongoDB for '{cid}': {e}")

        # --- Merge in-memory fallback (same pattern as get_recent_trades) ---
        client_mem = self._in_memory_trades.get(cid, {})
        for inst_key in INDEX_COLLECTIONS:
            for t in client_mem.get(inst_key, []):
                tid = str(t.get("trade_id") or t.get("_id") or "")
                if tid and tid in seen_trade_ids:
                    continue
                _accumulate(dict(t))

        # --- Finalise rows and total ---
        by_instrument = []
        total = _empty_row("ALL")
        for inst in INDEX_COLLECTIONS:
            row = accumulators[inst]
            row["gross_profit"] = round(row["gross_profit"], 2)
            row["gross_loss"] = round(row["gross_loss"], 2)
            row["net_pnl"] = round(row["net_pnl"], 2)
            by_instrument.append(row)
            total["gross_profit"] += row["gross_profit"]
            total["gross_loss"] += row["gross_loss"]
            total["net_pnl"] += row["net_pnl"]
            total["trade_count"] += row["trade_count"]
            total["win_count"] += row["win_count"]
            total["loss_count"] += row["loss_count"]

        total["gross_profit"] = round(total["gross_profit"], 2)
        total["gross_loss"] = round(total["gross_loss"], 2)
        total["net_pnl"] = round(total["net_pnl"], 2)

        return {
            "range": {
                "from": range_start.isoformat(),
                "to": range_end.isoformat(),
            },
            "by_instrument": by_instrument,
            "total": total,
        }

    def get_status(self, client_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "is_connected": self.is_connected,
            "database": self.db_name,
            "scoped_database": self.db_name,
        }


# Global singleton instance
mongo_service = MongoService()
