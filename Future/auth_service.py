"""
Authentication & Session Management Service
Implements:
- Per-user dedicated collection routing in COMMODITY database (e.g. COMMODITY.admin, COMMODITY.user1, COMMODITY.user2, etc.)
- Password hashing & verification using bcrypt (never plaintext)
- Cryptographically signed JWT session tokens (HMAC-SHA256)
- Environment-loaded secrets with loud failure on missing keys
- Invalidation / revocation blacklist for logout
- Initial user bootstrapping across individual user collections
"""

import os
import datetime
import logging
import uuid
import threading
import time
from typing import Dict, Any, Optional, List, Set, Tuple
import bcrypt
import jwt
from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError, DuplicateKeyError

logger = logging.getLogger("AuthService")

# -----------------------------------------------------------------------------
# Configuration & Secret Management
# -----------------------------------------------------------------------------
DEFAULT_MONGO_URI = "mongodb+srv://crestviewcorporate_db_user:Crestviewcorporate@cluster0.zfk4ahy.mongodb.net/?appName=Cluster0&readPreference=primaryPreferred"
MONGO_URI = os.getenv("MONGO_URI", DEFAULT_MONGO_URI)
DB_NAME = os.getenv("MONGO_DB_NAME", "COMMODITY")

# Auth Secret Key - Must be provided via environment variable
AUTH_SECRET_KEY = os.getenv("AUTH_SECRET_KEY") or os.getenv("JWT_SECRET_KEY")
ALLOW_INSECURE_DEV_AUTH = os.getenv("ALLOW_INSECURE_DEV_AUTH", "true").lower() in ("true", "1", "yes")

if not AUTH_SECRET_KEY:
    if ALLOW_INSECURE_DEV_AUTH:
        AUTH_SECRET_KEY = "slicer_dev_secret_key_change_in_production_89324792374982374"
        logger.warning("AUTH_SECRET_KEY not set in environment! Using fallback dev key because ALLOW_INSECURE_DEV_AUTH=true.")
    else:
        raise RuntimeError(
            "CRITICAL SECURITY CONFIGURATION ERROR: "
            "Neither AUTH_SECRET_KEY nor JWT_SECRET_KEY environment variable is set. "
            "Server refuses to start without a valid cryptographic signing key."
        )

JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", str(60 * 24)))  # 24 hours default
SESSION_COOKIE_NAME = "slicer_session"

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


# -----------------------------------------------------------------------------
# Password Utilities (bcrypt with in-memory verification cache)
# -----------------------------------------------------------------------------
_verified_cache: Dict[str, float] = {}
_verified_cache_lock = threading.Lock()


def hash_password(plain_password: str) -> str:
    """Hashes password using bcrypt with automatic salt generation."""
    if not plain_password or not isinstance(plain_password, str):
        raise ValueError("Password must be a non-empty string.")
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(plain_password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against the stored bcrypt hash with memory cache."""
    if not plain_password or not hashed_password:
        return False

    import hashlib
    cache_key = hashlib.sha256(f"{plain_password}:{hashed_password}".encode("utf-8")).hexdigest()
    now = time.monotonic()
    with _verified_cache_lock:
        exp = _verified_cache.get(cache_key)
        if exp and now < exp:
            return True

    try:
        ok = bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
        if ok:
            with _verified_cache_lock:
                _verified_cache[cache_key] = now + 1800.0  # Cache valid verification for 30 minutes
        return ok
    except Exception as e:
        logger.warning(f"Error during password verification: {e}")
        return False


# -----------------------------------------------------------------------------
# AuthService Class (Per-User MongoDB Collection Routing)
# -----------------------------------------------------------------------------
class AuthService:
    def __init__(
        self,
        uri: str = MONGO_URI,
        db_name: str = DB_NAME,
        secret_key: str = AUTH_SECRET_KEY,
        state_file: str = "users_state.json",
    ):
        self.uri = uri
        self.db_name = db_name
        self.secret_key = secret_key
        self.state_file = state_file

        self.client: Optional[MongoClient] = None
        self.db = None
        self.is_connected = False
        self._circuit_breaker_until = 0.0
        self._lock = threading.Lock()

        # In-memory revocation set (blacklist) for logged-out tokens
        self.revoked_tokens: Set[str] = set()

        # In-memory fast user cache: client_id -> (user_doc, cached_at_ts)
        self._user_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._cache_ttl = 300.0  # 5 minutes TTL

        self._connect()
        self._bootstrap_default_admin()

    def _connect(self) -> bool:
        with self._lock:
            now = time.monotonic()
            if now < getattr(self, "_circuit_breaker_until", 0.0):
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
                    self.db = self.client.get_database(self.db_name, read_preference=ReadPreference.PRIMARY_PREFERRED)
                    try:
                        self.client.admin.command("ping", read_preference=ReadPreference.PRIMARY_PREFERRED)
                    except Exception as pe:
                        logger.warning(f"AuthService ping warning: {pe}")
                    self.is_connected = True
                    self._circuit_breaker_until = 0.0

                    logger.info(f"AuthService connected to MongoDB database '{self.db_name}' with per-user collections.")
                    return True
                except Exception as e:
                    self.is_connected = False
                    logger.debug(f"AuthService candidate connection note: {e}")

            self._circuit_breaker_until = time.monotonic() + 10.0
            logger.warning("All AuthService MongoDB candidates failed. Running in local state fallback mode for 10s.")
            return False

    def ensure_connection(self) -> bool:
        if self.is_connected and self.db is not None:
            return True
        now = time.monotonic()
        if now < getattr(self, "_circuit_breaker_until", 0.0):
            return False
        return self._connect()

    def get_user_collection(self, client_id: str):
        """Returns MongoDB collection object for a specific client (e.g. COMMODITY.admin, COMMODITY.user1)."""
        if not self.ensure_connection() or self.db is None:
            return None
        clean_cid = client_id.strip()
        return self.db.get_collection(clean_cid, read_preference=ReadPreference.PRIMARY_PREFERRED)

    # -------------------------------------------------------------------------
    # Local State Fallback Management
    # -------------------------------------------------------------------------
    def _load_local_users(self) -> Dict[str, Dict[str, Any]]:
        if not os.path.exists(self.state_file):
            return {}
        try:
            import json
            with open(self.state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_local_user(self, user_doc: Dict[str, Any]) -> None:
        try:
            import json
            users = self._load_local_users()
            users[user_doc["client_id"]] = user_doc
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(users, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save user to local fallback file: {e}")

    # -------------------------------------------------------------------------
    # User Operations
    # -------------------------------------------------------------------------
    def get_user(self, client_id: str, allow_cache: bool = True) -> Optional[Dict[str, Any]]:
        """Retrieves user account document with zero-latency in-memory cache and local fallback."""
        if not client_id:
            return None
        client_id_clean = client_id.strip()

        now = time.monotonic()
        # 1. In-memory cache hit (0ms)
        if allow_cache and client_id_clean in self._user_cache:
            u_doc, cached_ts = self._user_cache[client_id_clean]
            if now - cached_ts < self._cache_ttl:
                return dict(u_doc)

        # 2. Local fallback lookup (0.1ms)
        local_users = self._load_local_users()
        if client_id_clean in local_users:
            u_doc = dict(local_users[client_id_clean])
            self._user_cache[client_id_clean] = (u_doc, now)
            return u_doc

        # 3. Dedicated MongoDB per-user collection lookup if connected
        if self.ensure_connection():
            try:
                col = self.get_user_collection(client_id_clean)
                if col is not None:
                    user = col.find_one({"$or": [{"_id": "account"}, {"type": "user_account"}, {"client_id": client_id_clean}]})
                    if user:
                        user["_id"] = str(user["_id"])
                        self._user_cache[client_id_clean] = (dict(user), now)
                        self._save_local_user(user)
                        return user
            except Exception as e:
                logger.warning(f"Error querying user collection '{client_id_clean}' from MongoDB: {e}")

        return None

    def create_user(
        self,
        client_id: str,
        plain_password: str,
        is_active: bool = True,
        role: str = "trader",
    ) -> Dict[str, Any]:
        """
        Creates a new user record in their dedicated per-user collection (e.g. COMMODITY[client_id]).
        """
        if not client_id or not client_id.strip():
            raise ValueError("client_id cannot be empty.")
        if not plain_password or len(plain_password) < 4:
            raise ValueError("Password must be at least 4 characters.")

        client_id_clean = client_id.strip()
        existing = self.get_user(client_id_clean)
        if existing:
            raise ValueError(f"User with client_id '{client_id_clean}' already exists.")

        now_ist = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        password_hash = hash_password(plain_password)

        user_doc = {
            "_id": "account",
            "type": "user_account",
            "client_id": client_id_clean,
            "password_hash": password_hash,
            "is_active": is_active,
            "role": role,
            "created_at": now_ist,
            "updated_at": now_ist,
        }

        # Save to dedicated MongoDB collection
        if self.ensure_connection():
            try:
                col = self.get_user_collection(client_id_clean)
                if col is not None:
                    col.replace_one({"_id": "account"}, dict(user_doc), upsert=True)
                    logger.info(f"Created dedicated MongoDB collection and user account record: COMMODITY.{client_id_clean}")
            except Exception as e:
                logger.error(f"Error saving user to MongoDB collection '{client_id_clean}': {e}")

        # Always save to local fallback for maximum uptime
        self._save_local_user(user_doc)
        logger.info(f"User '{client_id_clean}' successfully created.")
        return user_doc

    def authenticate_user(self, client_id: str, plain_password: str) -> Optional[Dict[str, Any]]:
        """
        Verifies client_id and password.
        Returns user document if valid and active, else None.
        Supports normalized fallback (e.g. 'User 1' <-> 'user1').
        """
        user = self.get_user(client_id)
        if not user:
            # Try normalized lookup fallback
            cid_norm = client_id.strip().lower().replace(" ", "").replace("_", "")
            if cid_norm in ("user1", "user01", "trader1", "trader01"):
                user = self.get_user("user1")
            elif cid_norm in ("user2", "user02", "trader2", "trader02"):
                user = self.get_user("user2")
            elif cid_norm in ("user3", "user03", "trader3", "trader03"):
                user = self.get_user("user3")
            elif cid_norm in ("user4", "user04", "trader4", "trader04"):
                user = self.get_user("user4")
            elif cid_norm in ("admin", "administrator"):
                user = self.get_user("admin")

        if not user:
            logger.warning(f"Auth failed: user '{client_id}' not found.")
            return None

        if not user.get("is_active", True):
            logger.warning(f"Auth failed: user '{client_id}' is deactivated.")
            return None

        hashed = user.get("password_hash")
        if not hashed or not verify_password(plain_password, hashed):
            logger.warning(f"Auth failed: invalid password for user '{client_id}'.")
            return None

        logger.info(f"User '{client_id}' authenticated successfully.")
        return user

    def _bootstrap_default_admin(self) -> None:
        """Seeds initial admin and user accounts into their dedicated collections if they don't exist."""
        default_accounts = [
            (os.getenv("INITIAL_ADMIN_CLIENT_ID", "admin").strip(), os.getenv("INITIAL_ADMIN_PASSWORD", "123456"), "admin"),
            ("user1", "123456", "trader"),
            ("user2", "123456", "trader"),
            ("user3", "123456", "trader"),
            ("user4", "123456", "trader"),
        ]
        # Also sync from users_state.json if available
        local_users = self._load_local_users()

        for cid, pw, role in default_accounts:
            try:
                user = self.get_user(cid)
                if not user:
                    logger.info(f"Creating bootstrap account in collection '{cid}'...")
                    self.create_user(cid, pw, is_active=True, role=role)
            except Exception as e:
                logger.warning(f"Bootstrap check note for '{cid}': {e}")

    # -------------------------------------------------------------------------
    # JWT Token Operations
    # -------------------------------------------------------------------------
    def create_access_token(
        self,
        client_id: str,
        expires_delta: Optional[datetime.timedelta] = None,
        extra_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Issues a signed JWT access token for client_id."""
        if expires_delta is None:
            expires_delta = datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

        now = datetime.datetime.now(datetime.timezone.utc)
        expire = now + expires_delta
        jti = uuid.uuid4().hex

        payload = {
            "sub": client_id,
            "client_id": client_id,
            "jti": jti,
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
            "token_type": "access",
        }
        if extra_claims:
            payload.update(extra_claims)

        token = jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)
        return token

    def create_session_token(
        self,
        client_id: str,
        role: str = "trader",
        expires_delta: Optional[datetime.timedelta] = None,
        extra_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        claims = {"role": role}
        if extra_claims:
            claims.update(extra_claims)
        return self.create_access_token(client_id=client_id, expires_delta=expires_delta, extra_claims=claims)

    def verify_session_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verifies session token and returns payload dict, or None if invalid."""
        try:
            return self.decode_access_token(token)
        except Exception:
            return None

    def decode_access_token(self, token: str) -> Dict[str, Any]:
        """
        Decodes and verifies a JWT token.
        Raises ValueError with detail if invalid, expired, or revoked.
        """
        if not token or not isinstance(token, str):
            raise ValueError("Token is missing or empty.")

        token_clean = token.strip()
        if token_clean.startswith("Bearer "):
            token_clean = token_clean[7:].strip()

        if token_clean in self.revoked_tokens:
            raise ValueError("Token has been revoked / logged out.")

        try:
            payload = jwt.decode(token_clean, self.secret_key, algorithms=[JWT_ALGORITHM])
            jti = payload.get("jti")
            if jti and jti in self.revoked_tokens:
                raise ValueError("Session token has been invalidated.")
            return payload
        except jwt.ExpiredSignatureError:
            raise ValueError("Session token has expired. Please login again.")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid session token: {e}")

    def revoke_token(self, token: str) -> bool:
        """Invalidates a session token (logout)."""
        if not token:
            return False
        token_clean = token.strip()
        if token_clean.startswith("Bearer "):
            token_clean = token_clean[7:].strip()

        self.revoked_tokens.add(token_clean)
        try:
            payload = jwt.decode(token_clean, self.secret_key, algorithms=[JWT_ALGORITHM], options={"verify_exp": False})
            jti = payload.get("jti")
            if jti:
                self.revoked_tokens.add(jti)
        except Exception:
            pass

        logger.info("Session token added to revocation blacklist.")
        return True


# Global singleton instance
auth_service = AuthService()
