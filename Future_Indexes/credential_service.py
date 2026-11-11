"""
Encrypted Per-Client Broker Credential Service
Provides authenticated Fernet (AES-128-CBC + HMAC-SHA256) encryption at rest
for broker API keys, TOTP secrets, and MPINs.
Zero plaintext credentials stored in databases, files, or returned in API responses.
All records are saved directly in each user's dedicated collection in the FUTURE_INDEXES database.
"""

import base64
import datetime
import hashlib
import json
import logging
import os
import threading
from typing import Dict, Any, Optional
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger("CredentialService")

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# Load Master Encryption Key from Environment
ENV_MASTER_KEY = os.getenv("ENCRYPTION_MASTER_KEY") or os.getenv("AUTH_SECRET_KEY")
ALLOW_INSECURE_DEV_AUTH = os.getenv("ALLOW_INSECURE_DEV_AUTH", "1") == "1"

if not ENV_MASTER_KEY:
    if not ALLOW_INSECURE_DEV_AUTH:
        raise RuntimeError(
            "CRITICAL SECURITY CONFIGURATION ERROR: 'ENCRYPTION_MASTER_KEY' or 'AUTH_SECRET_KEY' "
            "must be explicitly configured in production environment variables."
        )
    logger.warning("Using ephemeral dev master key for credential encryption. Set ENCRYPTION_MASTER_KEY in production.")
    ENV_MASTER_KEY = "dev_slicer_master_encryption_key_2026"


def derive_fernet_key(master_secret: str) -> bytes:
    """Derives a valid 32-byte URL-safe base64-encoded Fernet key from arbitrary secret."""
    digest = hashlib.sha256(master_secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


class CredentialService:
    def __init__(self, master_key: str = ENV_MASTER_KEY, fallback_file: str = "credentials_state.json"):
        self.fernet_key = derive_fernet_key(master_key)
        self.cipher = Fernet(self.fernet_key)
        self.fallback_file = fallback_file
        self._lock = threading.Lock()
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load_fallback()
        self._bootstrap_default_credentials()

    def _bootstrap_default_credentials(self) -> None:
        """Seeds testing Angel One credentials for default users if not already configured."""
        default_client_id = os.getenv("ANGEL_CLIENT_ID", "")
        default_api_key = os.getenv("ANGEL_API_KEY", "")
        default_totp = os.getenv("ANGEL_TOTP_SECRET", "")
        default_mpin = os.getenv("ANGEL_MPIN", "")

        if default_client_id and default_api_key:
            creds = {
                "client_id": default_client_id,
                "broker_client_id": default_client_id,
                "api_key": default_api_key,
                "totp_secret": default_totp,
                "mpin": default_mpin,
            }
            targets = ["admin", "User1"]
            for target in targets:
                key = f"{target}:angel_one"
                if key not in self._cache:
                    self.save_broker_credentials(target, "angel_one", creds)

    def _require_client_id(self, client_id: Optional[str]) -> str:
        if not client_id or not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("CRITICAL SECURITY GUARD: client_id is strictly required for broker credentials.")
        return client_id.strip()

    def encrypt_string(self, plain_text: str) -> str:
        """Encrypts plaintext string to URL-safe Fernet ciphertext."""
        if not plain_text:
            return ""
        return self.cipher.encrypt(plain_text.encode("utf-8")).decode("utf-8")

    def decrypt_string(self, cipher_text: str) -> str:
        """Decrypts Fernet ciphertext to plaintext string in-memory."""
        if not cipher_text:
            return ""
        try:
            return self.cipher.decrypt(cipher_text.encode("utf-8")).decode("utf-8")
        except InvalidToken:
            raise ValueError("Failed to decrypt credentials: signature invalid or key mismatch.")

    def _load_fallback(self) -> None:
        """Loads local encrypted credentials state."""
        if os.path.exists(self.fallback_file):
            try:
                with open(self.fallback_file, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load local credentials state: {e}")

    def _save_fallback(self) -> None:
        """Saves local encrypted credentials state."""
        try:
            tmp_file = f"{self.fallback_file}.tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)
            os.replace(tmp_file, self.fallback_file)
        except Exception as e:
            logger.error(f"Failed to persist local credentials fallback: {e}")

    def save_broker_credentials(
        self,
        client_id: str,
        broker_name: str,
        credentials: Dict[str, str],
    ) -> bool:
        """
        Encrypts and stores broker credentials at rest in the client's dedicated collection in FUTURE_INDEXES db.
        Supported keys in credentials dict: broker_client_id, api_key, totp_secret, mpin.
        """
        cid = self._require_client_id(client_id)
        bname = (broker_name or "angel_one").strip().lower()

        # Preserve existing trading_mode or default to "paper"
        existing_mode = self.get_trading_mode(cid, bname)

        doc_id = f"credentials_{bname}"
        encrypted_record = {
            "_id": doc_id,
            "type": "broker_credentials",
            "client_id": cid,
            "broker_name": bname,
            "encrypted_client_id": self.encrypt_string(credentials.get("broker_client_id") or credentials.get("client_id") or ""),
            "encrypted_api_key": self.encrypt_string(credentials.get("api_key") or ""),
            "encrypted_totp_secret": self.encrypt_string(credentials.get("totp_secret") or ""),
            "encrypted_mpin": self.encrypt_string(credentials.get("mpin") or ""),
            "masked_client_id": self._mask_id(credentials.get("broker_client_id") or credentials.get("client_id") or ""),
            "trading_mode": existing_mode if existing_mode in ("paper", "live") else "paper",
            "updated_at": datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "is_configured": True,
        }

        # Save to user's dedicated MongoDB collection in FUTURE_INDEXES db
        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    col.replace_one(
                        {"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]},
                        encrypted_record,
                        upsert=True,
                    )
        except Exception as me:
            logger.warning(f"MongoDB broker credential save note: {me}")

        # Always update local cache & fallback file
        with self._lock:
            key = f"{cid}:{bname}"
            self._cache[key] = encrypted_record
            self._save_fallback()

        logger.info(f"Encrypted credentials saved at rest for client '{cid}' (Broker: {bname}, Mode: {encrypted_record['trading_mode']})")
        return True

    def get_decrypted_broker_credentials(
        self,
        client_id: str,
        broker_name: str = "angel_one",
    ) -> Optional[Dict[str, str]]:
        """
        Retrieves and decrypts broker credentials strictly in-memory.
        Never exposed over API.
        """
        cid = self._require_client_id(client_id)
        bname = (broker_name or "angel_one").strip().lower()
        doc_id = f"credentials_{bname}"

        record = None
        # Try Mongo first from user's dedicated collection
        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    record = col.find_one({"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]})
        except Exception:
            pass

        # Try local fallback cache
        if not record:
            with self._lock:
                record = self._cache.get(f"{cid}:{bname}")

        if not record:
            if cid != "admin":
                return self.get_decrypted_broker_credentials("admin", broker_name)
            return None

        try:
            return {
                "client_id": self.decrypt_string(record.get("encrypted_client_id", "")),
                "api_key": self.decrypt_string(record.get("encrypted_api_key", "")),
                "totp_secret": self.decrypt_string(record.get("encrypted_totp_secret", "")),
                "mpin": self.decrypt_string(record.get("encrypted_mpin", "")),
                "broker_name": bname,
            }
        except Exception as de:
            logger.error(f"Failed to decrypt credentials in-memory for '{cid}': {de}")
            return None

    def get_trading_mode(
        self,
        client_id: str,
        broker_name: str = "angel_one",
    ) -> str:
        """
        Retrieves the client's current trading mode ('paper' or 'live').
        Strictly defaults to 'paper' if unconfigured or missing.
        """
        try:
            cid = self._require_client_id(client_id)
        except Exception:
            return "paper"

        bname = (broker_name or "angel_one").strip().lower()
        doc_id = f"credentials_{bname}"

        record = None
        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    record = col.find_one({"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]})
        except Exception:
            pass

        if not record:
            with self._lock:
                record = self._cache.get(f"{cid}:{bname}")

        if record and isinstance(record, dict):
            mode = (record.get("trading_mode") or "").strip().lower()
            if mode in ("paper", "live"):
                return mode

        return "paper"

    def set_trading_mode(
        self,
        client_id: str,
        mode: str,
        broker_name: str = "angel_one",
    ) -> bool:
        """
        Sets client's trading mode ('paper' or 'live').
        Persists in Mongo user collection and local cache.
        """
        cid = self._require_client_id(client_id)
        bname = (broker_name or "angel_one").strip().lower()
        mode_val = (mode or "").strip().lower()

        if mode_val not in ("paper", "live"):
            raise ValueError(f"Invalid trading mode '{mode}'. Must be 'paper' or 'live'.")

        now_str = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        doc_id = f"credentials_{bname}"

        # Update in Mongo
        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    col.update_one(
                        {"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]},
                        {"$set": {"trading_mode": mode_val, "updated_at": now_str}},
                        upsert=False,
                    )
        except Exception as me:
            logger.warning(f"MongoDB trading mode update note: {me}")

        # Update local cache
        with self._lock:
            key = f"{cid}:{bname}"
            if key in self._cache:
                self._cache[key]["trading_mode"] = mode_val
                self._cache[key]["updated_at"] = now_str
            else:
                self._cache[key] = {
                    "_id": doc_id,
                    "type": "broker_credentials",
                    "client_id": cid,
                    "broker_name": bname,
                    "trading_mode": mode_val,
                    "is_configured": False,
                    "updated_at": now_str,
                }
            self._save_fallback()

        logger.info(f"Trading mode updated to '{mode_val}' for client '{cid}' (Broker: {bname})")
        return True

    def get_credential_status(
        self,
        client_id: str,
        broker_name: str = "angel_one",
    ) -> Dict[str, Any]:
        """
        Returns non-sensitive configuration status (boolean flags, masked ID, trading_mode).
        Safe for API responses.
        """
        cid = self._require_client_id(client_id)
        bname = (broker_name or "angel_one").strip().lower()
        doc_id = f"credentials_{bname}"

        record = None
        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    record = col.find_one({"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]})
        except Exception:
            pass

        if not record:
            with self._lock:
                record = self._cache.get(f"{cid}:{bname}")

        if not record:
            return {
                "client_id": cid,
                "broker_name": bname,
                "has_credentials": False,
                "masked_client_id": None,
                "trading_mode": "paper",
                "is_configured": False,
                "updated_at": None,
            }

        return {
            "client_id": cid,
            "broker_name": bname,
            "has_credentials": bool(record.get("encrypted_api_key")),
            "masked_client_id": record.get("masked_client_id"),
            "trading_mode": record.get("trading_mode", "paper"),
            "is_configured": bool(record.get("is_configured", False)),
            "updated_at": record.get("updated_at"),
        }

    def delete_credentials(
        self,
        client_id: str,
        broker_name: str = "angel_one",
    ) -> bool:
        """Removes encrypted credentials for client and resets to defaults."""
        cid = self._require_client_id(client_id)
        bname = (broker_name or "angel_one").strip().lower()
        doc_id = f"credentials_{bname}"

        try:
            from mongo_service import mongo_service
            if mongo_service.ensure_connection() and mongo_service.client is not None:
                col = mongo_service.get_user_collection(cid)
                if col is not None:
                    col.delete_one({"$or": [{"_id": doc_id}, {"type": "broker_credentials", "broker_name": bname}]})
        except Exception as me:
            logger.warning(f"MongoDB delete credentials note: {me}")

        with self._lock:
            key = f"{cid}:{bname}"
            self._cache.pop(key, None)
            self._save_fallback()

        logger.info(f"Broker credentials purged for client '{cid}' (Broker: {bname})")
        return True

    @staticmethod
    def _mask_id(plain_id: str) -> str:
        """Masks client_id for safe display: ABC12345 -> AB****45."""
        if not plain_id:
            return ""
        s = plain_id.strip()
        if len(s) <= 4:
            return "****"
        return f"{s[:2]}****{s[-2:]}"


# Global singleton instance
credential_service = CredentialService()
