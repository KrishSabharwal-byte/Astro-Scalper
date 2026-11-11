"""
Cross-Client Order-Safety Validation Service.
Provides fail-closed security guards ensuring no broker order is dispatched
unless authenticated_client_id, run_client_id, and credential_owner_client_id match 100%.
"""

import datetime
import logging
from typing import Dict, Any, Optional

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
logger = logging.getLogger("OrderSafetyService")


class OrderSafetyViolation(Exception):
    """Raised when a cross-client or mismatched order attempt is detected."""
    def __init__(self, message: str, audit_data: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.audit_data = audit_data or {}


def record_security_audit_event(
    client_id: str,
    run_id: str,
    attempted_action: str,
    reason: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Logs and records a high-priority security violation audit event.
    Writes to MongoDB client audit collection and server logs.
    """
    event = {
        "event_type": "SECURITY_ORDER_SAFETY_VIOLATION",
        "severity": "CRITICAL",
        "client_id": client_id,
        "run_id": run_id,
        "attempted_action": attempted_action,
        "reason": reason,
        "details": details or {},
        "timestamp": datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_iso": datetime.datetime.now(IST).isoformat(),
    }

    logger.critical(
        f"🚨 ORDER SAFETY VIOLATION BLOCKED: Client '{client_id}', Run '{run_id}', "
        f"Action '{attempted_action}' - Reason: {reason} | Details: {details}"
    )

    try:
        from mongo_service import mongo_service
        if client_id and client_id.strip():
            mongo_service.insert_audit_log(event, client_id=client_id.strip())
    except Exception as e:
        logger.warning(f"Note writing security audit event to MongoDB: {e}")

    return event


def validate_order_safety(
    client_id: str,
    run_id: str,
    run_owner_client_id: str,
    credential_owner_client_id: str,
    order_params: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Hard validation step executed before any order is sent to a broker.
    Strict fail-closed check:
    1. client_id must be non-empty string.
    2. run_owner_client_id must match client_id.
    3. credential_owner_client_id must match client_id.
    4. order_params must contain valid trading contract and quantity.

    Raises OrderSafetyViolation if any check fails.
    """
    cid = (client_id or "").strip()
    rid = (run_id or "").strip()
    run_owner = (run_owner_client_id or "").strip()
    cred_owner = (credential_owner_client_id or "").strip()
    params = order_params or {}

    # Check 1: Non-empty authenticated client ID
    if not cid:
        audit = record_security_audit_event(
            client_id="ANONYMOUS",
            run_id=rid,
            attempted_action="DISPATCH_ORDER",
            reason="Missing authenticated client_id on order attempt.",
            details=params,
        )
        raise OrderSafetyViolation("Order blocked: Missing authenticated client_id.", audit)

    # Check 2: Run / Slot ownership match
    if run_owner != cid:
        audit = record_security_audit_event(
            client_id=cid,
            run_id=rid,
            attempted_action="DISPATCH_ORDER",
            reason=f"Run owner client mismatch: run is owned by '{run_owner}', but execution requested by '{cid}'.",
            details=params,
        )
        raise OrderSafetyViolation(
            f"Order safety violation: client '{cid}' does not own run '{rid}'.",
            audit,
        )

    # Check 3: Credential ownership match
    if cred_owner != cid:
        audit = record_security_audit_event(
            client_id=cid,
            run_id=rid,
            attempted_action="DISPATCH_ORDER",
            reason=f"Credential owner mismatch: broker session belongs to '{cred_owner}', but order invoked for '{cid}'.",
            details=params,
        )
        raise OrderSafetyViolation(
            f"Order safety violation: broker credentials belong to '{cred_owner}', not '{cid}'.",
            audit,
        )

    # Check 4: Valid contract symbol, token, and quantity
    sym = params.get("symbol")
    token = params.get("token")
    qty = params.get("quantity")
    if not sym or not token:
        audit = record_security_audit_event(
            client_id=cid,
            run_id=rid,
            attempted_action="DISPATCH_ORDER",
            reason="Missing contract symbol or token in order parameters.",
            details=params,
        )
        raise OrderSafetyViolation("Order blocked: Missing contract symbol or token.", audit)

    try:
        qty_int = int(qty)
        if qty_int <= 0:
            raise ValueError()
    except Exception:
        audit = record_security_audit_event(
            client_id=cid,
            run_id=rid,
            attempted_action="DISPATCH_ORDER",
            reason=f"Invalid order quantity '{qty}'. Quantity must be a positive integer.",
            details=params,
        )
        raise OrderSafetyViolation(f"Order blocked: Invalid quantity '{qty}'.", audit)

    return True
