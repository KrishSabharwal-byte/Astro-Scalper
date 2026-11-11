"""
Astro Commodity Futures Scalper — FastAPI Backend
Multi-tenant backend for MCX commodity futures scalping (SILVERM, GOLDM, COPPER).
Features:
- Symmetrical BUY (Long) and SELL (Short) step-down slicing grid engines
- Multi-tenant ClientSession isolation per logged-in user
- COMMODITY MongoDB database with separate collections for each commodity
- Angel One OpenAPI MCX FUTCOM integration with dynamic monthly expiry resolution
- Real-time SSE state streaming to browser terminals
- Astro CSV 3-row cluster evaluation and automated slot dispatching
"""

import asyncio
import datetime
import json
import logging
import os
import threading
from typing import Dict, Any, Optional, List
from dataclasses import asdict
from fastapi import FastAPI, Request, Query, Depends, HTTPException, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from astro_signal_engine import astro_signal_engine, parse_astro_csv
from futures_slice_trading_engine import (
    RunManager,
    ScalperRunConfig,
    ScalperRun,
    COMMODITY_CONFIG,
    normalize_commodity_name,
    recenter_range,
    validate_config,
    IST,
    SLOT_CANONICAL_MAP,
    SLOT_LEGACY_MAP,
    is_market_open_ist,
)
import sys
from angel_one_service import (
    AngelOneFeed,
    ANGEL_CLIENT_ID,
    ANGEL_API_KEY,
    ANGEL_TOTP_SECRET,
    ANGEL_MPIN,
    is_plausible_ltp,
)
from mongo_service import mongo_service
from credential_service import credential_service
from auth_service import (
    auth_service,
    SESSION_COOKIE_NAME,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)
from client_session import ClientSession
from order_safety_service import OrderSafetyViolation

logger = logging.getLogger("CommodityScalperApp")
logging.basicConfig(level=logging.INFO)

from fastapi.middleware.gzip import GZipMiddleware

app = FastAPI(title="Astro Commodity Futures Scalper")

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(OrderSafetyViolation)
async def order_safety_exception_handler(request: Request, exc: OrderSafetyViolation):
    logger.critical(f"Intercepted OrderSafetyViolation: {exc.message}")
    return JSONResponse(
        status_code=403,
        content={
            "status": "error",
            "error_type": "ORDER_SAFETY_VIOLATION",
            "message": exc.message,
            "audit": exc.audit_data,
        },
    )


# -------------------------------------------------------------------
# Multi-Tenant Client Sessions Registry
# -------------------------------------------------------------------
client_sessions: Dict[str, ClientSession] = {}
client_sessions_lock = threading.Lock()


async def eod_supervisor_worker() -> None:
    """
    Continuous background daemon running every 5 seconds.
    Enforces:
    1. At or after 23:30 (11:30 PM IST), squares off all open positions and cuts all ladders.
    2. Keeps all runs completely closed on Saturday and Sunday (weekends).
    3. Guarantees no stale ladders are carried forward to the next day.
    4. Automatically cleans runtime_runs_state.json on disk for offline clients.
    """
    logger.info("[EOD Supervisor] Started background EOD & Weekend supervisor worker.")
    while True:
        try:
            now_ist = datetime.datetime.now(IST)
            is_open, closed_reason = is_market_open_ist(now_ist)

            # If market is closed (at/after 23:30, before 09:00, or weekend)
            if not is_open:
                # 1. Inspect in-memory client sessions
                with client_sessions_lock:
                    active_cids = list(client_sessions.keys())

                for cid in active_cids:
                    with client_sessions_lock:
                        session = client_sessions.get(cid)
                    if session:
                        state_changed = False
                        for r in session.run_manager.runs.values():
                            if r.is_active or r.active_slices or r.grid_ladder:
                                r.check_eod_cutoff(
                                    current_time_ist=now_ist.time(),
                                    current_date=now_ist.date(),
                                )
                                state_changed = True
                        if state_changed:
                            session.persist_state()

                # 2. Inspect on-disk runtime_runs_state.json for offline clients
                state_file = os.path.join(os.path.dirname(__file__), "runtime_runs_state.json")
                if os.path.exists(state_file):
                    try:
                        with open(state_file, "r", encoding="utf-8") as f:
                            disk_data = json.load(f)
                        modified = False
                        for u_id, u_data in disk_data.items():
                            if isinstance(u_data, dict) and "runs" in u_data:
                                for s_id, s_data in u_data["runs"].items():
                                    if (
                                        s_data.get("is_active")
                                        or len(s_data.get("active_slices", [])) > 0
                                        or len(s_data.get("grid_ladder", [])) > 0
                                    ):
                                        s_data["is_active"] = False
                                        s_data["eod_squared_off"] = True
                                        s_data["last_stop_reason"] = "EOD_SQUAREOFF"
                                        s_data["active_slices"] = []
                                        s_data["grid_ladder"] = []
                                        modified = True
                        if modified:
                            with open(state_file, "w", encoding="utf-8") as f:
                                json.dump(disk_data, f, indent=2)
                            logger.info("[EOD Supervisor] Cleaned up offline runs and ladders in runtime_runs_state.json.")
                    except Exception as e:
                        logger.warning(f"[EOD Supervisor] Error inspecting disk state: {e}")
        except Exception as e:
            logger.error(f"[EOD Supervisor] Error in supervisor loop: {e}")
        await asyncio.sleep(5)


@app.on_event("startup")
async def app_startup_event():
    logger.info("Starting Astro Commodity Scalper application and EOD supervisor...")
    asyncio.create_task(eod_supervisor_worker())


def get_client_session(client_id: str) -> ClientSession:
    """Retrieves or lazily initializes an encapsulated ClientSession for client_id."""
    if not client_id or not isinstance(client_id, str) or not client_id.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="CRITICAL: Resolved client_id required for session access.",
        )
    cid = client_id.strip()

    with client_sessions_lock:
        if cid not in client_sessions:
            session = ClientSession(client_id=cid)
            client_sessions[cid] = session
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    session.start_live_feed()
            except RuntimeError:
                pass

        session = client_sessions[cid]
        session.touch()
        return session


def close_client_session(client_id: str) -> None:
    """Closes and tears down a ClientSession on logout."""
    cid = client_id.strip() if client_id else ""
    with client_sessions_lock:
        if cid in client_sessions:
            session = client_sessions.pop(cid)
            session.close()


async def broadcast_state(client_id: str):
    """Broadcasts state through the client's own ClientSession."""
    cid = client_id.strip() if client_id else ""
    with client_sessions_lock:
        session = client_sessions.get(cid)
    if session:
        try:
            session.persist_state()
        except Exception as e:
            logger.warning(f"Error persisting state during broadcast for {cid}: {e}")
        await session.broadcast_state()


# -------------------------------------------------------------------
# Request Models
# -------------------------------------------------------------------
class LoginRequest(BaseModel):
    client_id: str
    password: str


class CreateClientRequest(BaseModel):
    client_id: str
    password: str
    role: Optional[str] = "trader"
    is_active: Optional[bool] = True


class StartRunRequest(BaseModel):
    run_id: Optional[str] = None
    commodity: Optional[str] = None
    trade_side: Optional[str] = None
    product_type: Optional[str] = "INTRADAY"
    range_points: Optional[float] = None
    slicer_count: Optional[int] = None
    slice_interval: Optional[float] = None
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    qty_per_slice_lots: Optional[int] = None
    profit_point: Optional[float] = None
    loss_point: Optional[float] = None
    poll_interval_seconds: Optional[float] = None
    lock_contract_on_entry: Optional[bool] = True
    auto_reresolve: Optional[bool] = False
    gap_fill_mode: Optional[str] = "all_crossed"
    cutoff_time_ist: Optional[str] = "23:30"
    market_open_ist: Optional[str] = "09:00"
    auto_eod_squareoff: Optional[bool] = True
    force: Optional[bool] = False


class SetCommodityRequest(BaseModel):
    commodity: str
    qty_per_slice_lots: Optional[int] = None


class SetTradeSideRequest(BaseModel):
    trade_side: str


class SetProductTypeRequest(BaseModel):
    product_type: str


class TickRequest(BaseModel):
    price: Optional[float] = None
    ltp: Optional[float] = None
    seq_num: Optional[int] = None


class RecenterRequest(BaseModel):
    price: Optional[float] = None


class AutoTriggerRequest(BaseModel):
    enabled: bool
    slot_id: Optional[str] = None


class AngelCredentialsRequest(BaseModel):
    client_id: Optional[str] = None
    client_code: Optional[str] = None
    api_key: str
    totp_secret: str
    mpin: Optional[str] = None
    pin: Optional[str] = None


class TradingModeRequest(BaseModel):
    mode: str


class ClearHistoryRequest(BaseModel):
    commodity: Optional[str] = None


class SwitchRunRequest(BaseModel):
    run_id: str


# -------------------------------------------------------------------
# Auth Helper
# -------------------------------------------------------------------
def get_current_client_id(request: Optional[Request] = None) -> str:
    """Extracts client_id from Authorization Header, Cookie, or query param, falling back to 'admin' in dev."""
    if request is None:
        return "admin"
    auth_header = request.headers.get("Authorization")
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
    if not token:
        token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        token = request.query_params.get("token")

    if token:
        payload = auth_service.verify_session_token(token)
        if payload and payload.get("sub"):
            return payload["sub"]

    return "admin"


# -------------------------------------------------------------------
# Auth Endpoints
# -------------------------------------------------------------------
@app.post("/auth/login")
async def login(req: LoginRequest):
    loop = asyncio.get_running_loop()
    user = await loop.run_in_executor(None, auth_service.authenticate_user, req.client_id, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid client_id or password.")

    token = auth_service.create_session_token(user["client_id"], role=user.get("role", "trader"))

    # Construct and return fast login response immediately (sub-50ms)
    response = JSONResponse({
        "status": "success",
        "client_id": user["client_id"],
        "role": user.get("role", "trader"),
        "token": token,
        "access_token": token,
    })
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        samesite="lax",
    )

    # Lazily warm up session in background thread without blocking response delivery
    async def _warm_session():
        try:
            sess = await loop.run_in_executor(None, get_client_session, user["client_id"])
            sess.start_live_feed()
        except Exception as e:
            logger.debug(f"Background session warmup note: {e}")

    asyncio.create_task(_warm_session())
    return response


@app.post("/auth/logout")
async def logout(request: Request):
    client_id = get_current_client_id(request)
    close_client_session(client_id)
    response = JSONResponse({"status": "success", "message": "Logged out successfully."})
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.get("/auth/me")
async def get_me(request: Request):
    client_id = get_current_client_id(request)
    user = auth_service.get_user(client_id)
    return {
        "client_id": client_id,
        "role": user.get("role", "trader") if user else "trader",
        "is_authenticated": True,
    }


# -------------------------------------------------------------------
# Broker Credential Endpoints
# -------------------------------------------------------------------
@app.post("/credentials/angel_one")
async def save_credentials(req: AngelCredentialsRequest, request: Request):
    client_id = get_current_client_id(request)
    broker_cid = (req.client_id or req.client_code or "").strip()
    mpin = (req.mpin or req.pin or "").strip()
    credential_service.save_broker_credentials(
        client_id=client_id,
        broker_name="angel_one",
        credentials={
            "client_id": broker_cid,
            "api_key": req.api_key.strip(),
            "totp_secret": req.totp_secret.strip(),
            "mpin": mpin,
        },
    )
    session = get_client_session(client_id)
    session.reload_broker_credentials()
    await broadcast_state(client_id)
    return {"status": "success", "message": "Angel One credentials saved."}


@app.get("/credentials/status")
async def get_credential_status(request: Request):
    client_id = get_current_client_id(request)
    return credential_service.get_credential_status(client_id)


@app.post("/credentials/reconnect")
async def reconnect_broker(request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    session.reload_broker_credentials()
    loop = asyncio.get_running_loop()
    login_ok = await loop.run_in_executor(None, lambda: session.angel_feed.login(force=True))
    await broadcast_state(client_id)
    return {
        "status": "success" if login_ok else "error",
        "authenticated": login_ok,
        "error": session.angel_feed.error_message or session.angel_feed.feed_error,
    }


@app.post("/credentials/trading_mode")
async def set_trading_mode(req: TradingModeRequest, request: Request):
    client_id = get_current_client_id(request)
    mode = req.mode.strip().lower()
    if mode not in ("paper", "live"):
        raise HTTPException(status_code=400, detail="Trading mode must be 'paper' or 'live'.")

    credential_service.set_trading_mode(client_id, mode)
    session = get_client_session(client_id)
    session.reload_trading_mode()
    await broadcast_state(client_id)
    return {"status": "success", "trading_mode": mode, "state": session.get_state_payload()}


# -------------------------------------------------------------------
# Commodity Run Management Endpoints
# -------------------------------------------------------------------
@app.post("/runs/active")
async def set_active_run(req: SwitchRunRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    target = req.run_id.strip()
    run = session.run_manager.get_run(target)
    if not run:
        raise HTTPException(status_code=404, detail="Run/Slot not found.")
    session.run_manager.active_run_id = run.run_id
    await broadcast_state(client_id)
    return {
        "status": "success",
        "active_run_id": run.run_id,
        "run": run.get_status(),
        "state": session.get_state_payload(),
    }


@app.get("/market/ltp")
async def get_market_ltp(request: Request, commodity: Optional[str] = Query(None)):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    comm_key = normalize_commodity_name(commodity) if commodity else "SILVERM"

    # Fast-path: Return live cached LTP immediately (<1ms) if already captured by background feed
    cached_ltp = session.angel_feed.latest_ltp_by_comm.get(comm_key) if session.angel_feed else None
    target_contract = session.angel_feed.resolve_future_contract(comm_key) if session.angel_feed else None
    if cached_ltp and is_plausible_ltp(comm_key, cached_ltp):
        return {
            "status": "success",
            "commodity": comm_key,
            "contract": target_contract,
            "ltp": cached_ltp,
            "target_ltp": cached_ltp,
            "cached": True,
            "latest_ltp_by_comm": dict(session.angel_feed.latest_ltp_by_comm),
        }

    loop = asyncio.get_running_loop()

    # Ensure login if credentials configured
    if not session.angel_feed.is_authenticated and session.angel_feed.client_id and session.angel_feed.api_key:
        try:
            await loop.run_in_executor(None, session.angel_feed.login)
        except Exception as e:
            logger.warning(f"Error logging into Angel One: {e}")

    # Fetch batch LTP for MCX commodities
    comm_contracts = {}
    mcx_tokens = []
    for comm in ["SILVERM", "GOLDM", "COPPER", "CRUDEOIL"]:
        contract = session.angel_feed.resolve_future_contract(comm)
        if contract:
            comm_contracts[comm] = contract
            tok = str(contract["token"])
            if tok not in mcx_tokens:
                mcx_tokens.append(tok)

    batch_ltps = {}
    if mcx_tokens:
        try:
            batch_ltps = await loop.run_in_executor(
                None, session.angel_feed.fetch_market_data_batch, {"MCX": mcx_tokens}
            )
        except Exception as e:
            logger.warning(f"Error fetching market data: {e}")

    # Update runs with live LTP
    for s_id, r in session.run_manager.runs.items():
        comm = r.config.commodity
        contract = comm_contracts.get(comm)
        if contract:
            tok = str(contract["token"])
            r.config.contract_symbol = contract["symbol"]
            r.config.contract_token = tok
            r.config.expiry = contract.get("expiry_str")
            fut_ltp = batch_ltps.get(tok)
            if fut_ltp and fut_ltp > 0:
                if is_plausible_ltp(comm, fut_ltp):
                    r.last_ltp = fut_ltp
                    session.angel_feed.latest_ltp_by_comm[comm] = fut_ltp
                    r.update_feed_health(True)
                    logger.info(f"[LTP_TRACE] endpoint=/market/ltp commodity={comm} slot={s_id} token={tok} fut_ltp={fut_ltp}")
                else:
                    logger.error(f"[LTP_TRACE] [SANITY_GUARD_REJECT] endpoint=/market/ltp commodity={comm} slot={s_id} token={tok} rejected_fut_ltp={fut_ltp}")

    await session.broadcast_state()

    target_contract = comm_contracts.get(comm_key)
    target_tok = str(target_contract["token"]) if target_contract else None
    target_ltp = batch_ltps.get(target_tok) if target_tok else session.angel_feed.latest_ltp_by_comm.get(comm_key)
    if target_ltp and not is_plausible_ltp(comm_key, target_ltp):
        logger.error(f"[LTP_TRACE] [SANITY_GUARD_REJECT] endpoint=/market/ltp target_ltp rejected for {comm_key}: {target_ltp}")
        target_ltp = None

    logger.info(f"[LTP_TRACE] endpoint=/market/ltp comm_key={comm_key} target_tok={target_tok} target_ltp={target_ltp} latest_ltp_by_comm={session.angel_feed.latest_ltp_by_comm}")

    return {
        "status": "success",
        "commodity": comm_key,
        "contract": target_contract,
        "ltp": target_ltp,
        "batch_ltps": batch_ltps,
        "latest_ltp_by_comm": session.angel_feed.latest_ltp_by_comm,
    }


@app.post("/runs/{run_id}/commodity")
async def set_commodity(run_id: str, req: SetCommodityRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    old_comm = run.config.commodity
    comm = normalize_commodity_name(req.commodity)
    cfg_defaults = COMMODITY_CONFIG.get(comm, COMMODITY_CONFIG["SILVERM"])

    run.config.commodity = comm
    # Strict isolation: if slot changed commodity, previous last_ltp is invalid
    if old_comm != comm:
        logger.info(f"[LTP_TRACE] Commodity switched on slot {run_id} from {old_comm} to {comm}. Discarding old last_ltp ({run.last_ltp}).")
        run.last_ltp = None

    valid_run_ltp = run.last_ltp if (run.last_ltp and is_plausible_ltp(comm, run.last_ltp)) else None
    feed_ltp = session.angel_feed.latest_ltp_by_comm.get(comm)
    valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(comm, feed_ltp)) else None

    live_ltp = valid_run_ltp or valid_feed_ltp or cfg_defaults.get("default_ltp", 90000.0)
    logger.info(f"[LTP_TRACE] commodity={comm} source=set_commodity slot={run_id} valid_run_ltp={valid_run_ltp} valid_feed_ltp={valid_feed_ltp} chosen_live_ltp={live_ltp}")
    span = cfg_defaults.get("default_range_points", 2000.0)
    step = cfg_defaults.get("default_step", 250.0)
    new_high, new_low = recenter_range(
        current_price=live_ltp,
        span=span,
        step=step,
        trade_side=run.config.trade_side,
    )
    run.config.range_high = new_high
    run.config.range_low = new_low
    run.config.range_points = span
    run.config.slicer_count = cfg_defaults.get("default_slicer_count", int(round(span / step)))
    run.config.slice_interval = step
    run.config.profit_point = cfg_defaults["default_profit_point"]
    run.config.loss_point = cfg_defaults["default_loss_point"]
    if req.qty_per_slice_lots is not None:
        run.config.qty_per_slice_lots = req.qty_per_slice_lots
    elif not run.is_active or old_comm != comm:
        run.config.qty_per_slice_lots = cfg_defaults.get("default_qty_per_slice_lots", 1)
    run._generate_grid()

    await broadcast_state(client_id)
    return {"status": "success", "commodity": comm, "run_id": run_id}


@app.post("/runs/{run_id}/trade_side")
async def set_trade_side(run_id: str, req: SetTradeSideRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    side = req.trade_side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Trade side must be 'BUY' or 'SELL'.")

    run.config.trade_side = side
    comm = run.config.commodity
    cfg_defaults = COMMODITY_CONFIG.get(comm, COMMODITY_CONFIG["SILVERM"])
    valid_run_ltp = run.last_ltp if (run.last_ltp and is_plausible_ltp(comm, run.last_ltp)) else None
    feed_ltp = session.angel_feed.latest_ltp_by_comm.get(comm)
    valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(comm, feed_ltp)) else None
    live_ltp = valid_run_ltp or valid_feed_ltp or cfg_defaults.get("default_ltp", 90000.0)
    logger.info(f"[LTP_TRACE] commodity={comm} source=set_trade_side slot={run_id} valid_run_ltp={valid_run_ltp} valid_feed_ltp={valid_feed_ltp} chosen_live_ltp={live_ltp}")
    span = run.config.range_points or run.config.original_span
    step = run.config.slice_interval
    new_high, new_low = recenter_range(
        current_price=live_ltp,
        span=span,
        step=step,
        trade_side=side,
    )
    run.config.range_high = new_high
    run.config.range_low = new_low
    run._generate_grid()
    await broadcast_state(client_id)
    return {"status": "success", "trade_side": side, "run_id": run_id}


@app.post("/runs/{run_id}/product_type")
async def set_product_type(run_id: str, req: SetProductTypeRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    ptype = req.product_type.upper()
    if ptype not in ("INTRADAY", "CARRYFORWARD"):
        raise HTTPException(status_code=400, detail="Product type must be 'INTRADAY' or 'CARRYFORWARD'.")

    run.config.product_type = ptype
    await broadcast_state(client_id)
    return {"status": "success", "product_type": ptype, "run_id": run_id}


@app.post("/runs/{run_id}/start")
async def start_run(run_id: str, req: Optional[StartRunRequest] = None, request: Request = None):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    # Validate MCX market open hours (09:00 to 23:30 IST, Monday-Friday)
    now_ist = datetime.datetime.now(IST)
    is_open, closed_reason = is_market_open_ist(now_ist)
    is_testing = os.environ.get("ENV") == "testing" or "pytest" in sys.modules or (req and req.force)
    if not is_open and not is_testing:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot start scalper. MCX Commodity Market is closed ({closed_reason}). Scalper runs and ladders can only be opened Monday-Friday between 09:00 and 23:30 IST.",
        )

    custom_explicit_bounds = False
    if req:
        if req.commodity:
            run.config.commodity = normalize_commodity_name(req.commodity)
        if req.trade_side:
            run.config.trade_side = req.trade_side.upper()
        if req.product_type:
            run.config.product_type = req.product_type.upper()
        if req.range_points is not None:
            run.config.range_points = req.range_points
        if req.slicer_count is not None:
            run.config.slicer_count = req.slicer_count
            if req.range_points is not None and req.slicer_count > 0:
                run.config.slice_interval = round(req.range_points / req.slicer_count, 2)
        elif req.slice_interval is not None:
            run.config.slice_interval = req.slice_interval
        if req.profit_point is not None:
            run.config.profit_point = req.profit_point
        if req.loss_point is not None:
            run.config.loss_point = req.loss_point
        if req.qty_per_slice_lots is not None:
            run.config.qty_per_slice_lots = req.qty_per_slice_lots
        if req.auto_eod_squareoff is not None:
            run.config.auto_eod_squareoff = req.auto_eod_squareoff
        if req.cutoff_time_ist:
            run.config.cutoff_time_ist = req.cutoff_time_ist

        if req.range_high is not None and req.range_low is not None and req.range_high > req.range_low:
            run.config.range_high = req.range_high
            run.config.range_low = req.range_low
            custom_explicit_bounds = True

    comm = run.config.commodity
    cfg_defaults = COMMODITY_CONFIG.get(comm, COMMODITY_CONFIG["SILVERM"])
    if req and req.qty_per_slice_lots is not None:
        run.config.qty_per_slice_lots = req.qty_per_slice_lots
    elif not req or req.qty_per_slice_lots is None:
        run.config.qty_per_slice_lots = cfg_defaults.get("default_qty_per_slice_lots", 1)
    valid_run_ltp = run.last_ltp if (run.last_ltp and is_plausible_ltp(comm, run.last_ltp)) else None
    feed_ltp = session.angel_feed.latest_ltp_by_comm.get(comm)
    valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(comm, feed_ltp)) else None
    live_ltp = valid_run_ltp or valid_feed_ltp or cfg_defaults.get("default_ltp", 90000.0)
    logger.info(f"[LTP_TRACE] commodity={comm} source=start_run slot={run_id} valid_run_ltp={valid_run_ltp} valid_feed_ltp={valid_feed_ltp} chosen_live_ltp={live_ltp}")

    if not custom_explicit_bounds:
        span = run.config.range_points or run.config.original_span
        step = run.config.slice_interval
        new_high, new_low = recenter_range(
            current_price=live_ltp,
            span=span,
            step=step,
            trade_side=run.config.trade_side,
        )
        run.config.range_high = new_high
        run.config.range_low = new_low

    run._generate_grid()
    run.start()

    session.run_manager.record_slot_event(
        slot_id=run.slot_id,
        event_type="START",
        signal=run.config.trade_side,
        price=live_ltp,
        quantity=run.config.total_slice_quantity,
        details={
            "commodity": run.config.commodity,
            "trade_side": run.config.trade_side,
            "range_high": run.config.range_high,
            "range_low": run.config.range_low,
            "live_anchor_price": live_ltp,
        },
    )

    await broadcast_state(client_id)
    return {
        "status": "success",
        "run": run.get_status(),
        "state": session.get_state_payload(),
    }


@app.post("/runs/{run_id}/stop")
async def stop_run(run_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    exited = run.stop()
    await broadcast_state(client_id)
    return {
        "status": "success",
        "exited_trades": len(exited),
        "run": run.get_status(),
        "state": session.get_state_payload(),
    }


@app.post("/runs/{run_id}/squareoff")
async def squareoff_run(run_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    exited = run.eod_squareoff()
    await broadcast_state(client_id)
    return {
        "status": "success",
        "exited_count": len(exited),
        "run": run.get_status(),
        "state": session.get_state_payload(),
    }


@app.post("/runs/{run_id}/exit_slice")
async def exit_slice(run_id: str, request: Request, identifier: Optional[str] = Query(None)):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    rec = run.exit_slice(slice_identifier=identifier)
    await broadcast_state(client_id)
    if rec:
        return {
            "status": "success",
            "trade": asdict(rec),
            "run": run.get_status(),
            "state": session.get_state_payload(),
        }
    return {"status": "error", "message": "Slice not found or already exited."}


@app.post("/runs/{run_id}/recenter")
async def recenter_ladder(
    run_id: str,
    request: Request,
    req: Optional[RecenterRequest] = None,
):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    comm = run.config.commodity
    cfg_defaults = COMMODITY_CONFIG.get(comm, COMMODITY_CONFIG["SILVERM"])
    req_price = (req.price if req and req.price and is_plausible_ltp(comm, req.price) else None)
    valid_run_ltp = run.last_ltp if (run.last_ltp and is_plausible_ltp(comm, run.last_ltp)) else None
    feed_ltp = session.angel_feed.latest_ltp_by_comm.get(comm)
    valid_feed_ltp = feed_ltp if (feed_ltp and is_plausible_ltp(comm, feed_ltp)) else None
    price = req_price or valid_run_ltp or valid_feed_ltp or cfg_defaults.get("default_ltp", 90000.0)
    logger.info(f"[LTP_TRACE] commodity={comm} source=recenter slot={run_id} chosen_price={price}")

    new_high, new_low = recenter_range(
        current_price=price,
        span=run.config.original_span,
        step=run.config.slice_interval,
        trade_side=run.config.trade_side,
    )
    run.config.range_high = new_high
    run.config.range_low = new_low
    run._generate_grid()

    await broadcast_state(client_id)
    return {
        "status": "success",
        "range_high": new_high,
        "range_low": new_low,
        "run": run.get_status(),
        "state": session.get_state_payload(),
    }


@app.post("/runs/{run_id}/tick")
async def inject_tick(run_id: str, req: TickRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    price = req.price if req.price is not None else req.ltp
    if price is None:
        raise HTTPException(status_code=400, detail="Missing price or ltp in tick request.")

    actions = run.process_tick(price, seq_num=req.seq_num)
    await broadcast_state(client_id)
    return {"status": "success", "actions": actions, "run": run.get_status()}


@app.post("/runs/{run_id}/auto_trigger")
async def set_auto_trigger(run_id: str, req: AutoTriggerRequest, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    target_slot = req.slot_id or run_id

    if target_slot == "all":
        session.astro_auto_trigger["all"] = req.enabled
        for s in session.astro_auto_trigger:
            session.astro_auto_trigger[s] = req.enabled
    else:
        canonical = SLOT_CANONICAL_MAP.get(target_slot, target_slot)
        session.astro_auto_trigger[canonical] = req.enabled

    await broadcast_state(client_id)
    return {"status": "success", "auto_trigger": session.astro_auto_trigger}


@app.post("/runs/{run_id}/clear_history")
async def clear_run_history(run_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    run = session.run_manager.get_run(run_id)
    if run:
        run.clear_history()
        canonical = SLOT_CANONICAL_MAP.get(run_id, run_id)
        session.run_manager.master_trade_history = [
            t for t in session.run_manager.master_trade_history
            if t.get("run_id") != canonical
            and t.get("run_id") != run_id
            and t.get("slot_id") != canonical
            and not (t.get("commodity") == run.config.commodity and t.get("trade_side") == run.config.trade_side)
        ]
    await broadcast_state(client_id)
    return {"status": "success", "message": f"History cleared for slot {run_id}."}


# -------------------------------------------------------------------
# Trade History Endpoints (Scoped to COMMODITY database collections)
# -------------------------------------------------------------------
@app.get("/history")
async def get_history(
    request: Request,
    commodity: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    limit: int = Query(2000),
):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    db_trades = mongo_service.get_recent_trades(
        client_id=client_id,
        limit=limit,
        mode=mode,
        commodity=commodity,
    )
    return {"status": "success", "trades": db_trades, "count": len(db_trades)}


@app.get("/history/summary")
async def get_history_summary(
    request: Request,
    period: str = Query("day"),
    date: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    target_mode = mode.strip().lower() if mode and mode.strip().lower() in ("paper", "live") else None
    in_memory = session.run_manager.get_all_trade_history() if session and session.run_manager else []

    summary = mongo_service.get_pnl_summary(
        client_id=client_id,
        period=period,
        ref_date=date,
        mode=target_mode,
        in_memory_trades=in_memory,
    )
    return JSONResponse(
        content={
            "status": "success",
            "period": summary.get("period", period),
            "range": summary.get("range", {}),
            "by_instrument": summary.get("by_instrument", []),
            "by_slot": summary.get("by_slot", []),
            "total": summary.get("total", {}),
        },
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/history/clear")
async def clear_history(
    request: Request,
    req: Optional[ClearHistoryRequest] = None,
    commodity: Optional[str] = Query(None),
):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    target_commodity = (req.commodity if req and req.commodity else None) or commodity
    mongo_service.clear_trades(client_id=client_id, commodity=target_commodity)
    session.run_manager.master_trade_history.clear()
    for r in session.run_manager.runs.values():
        if not target_commodity or r.config.commodity == normalize_commodity_name(target_commodity):
            r.clear_history()

    await broadcast_state(client_id)
    return {"status": "success", "message": "Trade history cleared."}


@app.delete("/history/trade/{trade_id}")
async def delete_single_trade(trade_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    deleted = mongo_service.delete_trade(client_id=client_id, trade_id=trade_id)
    session.run_manager.master_trade_history = [
        t for t in session.run_manager.master_trade_history if t.get("trade_id") != trade_id and str(t.get("_id")) != trade_id
    ]
    session.run_manager.sync_runs_from_history()

    await broadcast_state(client_id)
    return {"status": "success", "deleted": deleted, "trade_id": trade_id}


# -------------------------------------------------------------------
# Astro CSV Report Endpoints
# -------------------------------------------------------------------
@app.post("/astro/upload")
async def upload_astro_file(request: Request, file: UploadFile = File(...)):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    try:
        content_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read uploaded file: {e}")

    if not content_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        df, meta = parse_astro_csv(content_bytes)
    except Exception as e:
        logger.warning(f"Error parsing uploaded Astro CSV: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid Astro CSV: {e}")

    # Decode string for database storage
    content_str = None
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            content_str = content_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if content_str is None:
        content_str = content_bytes.decode("utf-8", errors="replace")

    filename = file.filename or "astro_report.csv"
    file_id = mongo_service.save_astro_file(
        client_id=client_id,
        filename=filename,
        content=content_str,
        row_count=meta["row_count"],
        is_active=True,
    )
    eval_res = {}
    try:
        eval_res = await session.evaluate_fixed_slots(active_astro_content=content_str)
        await broadcast_state(client_id)
    except Exception as e:
        logger.warning(f"Error evaluating astro cluster after upload: {e}")

    return {
        "status": "success",
        "file_id": file_id,
        "filename": filename,
        "row_count": meta["row_count"],
        "metadata": meta,
        "evaluation": eval_res,
        "auto_trigger_by_slot": dict(session.astro_auto_trigger),
    }


@app.get("/astro/files")
async def list_astro_files(request: Request):
    client_id = get_current_client_id(request)
    files = mongo_service.list_astro_files(client_id=client_id)
    return {"status": "success", "files": files}


@app.post("/astro/activate/{file_id}")
async def activate_astro_file(file_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    activated = mongo_service.set_active_astro_file(client_id=client_id, file_id=file_id)
    if not activated:
        raise HTTPException(status_code=404, detail="File not found.")

    eval_res = await session.evaluate_fixed_slots()
    await broadcast_state(client_id)
    return {
        "status": "success",
        "active_file_id": file_id,
        "evaluation": eval_res,
        "auto_trigger_by_slot": dict(session.astro_auto_trigger),
    }


@app.get("/state")
async def get_client_state_endpoint(request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    return session.get_state_payload()


@app.get("/astro/state")
async def get_astro_state_endpoint(request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)
    payload = session.get_state_payload()
    return {"status": "success", "astro_state": payload.get("astro_state", {})}


@app.delete("/astro/files/{file_id}")
async def delete_astro_file(file_id: str, request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    deleted = mongo_service.delete_astro_file(client_id=client_id, file_id=file_id)
    eval_res = await session.evaluate_fixed_slots()
    await broadcast_state(client_id)
    return {"status": "success", "deleted": deleted, "file_id": file_id, "evaluation": eval_res}


class AstroPreviewRequest(BaseModel):
    content: Optional[str] = None


@app.post("/astro/preview")
async def preview_astro_cluster(request: Request, req: Optional[AstroPreviewRequest] = None):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    preview_rows = []
    content_str = req.content if req and req.content else None
    if content_str:
        try:
            df, meta = parse_astro_csv(content_str)
            preview_rows = df.head(10).to_dict(orient="records")
        except Exception:
            pass

    eval_summary = await session.evaluate_fixed_slots(active_astro_content=content_str)
    return {
        "status": "success",
        "preview": eval_summary,
        "preview_rows": preview_rows,
    }


# -------------------------------------------------------------------
# Real-Time SSE Stream Endpoint
# -------------------------------------------------------------------
@app.get("/events")
async def sse_events(request: Request):
    client_id = get_current_client_id(request)
    session = get_client_session(client_id)

    queue = asyncio.Queue()
    session.sse_subscribers.append(queue)

    # Trigger immediate state push
    asyncio.create_task(session.broadcast_state())

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                data = await queue.get()
                yield data
        except asyncio.CancelledError:
            pass
        finally:
            if queue in session.sse_subscribers:
                session.sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# -------------------------------------------------------------------
# Static UI Assets
# -------------------------------------------------------------------
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(
            os.path.join(static_dir, "index.html"),
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3006))
    uvicorn.run(app, host="0.0.0.0", port=port)
