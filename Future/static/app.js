/**
 * Astro Commodity Futures Scalper — Cockpit Client v1.0
 * Connects to /events SSE stream for live state
 * Handles 6 commodity slots (SILVERM-B/S, GOLDM-B/S, COPPER-B/S)
 * Symmetrical long/short ladder rendering, MCX futures pricing & execution
 */

let currentState = null;
let eventSource = null;
let activeSlotId = 'SILVERM-B';
let currentCommodity = 'SILVERM';
let currentTradeSide = 'BUY';
let currentProductType = 'INTRADAY';

// Guard: tracks slots whose auto-trigger toggle is in-flight (pending server confirmation).
// While a slot is in this set, SSE renders will NOT overwrite the toggle's optimistic value.
const pendingAutoTriggerSlots = new Set();

const COMMODITY_CONFIG = {
  SILVERM: {
    name: 'SILVERM',
    displayName: 'Silver Micro',
    lotSize: 1,
    unit: 'kg',
    tickSize: 1.0,
    defaultLtp: 90000,
    defaultRangePoints: 2000,
    defaultSlicerCount: 8,
    defaultRangeHigh: 91000,
    defaultRangeLow: 89000,
    defaultStep: 250,
    defaultProfitPoint: 250,
    defaultLossPoint: 375,
    defaultQtyPerSliceLots: 1,
  },
  GOLDM: {
    name: 'GOLDM',
    displayName: 'Gold Mini',
    lotSize: 10,
    unit: 'g',
    tickSize: 1.0,
    defaultLtp: 75000,
    defaultRangePoints: 500,
    defaultSlicerCount: 5,
    defaultRangeHigh: 75250,
    defaultRangeLow: 74750,
    defaultStep: 100,
    defaultProfitPoint: 100,
    defaultLossPoint: 150,
    defaultQtyPerSliceLots: 1,
  },
  COPPER: {
    name: 'COPPER',
    displayName: 'Copper',
    lotSize: 250,
    unit: 'kg',
    tickSize: 0.05,
    defaultLtp: 850,
    defaultRangePoints: 60.0,
    defaultSlicerCount: 6,
    defaultRangeHigh: 880.0,
    defaultRangeLow: 820.0,
    defaultStep: 10.0,
    defaultProfitPoint: 10.0,
    defaultLossPoint: 15.0,
    defaultQtyPerSliceLots: 1,
  },
  CRUDEOIL: {
    name: 'CRUDEOIL',
    displayName: 'Crude Oil Mini',
    lotSize: 10,
    unit: 'bbl',
    tickSize: 1.0,
    defaultLtp: 6000,
    defaultRangePoints: 200,
    defaultSlicerCount: 8,
    defaultRangeHigh: 6100,
    defaultRangeLow: 5900,
    defaultStep: 25,
    defaultProfitPoint: 25,
    defaultLossPoint: 37.5,
    defaultQtyPerSliceLots: 1,
  },
};

const COMMODITY_PLAUSIBLE_RANGES = {
  SILVERM: [40000, 500000],
  GOLDM: [40000, 300000],
  COPPER: [400, 3500],
  CRUDEOIL: [1000, 25000],
};

function isPlausibleLtp(commodity, price) {
  if (typeof price !== 'number' || isNaN(price) || price <= 0) return false;
  const comm = (commodity || '').toUpperCase();
  const rng = COMMODITY_PLAUSIBLE_RANGES[comm];
  if (!rng) return price > 0;
  return price >= rng[0] && price <= rng[1];
}


const ALL_SLOTS = [
  { id: 'SILVERM-B', commodity: 'SILVERM', side: 'BUY', label: 'SILVERM BUY', badge: 'SILVERM-B', color: 'badge-call' },
  { id: 'SILVERM-S', commodity: 'SILVERM', side: 'SELL', label: 'SILVERM SELL', badge: 'SILVERM-S', color: 'badge-put' },
  { id: 'GOLDM-B', commodity: 'GOLDM', side: 'BUY', label: 'GOLDM BUY', badge: 'GOLDM-B', color: 'badge-call' },
  { id: 'GOLDM-S', commodity: 'GOLDM', side: 'SELL', label: 'GOLDM SELL', badge: 'GOLDM-S', color: 'badge-put' },
  { id: 'COPPER-B', commodity: 'COPPER', side: 'BUY', label: 'COPPER BUY', badge: 'COPPER-B', color: 'badge-call' },
  { id: 'COPPER-S', commodity: 'COPPER', side: 'SELL', label: 'COPPER SELL', badge: 'COPPER-S', color: 'badge-put' },
  { id: 'CRUDEOIL-B', commodity: 'CRUDEOIL', side: 'BUY', label: 'CRUDEOIL BUY', badge: 'CRUDEOIL-B', color: 'badge-call' },
  { id: 'CRUDEOIL-S', commodity: 'CRUDEOIL', side: 'SELL', label: 'CRUDEOIL SELL', badge: 'CRUDEOIL-S', color: 'badge-put' },
];

const SLOT_MAP = {
  'run01': 'SILVERM-B',
  'run02': 'SILVERM-S',
  'run03': 'GOLDM-B',
  'run04': 'GOLDM-S',
  'run05': 'COPPER-B',
  'run06': 'COPPER-S',
  'run07': 'CRUDEOIL-B',
  'run08': 'CRUDEOIL-S',
  'SILVERM-B': 'SILVERM-B',
  'SILVERM-S': 'SILVERM-S',
  'GOLDM-B': 'GOLDM-B',
  'GOLDM-S': 'GOLDM-S',
  'COPPER-B': 'COPPER-B',
  'COPPER-S': 'COPPER-S',
  'CRUDEOIL-B': 'CRUDEOIL-B',
  'CRUDEOIL-S': 'CRUDEOIL-S',
};

const SLOT_NUMBERS = {
  'SILVERM-B': 'Slot 1',
  'SILVERM-S': 'Slot 2',
  'GOLDM-B': 'Slot 3',
  'GOLDM-S': 'Slot 4',
  'COPPER-B': 'Slot 5',
  'COPPER-S': 'Slot 6',
  'CRUDEOIL-B': 'Slot 7',
  'CRUDEOIL-S': 'Slot 8',
};

function getTradeCommodity(t) {
  if (!t) return '';
  if (t.commodity) {
    const c = String(t.commodity).toUpperCase().trim();
    if (c === 'SILVERM' || c === 'GOLDM' || c === 'COPPER' || c === 'CRUDEOIL') return c;
    if (c.includes('SILVER')) return 'SILVERM';
    if (c.includes('GOLD')) return 'GOLDM';
    if (c.includes('COPPER')) return 'COPPER';
    if (c.includes('CRUDE')) return 'CRUDEOIL';
  }
  const r = t.run_id || t.slot_id;
  const canonical = (typeof SLOT_MAP !== 'undefined' && SLOT_MAP[r]) ? SLOT_MAP[r] : r;
  if (canonical) {
    const cUpper = String(canonical).toUpperCase().trim();
    if (cUpper.includes('SILVERM') || cUpper === 'RUN01' || cUpper === 'RUN02') return 'SILVERM';
    if (cUpper.includes('GOLDM') || cUpper === 'RUN03' || cUpper === 'RUN04') return 'GOLDM';
    if (cUpper.includes('COPPER') || cUpper === 'RUN05' || cUpper === 'RUN06') return 'COPPER';
    if (cUpper.includes('CRUDEOIL') || cUpper === 'RUN07' || cUpper === 'RUN08') return 'CRUDEOIL';
  }
  if (t.contract_symbol) {
    const sym = String(t.contract_symbol).toUpperCase();
    if (sym.includes('SILVER')) return 'SILVERM';
    if (sym.includes('GOLD')) return 'GOLDM';
    if (sym.includes('COPPER')) return 'COPPER';
    if (sym.includes('CRUDE')) return 'CRUDEOIL';
  }
  return '';
}

function getTradeDateStr(t) {
  if (!t) return '';
  // Check exit/closed timestamp first, then created/timestamp, then entry
  const raw = t.exit_time || t.exited_at || t.created_at || t.timestamp_utc || t.timestamp || t.filled_at || t.entry_time || '';
  if (!raw) return '';
  const match = String(raw).match(/(\d{4}-\d{2}-\d{2})/);
  if (match) return match[1];
  try {
    const d = new Date(raw);
    if (!isNaN(d.getTime())) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      return `${y}-${m}-${day}`;
    }
  } catch (e) {}
  return '';
}

function getTodayDateStr(state) {
  if (state && state.timestamp) {
    const match = String(state.timestamp).match(/(\d{4}-\d{2}-\d{2})/);
    if (match) return match[1];
  }
  // Default to today in IST
  try {
    const istDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata' }).format(new Date());
    if (istDate && /^\d{4}-\d{2}-\d{2}$/.test(istDate)) return istDate;
  } catch (e) {}
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function isTradeToday(t, state) {
  const tDate = getTradeDateStr(t);
  if (!tDate) return false;
  const today = getTodayDateStr(state);
  return tDate === today;
}



// ── Authentication & Session Management ────────────────────────
const AUTH_TOKEN_KEY = 'slicer_jwt_token';
const AUTH_CLIENT_ID_KEY = 'slicer_client_id';

function getAuthToken() {
  try {
    return sessionStorage.getItem(AUTH_TOKEN_KEY) || '';
  } catch (e) {
    return '';
  }
}

function setAuthToken(token, clientId) {
  try {
    sessionStorage.setItem(AUTH_TOKEN_KEY, token);
    if (clientId) sessionStorage.setItem(AUTH_CLIENT_ID_KEY, clientId);
  } catch (e) { }
  updateUserBadge();
}

function removeAuthToken() {
  try {
    sessionStorage.removeItem(AUTH_TOKEN_KEY);
    sessionStorage.removeItem(AUTH_CLIENT_ID_KEY);
  } catch (e) { }
  updateUserBadge();
}

function getStoredClientId() {
  try {
    return sessionStorage.getItem(AUTH_CLIENT_ID_KEY) || 'admin';
  } catch (e) {
    return 'admin';
  }
}

function updateUserBadge() {
  const badge = document.getElementById('userSessionBadge');
  const userLabel = document.getElementById('userBadgeClientId');
  const signInBtn = document.getElementById('signInHeaderBtn');
  const token = getAuthToken();
  const clientId = getStoredClientId();
  if (token && clientId && badge && userLabel) {
    badge.style.display = 'inline-flex';
    userLabel.innerText = clientId;
    if (signInBtn) signInBtn.style.display = 'none';
  } else {
    if (badge) badge.style.display = 'none';
    if (signInBtn) signInBtn.style.display = 'inline-flex';
  }
}

// ── Trading Mode State ─────────────────────────────────
let currentClientTradingMode = 'paper';
let currentLedgerTab = 'slices';
let currentLedgerModeFilter = 'all';
let currentLedgerCommFilter = 'ALL';

function updateTradingModeUI(mode) {
  currentClientTradingMode = (mode || 'paper').toLowerCase();
  const banner = document.getElementById('topModeBanner');
  const beacon = document.getElementById('modeBannerBeacon');
  const title = document.getElementById('modeBannerTitle');
  const sub = document.getElementById('modeBannerSub');
  const btn = document.getElementById('btnToggleTradingMode');
  const hudPill = document.getElementById('hudModePill');
  const hudPillText = document.getElementById('hudModePillText');

  if (currentClientTradingMode === 'live') {
    if (banner) banner.className = 'top-mode-banner mode-banner-live';
    if (beacon) beacon.innerText = '🔴';
    if (title) title.innerText = 'LIVE MCX TRADING ACTIVE — REAL MONEY AT RISK';
    if (sub) sub.innerText = '(Angel One SmartAPI Live Execution on MCX · Fail-Closed Active)';
    if (btn) {
      btn.innerText = 'Switch to PAPER (Simulation)';
      btn.className = 'btn-mode-toggle to-paper';
    }
    if (hudPill) hudPill.className = 'hud-mode-pill live';
    if (hudPillText) hudPillText.innerText = '🔴 LIVE (REAL)';
  } else {
    if (banner) banner.className = 'top-mode-banner mode-banner-paper';
    if (beacon) beacon.innerText = '🟢';
    if (title) title.innerText = 'PAPER TRADING MODE — Zero-Risk Simulation';
    if (sub) sub.innerText = '(Market Data Active · Orders Simulated with SmartAPI LTP · No Real Capital at Risk)';
    if (btn) {
      btn.innerText = 'Switch to LIVE (Real Money)';
      btn.className = 'btn-mode-toggle to-live';
    }
    if (hudPill) hudPill.className = 'hud-mode-pill paper';
    if (hudPillText) hudPillText.innerText = '🟢 PAPER MODE';
  }
}

function handleModeSwitchClick() {
  if (currentClientTradingMode === 'paper') {
    promptSwitchToLive();
  } else {
    executeSwitchToPaper();
  }
}

function promptSwitchToLive() {
  const modal = document.getElementById('liveConfirmModal');
  const input = document.getElementById('liveConfirmInput');
  const submitBtn = document.getElementById('btnSubmitLiveConfirm');
  const errorPill = document.getElementById('liveModalError');
  if (modal) modal.style.display = 'flex';
  if (input) {
    input.value = '';
    input.focus();
  }
  if (submitBtn) submitBtn.disabled = true;
  if (errorPill) errorPill.style.display = 'none';
}

function closeLiveConfirmModal() {
  const modal = document.getElementById('liveConfirmModal');
  if (modal) modal.style.display = 'none';
}

function onLiveConfirmInputChange(val) {
  const submitBtn = document.getElementById('btnSubmitLiveConfirm');
  if (submitBtn) {
    submitBtn.disabled = (val.trim() !== 'LIVE');
  }
}

async function executeSwitchToLive() {
  updateTradingModeUI('live');
  closeLiveConfirmModal();
  try {
    const res = await fetch('/credentials/trading_mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({ mode: 'live' }),
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error switching to live:', e);
  }
}

async function executeSwitchToPaper() {
  updateTradingModeUI('paper');
  try {
    const res = await fetch('/credentials/trading_mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({ mode: 'paper' }),
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error switching to paper:', e);
  }
}

async function fetchState() {
  try {
    const headers = {};
    const token = getAuthToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const res = await fetch('/state', { headers });
    if (res.ok) {
      const data = await res.json();
      currentState = data;
      renderAll(data);
    }
  } catch (e) {
    console.warn('Error fetching client state:', e);
  }
}

// ── SSE Stream Connection ─────────────────────────────────
function connectEventStream() {
  if (eventSource) {
    eventSource.close();
  }
  const token = getAuthToken();
  const url = token ? `/events?token=${encodeURIComponent(token)}` : '/events';

  eventSource = new EventSource(url);

  eventSource.onopen = () => {
    const connEl = document.getElementById('connectionStatus');
    if (connEl) {
      connEl.className = 'hud-stream-pill connected';
      connEl.innerHTML = '<span class="stream-ping-dot pulse-indicator"></span><span class="stream-label status-text">Live MCX</span>';
    }
  };

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      currentState = data;
      renderAll(data);
      const histLen = (data.history || []).length;
      if (lastKnownHistoryLen !== histLen) {
        lastKnownHistoryLen = histLen;
        fetchPnlSummary();
      }
    } catch (e) {
      console.error('Error parsing SSE event:', e);
    }
  };

  eventSource.onerror = () => {
    const connEl = document.getElementById('connectionStatus');
    if (connEl) {
      connEl.className = 'hud-stream-pill disconnected';
      connEl.innerHTML = '<span class="stream-ping-dot" style="background:#ef4444;"></span><span class="stream-label status-text">Reconnecting...</span>';
    }
  };
}

// ── Slot Resolution Helper (Guarantees no fluctuating or cross-slot bleed) ──
function getSelectedSlotRun(state) {
  if (!state) return null;
  const targetId = activeSlotId;
  if (state.runs) {
    if (Array.isArray(state.runs)) {
      const r = state.runs.find(x => (x.slot_id === targetId || x.run_id === targetId));
      if (r) return r;
    } else if (state.runs[targetId]) {
      return state.runs[targetId];
    }
  }
  if (state.active_run && (state.active_run.slot_id === targetId || state.active_run.run_id === targetId)) {
    return state.active_run;
  }
  const slotObj = ALL_SLOTS.find(s => s.id === targetId);
  const matrixData = (state.slots_matrix || {})[targetId] || {};
  if (slotObj) {
    const cfg = COMMODITY_CONFIG[slotObj.commodity] || COMMODITY_CONFIG.SILVERM;
    return {
      slot_id: targetId,
      run_id: targetId,
      commodity: slotObj.commodity,
      trade_side: slotObj.side,
      is_active: matrixData.is_active || false,
      grid_ladder: [],
      active_slices: [],
      trade_history: [],
      total_pnl: matrixData.total_pnl || 0,
      config: {
        commodity: slotObj.commodity,
        trade_side: slotObj.side,
        range_points: cfg.defaultRangePoints,
        slicer_count: cfg.defaultSlicerCount,
        profit_point: cfg.defaultProfitPoint,
        loss_point: cfg.defaultLossPoint,
        qty_per_slice_lots: cfg.defaultQtyPerSliceLots || 1,
        cutoff_time_ist: '23:30',
        auto_eod_squareoff: true,
      },
    };
  }
  return state.active_run || {};
}

// ── Core Rendering Logic ─────────────────────────────────
function renderAll(state) {
  if (!state) return;

  if (state.trading_mode) {
    updateTradingModeUI(state.trading_mode);
  }

  renderFeedErrorBanner(state);
  renderSlotTabBar(state);
  renderTelemetryHUD(state);
  renderActiveRun(state);
  renderLadder(state);
  renderOpenSlices(state);
  renderAstroSection(state);
  render6SlotsMatrix(state);
  renderHistoryLedger(state);
}

function renderFeedErrorBanner(state) {
  const banner = document.getElementById('liveFeedErrorBanner');
  const title = document.getElementById('feedErrorTitle');
  const sub = document.getElementById('feedErrorSub');
  const icon = document.getElementById('feedErrorIcon');
  if (!banner) return;

  const feedErr = state.feed_error || (state.angel_feed && state.angel_feed.feed_error);
  const isEscalated = state.feed_error_escalated || (state.angel_feed && state.angel_feed.feed_error_escalated);
  const persistedSec = Math.round(state.feed_error_persisted_sec || (state.angel_feed && state.angel_feed.feed_error_persisted_sec) || 0);

  if (feedErr) {
    banner.style.display = 'block';
    const isRateLimit = feedErr.toLowerCase().includes('rate') ||
                        feedErr.toLowerCase().includes('exceeding') ||
                        feedErr.toLowerCase().includes('access denied');
    const retryBtn = document.getElementById('btnRetryFeedLogin');
    if (isRateLimit) {
      banner.className = 'feed-error-banner';
      if (icon) icon.innerText = '⏳';
      if (title) title.innerText = 'Angel One Feed Pacing: Serving cached prices';
      if (sub) sub.innerText = `Temporary API rate throttle (${persistedSec}s) · Request pacing active. Live stream resuming automatically.`;
      if (retryBtn) retryBtn.style.display = 'none';
    } else if (isEscalated) {
      banner.className = 'feed-error-banner escalated';
      if (icon) icon.innerText = '🚨';
      if (title) title.innerText = `CRITICAL FEED OUTAGE: ${feedErr}`;
      if (sub) sub.innerText = `Persisting for ${persistedSec}s across cycles · Action Required: Ensure MCX Commodity Segment is enabled in Angel One portal for this API key.`;
      if (retryBtn) retryBtn.style.display = 'inline-block';
    } else {
      banner.className = 'feed-error-banner';
      if (icon) icon.innerText = '⚠️';
      if (title) title.innerText = `Live Feed Error: ${feedErr} — Auto-retrying session`;
      if (sub) sub.innerText = `Persisting for ${persistedSec}s · Retrying broker token authentication in background...`;
      if (retryBtn) retryBtn.style.display = 'inline-block';
    }
  } else {
    banner.style.display = 'none';
  }
}

async function triggerManualFeedRetry() {
  const btn = document.getElementById('btnRetryFeedLogin');
  if (btn) {
    btn.innerText = '⏳ Connecting...';
    btn.disabled = true;
  }
  try {
    const res = await fetch('/credentials/reconnect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (data.status === 'success' && data.authenticated) {
      if (btn) btn.innerText = '✅ Reconnected!';
      setTimeout(() => {
        if (btn) {
          btn.innerText = '🔄 Re-Authenticate Feed';
          btn.disabled = false;
        }
      }, 2000);
    } else {
      if (btn) {
        btn.innerText = '❌ Failed (Retry)';
        btn.disabled = false;
      }
    }
  } catch (e) {
    console.error('Error in triggerManualFeedRetry:', e);
    if (btn) {
      btn.innerText = '❌ Network Error';
      btn.disabled = false;
    }
  }
}

function renderSlotTabBar(state) {
  const container = document.getElementById('slotTabBar');
  if (!container) return;

  const matrix = (state.slots_matrix || {});
  const history = (state.history || []);
  let html = '';

  ALL_SLOTS.forEach(slot => {
    const sData = matrix[slot.id] || {};
    const isActive = (slot.id === activeSlotId);
    const isRunning = sData.is_active || false;
    const sideClass = slot.side === 'BUY' ? 'tab-buy' : 'tab-sell';
    const statusClass = isRunning ? 'running' : (sData.slot_status === 'CLOSED' ? 'closed' : 'empty');

    // Overall P/L for this particular slot (all-time realized + floating unrealized)
    const slotTrades = history.filter(t => {
      const tRun = t.run_id || t.slot_id;
      if (tRun && (tRun === slot.id || SLOT_MAP[tRun] === slot.id)) return true;
      if (t.commodity === slot.commodity && t.trade_side === slot.side) return true;
      return false;
    });

    let slotRealized = 0;
    slotTrades.forEach(t => {
      slotRealized += parseFloat(t.pnl_rupees || 0);
    });
    if (slotRealized === 0 && sData.realized_pnl) {
      slotRealized = parseFloat(sData.realized_pnl || 0);
    }

    // Today's P/L for this slot
    const todaySlotTrades = slotTrades.filter(t => isTradeToday(t, state));
    let slotTodayRealized = 0;
    todaySlotTrades.forEach(t => {
      slotTodayRealized += parseFloat(t.pnl_rupees || 0);
    });
    if (slotTodayRealized === 0 && sData.today_realized_pnl) {
      slotTodayRealized = parseFloat(sData.today_realized_pnl || 0);
    }

    const uPnl = parseFloat(sData.unrealized_pnl || 0);
    const pnlVal = slotRealized;
    const pnlSign = pnlVal >= 0 ? '+' : '';
    const todaySign = slotTodayRealized >= 0 ? '+' : '';
    const todayColor = slotTodayRealized > 0 ? 'var(--neon-green,#10b981)' : (slotTodayRealized < 0 ? 'var(--neon-red,#ef4444)' : 'var(--text-muted,#94a3b8)');

    html += `
      <button class="slot-tab-btn ${isActive ? 'active' : ''} ${sideClass} ${statusClass}" onclick="switchSlot('${slot.id}')" title="${slot.label}: Today P&L: ₹${slotTodayRealized.toFixed(2)} | Overall P&L: ₹${pnlVal.toFixed(2)}${uPnl !== 0 ? ` (Floating: ₹${uPnl.toFixed(2)})` : ''}">
        <div class="tab-top">
          <span class="tab-slot-badge ${slot.color}">${slot.badge}</span>
          <span class="tab-status-dot ${isRunning ? 'pulse' : ''}"></span>
        </div>
        <div class="tab-main">
          <span class="tab-label">${slot.label}</span>
          <div class="tab-pnl-group" style="display:flex; flex-direction:column; align-items:flex-end; gap:1px;">
            <span class="tab-today-pnl" style="font-size:0.7rem; font-weight:800; color:${todayColor}">Today: ${todaySign}₹${slotTodayRealized.toFixed(0)}</span>
            <span class="tab-pnl ${pnlVal >= 0 ? 'pos' : 'neg'}" style="font-size:0.65rem; opacity:0.85;">Total: ${pnlSign}₹${pnlVal.toFixed(0)}</span>
          </div>
        </div>
      </button>
    `;
  });

  container.innerHTML = html;
}

function renderTelemetryHUD(state) {
  const activeRun = getSelectedSlotRun(state) || {};
  const activeConfig = activeRun.config || {};
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId) || { label: activeSlotId, commodity: currentCommodity, side: currentTradeSide };
  const comm = slotObj.commodity || currentCommodity;
  const cfg = COMMODITY_CONFIG[comm] || COMMODITY_CONFIG.SILVERM;

  const angelFeed = state.angel_feed || {};
  const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || state.latest_ltp_by_comm || {};
  const commLtp = commLtpMap[comm];
  const validActiveLtp = (activeConfig.commodity === comm && isPlausibleLtp(comm, activeRun.last_ltp)) ? activeRun.last_ltp : null;
  const validCommLtp = isPlausibleLtp(comm, commLtp) ? commLtp : null;
  // Strictly per-commodity for activeSlotId
  const ltpVal = validActiveLtp || validCommLtp || cfg.defaultLtp;
  const isStale = !validActiveLtp && !validCommLtp;

  console.log(`[LTP_TRACE] UI renderTelemetryHUD commodity=${comm} validActiveLtp=${validActiveLtp} validCommLtp=${validCommLtp} chosen=${ltpVal} isStale=${isStale}`);

  const ltpEl = document.getElementById('metricLiveLtp');
  if (ltpEl) {
    if (typeof ltpVal === 'number' && ltpVal > 0) {
      ltpEl.innerText = `₹${ltpVal.toFixed(2)}`;
      ltpEl.style.opacity = isStale ? '0.6' : '1.0';
    } else {
      ltpEl.innerText = '--';
    }
  }
  const ltpSub = document.getElementById('metricLiveLtpSub');
  if (ltpSub) {
    ltpSub.innerText = isStale ? `${cfg.displayName} Futures (Default Anchor)` : `${cfg.displayName} Futures LTP`;
  }

  // Slot P&L & Trade Counts for Active Slot
  const slotTag = document.getElementById('metricSlotTag');
  if (slotTag) slotTag.innerText = `SLOT P&L [${activeSlotId}]`;

  const history = state.history || [];
  const matrix = state.slots_matrix || {};
  const sData = matrix[activeSlotId] || {};

  // Filter history strictly for the currently active slot
  const slotTrades = history.filter(t => {
    const tRun = t.run_id || t.slot_id;
    if (tRun && (tRun === activeSlotId || SLOT_MAP[tRun] === activeSlotId)) return true;
    if (t.commodity === slotObj.commodity && t.trade_side === slotObj.side) return true;
    return false;
  });

  // Overall trade history P&L for this slot
  let slotOverallRealizedPnl = 0;
  slotTrades.forEach(t => {
    slotOverallRealizedPnl += parseFloat(t.pnl_rupees || 0);
  });
  if (slotOverallRealizedPnl === 0 && sData.realized_pnl) {
    slotOverallRealizedPnl = parseFloat(sData.realized_pnl || 0);
  }

  // Today's trades only for this slot
  const todaySlotTrades = slotTrades.filter(t => isTradeToday(t, state));
  let slotTodayRealizedPnl = 0;
  todaySlotTrades.forEach(t => {
    slotTodayRealizedPnl += parseFloat(t.pnl_rupees || 0);
  });
  if (slotTodayRealizedPnl === 0 && sData.today_realized_pnl) {
    slotTodayRealizedPnl = parseFloat(sData.today_realized_pnl || 0);
  }
  const todaySlotCount = todaySlotTrades.length || (sData.today_trade_count || 0);

  // Overall portfolio today P&L across all slots
  let allSlotsTodayRealizedPnl = 0;
  let allSlotsTodayCount = 0;
  history.forEach(t => {
    if (isTradeToday(t, state)) {
      allSlotsTodayRealizedPnl += parseFloat(t.pnl_rupees || 0);
      allSlotsTodayCount++;
    }
  });
  if (allSlotsTodayCount === 0) {
    Object.values(matrix).forEach(mSlot => {
      allSlotsTodayRealizedPnl += parseFloat(mSlot.today_realized_pnl || 0);
      allSlotsTodayCount += parseInt(mSlot.today_trade_count || 0, 10);
    });
  }

  // Render Overall Slot P&L
  const slotOverallPnl = document.getElementById('metricSlotOverallPnl');
  if (slotOverallPnl) {
    slotOverallPnl.innerText = `${slotOverallRealizedPnl >= 0 ? '+' : ''}₹${slotOverallRealizedPnl.toFixed(2)}`;
    slotOverallPnl.style.color = slotOverallRealizedPnl >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  const slotOverallCount = document.getElementById('metricSlotOverallCount');
  if (slotOverallCount) {
    slotOverallCount.innerText = `${slotTrades.length} Closed`;
  }

  // Render Today's Slot P&L (strictly today's trades)
  const slotTodayPnl = document.getElementById('metricSlotTodayPnl');
  if (slotTodayPnl) {
    slotTodayPnl.innerText = `${slotTodayRealizedPnl >= 0 ? '+' : ''}₹${slotTodayRealizedPnl.toFixed(2)}`;
    slotTodayPnl.style.color = slotTodayRealizedPnl > 0 ? 'var(--neon-green, #10b981)' : (slotTodayRealizedPnl < 0 ? 'var(--neon-red, #ef4444)' : 'var(--neon-cyan, #00f0ff)');
  }
  const slotTodayCount = document.getElementById('metricSlotTodayCount');
  if (slotTodayCount) {
    if (allSlotsTodayCount > todaySlotCount) {
      const allSign = allSlotsTodayRealizedPnl >= 0 ? '+' : '';
      slotTodayCount.innerText = `${todaySlotCount} Today (All: ${allSign}₹${allSlotsTodayRealizedPnl.toFixed(0)})`;
      slotTodayCount.title = `Active Slot Today: ₹${slotTodayRealizedPnl.toFixed(2)} (${todaySlotCount} closed) | All Slots Today: ₹${allSlotsTodayRealizedPnl.toFixed(2)} (${allSlotsTodayCount} closed)`;
    } else {
      slotTodayCount.innerText = `${todaySlotCount} Today`;
    }
  }

  // Gross Realized for the Active Slot's Commodity (combining both BUY and SELL)
  const grossTag = document.getElementById('metricGrossTag');
  if (grossTag) {
    grossTag.innerText = `OVERALL REALIZED [${comm}]`;
  }

  const grossPaper = document.getElementById('metricGrossPaperPnl');
  const grossLive = document.getElementById('metricGrossLivePnl');
  const grossPaperCount = document.getElementById('metricGrossPaperCount');
  const grossLiveCount = document.getElementById('metricGrossLiveCount');
  let paperTot = 0, liveTot = 0, paperCnt = 0, liveCnt = 0;

  history.forEach(t => {
    if (getTradeCommodity(t) === comm) {
      const p = parseFloat(t.pnl_rupees || 0);
      if ((t.mode || 'paper') === 'live') {
        liveTot += p;
        liveCnt++;
      } else {
        paperTot += p;
        paperCnt++;
      }
    }
  });

  if (grossPaper) {
    grossPaper.innerText = `${paperTot >= 0 ? '+' : ''}₹${paperTot.toFixed(2)}`;
    grossPaper.style.color = paperTot >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  if (grossPaperCount) grossPaperCount.innerText = `${paperCnt} Closed`;
  if (grossLive) {
    grossLive.innerText = `${liveTot >= 0 ? '+' : ''}₹${liveTot.toFixed(2)}`;
    grossLive.style.color = liveTot >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  if (grossLiveCount) grossLiveCount.innerText = `${liveCnt} Closed`;


  // Unrealized P&L
  const unEl = document.getElementById('metricUnrealizedPnl');
  if (unEl) {
    const u = activeRun.unrealized_pnl_rupees || 0;
    unEl.innerText = `${u >= 0 ? '+' : ''}₹${u.toFixed(2)}`;
    unEl.style.color = u >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  const openCount = document.getElementById('metricOpenCount');
  if (openCount) {
    const cnt = (activeRun.active_slices || []).length;
    openCount.innerText = `${cnt} Open Positions`;
  }

  // Span / Step
  const spanStep = document.getElementById('metricSpanStep');
  if (spanStep) {
    const isCorrupted = !activeRun.is_active && activeConfig.slice_interval && Math.abs(activeConfig.slice_interval - cfg.defaultStep) > 0.01;
    const span = (!isCorrupted && activeConfig.range_points) ? activeConfig.range_points : cfg.defaultRangePoints;
    const step = (!isCorrupted && activeConfig.slice_interval) ? activeConfig.slice_interval : cfg.defaultStep;
    spanStep.innerText = `${span.toFixed(1)} pts / ${step.toFixed(1)} pts`;
  }
  const levelsEl = document.getElementById('metricTotalLevels');
  if (levelsEl) {
    const totalLevels = (activeRun.grid_ladder || []).length || (activeConfig.slicer_count || cfg.defaultSlicerCount || 8);
    levelsEl.innerText = `${totalLevels} Total Levels`;
  }

  // Stop Loss
  const slEl = document.getElementById('metricLossTrigger');
  if (slEl) {
    const sl = activeRun.loss_trigger_price;
    const openSlices = activeRun.active_slices || [];
    if (openSlices.length > 0 && sl) {
      slEl.innerText = `₹${sl.toFixed(2)}`;
    } else {
      slEl.innerText = 'None (No Positions)';
    }
  }
}

function renderActiveRun(state) {
  const activeRun = getSelectedSlotRun(state) || {};
  const activeConfig = activeRun.config || {};
  const config = activeConfig;

  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId);
  if (slotObj) {
    currentCommodity = slotObj.commodity;
    currentTradeSide = slotObj.side;
  } else {
    currentCommodity = config.commodity || currentCommodity;
    currentTradeSide = config.trade_side || currentTradeSide;
  }
  currentProductType = config.product_type || currentProductType;

  const commSelect = document.getElementById('commoditySelect');
  if (commSelect && commSelect.value !== currentCommodity) {
    commSelect.value = currentCommodity;
  }
  const sideSelect = document.getElementById('tradeSideSelect');
  if (sideSelect && sideSelect.value !== currentTradeSide) {
    sideSelect.value = currentTradeSide;
  }
  const prodSelect = document.getElementById('productTypeSelect');
  if (prodSelect && prodSelect.value !== currentProductType) {
    prodSelect.value = currentProductType;
  }

  const slotChanged = (lastSyncedSlotId !== activeSlotId);
  lastSyncedSlotId = activeSlotId;
  syncFormInputs(activeConfig, currentCommodity, slotChanged);

  const symbolText = document.getElementById('symbolText');
  if (symbolText) {
    symbolText.innerText = activeRun.contract_symbol || `${currentCommodity} FUT`;
  }
  const resSymbol = document.getElementById('resContractSymbol');
  if (resSymbol) {
    resSymbol.innerText = activeRun.contract_symbol || `${currentCommodity} FUT`;
  }

  const cfg = COMMODITY_CONFIG[currentCommodity] || COMMODITY_CONFIG.SILVERM;
  const lotTag = document.getElementById('lotUnitTag');
  if (lotTag) {
    lotTag.innerText = `lots (${cfg.lotSize} ${cfg.unit})`;
  }
  const resDesc = document.getElementById('resContractVal');
  if (resDesc) {
    resDesc.innerText = `Lot Size: ${cfg.lotSize} ${cfg.unit} · Tick: ₹${cfg.tickSize}`;
  }

  // Update Live Futures LTP display in Radar card
  const angelFeed = state.angel_feed || {};
  const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || state.latest_ltp_by_comm || {};
  const commLtp = commLtpMap[currentCommodity];
  const validCommLtp = isPlausibleLtp(currentCommodity, commLtp) ? commLtp : null;
  const validActiveLtp = (activeRun.config && activeRun.config.commodity === currentCommodity && isPlausibleLtp(currentCommodity, activeRun.last_ltp)) ? activeRun.last_ltp : null;
  // CRITICAL FIX: Strictly per-commodity. NEVER fall back to state.last_ltp!
  const ltpVal = validCommLtp || validActiveLtp || cfg.defaultLtp;
  const isFeedStale = !validCommLtp && !validActiveLtp;

  console.log(`[LTP_TRACE] UI renderRadarConfig commodity=${currentCommodity} validCommLtp=${validCommLtp} validActiveLtp=${validActiveLtp} chosen=${ltpVal} isFeedStale=${isFeedStale}`);

  const resOptLtp = document.getElementById('resOptLtpVal');
  if (resOptLtp) {
    if (typeof ltpVal === 'number' && ltpVal > 0) {
      resOptLtp.innerText = `₹${ltpVal.toFixed(2)}${isFeedStale ? ' (Standby)' : ''}`;
      resOptLtp.style.color = isFeedStale ? '#f59e0b' : '';
    } else {
      resOptLtp.innerText = '--';
    }
  }
  const optLtpInput = document.getElementById('optLtpInput');
  if (optLtpInput && !optLtpInput.value && typeof ltpVal === 'number' && ltpVal > 0) {
    optLtpInput.placeholder = `e.g. ${ltpVal.toFixed(2)}`;
  }

  const btnRun = document.getElementById('btnRunSlicer');
  const btnEod = document.getElementById('btnEodSquareoff');
  const ladderNote = document.getElementById('ladderNote');

  const rHigh = (activeConfig.range_high || cfg.defaultRangeHigh || 0).toFixed(1);
  const rLow = (activeConfig.range_low || cfg.defaultRangeLow || 0).toFixed(1);
  const cycleNum = activeRun.cycle_count || activeRun.run_number || 1;

  if (activeRun.is_active) {
    if (btnRun) {
      btnRun.className = 'btn-cyber-launch btn-stop-slicer';
      btnRun.innerHTML = '<span class="launch-icon">⏹</span><span class="launch-text">STOP SCALPER</span>';
      btnRun.onclick = () => stopSlicer();
    }
    if (btnEod) btnEod.style.display = 'inline-flex';
    if (ladderNote) {
      ladderNote.innerText = `Cycle #${cycleNum} (${rHigh} → ${rLow})`;
      ladderNote.style.color = '#10b981';
    }
  } else {
    if (btnRun) {
      btnRun.className = 'btn-cyber-launch btn-run-slicer';
      btnRun.innerHTML = '<span class="launch-icon">🚀</span><span class="launch-text">RUN SCALPER</span>';
      btnRun.onclick = () => runSlicer();
    }
    if (btnEod) btnEod.style.display = 'none';
    if (ladderNote) {
      ladderNote.innerText = `Cycle #${cycleNum} (${rHigh} → ${rLow}) · Scalper Stopped`;
      ladderNote.style.color = '#94a3b8';
    }
  }

  const ladderSideTag = document.getElementById('ladderSideTag');
  if (ladderSideTag) {
    ladderSideTag.innerText = currentTradeSide === 'SELL' ? 'SELL LADDER (SHORT)' : 'BUY LADDER (LONG)';
    ladderSideTag.className = currentTradeSide === 'SELL' ? 'hud-badge-tag rose-badge' : 'hud-badge-tag blue-badge';
  }
}

function renderLadder(state) {
  const container = document.getElementById('ladderContainer');
  if (!container) return;

  const activeRun = getSelectedSlotRun(state) || {};
  const activeConfig = activeRun.config || {};
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId);
  const comm = slotObj ? slotObj.commodity : (activeConfig.commodity || currentCommodity);
  const cfg = COMMODITY_CONFIG[comm] || COMMODITY_CONFIG.SILVERM;
  const ladder = activeRun.grid_ladder || [];
  const angelFeed = state.angel_feed || {};
  const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || state.latest_ltp_by_comm || {};
  const validActiveLtp = (activeConfig.commodity === comm && isPlausibleLtp(comm, activeRun.last_ltp)) ? activeRun.last_ltp : null;
  const validCommLtp = isPlausibleLtp(comm, commLtpMap[comm]) ? commLtpMap[comm] : null;
  const currentLtp = validActiveLtp || validCommLtp || cfg.defaultLtp;
  const isSell = (slotObj ? slotObj.side : (activeRun.trade_side || currentTradeSide)) === 'SELL';
  const isActive = Boolean(activeRun.is_active || (activeRun.active_slices && activeRun.active_slices.length > 0));

  if (!isActive) {
    const isCorrupted = activeConfig.slice_interval && Math.abs(activeConfig.slice_interval - cfg.defaultStep) > 0.01;
    const spanVal = (!isCorrupted && activeConfig.range_points) ? activeConfig.range_points : cfg.defaultRangePoints;
    const stepVal = (!isCorrupted && activeConfig.slice_interval) ? activeConfig.slice_interval : cfg.defaultStep;
    const ltpDisplay = (typeof currentLtp === 'number' && currentLtp > 0) ? `₹${currentLtp.toFixed(2)}` : '--';

    container.innerHTML = `
      <div class="ladder-empty-placeholder ladder-standby-view">
        <div class="empty-radar-scan"></div>
        <div class="standby-badge-tag ${currentTradeSide === 'SELL' ? 'tag-sell' : 'tag-buy'}">${currentTradeSide === 'SELL' ? 'SELL (SHORT) LADDER' : 'BUY (LONG) LADDER'} — STANDBY</div>
        <h4 class="standby-title">Ladder Gated // Awaiting ${currentTradeSide} Signal</h4>
        <p class="standby-desc">
          Grid ladder opens dynamically anchored to the live spot price (<strong>${ltpDisplay}</strong>) when a <strong>${currentTradeSide}</strong> signal is approved or when the Scalper is launched.
        </p>
        <div class="standby-meta-pills">
          <span class="hud-stat-chip"><span class="chip-label">Instrument:</span> <span class="chip-val">${cfg.displayName}</span></span>
          <span class="hud-stat-chip"><span class="chip-label">Live Spot Anchor:</span> <span class="chip-val chip-live-ltp">${ltpDisplay}</span></span>
          <span class="hud-stat-chip"><span class="chip-label">Span:</span> <span class="chip-val">${spanVal} pts</span></span>
          <span class="hud-stat-chip"><span class="chip-label">Step:</span> <span class="chip-val">${stepVal} pts</span></span>
        </div>
      </div>
    `;
    return;
  }

  if (!ladder || ladder.length === 0) {
    container.innerHTML = `
      <div class="ladder-empty-placeholder">
        <div class="empty-radar-scan"></div>
        <p>No grid levels active. Click <strong>🚀 Run Scalper</strong> to initialize live levels.</p>
      </div>
    `;
    return;
  }

  const activeSlices = activeRun.active_slices || [];
  let html = `<div class="ladder-deck-wrapper">`;

  ladder.forEach(lvl => {
    const price = lvl.level_price;
    const matchedSlice = activeSlices.find(s => Math.abs((s.level_price || 0) - price) < 0.01);
    const status = matchedSlice ? 'FILLED' : (lvl.status || 'UNUSED');
    const isFilled = status === 'FILLED';
    const isUsed = status === 'USED' || status === 'EXITED';
    const label = (matchedSlice && matchedSlice.label) || lvl.label || (isFilled ? 'A' : '');
    const fillPrice = matchedSlice ? (matchedSlice.fill_price || matchedSlice.level_price) : (lvl.fill_price || price);
    const profitTarget = matchedSlice ? matchedSlice.profit_target : (lvl.profit_target || (isSell ? price - (activeConfig.profit_point || 100) : price + (activeConfig.profit_point || 100)));
    const isNearLtp = currentLtp > 0 && Math.abs(currentLtp - price) <= ((lvl.step || activeConfig.slice_interval || 100) / 2);

    if (isFilled) {
      html += `
        <div class="ladder-deck-row row-filled">
          <div class="deck-row-left">
            <span class="rung-bullet bullet-filled"></span>
            <span class="deck-price price-filled">${price.toFixed(2)}</span>
            <span class="deck-fill-info">${isSell ? `Level: ₹${price.toFixed(2)} · Filled: ₹${fillPrice.toFixed(2)} → Cover Target: ₹${profitTarget.toFixed(2)} (slippage-adjusted from fill)` : `Level: ₹${price.toFixed(2)} · Filled: ₹${fillPrice.toFixed(2)} → Exit Target: ₹${profitTarget.toFixed(2)} (slippage-adjusted from fill)`}</span>
          </div>
          <div class="deck-row-right">
            <span class="deck-badge badge-filled">[${label}] FILLED</span>
          </div>
        </div>
      `;
    } else if (isUsed) {
      html += `
        <div class="ladder-deck-row row-used">
          <div class="deck-row-left">
            <span class="rung-bullet bullet-used"></span>
            <span class="deck-price price-used">${price.toFixed(2)}</span>
            ${lvl.exit_price ? `<span class="deck-exit-info">Exited @ ${lvl.exit_price.toFixed(2)}</span>` : ''}
          </div>
          <div class="deck-row-right">
            <span class="deck-badge badge-used">[${label || 'A'}] USED</span>
          </div>
        </div>
      `;
    } else {
      html += `
        <div class="ladder-deck-row row-unused ${isNearLtp ? 'row-near-ltp' : ''}">
          <div class="deck-row-left">
            <span class="rung-bullet bullet-unused"></span>
            <span class="deck-price price-unused">${price.toFixed(2)}</span>
          </div>
          <div class="deck-row-right">
            <span class="deck-status-text">UNUSED</span>
          </div>
        </div>
      `;
    }
  });

  // Range Stop-Loss line at bottom
  const slPrice = activeRun.range_stop_loss || activeRun.loss_trigger_price || (isSell ? ((activeConfig.range_high || 0) + (activeConfig.loss_point || 150)) : Math.max(0, ((activeConfig.range_low || 0) - (activeConfig.loss_point || 150))));

  html += `
    <div class="range-stop-loss-deck">
      <div class="sl-deck-left">
        <span class="sl-title">▲ RANGE STOP-LOSS</span>
      </div>
      <div class="sl-deck-right">
        <span class="sl-price-val">₹${slPrice.toFixed(2)}</span>
      </div>
    </div>
  </div>`;

  container.innerHTML = html;
}

function renderOpenSlices(state) {
  const tbody = document.getElementById('openSlicesTableBody');
  const badge = document.getElementById('openSlicesBadge');
  if (!tbody) return;

  const activeRun = getSelectedSlotRun(state) || {};
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId);
  const slices = activeRun.active_slices || [];
  const comm = slotObj ? slotObj.commodity : (activeRun.commodity || currentCommodity);
  const angelFeed = state.angel_feed || {};
  const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || state.latest_ltp_by_comm || {};
  const currentLtp = activeRun.last_ltp || commLtpMap[comm] || 0;
  const isSell = (slotObj ? slotObj.side : (activeRun.trade_side || currentTradeSide)) === 'SELL';

  if (badge) badge.innerText = `${slices.length} Open`;

  if (!slices || slices.length === 0) {
    tbody.innerHTML = `
      <tr class="empty-table-row empty-row">
        <td colspan="9">No active futures slice positions. Run Scalper to trigger levels.</td>
      </tr>
    `;
    return;
  }

  let html = '';
  slices.forEach(s => {
    const fill = s.fill_price || s.level_price;
    const target = s.profit_target || 0;
    const pnlPts = isSell ? (fill - currentLtp) : (currentLtp - fill);
    const pnlRup = pnlPts * s.quantity;
    const distPts = isSell ? (currentLtp - target) : (target - currentLtp);

    html += `
      <tr>
        <td><span class="slice-label-pill">${s.label || 'A'}</span></td>
        <td>₹${s.level_price.toFixed(2)}</td>
        <td>₹${fill.toFixed(2)}</td>
        <td>₹${target.toFixed(2)} <span style="font-size:0.65rem;color:var(--text-muted);display:block;">(from fill)</span></td>
        <td>${s.quantity}</td>
        <td style="color:${pnlRup >= 0 ? 'var(--neon-green,#10b981)' : 'var(--neon-red,#ef4444)'};font-weight:700;">
          ${pnlRup >= 0 ? '+' : ''}₹${pnlRup.toFixed(2)} (${pnlPts >= 0 ? '+' : ''}${pnlPts.toFixed(2)} pts)
        </td>
        <td>${distPts.toFixed(2)} pts</td>
        <td><span class="rung-status-badge status-filled">FILLED</span></td>
        <td style="text-align:center;">
          <button class="btn-slice-exit" onclick="exitSpecificSlice('${s.order_id || s.label}')">EXIT</button>
        </td>
      </tr>
    `;
  });

  tbody.innerHTML = html;
}

function renderAstroSection(state) {
  const astroState = state.astro_state || {};
  const preview = astroState.cluster_preview || {};
  const dropzoneWrapper = document.getElementById('astroUploadDropzoneWrapper');
  const activeFileCard = document.getElementById('astroActiveFileCard');
  const fileName = document.getElementById('astroActiveFileName');
  const fileMeta = document.getElementById('astroActiveFileMeta');
  const statusBadge = document.getElementById('astroStatusBadge');
  const clusterTime = document.getElementById('astroClusterTime');
  const hasActive = Boolean(astroState.has_active_file);

  if (hasActive) {
    if (dropzoneWrapper) dropzoneWrapper.style.display = 'none';
    if (activeFileCard) activeFileCard.style.display = 'flex';
    if (fileName) fileName.innerText = astroState.filename || 'astro_report.csv';
    const uploadTimeStr = astroState.uploaded_at ? ` · Uploaded ${astroState.uploaded_at}` : '';
    if (fileMeta) fileMeta.innerText = `${astroState.row_count || 0} signal rows active in COMMODITY.astro_files${uploadTimeStr}`;
    if (statusBadge) {
      statusBadge.innerText = `Astro: Active (${astroState.row_count || 0} rows)`;
      statusBadge.className = 'hud-badge-tag emerald-badge badge badge-astro active';
    }
    if (clusterTime) {
      clusterTime.innerText = preview.timestamp ? `Evaluated @ ${preview.timestamp}` : `Active (${astroState.row_count || 0} rows)`;
    }
  } else {
    if (dropzoneWrapper) dropzoneWrapper.style.display = 'block';
    if (activeFileCard) activeFileCard.style.display = 'none';
    if (statusBadge) {
      statusBadge.innerText = 'Astro: Standby (No File)';
      statusBadge.className = 'hud-badge-tag badge badge-astro no-file';
    }
    if (clusterTime) {
      clusterTime.innerText = 'No Active CSV';
    }
  }

  const clusterWindow = (hasActive && preview.window) ? preview.window : [];
  for (let i = 0; i < 3; i++) {
    const chipTime = document.getElementById(`clusterTime${i}`);
    const chipDir = document.getElementById(`clusterDir${i}`);
    if (hasActive && clusterWindow[i]) {
      const row = clusterWindow[i];
      if (chipTime) chipTime.innerText = row.time || '--:--';
      if (chipDir) {
        chipDir.innerText = (row.normalized || 'NEUTRAL').toUpperCase();
        chipDir.className = `node-tag chip-dir dir-${row.normalized || 'neutral'}`;
      }
    } else if (hasActive) {
      if (chipTime) chipTime.innerText = '--:--';
      if (chipDir) {
        chipDir.innerText = 'WAITING';
        chipDir.className = 'node-tag chip-dir dir-neutral';
      }
    } else {
      if (chipTime) chipTime.innerText = '--:--';
      if (chipDir) {
        chipDir.innerText = 'NO FILE';
        chipDir.className = 'node-tag chip-dir dir-empty';
      }
    }
  }

  const consensusBanner = document.getElementById('astroConsensusBanner');
  const consensusIcon = document.getElementById('astroConsensusIcon');
  const consensusLabel = document.getElementById('astroConsensusLabel');
  const sigName = preview.signal_name || 'NEUTRAL';

  if (consensusBanner && consensusIcon && consensusLabel) {
    if (!hasActive) {
      consensusBanner.className = 'consensus-banner cluster-consensus-banner consensus-no-file';
      consensusIcon.innerText = '📂';
      consensusLabel.innerText = 'No Astro report loaded — upload a CSV to begin';
    } else {
      const triggered = preview.triggered_slots || [];
      const skipped = preview.skipped || [];
      let execDetails = '';
      if (triggered.length > 0) {
        execDetails += ` | ⚡ Auto-Triggered: ${triggered.join(', ')}`;
      }
      if (skipped.length > 0) {
        const skipSummary = skipped.map(s => `${s.slot} (${s.reason})`).join(', ');
        execDetails += ` | ⏸️ Skipped: ${skipSummary}`;
      }

      if (sigName === 'BUY') {
        consensusBanner.className = 'consensus-banner cluster-consensus-banner consensus-buy';
        consensusIcon.innerText = '🟢';
        consensusLabel.innerText = `3-ROW UPSIDE CONSENSUS → APPROVED BUY (LONG)${execDetails}`;
      } else if (sigName === 'SELL') {
        consensusBanner.className = 'consensus-banner cluster-consensus-banner consensus-sell';
        consensusIcon.innerText = '🔴';
        consensusLabel.innerText = `3-ROW DOWNSIDE CONSENSUS → APPROVED SELL (SHORT)${execDetails}`;
      } else {
        consensusBanner.className = 'consensus-banner cluster-consensus-banner consensus-neutral';
        consensusIcon.innerText = '⚪';
        consensusLabel.innerText = preview.reason ? `Signal Neutral (${preview.reason})` : `Watching 3-row cluster window (${astroState.filename || 'active file'})...`;
      }
    }
  }

  const isAuto = (astroState.auto_trigger_by_slot || {})[activeSlotId] || false;
  const autoToggle = document.getElementById('astroAutoToggle');
  if (autoToggle) {
    // Only update the DOM if no pending in-flight toggle for this slot (prevents flickering).
    if (!pendingAutoTriggerSlots.has(activeSlotId)) {
      autoToggle.checked = isAuto;
    }
  }
  const slotTag = document.getElementById('astroSlotTag');
  if (slotTag) {
    if (isAuto) {
      slotTag.innerHTML = `🟢 Auto-Trigger <strong>ARMED</strong> for <code>${activeSlotId}</code>`;
      slotTag.style.color = '#10b981';
    } else {
      slotTag.innerHTML = `⚠️ Auto-Trigger <strong>OFF</strong> for <code>${activeSlotId}</code> (Signals evaluated only)`;
      slotTag.style.color = '#f59e0b';
    }
  }
}

function render6SlotsMatrix(state) {
  const container = document.getElementById('commoditySlotsGrid');
  if (!container) return;

  const matrix = state.slots_matrix || {};
  const autoBySlot = (state.astro_state && state.astro_state.auto_trigger_by_slot) || {};
  let html = '';

  ALL_SLOTS.forEach(slot => {
    const s = matrix[slot.id] || {};
    const isSel = (slot.id === activeSlotId);
    const isRun = s.is_active || false;
    const isAutoSlot = Boolean(autoBySlot[slot.id] || autoBySlot['all']);
    const pnl = s.total_pnl || 0;
    const pnlSign = pnl >= 0 ? '+' : '';

    const slotTrades = (state.history || []).filter(t => {
      const tRun = t.run_id || t.slot_id;
      if (tRun && (tRun === slot.id || SLOT_MAP[tRun] === slot.id)) return true;
      if (t.commodity === slot.commodity && t.trade_side === slot.side) return true;
      return false;
    });
    const todayTrades = slotTrades.filter(t => isTradeToday(t, state));
    let slotTodayRealized = 0;
    todayTrades.forEach(t => {
      slotTodayRealized += parseFloat(t.pnl_rupees || 0);
    });
    if (slotTodayRealized === 0 && s.today_realized_pnl) {
      slotTodayRealized = parseFloat(s.today_realized_pnl || 0);
    }
    const todaySign = slotTodayRealized >= 0 ? '+' : '';
    const todayColor = slotTodayRealized > 0 ? 'var(--neon-green,#10b981)' : (slotTodayRealized < 0 ? 'var(--neon-red,#ef4444)' : 'var(--neon-cyan,#00f0ff)');

    html += `
      <div class="slot-matrix-card ${isSel ? 'selected' : ''} ${isRun ? 'running' : ''}" onclick="switchSlot('${slot.id}')">
        <div class="slot-card-top">
          <span class="slot-badge-id ${slot.color}">${slot.badge}</span>
          <span class="slot-auto-pill ${isAutoSlot ? 'auto-on' : 'auto-off'}">${isAutoSlot ? '⚡ AUTO ON' : '⏸️ AUTO OFF'}</span>
          <span class="slot-status-pill status-${(s.slot_status || 'empty').toLowerCase()}">${s.slot_status || 'EMPTY'}</span>
        </div>
        <div class="slot-card-body">
          <div class="slot-card-name">${slot.label}</div>
          <div class="slot-card-symbol">${s.contract_symbol || `${slot.commodity} FUT`}</div>
        </div>
        <div class="slot-card-stats">
          <div class="stat-line">
            <span class="stat-k">LTP</span>
            <span class="stat-v">${s.last_ltp ? `₹${s.last_ltp.toFixed(2)}` : '--'}</span>
          </div>
          <div class="stat-line">
            <span class="stat-k">Today P&amp;L</span>
            <span class="stat-v" style="color:${todayColor}; font-weight:800;">
              ${todaySign}₹${slotTodayRealized.toFixed(2)}
            </span>
          </div>
          <div class="stat-line">
            <span class="stat-k">Total P&amp;L</span>
            <span class="stat-v stat-v-pnl" style="color:${pnl >= 0 ? 'var(--neon-green,#10b981)' : 'var(--neon-red,#ef4444)'}">
              ${pnlSign}₹${pnl.toFixed(2)}
            </span>
          </div>
        </div>
      </div>
    `;
  });

  container.innerHTML = html;
}



function renderHistoryLedger(state) {
  const slicesTbody = document.getElementById('closedSlicesTableBody');
  const badge = document.getElementById('closedSlicesBadge');
  const ledgerSub = document.getElementById('ledgerSubHeadline');
  const btnClear = document.getElementById('btnClearHistory');
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId) || { label: activeSlotId, commodity: currentCommodity, side: currentTradeSide };
  const targetComm = currentLedgerCommFilter || 'ALL';

  if (ledgerSub) {
    if (targetComm === 'ALL') {
      ledgerSub.innerText = `Complete Trade History Across All MCX Commodities (SILVERM, GOLDM, COPPER, CRUDEOIL)`;
    } else {
      ledgerSub.innerText = `Complete Trade History for ${targetComm} (All Slots & Sides)`;
    }
  }
  if (btnClear) {
    if (targetComm === 'ALL') {
      btnClear.innerText = `🗑 Clear All Closed Trades`;
      btnClear.title = `Clear trade history across all commodities`;
    } else {
      btnClear.innerText = `🗑 Clear ${targetComm} Trades`;
      btnClear.title = `Clear trade history for ${targetComm}`;
    }
  }

  const allHistory = state.history || [];
  // Filter history: if 'ALL', include all commodities; otherwise filter strictly by targetComm
  const commTrades = (targetComm === 'ALL')
    ? allHistory
    : allHistory.filter(t => {
        const tComm = getTradeCommodity(t);
        return tComm === targetComm;
      });

  let filtered = commTrades;
  if (currentLedgerModeFilter !== 'all') {
    filtered = commTrades.filter(t => (t.mode || 'paper').toLowerCase() === currentLedgerModeFilter);
  }

  // Sort newest first by exit time / closed timestamp
  filtered.sort((a, b) => {
    const timeA = a.exit_time || a.exited_at || a.created_at || a.timestamp_utc || a.timestamp || a.entry_time || '';
    const timeB = b.exit_time || b.exited_at || b.created_at || b.timestamp_utc || b.timestamp || b.entry_time || '';
    return timeB.localeCompare(timeA);
  });

  if (badge) {
    badge.innerText = `${filtered.length} Closed [${targetComm}]`;
  }
  if (!slicesTbody) return;

  if (filtered.length === 0) {
    slicesTbody.innerHTML = `
      <tr class="empty-table-row empty-row">
        <td colspan="14">No closed futures executions for <strong>${targetComm}</strong> yet.</td>
      </tr>
    `;
    return;
  }

  let html = '';
  filtered.forEach(t => {
    const pnl = parseFloat(t.pnl_rupees || 0);
    const pnlSign = pnl >= 0 ? '+' : '';
    const mode = (t.mode || 'paper').toUpperCase();
    const modeBadge = mode === 'LIVE' ? '<span class="filter-pill pill-live">LIVE</span>' : '<span class="filter-pill pill-paper">PAPER</span>';
    const side = (t.trade_side || slotObj.side || 'BUY').toUpperCase();
    const sideBadge = side === 'BUY' ? 'badge-call' : 'badge-put';
    const rungStatusBadge = side === 'SELL' ? 'status-used' : 'status-filled';
    const tCommodity = t.commodity || getTradeCommodity(t) || targetComm;

    html += `
      <tr>
        <td>${modeBadge}</td>
        <td><strong style="color:var(--neon-cyan,#00f0ff);">${tCommodity}</strong></td>
        <td><span class="slot-badge-id ${sideBadge}">${t.run_id || t.slot_id || activeSlotId}</span></td>
        <td><span class="rung-status-badge ${rungStatusBadge}">${side}</span></td>
        <td><span class="slice-label-pill">${t.label || 'A'}</span></td>
        <td>${t.contract_symbol || '--'}</td>
        <td>₹${parseFloat(t.entry_price || t.fill_price || 0).toFixed(2)}</td>
        <td>₹${parseFloat(t.exit_price || 0).toFixed(2)}</td>
        <td>${t.quantity || 1}</td>
        <td style="color:${pnl >= 0 ? 'var(--neon-green,#10b981)' : 'var(--neon-red,#ef4444)'};font-weight:700;">
          ${pnlSign}₹${pnl.toFixed(2)}
        </td>
        <td>${t.exit_reason || 'MANUAL'}</td>
        <td>${t.entry_time || '--'}</td>
        <td>${t.exit_time || '--'}</td>
        <td>
          <button class="btn-history-delete" onclick="deleteSingleTrade('${t.trade_id || t._id}')" title="Delete trade record">🗑</button>
        </td>
      </tr>
    `;
  });

  slicesTbody.innerHTML = html;
}

let lastSyncedSlotId = null;

function syncFormInputs(activeConfig, commodity, force = false) {
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId);
  const comm = slotObj ? slotObj.commodity : (commodity || currentCommodity || 'SILVERM');
  const cfg = COMMODITY_CONFIG[comm] || COMMODITY_CONFIG.SILVERM;
  const cfgObj = activeConfig || {};

  const activeRun = getSelectedSlotRun(currentState) || {};
  const isSlotActive = cfgObj.is_active || Boolean(activeRun.is_active);
  // If slot is not active and slice_interval is corrupted/stale (differs from cfg.defaultStep), force defaults
  const isCorrupted = !isSlotActive && cfgObj.slice_interval && Math.abs(cfgObj.slice_interval - cfg.defaultStep) > 0.01;

  const targetRange = (!isCorrupted && cfgObj.range_points) ? cfgObj.range_points : cfg.defaultRangePoints;
  const targetSlicer = (!isCorrupted && cfgObj.slicer_count) ? cfgObj.slicer_count : cfg.defaultSlicerCount;
  const targetProfit = (!isCorrupted && cfgObj.profit_point) ? cfgObj.profit_point : cfg.defaultProfitPoint;
  const targetLoss = (!isCorrupted && cfgObj.loss_point) ? cfgObj.loss_point : cfg.defaultLossPoint;
  const targetQty = (!isCorrupted && cfgObj.qty_per_slice_lots) ? cfgObj.qty_per_slice_lots : (cfg.defaultQtyPerSliceLots || 1);

  const rangeInput = document.getElementById('rangePoints');
  const slicerInput = document.getElementById('slicerCount');
  const profitInput = document.getElementById('profitPoint');
  const lossInput = document.getElementById('lossPoint');
  const qtyInput = document.getElementById('qtyPerSlice');

  const activeEl = document.activeElement;

  if (rangeInput && (force || activeEl !== rangeInput)) {
    rangeInput.value = targetRange;
  }
  if (slicerInput && (force || activeEl !== slicerInput)) {
    slicerInput.value = targetSlicer;
  }
  if (profitInput && (force || activeEl !== profitInput)) {
    profitInput.value = targetProfit;
  }
  if (lossInput && (force || activeEl !== lossInput)) {
    lossInput.value = targetLoss;
  }
  if (qtyInput && (force || activeEl !== qtyInput)) {
    qtyInput.value = targetQty;
  }

  const lotTag = document.getElementById('lotUnitTag');
  if (lotTag) {
    lotTag.innerText = `lots (${cfg.lotSize} ${cfg.unit})`;
  }

  updateSpanBadge();
}

// ── Control Handlers ─────────────────────────────────
function switchSlot(slotId) {
  if (!slotId) return;
  activeSlotId = slotId;
  const slotObj = ALL_SLOTS.find(s => s.id === slotId);
  if (slotObj) {
    currentCommodity = slotObj.commodity;
    currentTradeSide = slotObj.side;
  }
  const commSelect = document.getElementById('commoditySelect');
  if (commSelect) commSelect.value = currentCommodity;
  const sideSelect = document.getElementById('tradeSideSelect');
  if (sideSelect) sideSelect.value = currentTradeSide;

  // 1. Instant local state update & immediate 0ms render
  let slotRun = null;
  if (currentState) {
    currentState.active_run_id = slotId;
    if (currentState.runs) {
      if (Array.isArray(currentState.runs)) {
        slotRun = currentState.runs.find(r => (r.slot_id === slotId || r.run_id === slotId));
      } else {
        slotRun = currentState.runs[slotId];
      }
    }
    if (slotRun) {
      currentState.active_run = slotRun;
    }
    syncFormInputs(slotRun ? slotRun.config : null, currentCommodity, true);
    lastSyncedSlotId = slotId;
    renderAll(currentState);
  } else {
    syncFormInputs(null, currentCommodity, true);
    lastSyncedSlotId = slotId;
  }

  // 2. Instantly prefill optLtpInput from cached LTP if available
  if (currentState) {
    const angelFeed = currentState.angel_feed || {};
    const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || currentState.latest_ltp_by_comm || {};
    const cachedLtp = commLtpMap[currentCommodity];
    if (cachedLtp && isPlausibleLtp(currentCommodity, cachedLtp)) {
      const input = document.getElementById('optLtpInput');
      if (input) input.value = cachedLtp;
      const resOptLtp = document.getElementById('resOptLtpVal');
      if (resOptLtp) resOptLtp.innerText = `₹${cachedLtp.toFixed(2)}`;
      const metricLtp = document.getElementById('metricLiveLtp');
      if (metricLtp) metricLtp.innerText = `₹${cachedLtp.toFixed(2)}`;
    }
  }

  // 3. Asynchronously sync backend in background (non-blocking)
  fetch('/runs/active', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
    body: JSON.stringify({ run_id: slotId }),
  }).then(async res => {
    if (res.ok) {
      const data = await res.json();
      if (data && data.state) {
        currentState = data.state;
        renderAll(currentState);
      }
    }
  }).catch(e => console.error('Error switching active slot on backend:', e));

  // 4. Background non-blocking auto-opt refresh
  fetchAndApplyAutoOptLtp();
}

function onCommodityChange(val) {
  currentCommodity = val;
  // If one of the slots for this commodity is currently active or running, prefer that slot!
  let targetSlot = ALL_SLOTS.find(s => s.commodity === val && s.side === currentTradeSide);
  if (currentState && currentState.slots_matrix) {
    const slotsForComm = ALL_SLOTS.filter(s => s.commodity === val);
    const runningSlot = slotsForComm.find(s => {
      const sm = currentState.slots_matrix[s.id];
      return sm && (sm.is_active || (sm.active_slices_count && sm.active_slices_count > 0));
    });
    if (runningSlot) {
      targetSlot = runningSlot;
    }
  }
  if (targetSlot) {
    switchSlot(targetSlot.id);
  }
}

function onTradeSideChange(val) {
  currentTradeSide = val;
  const slot = ALL_SLOTS.find(s => s.commodity === currentCommodity && s.side === val);
  if (slot) {
    switchSlot(slot.id);
  }
}

function onProductTypeChange(val) {
  currentProductType = val;
  fetch(`/runs/${activeSlotId}/product_type`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
    body: JSON.stringify({ product_type: val }),
  });
}

function updateSpanBadge() {
  const comm = currentCommodity || 'SILVERM';
  const cfg = COMMODITY_CONFIG[comm] || COMMODITY_CONFIG.SILVERM;
  const rangeInput = document.getElementById('rangePoints');
  const slicerInput = document.getElementById('slicerCount');

  const rangePts = (rangeInput && parseFloat(rangeInput.value) > 0) ? parseFloat(rangeInput.value) : cfg.defaultRangePoints;
  const slicerCnt = (slicerInput && parseInt(slicerInput.value) > 0) ? parseInt(slicerInput.value) : cfg.defaultSlicerCount;
  const step = rangePts / Math.max(1, slicerCnt);

  const spanBadge = document.getElementById('spanBadge');
  if (spanBadge) spanBadge.innerText = `Span: ${rangePts.toFixed(2)} pts`;
  const stepVal = document.getElementById('computedStepVal');
  if (stepVal) stepVal.innerText = `${step.toFixed(2)} pts`;
  const stepFormula = document.getElementById('computedStepFormula');
  if (stepFormula) stepFormula.innerText = `(${rangePts.toFixed(2)} pts / ${slicerCnt} levels)`;

  const spanStep = document.getElementById('metricSpanStep');
  if (spanStep) {
    spanStep.innerText = `${rangePts.toFixed(1)} pts / ${step.toFixed(1)} pts`;
  }
  const levelsEl = document.getElementById('metricTotalLevels');
  if (levelsEl) {
    levelsEl.innerText = `${slicerCnt} Total Levels`;
  }
}

async function runSlicer() {
  const btnRun = document.getElementById('btnRunSlicer');
  if (btnRun) {
    btnRun.innerHTML = '<span class="launch-icon">⏳</span><span class="launch-text">STARTING...</span>';
    btnRun.disabled = true;
  }

  const rangePts = parseFloat(document.getElementById('rangePoints').value) || 800;
  const slicerCnt = parseInt(document.getElementById('slicerCount').value) || 8;
  const profitPt = parseFloat(document.getElementById('profitPoint').value) || 100;
  const lossPt = parseFloat(document.getElementById('lossPoint').value) || 150;
  const qty = parseInt(document.getElementById('qtyPerSlice').value) || 1;
  const cutoff = document.getElementById('eodCutoffInput').value || '23:30';

  const payload = {
    commodity: currentCommodity,
    trade_side: currentTradeSide,
    product_type: currentProductType,
    range_points: rangePts,
    slicer_count: slicerCnt,
    profit_point: profitPt,
    loss_point: lossPt,
    qty_per_slice_lots: qty,
    cutoff_time_ist: cutoff,
    auto_eod_squareoff: true,
  };

  try {
    const res = await fetch(`/runs/${activeSlotId}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    } else if (data && data.run && currentState) {
      currentState.active_run = data.run;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error starting run:', e);
  } finally {
    if (btnRun) btnRun.disabled = false;
  }
}

async function stopSlicer() {
  const btnRun = document.getElementById('btnRunSlicer');
  if (btnRun) {
    btnRun.innerHTML = '<span class="launch-icon">⏳</span><span class="launch-text">STOPPING...</span>';
    btnRun.disabled = true;
  }
  try {
    const res = await fetch(`/runs/${activeSlotId}/stop`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    } else if (data && data.run && currentState) {
      currentState.active_run = data.run;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error stopping run:', e);
  } finally {
    if (btnRun) btnRun.disabled = false;
  }
}

async function triggerEodSquareoff() {
  const btnEod = document.getElementById('btnEodSquareoff');
  if (btnEod) {
    btnEod.innerHTML = '⏳ SQUARING OFF...';
    btnEod.disabled = true;
  }
  try {
    const res = await fetch(`/runs/${activeSlotId}/squareoff`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    } else if (data && data.run && currentState) {
      currentState.active_run = data.run;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error in squareoff:', e);
  } finally {
    if (btnEod) btnEod.disabled = false;
  }
}

async function recenterLadder() {
  const btnRecenter = document.getElementById('btnRecenter');
  if (btnRecenter) {
    btnRecenter.innerHTML = '⏳ Recentering...';
    btnRecenter.disabled = true;
  }
  try {
    const res = await fetch(`/runs/${activeSlotId}/recenter`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    } else if (data && data.run && currentState) {
      currentState.active_run = data.run;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error recentering ladder:', e);
  } finally {
    if (btnRecenter) {
      btnRecenter.innerHTML = '<span class="btn-icon">🎯</span> Recenter Ladder';
      btnRecenter.disabled = false;
    }
  }
}

async function exitSpecificSlice(identifier) {
  try {
    const res = await fetch(`/runs/${activeSlotId}/exit_slice?identifier=${encodeURIComponent(identifier)}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (data && data.state) {
      currentState = data.state;
      renderAll(currentState);
    } else if (data && data.run && currentState) {
      currentState.active_run = data.run;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error exiting slice:', e);
  }
}

async function sendManualOptTick() {
  const input = document.getElementById('optLtpInput');
  if (!input || !input.value) return;
  const price = parseFloat(input.value);
  if (isNaN(price) || price <= 0) return;
  try {
    const res = await fetch(`/runs/${activeSlotId}/tick`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({ price }),
    });
    const data = await res.json();
    if (data && (data.status === 'success' || data.success)) {
      const resOptLtp = document.getElementById('resOptLtpVal');
      if (resOptLtp) resOptLtp.innerText = `₹${price.toFixed(2)}`;
      const metricLtp = document.getElementById('metricLiveLtp');
      if (metricLtp) metricLtp.innerText = `₹${price.toFixed(2)}`;
      if (data.state) {
        currentState = data.state;
        renderAll(currentState);
      }
    }
  } catch (e) {
    console.error('Error injecting tick:', e);
  }
}

async function fetchAndApplyAutoOptLtp() {
  // 1. Instantly use cached LTP if present in local state
  if (currentState) {
    const angelFeed = currentState.angel_feed || {};
    const commLtpMap = angelFeed.latest_ltp_by_comm || angelFeed.ltp_by_commodity || currentState.latest_ltp_by_comm || {};
    const cachedLtp = commLtpMap[currentCommodity];
    if (cachedLtp && isPlausibleLtp(currentCommodity, cachedLtp)) {
      const input = document.getElementById('optLtpInput');
      if (input && !input.matches(':focus')) input.value = cachedLtp;
      const resOptLtp = document.getElementById('resOptLtpVal');
      if (resOptLtp) resOptLtp.innerText = `₹${cachedLtp.toFixed(2)}`;
      const metricLtp = document.getElementById('metricLiveLtp');
      if (metricLtp) metricLtp.innerText = `₹${cachedLtp.toFixed(2)}`;
    }
  }

  const btn = document.getElementById('btnAutoOptLtp');
  if (btn) {
    btn.classList.add('loading');
    btn.innerText = '⏳ Fetching...';
  }
  try {
    const res = await fetch(`/market/ltp?commodity=${encodeURIComponent(currentCommodity)}`, {
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (data && data.status === 'success') {
      const rawLtp = data.target_ltp || data.ltp || (data.latest_ltp_by_comm && data.latest_ltp_by_comm[currentCommodity]);
      const ltp = isPlausibleLtp(currentCommodity, rawLtp) ? rawLtp : null;
      if (ltp && typeof ltp === 'number' && ltp > 0) {
        const input = document.getElementById('optLtpInput');
        if (input && !input.matches(':focus')) input.value = ltp;
        const resOptLtp = document.getElementById('resOptLtpVal');
        if (resOptLtp) resOptLtp.innerText = `₹${ltp.toFixed(2)}`;
        const metricLtp = document.getElementById('metricLiveLtp');
        if (metricLtp) metricLtp.innerText = `₹${ltp.toFixed(2)}`;
      }
    }
  } catch (e) {
    console.error('Error fetching auto LTP:', e);
  } finally {
    if (btn) {
      btn.classList.remove('loading');
      btn.innerText = '⚡ Auto Fetch';
    }
  }
}

async function toggleAstroAutoExecution(enabled) {
  const slotId = activeSlotId;

  // 1. Optimistic update: immediately reflect the user's intent in currentState
  //    so the next SSE render does NOT flicker the toggle back.
  if (!currentState) currentState = {};
  if (!currentState.astro_state) currentState.astro_state = {};
  if (!currentState.astro_state.auto_trigger_by_slot) currentState.astro_state.auto_trigger_by_slot = {};
  currentState.astro_state.auto_trigger_by_slot[slotId] = enabled;

  // 2. Mark this slot as in-flight so renderAstroSection skips updating the toggle.
  pendingAutoTriggerSlots.add(slotId);

  // 3. Also update the toggle DOM directly right now (belt-and-suspenders).
  const autoToggle = document.getElementById('astroAutoToggle');
  if (autoToggle) autoToggle.checked = enabled;

  try {
    const res = await fetch(`/runs/${slotId}/auto_trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({ enabled, slot_id: slotId }),
    });
    const data = await res.json().catch(() => ({}));
    if (data && data.auto_trigger) {
      // Merge server-confirmed state into currentState
      currentState.astro_state.auto_trigger_by_slot = data.auto_trigger;
      renderAll(currentState);
    } else {
      // Server did not confirm — revert optimistic update so we don't lie to the user
      currentState.astro_state.auto_trigger_by_slot[slotId] = !enabled;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error toggling auto execution:', e);
    // Revert on error
    currentState.astro_state.auto_trigger_by_slot[slotId] = !enabled;
    renderAll(currentState);
  } finally {
    // 4. Release the in-flight guard after server responds
    pendingAutoTriggerSlots.delete(slotId);
  }
}

function setAstroUploadFeedback(message, type = 'info') {
  const fb = document.getElementById('astroUploadFeedback');
  if (!fb) return;
  if (!message) {
    fb.style.display = 'none';
    fb.className = 'astro-upload-feedback';
    fb.innerText = '';
    return;
  }
  fb.style.display = 'block';
  fb.className = `astro-upload-feedback ${type}`;
  fb.innerText = message;
}

async function uploadAstroFileDirect(file) {
  if (!file) return;
  const isCsv = file.name.toLowerCase().endsWith('.csv') || file.type.includes('csv') || file.type.includes('text');
  if (!isCsv) {
    setAstroUploadFeedback('⚠️ Please select or drop a valid CSV file (.csv)', 'error');
    setTimeout(() => setAstroUploadFeedback(null), 6000);
    return;
  }

  setAstroUploadFeedback(`⏳ Uploading & parsing "${file.name}"...`, 'loading');

  const formData = new FormData();
  formData.append('file', file);

  const headers = {};
  const token = getAuthToken();
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  try {
    const res = await fetch('/astro/upload', {
      method: 'POST',
      headers: headers,
      body: formData,
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const errMsg = data.detail || data.message || `Upload failed (Status ${res.status})`;
      setAstroUploadFeedback(`❌ ${errMsg}`, 'error');
      setTimeout(() => setAstroUploadFeedback(null), 8000);
      return;
    }

    if (data && data.status === 'success') {
      if (!currentState) currentState = {};
      if (!currentState.astro_state) currentState.astro_state = {};
      currentState.astro_state.has_active_file = true;
      currentState.astro_state.filename = data.filename || file.name;
      currentState.astro_state.file_id = data.file_id;
      currentState.astro_state.row_count = data.row_count;
      currentState.astro_state.uploaded_at = new Date().toLocaleTimeString();
      if (data.auto_trigger_by_slot) currentState.astro_state.auto_trigger_by_slot = data.auto_trigger_by_slot;
      if (data.evaluation) {
        currentState.astro_state.cluster_preview = data.evaluation;
        currentState.astro_state.active_signal = data.evaluation.signal || 'NEUTRAL';
      }
      renderAll(currentState);
    }

    const autoMap = data.auto_trigger_by_slot || (currentState && currentState.astro_state && currentState.astro_state.auto_trigger_by_slot) || {};
    const enabledSlots = Object.keys(autoMap).filter(k => k !== 'all' && autoMap[k]);
    let autoNotice = '';
    if (enabledSlots.length === 0) {
      autoNotice = ' — ⚠️ Auto-Trigger is OFF for all slots (signals will be evaluated but not traded until enabled)';
    } else {
      autoNotice = ` — ⚡ Auto-Trigger ARMED for: ${enabledSlots.join(', ')}`;
    }

    setAstroUploadFeedback(`✅ Uploaded "${data.filename || file.name}" (${data.row_count || 0} signal rows active in COMMODITY.astro_files)${autoNotice}`, enabledSlots.length > 0 ? 'success' : 'info');
    setTimeout(() => setAstroUploadFeedback(null), 10000);
  } catch (e) {
    console.error('Error uploading Astro CSV:', e);
    setAstroUploadFeedback(`❌ Upload error: ${e.message}`, 'error');
    setTimeout(() => setAstroUploadFeedback(null), 8000);
  }
}

async function handleAstroCsvSelected(input) {
  if (!input || !input.files || input.files.length === 0) return;
  const file = input.files[0];
  try {
    await uploadAstroFileDirect(file);
  } finally {
    // Reset file input so selecting the same file again triggers change event
    input.value = '';
  }
}

function initAstroDropzone() {
  const dropzone = document.querySelector('.astro-quantum-dropzone') || document.querySelector('.astro-file-dropzone');
  if (!dropzone) return;

  ['dragenter', 'dragover'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add('drag-active');
    }, false);
  });

  ['dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('drag-active');
    }, false);
  });

  dropzone.addEventListener('drop', (e) => {
    const dt = e.dataTransfer;
    const files = dt && dt.files;
    if (files && files.length > 0) {
      uploadAstroFileDirect(files[0]);
    }
  }, false);
}

async function deleteActiveAstroFile() {
  const activeFile = (currentState && currentState.astro_state && currentState.astro_state.file_id);
  if (!activeFile) return;

  const fileName = (currentState && currentState.astro_state && currentState.astro_state.filename) || 'this file';
  const confirmMsg = `Are you sure you want to remove active Astro report "${fileName}"?\n\nThis will stop signal evaluations until another report is uploaded or activated.`;
  if (!window.confirm(confirmMsg)) return;

  try {
    const headers = {};
    const token = getAuthToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const res = await fetch(`/astro/files/${activeFile}`, {
      method: 'DELETE',
      headers: headers,
    });
    if (res.ok) {
      if (currentState && currentState.astro_state) {
        currentState.astro_state.has_active_file = false;
        currentState.astro_state.filename = null;
        currentState.astro_state.file_id = null;
        currentState.astro_state.row_count = 0;
        currentState.astro_state.uploaded_at = null;
        currentState.astro_state.cluster_preview = null;
        currentState.astro_state.active_signal = 'NEUTRAL';
      }
      if (currentState) {
        currentState.active_signal = 'NEUTRAL';
      }
      renderAstroSection(currentState || {});
      setAstroUploadFeedback('🗑 Active Astro CSV deleted', 'info');
      setTimeout(() => setAstroUploadFeedback(null), 4000);
      await fetchState();
    } else {
      const errData = await res.json().catch(() => ({}));
      setAstroUploadFeedback(`Failed to delete: ${errData.detail || res.statusText}`, 'error');
    }
  } catch (e) {
    console.error('Error deleting Astro file:', e);
    setAstroUploadFeedback('Error deleting Astro report', 'error');
  }
}

async function clearTradeHistory() {
  const comm = (currentLedgerCommFilter && currentLedgerCommFilter !== 'ALL') ? currentLedgerCommFilter : (currentCommodity || 'SILVERM');
  const confirmText = (currentLedgerCommFilter === 'ALL') ? 'all MCX commodities' : comm;
  if (!confirm(`Are you sure you want to clear closed trade history for ${confirmText}?`)) return;
  try {
    const q = (currentLedgerCommFilter === 'ALL') ? '' : `?commodity=${encodeURIComponent(comm)}`;
    await fetch(`/history/clear${q}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    fetchPnlSummary();
  } catch (e) {
    console.error('Error clearing commodity history:', e);
  }
}

async function deleteSingleTrade(tradeId) {
  try {
    await fetch(`/history/trade/${tradeId}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    fetchPnlSummary();
  } catch (e) {
    console.error('Error deleting trade:', e);
  }
}

function setLedgerTab(tab) {
  currentLedgerTab = tab;
  const btnSlices = document.getElementById('btnViewSlices');
  const btnEvents = document.getElementById('btnViewSlotEvents');
  const viewSlices = document.getElementById('slicesViewport');
  const viewEvents = document.getElementById('slotEventsViewport');

  if (tab === 'slot_events') {
    if (btnSlices) btnSlices.classList.remove('active');
    if (btnEvents) btnEvents.classList.add('active');
    if (viewSlices) viewSlices.style.display = 'none';
    if (viewEvents) viewEvents.style.display = 'block';
  } else {
    if (btnSlices) btnSlices.classList.add('active');
    if (btnEvents) btnEvents.classList.remove('active');
    if (viewSlices) viewSlices.style.display = 'block';
    if (viewEvents) viewEvents.style.display = 'none';
  }
}

function setLedgerModeFilter(mode) {
  currentLedgerModeFilter = mode;
  ['btnFilterAll', 'btnFilterPaper', 'btnFilterLive'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.remove('active');
  });
  if (mode === 'paper') document.getElementById('btnFilterPaper')?.classList.add('active');
  else if (mode === 'live') document.getElementById('btnFilterLive')?.classList.add('active');
  else document.getElementById('btnFilterAll')?.classList.add('active');

  if (currentState) renderHistoryLedger(currentState);
}

function setLedgerCommFilter(comm) {
  currentLedgerCommFilter = (comm || 'ALL').toUpperCase();
  ['ALL', 'SILVERM', 'GOLDM', 'COPPER', 'CRUDEOIL'].forEach(c => {
    const el = document.getElementById(`btnLedgerComm${c}`);
    if (el) {
      if (c === currentLedgerCommFilter) el.classList.add('active');
      else el.classList.remove('active');
    }
  });

  if (currentState) renderHistoryLedger(currentState);
}

// ── P&L Summary State & Controls ─────────────────────────────────
let currentPnlPeriod = 'month';
let currentPnlMode = 'all';
let currentPnlViewType = 'slots';
let currentPnlCustomDate = '';
let lastKnownHistoryLen = -1;
let lastPnlData = null;

function computeClientPnlSummary(trades, period, mode, customDate) {
  const now = new Date();
  const istOffset = 5.5 * 60 * 60 * 1000;
  const nowIst = new Date(now.getTime() + (now.getTimezoneOffset() * 60 * 1000) + istOffset);
  const nowIstStr = `${nowIst.getFullYear()}-${String(nowIst.getMonth() + 1).padStart(2, '0')}-${String(nowIst.getDate()).padStart(2, '0')}`;

  let refDateStr = nowIstStr;
  if (period === 'custom' && customDate) {
    refDateStr = customDate.trim();
  }

  let fromDate = null;
  let toDate = null;
  const p = (period || 'month').toLowerCase();

  if (p === 'day' || p === 'today') {
    fromDate = refDateStr;
    toDate = refDateStr;
  } else if (p === 'week' || p === 'this_week') {
    const parts = refDateStr.split('-').map(Number);
    const d = new Date(parts[0], parts[1] - 1, parts[2]);
    const dayOfWeek = (d.getDay() + 6) % 7; // Monday = 0
    const mon = new Date(d);
    mon.setDate(d.getDate() - dayOfWeek);
    const sun = new Date(mon);
    sun.setDate(mon.getDate() + 6);
    fromDate = `${mon.getFullYear()}-${String(mon.getMonth() + 1).padStart(2, '0')}-${String(mon.getDate()).padStart(2, '0')}`;
    toDate = `${sun.getFullYear()}-${String(sun.getMonth() + 1).padStart(2, '0')}-${String(sun.getDate()).padStart(2, '0')}`;
  } else if (p === 'month' || p === 'this_month') {
    const parts = refDateStr.split('-').map(Number);
    const y = parts[0];
    const m = parts[1];
    const lastDay = new Date(y, m, 0).getDate();
    fromDate = `${y}-${String(m).padStart(2, '0')}-01`;
    toDate = `${y}-${String(m).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;
  } else if (p === 'all' || p === 'all_time') {
    fromDate = 'ALL';
    toDate = 'ALL';
  } else if (p === 'custom') {
    fromDate = refDateStr;
    toDate = refDateStr;
  } else {
    fromDate = 'ALL';
    toDate = 'ALL';
  }

  function getTradeDateStr(t) {
    for (const k of ['exit_time', 'exited_at', 'entry_time', 'created_at', 'timestamp_utc']) {
      const v = t[k];
      if (typeof v === 'string') {
        const m = v.match(/(\d{4})-(\d{2})-(\d{2})/);
        if (m) return `${m[1]}-${m[2]}-${m[3]}`;
      }
    }
    return '';
  }

  const filtered = (trades || []).filter(t => {
    if (mode && mode !== 'all') {
      const tMode = (t.mode || 'paper').toLowerCase();
      if (tMode !== mode.toLowerCase()) return false;
    }
    if (fromDate === 'ALL') return true;
    const tDate = getTradeDateStr(t);
    if (!tDate) return true;
    return tDate >= fromDate && tDate <= toDate;
  });

  const comms = ['SILVERM', 'GOLDM', 'COPPER', 'CRUDEOIL'];
  const byInstMap = {};
  comms.forEach(c => {
    byInstMap[c] = { commodity: c, gross_profit: 0, gross_loss: 0, net_pnl: 0, trade_count: 0, win_count: 0, loss_count: 0 };
  });

  const slotOrder = ['SILVERM-B', 'SILVERM-S', 'GOLDM-B', 'GOLDM-S', 'COPPER-B', 'COPPER-S', 'CRUDEOIL-B', 'CRUDEOIL-S'];
  const bySlotMap = {};
  slotOrder.forEach(sid => {
    const comm = sid.split('-')[0];
    const side = sid.split('-')[1] === 'B' ? 'BUY' : 'SELL';
    bySlotMap[sid] = { slot_id: sid, commodity: comm, side: side, gross_profit: 0, gross_loss: 0, net_pnl: 0, trade_count: 0, win_count: 0, loss_count: 0 };
  });

  filtered.forEach(t => {
    const rawComm = (t.commodity || t.symbol || t.instrument || '').toUpperCase();
    let normComm = 'SILVERM';
    if (rawComm.includes('SILVER')) normComm = 'SILVERM';
    else if (rawComm.includes('GOLD')) normComm = 'GOLDM';
    else if (rawComm.includes('COPPER')) normComm = 'COPPER';
    else if (rawComm.includes('CRUDE')) normComm = 'CRUDEOIL';

    const pnl = parseFloat(t.pnl_rupees || 0);

    if (byInstMap[normComm]) {
      const s = byInstMap[normComm];
      s.trade_count += 1;
      if (pnl > 0) { s.gross_profit += pnl; s.win_count += 1; }
      else if (pnl < 0) { s.gross_loss += pnl; s.loss_count += 1; }
      s.net_pnl = Math.round((s.gross_profit + s.gross_loss) * 100) / 100;
    }

    const rawSlot = String(t.run_id || t.slot_id || '').trim();
    let canonicalSlot = bySlotMap[rawSlot] ? rawSlot : null;
    if (!canonicalSlot) {
      const side = String(t.trade_side || 'BUY').toUpperCase();
      canonicalSlot = `${normComm}-${side === 'BUY' ? 'B' : 'S'}`;
    }
    if (bySlotMap[canonicalSlot]) {
      const ss = bySlotMap[canonicalSlot];
      ss.trade_count += 1;
      if (pnl > 0) { ss.gross_profit += pnl; ss.win_count += 1; }
      else if (pnl < 0) { ss.gross_loss += pnl; ss.loss_count += 1; }
      ss.net_pnl = Math.round((ss.gross_profit + ss.gross_loss) * 100) / 100;
    }
  });

  const byInstrument = comms.map(c => ({
    commodity: c,
    gross_profit: Math.round(byInstMap[c].gross_profit * 100) / 100,
    gross_loss: Math.round(byInstMap[c].gross_loss * 100) / 100,
    net_pnl: Math.round((byInstMap[c].gross_profit + byInstMap[c].gross_loss) * 100) / 100,
    trade_count: byInstMap[c].trade_count,
    win_count: byInstMap[c].win_count,
    loss_count: byInstMap[c].loss_count,
  }));

  const bySlot = slotOrder.map(sid => ({
    slot_id: sid,
    commodity: bySlotMap[sid].commodity,
    side: bySlotMap[sid].side,
    gross_profit: Math.round(bySlotMap[sid].gross_profit * 100) / 100,
    gross_loss: Math.round(bySlotMap[sid].gross_loss * 100) / 100,
    net_pnl: Math.round((bySlotMap[sid].gross_profit + bySlotMap[sid].gross_loss) * 100) / 100,
    trade_count: bySlotMap[sid].trade_count,
    win_count: bySlotMap[sid].win_count,
    loss_count: bySlotMap[sid].loss_count,
  }));

  const total = {
    commodity: 'ALL',
    gross_profit: Math.round(byInstrument.reduce((acc, x) => acc + x.gross_profit, 0) * 100) / 100,
    gross_loss: Math.round(byInstrument.reduce((acc, x) => acc + x.gross_loss, 0) * 100) / 100,
    net_pnl: Math.round(byInstrument.reduce((acc, x) => acc + x.net_pnl, 0) * 100) / 100,
    trade_count: byInstrument.reduce((acc, x) => acc + x.trade_count, 0),
    win_count: byInstrument.reduce((acc, x) => acc + x.win_count, 0),
    loss_count: byInstrument.reduce((acc, x) => acc + x.loss_count, 0),
  };

  return {
    status: 'success',
    period: p,
    range: { from: fromDate, to: toDate },
    by_instrument: byInstrument,
    by_slot: bySlot,
    total: total,
  };
}

function setPnlViewType(type) {
  currentPnlViewType = type;
  const btnSlots = document.getElementById('btnPnlViewSlots');
  const btnComms = document.getElementById('btnPnlViewCommodities');
  if (btnSlots && btnComms) {
    if (type === 'slots') {
      btnSlots.classList.add('active');
      btnComms.classList.remove('active');
    } else {
      btnSlots.classList.remove('active');
      btnComms.classList.add('active');
    }
  }
  if (lastPnlData) {
    renderPnlSummary(lastPnlData);
  } else if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
    const instantData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
    renderPnlSummary(instantData);
    fetchPnlSummary();
  } else {
    fetchPnlSummary();
  }
}

function onPnlPeriodChange(val) {
  currentPnlPeriod = val;
  const dateInput = document.getElementById('pnlCustomDateInput');
  if (val === 'custom') {
    if (dateInput) {
      dateInput.style.display = 'inline-block';
      if (!dateInput.value) {
        const todayStr = getTodayDateStr(currentState);
        dateInput.value = todayStr;
        currentPnlCustomDate = todayStr;
      } else {
        currentPnlCustomDate = dateInput.value;
      }
    }
  } else {
    if (dateInput) {
      dateInput.style.display = 'none';
    }
  }

  // Instant optimistic render from available client history
  if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
    const instantData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
    renderPnlSummary(instantData);
  }

  fetchPnlSummary();
}

function onPnlCustomDateChange(val) {
  currentPnlCustomDate = val;
  if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
    const instantData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
    renderPnlSummary(instantData);
  }
  fetchPnlSummary();
}

function setPnlModeFilter(mode) {
  currentPnlMode = mode;
  ['btnPnlFilterAll', 'btnPnlFilterPaper', 'btnPnlFilterLive'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.remove('active');
  });
  if (mode === 'paper') document.getElementById('btnPnlFilterPaper')?.classList.add('active');
  else if (mode === 'live') document.getElementById('btnPnlFilterLive')?.classList.add('active');
  else document.getElementById('btnPnlFilterAll')?.classList.add('active');

  // Instant optimistic render
  if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
    const instantData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
    renderPnlSummary(instantData);
  }

  fetchPnlSummary();
}

async function fetchPnlSummary() {
  const tbody = document.getElementById('pnlSummaryTableBody');
  const badge = document.getElementById('pnlRangeBadge');

  let url = `/history/summary?period=${encodeURIComponent(currentPnlPeriod)}&_t=${Date.now()}`;
  if (currentPnlMode && currentPnlMode !== 'all') {
    url += `&mode=${encodeURIComponent(currentPnlMode)}`;
  }
  if (currentPnlPeriod === 'custom' && currentPnlCustomDate) {
    url += `&date=${encodeURIComponent(currentPnlCustomDate)}`;
  }

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      // If server returned 0 trades but client history has trades, prefer client history
      if (data.total && data.total.trade_count === 0 && currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
        const clientData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
        if (clientData.total && clientData.total.trade_count > 0) {
          renderPnlSummary(clientData);
          return;
        }
      }
      renderPnlSummary(data);
    } else {
      console.warn('Failed to load P&L summary:', data);
      if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
        const clientData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
        renderPnlSummary(clientData);
      }
    }
  } catch (e) {
    console.error('Error fetching P&L summary:', e);
    if (currentState && Array.isArray(currentState.history) && currentState.history.length > 0) {
      const clientData = computeClientPnlSummary(currentState.history, currentPnlPeriod, currentPnlMode, currentPnlCustomDate);
      renderPnlSummary(clientData);
    }
  }
}

function renderPnlSummary(data) {
  lastPnlData = data;
  const tbody = document.getElementById('pnlSummaryTableBody');
  const thead = document.querySelector('#pnlSummaryTable thead');
  const badge = document.getElementById('pnlRangeBadge');
  if (!tbody) return;

  const range = data.range || {};
  if (badge) {
    if (range.from && range.to && range.from !== 'ALL') {
      badge.innerText = (range.from === range.to) ? `Range: ${range.from}` : `Range: ${range.from} → ${range.to}`;
    } else if (range.from === 'ALL') {
      badge.innerText = 'Range: All Time';
    } else {
      badge.innerText = `Range: ${data.period || 'This Month'}`;
    }
  }

  const byInst = data.by_instrument || [];
  const bySlot = data.by_slot || [];
  const total = data.total || {
    commodity: 'ALL',
    gross_profit: 0,
    gross_loss: 0,
    net_pnl: 0,
    trade_count: 0,
    win_count: 0,
    loss_count: 0,
  };

  const totalNet = parseFloat(total.net_pnl || 0);
  const totalGp = parseFloat(total.gross_profit || 0);
  const totalGl = parseFloat(total.gross_loss || 0);
  const totalNetSign = totalNet > 0 ? '+' : '';
  const totalNetClass = totalNet > 0 ? 'pnl-pos' : (totalNet < 0 ? 'pnl-neg' : 'pnl-neutral');
  const totalGpClass = totalGp > 0 ? 'pnl-pos' : 'pnl-neutral';
  const totalGlClass = totalGl < 0 ? 'pnl-neg' : 'pnl-neutral';

  let html = '';

  if (currentPnlViewType === 'slots') {
    if (thead) {
      thead.innerHTML = `
        <tr>
          <th>SLOT</th>
          <th>COMMODITY</th>
          <th>SIDE</th>
          <th style="text-align:right;">GROSS PROFIT (₹)</th>
          <th style="text-align:right;">GROSS LOSS (₹)</th>
          <th style="text-align:right;">NET P&amp;L (₹)</th>
          <th style="text-align:center;">TRADES</th>
          <th style="text-align:center;">WIN / LOSS</th>
        </tr>
      `;
    }

    bySlot.forEach(item => {
      const net = parseFloat(item.net_pnl || 0);
      const gp = parseFloat(item.gross_profit || 0);
      const gl = parseFloat(item.gross_loss || 0);
      const netSign = net > 0 ? '+' : '';
      const netClass = net > 0 ? 'pnl-pos' : (net < 0 ? 'pnl-neg' : 'pnl-neutral');
      const gpClass = gp > 0 ? 'pnl-pos' : 'pnl-neutral';
      const glClass = gl < 0 ? 'pnl-neg' : 'pnl-neutral';
      const sideClass = (item.side === 'BUY') ? 'badge-call' : 'badge-put';
      const rungClass = (item.side === 'SELL') ? 'status-used' : 'status-filled';

      html += `
        <tr>
          <td><span class="slot-badge-id ${sideClass}">${item.slot_id}</span></td>
          <td><strong>${item.commodity}</strong></td>
          <td><span class="rung-status-badge ${rungClass}">${item.side}</span></td>
          <td style="text-align:right;" class="${gpClass}">${gp > 0 ? '+' : ''}₹${gp.toFixed(2)}</td>
          <td style="text-align:right;" class="${glClass}">${gl < 0 ? '-' : ''}₹${Math.abs(gl).toFixed(2)}</td>
          <td style="text-align:right;" class="${netClass}">${netSign}₹${net.toFixed(2)}</td>
          <td style="text-align:center;">${item.trade_count}</td>
          <td style="text-align:center;"><span style="color:var(--neon-green,#10b981);font-weight:700;">${item.win_count}W</span> / <span style="color:var(--neon-red,#ef4444);font-weight:700;">${item.loss_count}L</span></td>
        </tr>
      `;
    });

    html += `
      <tr class="pnl-total-row">
        <td colspan="3"><strong>TOTAL</strong></td>
        <td style="text-align:right;" class="${totalGpClass}">${totalGp > 0 ? '+' : ''}₹${totalGp.toFixed(2)}</td>
        <td style="text-align:right;" class="${totalGlClass}">${totalGl < 0 ? '-' : ''}₹${Math.abs(totalGl).toFixed(2)}</td>
        <td style="text-align:right;" class="${totalNetClass}">${totalNetSign}₹${totalNet.toFixed(2)}</td>
        <td style="text-align:center;">${total.trade_count}</td>
        <td style="text-align:center;"><span style="color:var(--neon-green,#10b981);font-weight:700;">${total.win_count}W</span> / <span style="color:var(--neon-red,#ef4444);font-weight:700;">${total.loss_count}L</span></td>
      </tr>
    `;
  } else {
    // By Commodity
    if (thead) {
      thead.innerHTML = `
        <tr>
          <th>INSTRUMENT</th>
          <th style="text-align:right;">GROSS PROFIT (₹)</th>
          <th style="text-align:right;">GROSS LOSS (₹)</th>
          <th style="text-align:right;">NET P&amp;L (₹)</th>
          <th style="text-align:center;">TRADES</th>
          <th style="text-align:center;">WIN / LOSS</th>
        </tr>
      `;
    }

    byInst.forEach(item => {
      const net = parseFloat(item.net_pnl || 0);
      const gp = parseFloat(item.gross_profit || 0);
      const gl = parseFloat(item.gross_loss || 0);
      const netSign = net > 0 ? '+' : '';
      const netClass = net > 0 ? 'pnl-pos' : (net < 0 ? 'pnl-neg' : 'pnl-neutral');
      const gpClass = gp > 0 ? 'pnl-pos' : 'pnl-neutral';
      const glClass = gl < 0 ? 'pnl-neg' : 'pnl-neutral';

      html += `
        <tr>
          <td><strong>${item.commodity}</strong></td>
          <td style="text-align:right;" class="${gpClass}">${gp > 0 ? '+' : ''}₹${gp.toFixed(2)}</td>
          <td style="text-align:right;" class="${glClass}">${gl < 0 ? '-' : ''}₹${Math.abs(gl).toFixed(2)}</td>
          <td style="text-align:right;" class="${netClass}">${netSign}₹${net.toFixed(2)}</td>
          <td style="text-align:center;">${item.trade_count}</td>
          <td style="text-align:center;"><span style="color:var(--neon-green,#10b981);font-weight:700;">${item.win_count}W</span> / <span style="color:var(--neon-red,#ef4444);font-weight:700;">${item.loss_count}L</span></td>
        </tr>
      `;
    });

    html += `
      <tr class="pnl-total-row">
        <td><strong>TOTAL</strong></td>
        <td style="text-align:right;" class="${totalGpClass}">${totalGp > 0 ? '+' : ''}₹${totalGp.toFixed(2)}</td>
        <td style="text-align:right;" class="${totalGlClass}">${totalGl < 0 ? '-' : ''}₹${Math.abs(totalGl).toFixed(2)}</td>
        <td style="text-align:right;" class="${totalNetClass}">${totalNetSign}₹${totalNet.toFixed(2)}</td>
        <td style="text-align:center;">${total.trade_count}</td>
        <td style="text-align:center;"><span style="color:var(--neon-green,#10b981);font-weight:700;">${total.win_count}W</span> / <span style="color:var(--neon-red,#ef4444);font-weight:700;">${total.loss_count}L</span></td>
      </tr>
    `;
  }

  tbody.innerHTML = html;
}

function onEodCutoffSelectChange(val) {
  const input = document.getElementById('eodCutoffInput');
  if (val !== 'custom' && input) {
    input.value = val;
  }
}

function onEodCutoffInputChange(val) {
  const badge = document.getElementById('eodCutoffBadge');
  if (badge) badge.innerText = `Cutoff: ${val}`;
}

// ── Auth Modal Handlers ─────────────────────────────────
function showLoginModal() {
  const modal = document.getElementById('loginModal');
  if (modal) modal.style.display = 'flex';
}

function closeLoginModal() {
  const modal = document.getElementById('loginModal');
  if (modal) modal.style.display = 'none';
}

async function handleLoginSubmit(event) {
  event.preventDefault();
  const clientId = document.getElementById('loginClientId').value;
  const password = document.getElementById('loginPassword').value;
  const errorMsg = document.getElementById('loginErrorMsg');

  try {
    const res = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: clientId, password }),
    });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      setAuthToken(data.token, data.client_id);
      closeLoginModal();
      fetchState();
      connectEventStream();
      fetchPnlSummary();
    } else {
      if (errorMsg) {
        errorMsg.innerText = data.detail || 'Login failed';
        errorMsg.style.display = 'block';
      }
    }
  } catch (e) {
    if (errorMsg) {
      errorMsg.innerText = 'Network error during login';
      errorMsg.style.display = 'block';
    }
  }
}

async function handleLogout() {
  try {
    await fetch('/auth/logout', { method: 'POST', headers: { Authorization: `Bearer ${getAuthToken()}` } });
  } catch (e) { }
  removeAuthToken();
  location.reload();
}

// ── Initialization on Page Load ─────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  syncFormInputs(null, currentCommodity, true);
  updateUserBadge();
  updateSpanBadge();
  initAstroDropzone();
  connectEventStream();
  fetchAndApplyAutoOptLtp();
  fetchPnlSummary();
});
