/**
 * Astro Index Futures Scalper — Cockpit Client v1.0
 * Connects to /events SSE stream for live state
 * Handles 6 index slots (NIFTY-B/S, BANKNIFTY-B/S, SENSEX-B/S)
 * Symmetrical long/short ladder rendering, MCX futures pricing & execution
 */

let currentState = null;
let eventSource = null;
let activeSlotId = 'NIFTY-B';
let currentCommodity = 'NIFTY';
let currentTradeSide = 'BUY';
let currentProductType = 'INTRADAY';

const INDEX_CONFIG = {
  NIFTY: {
    name: 'NIFTY',
    displayName: 'Nifty 50 Futures',
    lotSize: 65,
    unit: 'pts',
    tickSize: 0.05,
    defaultLtp: 25000,
    defaultRangePoints: 600,
    defaultSlicerCount: 10,
    defaultRangeHigh: 25300,
    defaultRangeLow: 24700,
    defaultStep: 60,
    defaultProfitPoint: 60,
    defaultLossPoint: 90,
    defaultQtyPerSliceLots: 1,
  },
  BANKNIFTY: {
    name: 'BANKNIFTY',
    displayName: 'Bank Nifty Futures',
    lotSize: 30,
    unit: 'pts',
    tickSize: 0.05,
    defaultLtp: 54000,
    defaultRangePoints: 800,
    defaultSlicerCount: 10,
    defaultRangeHigh: 54400,
    defaultRangeLow: 53600,
    defaultStep: 80,
    defaultProfitPoint: 80,
    defaultLossPoint: 120,
    defaultQtyPerSliceLots: 1,
  },
  SENSEX: {
    name: 'SENSEX',
    displayName: 'BSE Sensex Futures',
    lotSize: 20,
    unit: 'pts',
    tickSize: 0.05,
    defaultLtp: 82000,
    defaultRangePoints: 1200,
    defaultSlicerCount: 8,
    defaultRangeHigh: 82600,
    defaultRangeLow: 81400,
    defaultStep: 150,
    defaultProfitPoint: 150,
    defaultLossPoint: 225,
    defaultQtyPerSliceLots: 1,
  },
};

const COMMODITY_CONFIG = INDEX_CONFIG;

const INDEX_PLAUSIBLE_RANGES = {
  NIFTY: [10000, 50000],
  BANKNIFTY: [20000, 100000],
  SENSEX: [30000, 150000],
};
const COMMODITY_PLAUSIBLE_RANGES = INDEX_PLAUSIBLE_RANGES;

function isPlausibleLtp(commodity, price) {
  if (typeof price !== 'number' || isNaN(price) || price <= 0) return false;
  const comm = (commodity || '').toUpperCase();
  const rng = COMMODITY_PLAUSIBLE_RANGES[comm];
  if (!rng) return price > 0;
  return price >= rng[0] && price <= rng[1];
}


const ALL_SLOTS = [
  { id: 'NIFTY-B', commodity: 'NIFTY', side: 'BUY', label: 'NIFTY BUY', badge: 'NIFTY-B', color: 'badge-call' },
  { id: 'NIFTY-S', commodity: 'NIFTY', side: 'SELL', label: 'NIFTY SELL', badge: 'NIFTY-S', color: 'badge-put' },
  { id: 'BANKNIFTY-B', commodity: 'BANKNIFTY', side: 'BUY', label: 'BANKNIFTY BUY', badge: 'BANKNIFTY-B', color: 'badge-call' },
  { id: 'BANKNIFTY-S', commodity: 'BANKNIFTY', side: 'SELL', label: 'BANKNIFTY SELL', badge: 'BANKNIFTY-S', color: 'badge-put' },
  { id: 'SENSEX-B', commodity: 'SENSEX', side: 'BUY', label: 'SENSEX BUY', badge: 'SENSEX-B', color: 'badge-call' },
  { id: 'SENSEX-S', commodity: 'SENSEX', side: 'SELL', label: 'SENSEX SELL', badge: 'SENSEX-S', color: 'badge-put' },
];

// ── Authentication & Session Management ────────────────────────
const AUTH_TOKEN_KEY = 'slicer_jwt_token';
const AUTH_CLIENT_ID_KEY = 'slicer_client_id';

function getAuthToken() {
  try {
    return sessionStorage.getItem(AUTH_TOKEN_KEY) || localStorage.getItem(AUTH_TOKEN_KEY) || '';
  } catch (e) {
    return '';
  }
}

function setAuthToken(token, clientId) {
  try {
    if (token) {
      sessionStorage.setItem(AUTH_TOKEN_KEY, token);
      localStorage.setItem(AUTH_TOKEN_KEY, token);
    }
    if (clientId) {
      sessionStorage.setItem(AUTH_CLIENT_ID_KEY, clientId);
      localStorage.setItem(AUTH_CLIENT_ID_KEY, clientId);
    }
  } catch (e) { }
  updateUserBadge();
}

function removeAuthToken() {
  try {
    sessionStorage.removeItem(AUTH_TOKEN_KEY);
    sessionStorage.removeItem(AUTH_CLIENT_ID_KEY);
    localStorage.removeItem(AUTH_TOKEN_KEY);
    localStorage.removeItem(AUTH_CLIENT_ID_KEY);
  } catch (e) { }
  updateUserBadge();
}

function getStoredClientId() {
  try {
    return sessionStorage.getItem(AUTH_CLIENT_ID_KEY) || localStorage.getItem(AUTH_CLIENT_ID_KEY) || '';
  } catch (e) {
    return '';
  }
}

function updateUserBadge() {
  const badge = document.getElementById('userSessionBadge');
  const nameEl = document.getElementById('headerCurrentUserName');
  const scopeEl = document.getElementById('headerUserDbScope');
  const iconEl = document.getElementById('userAvatarIcon');
  const cid = getStoredClientId();

  if (cid) {
    if (nameEl) nameEl.innerText = cid;
    if (scopeEl) scopeEl.innerText = `Future_Indexes.${cid}`;
    if (iconEl) iconEl.innerText = cid.toLowerCase() === 'admin' ? '👑' : '👤';
    if (badge) badge.style.display = 'inline-flex';
  } else {
    if (nameEl) nameEl.innerText = 'Not Signed In';
    if (scopeEl) scopeEl.innerText = 'Click to sign in';
    if (iconEl) iconEl.innerText = '🔒';
    if (badge) badge.style.display = 'inline-flex';
  }
}

// ── Trading Mode State ─────────────────────────────────
let currentClientTradingMode = 'paper';
let currentLedgerTab = 'slices';
let currentLedgerModeFilter = 'all';

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
    if (title) title.innerText = 'LIVE INDEX TRADING ACTIVE — REAL MONEY AT RISK';
    if (sub) sub.innerText = '(Angel One SmartAPI Live Execution on NFO / BFO · Fail-Closed Active)';
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
      connEl.innerHTML = '<span class="stream-ping-dot pulse-indicator"></span><span class="stream-label status-text">Live Market</span>';
    }
  };

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      currentState = data;
      renderAll(data);
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
    const cfg = COMMODITY_CONFIG[slotObj.commodity] || INDEX_CONFIG.NIFTY;
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
    if (isEscalated) {
      banner.className = 'feed-error-banner escalated';
      if (icon) icon.innerText = '🚨';
      if (title) title.innerText = `CRITICAL FEED OUTAGE: ${feedErr}`;
      if (sub) sub.innerText = `Persisting for ${persistedSec}s across cycles · Action Required: Ensure Index Futures (NFO/BFO) Segment is enabled in Angel One portal for this API key.`;
    } else {
      banner.className = 'feed-error-banner';
      if (icon) icon.innerText = '⚠️';
      if (title) title.innerText = `Live Feed Error: ${feedErr} — Auto-retrying session`;
      if (sub) sub.innerText = `Persisting for ${persistedSec}s · Retrying broker token authentication in background...`;
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
  let html = '';

  ALL_SLOTS.forEach(slot => {
    const sData = matrix[slot.id] || {};
    const isActive = (slot.id === activeSlotId);
    const isRunning = sData.is_active || false;
    const sideClass = slot.side === 'BUY' ? 'tab-buy' : 'tab-sell';
    const statusClass = isRunning ? 'running' : (sData.slot_status === 'CLOSED' ? 'closed' : 'empty');
    const pnlVal = sData.total_pnl || 0;
    const pnlSign = pnlVal >= 0 ? '+' : '';

    html += `
      <button class="slot-tab-btn ${isActive ? 'active' : ''} ${sideClass} ${statusClass}" onclick="switchSlot('${slot.id}')">
        <div class="tab-top">
          <span class="tab-slot-badge ${slot.color}">${slot.badge}</span>
          <span class="tab-status-dot ${isRunning ? 'pulse' : ''}"></span>
        </div>
        <div class="tab-main">
          <span class="tab-label">${slot.label}</span>
          <span class="tab-pnl ${pnlVal >= 0 ? 'pos' : 'neg'}">${pnlSign}₹${pnlVal.toFixed(2)}</span>
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
  const cfg = COMMODITY_CONFIG[comm] || INDEX_CONFIG.NIFTY;

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

  // Filter history strictly for the currently active slot
  const slotTrades = history.filter(t => {
    const tRun = t.run_id || t.slot_id;
    if (tRun && (tRun === activeSlotId || SLOT_MAP[tRun] === activeSlotId)) return true;
    if (t.commodity === slotObj.commodity && t.trade_side === slotObj.side) return true;
    return false;
  });

  let slotRealizedPnl = 0;
  slotTrades.forEach(t => {
    slotRealizedPnl += parseFloat(t.pnl_rupees || 0);
  });

  const slotOverallPnl = document.getElementById('metricSlotOverallPnl');
  if (slotOverallPnl) {
    slotOverallPnl.innerText = `${slotRealizedPnl >= 0 ? '+' : ''}₹${slotRealizedPnl.toFixed(2)}`;
    slotOverallPnl.style.color = slotRealizedPnl >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  const slotOverallCount = document.getElementById('metricSlotOverallCount');
  if (slotOverallCount) {
    slotOverallCount.innerText = `${slotTrades.length} Closed`;
  }

  const slotTodayPnl = document.getElementById('metricSlotTodayPnl');
  if (slotTodayPnl) {
    slotTodayPnl.innerText = `${slotRealizedPnl >= 0 ? '+' : ''}₹${slotRealizedPnl.toFixed(2)}`;
    slotTodayPnl.style.color = slotRealizedPnl >= 0 ? 'var(--neon-cyan, #00f0ff)' : 'var(--neon-red, #ef4444)';
  }
  const slotTodayCount = document.getElementById('metricSlotTodayCount');
  if (slotTodayCount) {
    slotTodayCount.innerText = `${slotTrades.length} Today`;
  }

  // Gross Realized per active Commodity / Index (BUY + SELL)
  const targetComm = (comm || currentCommodity || 'NIFTY').toUpperCase();
  const buySlotId = `${targetComm}-B`;
  const sellSlotId = `${targetComm}-S`;

  const grossTag = document.getElementById('metricGrossTag');
  if (grossTag) grossTag.innerText = `OVERALL REALIZED [${targetComm}]`;

  const grossBadge = document.getElementById('metricGrossBadge');
  const grossBuyLabel = document.getElementById('metricGrossCol1Label');
  const grossSellLabel = document.getElementById('metricGrossCol2Label');
  if (grossBuyLabel) grossBuyLabel.innerText = `🟢 BUY [${buySlotId}]`;
  if (grossSellLabel) grossSellLabel.innerText = `🔴 SELL [${sellSlotId}]`;

  const grossBuy = document.getElementById('metricGrossBuyPnl');
  const grossSell = document.getElementById('metricGrossSellPnl');
  const grossBuyCount = document.getElementById('metricGrossBuyCount');
  const grossSellCount = document.getElementById('metricGrossSellCount');

  const grossPaper = document.getElementById('metricGrossPaperPnl');
  const grossLive = document.getElementById('metricGrossLivePnl');
  const grossPaperCount = document.getElementById('metricGrossPaperCount');
  const grossLiveCount = document.getElementById('metricGrossLiveCount');

  // Filter history strictly for the currently active index (both BUY and SELL)
  const indexTrades = history.filter(t => {
    const tRun = t.run_id || t.slot_id || '';
    const canonicalRun = SLOT_MAP[tRun] || tRun;
    if (canonicalRun === buySlotId || canonicalRun === sellSlotId) return true;
    const tComm = (t.commodity || (tRun ? tRun.split('-')[0] : '')).toUpperCase();
    if (tComm === targetComm) return true;
    return false;
  });

  let buyPnl = 0, sellPnl = 0, buyCnt = 0, sellCnt = 0;
  let paperTot = 0, liveTot = 0, paperCnt = 0, liveCnt = 0;

  indexTrades.forEach(t => {
    const p = parseFloat(t.pnl_rupees || 0);
    const tRun = t.run_id || t.slot_id || '';
    const canonicalRun = SLOT_MAP[tRun] || tRun;
    const side = (t.trade_side || (canonicalRun.endsWith('-S') ? 'SELL' : 'BUY')).toUpperCase();

    if (side === 'SELL' || canonicalRun === sellSlotId) {
      sellPnl += p;
      sellCnt++;
    } else {
      buyPnl += p;
      buyCnt++;
    }

    if ((t.mode || 'paper').toLowerCase() === 'live') {
      liveTot += p;
      liveCnt++;
    } else {
      paperTot += p;
      paperCnt++;
    }
  });

  // Fallback to engine slots_matrix if history not yet synced or empty
  const matrix = state.slots_matrix || {};
  const buySlotData = matrix[buySlotId];
  const sellSlotData = matrix[sellSlotId];
  if (indexTrades.length === 0) {
    if (buySlotData && typeof buySlotData.realized_pnl === 'number') {
      buyPnl = buySlotData.realized_pnl;
    }
    if (sellSlotData && typeof sellSlotData.realized_pnl === 'number') {
      sellPnl = sellSlotData.realized_pnl;
    }
    paperTot = buyPnl + sellPnl;
  }

  const netOverallIndexRealized = Math.round((buyPnl + sellPnl) * 100) / 100;

  if (grossBuy) {
    grossBuy.innerText = `${buyPnl >= 0 ? '+' : ''}₹${buyPnl.toFixed(2)}`;
    grossBuy.style.color = buyPnl >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  if (grossBuyCount) grossBuyCount.innerText = `${buyCnt} Closed`;

  if (grossSell) {
    grossSell.innerText = `${sellPnl >= 0 ? '+' : ''}₹${sellPnl.toFixed(2)}`;
    grossSell.style.color = sellPnl >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }
  if (grossSellCount) grossSellCount.innerText = `${sellCnt} Closed`;

  if (grossBadge) {
    grossBadge.innerText = `NET: ${netOverallIndexRealized >= 0 ? '+' : ''}₹${netOverallIndexRealized.toFixed(2)}`;
    grossBadge.style.color = netOverallIndexRealized >= 0 ? 'var(--neon-green, #10b981)' : 'var(--neon-red, #ef4444)';
  }

  if (grossPaper) grossPaper.innerText = `${paperTot >= 0 ? '+' : ''}₹${paperTot.toFixed(2)}`;
  if (grossPaperCount) grossPaperCount.innerText = `${paperCnt}`;
  if (grossLive) grossLive.innerText = `${liveTot >= 0 ? '+' : ''}₹${liveTot.toFixed(2)}`;
  if (grossLiveCount) grossLiveCount.innerText = `${liveCnt}`;

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

  const cfg = COMMODITY_CONFIG[currentCommodity] || INDEX_CONFIG.NIFTY;
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
  const cfg = COMMODITY_CONFIG[comm] || INDEX_CONFIG.NIFTY;
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
    if (fileMeta) fileMeta.innerText = `${astroState.row_count || 0} signal rows active in FUTURE_INDEXES.astro_files${uploadTimeStr}`;
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
    autoToggle.checked = isAuto;
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

const SLOT_MAP = {
  'run01': 'NIFTY-B',
  'run02': 'NIFTY-S',
  'run03': 'BANKNIFTY-B',
  'run04': 'BANKNIFTY-S',
  'run05': 'SENSEX-B',
  'run06': 'SENSEX-S',
  'NIFTY-B': 'NIFTY-B',
  'NIFTY-S': 'NIFTY-S',
  'BANKNIFTY-B': 'BANKNIFTY-B',
  'BANKNIFTY-S': 'BANKNIFTY-S',
  'SENSEX-B': 'SENSEX-B',
  'SENSEX-S': 'SENSEX-S',
};

const SLOT_NUMBERS = {
  'NIFTY-B': 'Slot 1',
  'NIFTY-S': 'Slot 2',
  'BANKNIFTY-B': 'Slot 3',
  'BANKNIFTY-S': 'Slot 4',
  'SENSEX-B': 'Slot 5',
  'SENSEX-S': 'Slot 6',
};

function renderHistoryLedger(state) {
  const slicesTbody = document.getElementById('closedSlicesTableBody');
  const badge = document.getElementById('closedSlicesBadge');
  const ledgerSub = document.getElementById('ledgerSubHeadline');
  const btnClear = document.getElementById('btnClearHistory');
  const slotObj = ALL_SLOTS.find(s => s.id === activeSlotId) || { label: activeSlotId, commodity: currentCommodity, side: currentTradeSide };
  const slotNum = SLOT_NUMBERS[activeSlotId] || activeSlotId;

  if (ledgerSub) {
    ledgerSub.innerText = `Filtered for ${slotObj.label} (${slotNum})`;
  }
  if (btnClear) {
    btnClear.innerText = `🗑 Clear ${slotNum} Trades`;
    btnClear.title = `Clear trade history for ${slotObj.label} (${slotNum})`;
  }

  const allHistory = state.history || [];
  // Filter history strictly for the currently active slot
  const slotTrades = allHistory.filter(t => {
    const tRun = t.run_id || t.slot_id;
    if (tRun && (tRun === activeSlotId || SLOT_MAP[tRun] === activeSlotId)) return true;
    if (t.commodity === slotObj.commodity && t.trade_side === slotObj.side) return true;
    return false;
  });

  if (badge) {
    badge.innerText = `${slotTrades.length} Closed [${slotNum}]`;
  }
  if (!slicesTbody) return;

  let filtered = slotTrades;
  if (currentLedgerModeFilter !== 'all') {
    filtered = slotTrades.filter(t => (t.mode || 'paper').toLowerCase() === currentLedgerModeFilter);
  }

  if (filtered.length === 0) {
    slicesTbody.innerHTML = `
      <tr class="empty-table-row empty-row">
        <td colspan="14">No closed futures executions for <strong>${slotObj.label} (${slotNum})</strong> yet.</td>
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

    html += `
      <tr>
        <td>${modeBadge}</td>
        <td><strong>${t.commodity || slotObj.commodity}</strong></td>
        <td><span class="slot-badge-id ${slotObj.side === 'BUY' ? 'badge-call' : 'badge-put'}">${t.run_id || activeSlotId}</span></td>
        <td><span class="rung-status-badge ${t.trade_side === 'SELL' ? 'status-used' : 'status-filled'}">${t.trade_side || slotObj.side}</span></td>
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
  const comm = slotObj ? slotObj.commodity : (commodity || currentCommodity || 'NIFTY');
  const cfg = COMMODITY_CONFIG[comm] || INDEX_CONFIG.NIFTY;
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
  const slot = ALL_SLOTS.find(s => s.commodity === val && s.side === currentTradeSide);
  if (slot) {
    activeSlotId = slot.id;
  }
  syncFormInputs(null, val, true);
  lastSyncedSlotId = activeSlotId;

  fetch(`/runs/${activeSlotId}/commodity`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
    body: JSON.stringify({ commodity: val }),
  });
  fetchAndApplyAutoOptLtp();
}

function onTradeSideChange(val) {
  currentTradeSide = val;
  const slot = ALL_SLOTS.find(s => s.commodity === currentCommodity && s.side === val);
  if (slot) {
    activeSlotId = slot.id;
  }
  let slotRun = null;
  if (currentState && currentState.runs) {
    if (Array.isArray(currentState.runs)) {
      slotRun = currentState.runs.find(r => (r.slot_id === activeSlotId || r.run_id === activeSlotId));
    } else {
      slotRun = currentState.runs[activeSlotId];
    }
  }
  syncFormInputs(slotRun ? slotRun.config : null, currentCommodity, true);
  lastSyncedSlotId = activeSlotId;

  fetch(`/runs/${activeSlotId}/trade_side`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
    body: JSON.stringify({ trade_side: val }),
  });
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
  const comm = currentCommodity || 'NIFTY';
  const cfg = COMMODITY_CONFIG[comm] || INDEX_CONFIG.NIFTY;
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
  try {
    const res = await fetch(`/runs/${activeSlotId}/auto_trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${getAuthToken()}` },
      body: JSON.stringify({ enabled, slot_id: activeSlotId }),
    });
    const data = await res.json().catch(() => ({}));
    if (data && data.auto_trigger) {
      if (!currentState) currentState = {};
      if (!currentState.astro_state) currentState.astro_state = {};
      currentState.astro_state.auto_trigger_by_slot = data.auto_trigger;
      renderAll(currentState);
    }
  } catch (e) {
    console.error('Error toggling auto execution:', e);
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

    setAstroUploadFeedback(`✅ Uploaded "${data.filename || file.name}" (${data.row_count || 0} signal rows active in FUTURE_INDEXES.astro_files)${autoNotice}`, enabledSlots.length > 0 ? 'success' : 'info');
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
  try {
    await fetch(`/runs/${activeSlotId}/clear_history`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    fetchPnlSummary();
  } catch (e) {
    console.error('Error clearing slot history:', e);
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
function showLoginModal(isMandatory = false) {
  const modal = document.getElementById('loginModal');
  if (modal) modal.style.display = 'flex';

  const cidInput = document.getElementById('loginClientId');
  const pwInput = document.getElementById('loginPassword');
  const closeBtn = document.querySelector('.btn-modal-close');
  const cancelBtn = document.querySelector('.btn-cancel-modal');
  const logoutBtn = document.getElementById('btnModalLogout');
  const currentCid = getStoredClientId();
  const hasToken = Boolean(getAuthToken());

  if (closeBtn) closeBtn.style.display = (isMandatory || !hasToken) ? 'none' : 'inline-block';
  if (cancelBtn) cancelBtn.style.display = (isMandatory || !hasToken) ? 'none' : 'inline-block';
  if (logoutBtn) logoutBtn.style.display = hasToken ? 'inline-block' : 'none';

  if (cidInput) {
    cidInput.value = currentCid || '';
  }
  if (pwInput) {
    pwInput.value = '';
    if (cidInput && cidInput.value) {
      pwInput.focus();
    } else if (cidInput) {
      cidInput.focus();
    }
  }
}

function closeLoginModal() {
  if (!getAuthToken()) {
    const errorMsg = document.getElementById('loginErrorMsg');
    if (errorMsg) {
      errorMsg.innerText = 'Sign in is required to access the dashboard.';
      errorMsg.style.display = 'block';
    }
    return;
  }
  const modal = document.getElementById('loginModal');
  if (modal) modal.style.display = 'none';
  const errorMsg = document.getElementById('loginErrorMsg');
  const successMsg = document.getElementById('loginSuccessMsg');
  if (errorMsg) errorMsg.style.display = 'none';
  if (successMsg) successMsg.style.display = 'none';
}

function togglePasswordVisibility(inputId, btn) {
  const input = document.getElementById(inputId);
  if (!input) return;
  if (input.type === 'password') {
    input.type = 'text';
    if (btn) btn.innerText = '🔒';
  } else {
    input.type = 'password';
    if (btn) btn.innerText = '👁️';
  }
}

async function handleLoginSubmit(event) {
  event.preventDefault();
  const clientId = (document.getElementById('loginClientId').value || '').trim();
  const password = document.getElementById('loginPassword').value;
  const errorMsg = document.getElementById('loginErrorMsg');
  const successMsg = document.getElementById('loginSuccessMsg');
  const submitBtn = document.getElementById('btnLoginSubmit');

  if (!clientId || !password) {
    if (errorMsg) {
      errorMsg.innerText = 'Please enter both Client ID and Password.';
      errorMsg.style.display = 'block';
    }
    return;
  }

  if (errorMsg) errorMsg.style.display = 'none';
  if (successMsg) successMsg.style.display = 'none';
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.innerText = 'Signing In...';
  }

  try {
    const res = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: clientId, password }),
    });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      setAuthToken(data.token, data.client_id);
      if (successMsg) {
        successMsg.innerText = `✅ Signed in as ${data.client_id}`;
        successMsg.style.display = 'block';
      }
      setTimeout(() => {
        closeLoginModal();
        connectEventStream();
        fetchState();
        fetchHistoryLedger();
        if (typeof fetchCredentialStatus === 'function') fetchCredentialStatus();
      }, 500);
    } else {
      if (errorMsg) {
        errorMsg.innerText = data.detail || 'Invalid Client ID or Password.';
        errorMsg.style.display = 'block';
      }
    }
  } catch (e) {
    if (errorMsg) {
      errorMsg.innerText = 'Network error during login: ' + e.message;
      errorMsg.style.display = 'block';
    }
  } finally {
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.innerText = 'Sign In →';
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

// ── P&L Summary Card ─────────────────────────────────

let currentPnlPeriod = 'day';
let currentPnlModeFilter = 'all';
let currentPnlCustomDate = '';

/**
 * Fetches /history/summary and renders the P&L summary table.
 * Mirrors the pattern of setLedgerModeFilter → renderHistoryLedger.
 */
async function fetchPnlSummary() {
  const tbody = document.getElementById('pnlSummaryTableBody');
  const badge = document.getElementById('pnlSummaryBadge');
  const rangeLabel = document.getElementById('pnlSummaryRangeLabel');

  if (tbody) {
    tbody.innerHTML = '<tr class="empty-table-row empty-row"><td colspan="6">⏳ Loading…</td></tr>';
  }

  let url = `/history/summary?period=${encodeURIComponent(currentPnlPeriod)}`;
  if (currentPnlModeFilter !== 'all') {
    url += `&mode=${encodeURIComponent(currentPnlModeFilter)}`;
  }
  if (currentPnlPeriod === 'custom' && currentPnlCustomDate) {
    url += `&date=${encodeURIComponent(currentPnlCustomDate)}`;
  }

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${getAuthToken()}` },
    });
    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      if (tbody) {
        tbody.innerHTML = `<tr class="empty-table-row empty-row"><td colspan="6">Error: ${errData.detail || res.statusText}</td></tr>`;
      }
      return;
    }
    const data = await res.json();
    if (data && data.status === 'success') {
      renderPnlSummary(data);
    }
  } catch (e) {
    console.error('Error fetching P&L summary:', e);
    if (tbody) {
      tbody.innerHTML = '<tr class="empty-table-row empty-row"><td colspan="6">Network error loading P&L Summary.</td></tr>';
    }
  }
}

/**
 * Renders the P&L summary table from the /history/summary API response.
 */
function renderPnlSummary(data) {
  const tbody = document.getElementById('pnlSummaryTableBody');
  const badge = document.getElementById('pnlSummaryBadge');
  const rangeLabel = document.getElementById('pnlSummaryRangeLabel');

  if (!tbody) return;

  // Update range label
  const rangeFrom = (data.range && data.range.from) || '';
  const rangeTo = (data.range && data.range.to) || '';
  const periodLabels = { day: 'Today', week: 'This Week', month: 'This Month', custom: 'Custom Date' };
  const periodLabel = periodLabels[currentPnlPeriod] || currentPnlPeriod;
  const modeLabel = currentPnlModeFilter === 'all' ? 'All Modes' : (currentPnlModeFilter === 'paper' ? 'Paper' : 'Live');

  if (rangeLabel) {
    if (rangeFrom === rangeTo) {
      rangeLabel.innerText = `Gross P&L by Instrument — ${periodLabel} (${rangeFrom}) · ${modeLabel}`;
    } else {
      rangeLabel.innerText = `Gross P&L by Instrument — ${periodLabel} (${rangeFrom} → ${rangeTo}) · ${modeLabel}`;
    }
  }

  // Update badge with total trade count
  const total = data.total || {};
  if (badge) {
    badge.innerText = `${total.trade_count || 0} Trades`;
  }

  const rows = data.by_instrument || [];
  if (rows.length === 0 && (!total || total.trade_count === 0)) {
    tbody.innerHTML = '<tr class="empty-table-row empty-row"><td colspan="6">No trades found for this period.</td></tr>';
    return;
  }

  function fmtPnl(val, alwaysSign = false) {
    const n = parseFloat(val) || 0;
    const sign = n > 0 ? '+' : '';
    return `${alwaysSign ? sign : ''}₹${n.toFixed(2)}`;
  }
  function pnlColor(val) {
    const n = parseFloat(val) || 0;
    return n >= 0 ? 'var(--neon-green,#10b981)' : 'var(--neon-red,#ef4444)';
  }

  let html = '';

  rows.forEach(row => {
    const netPnl = parseFloat(row.net_pnl) || 0;
    html += `
      <tr>
        <td><strong>${row.instrument}</strong></td>
        <td style="color:var(--neon-green,#10b981);font-weight:600;">${fmtPnl(row.gross_profit)}</td>
        <td style="color:var(--neon-red,#ef4444);font-weight:600;">${fmtPnl(row.gross_loss)}</td>
        <td style="color:${pnlColor(netPnl)};font-weight:700;">${fmtPnl(netPnl, true)}</td>
        <td>${row.trade_count || 0}</td>
        <td><span style="color:var(--neon-green,#10b981);">${row.win_count || 0}W</span> / <span style="color:var(--neon-red,#ef4444);">${row.loss_count || 0}L</span></td>
      </tr>
    `;
  });

  // TOTAL row
  const totalNetPnl = parseFloat(total.net_pnl) || 0;
  html += `
    <tr style="border-top:1px solid rgba(255,255,255,0.15); font-weight:800;">
      <td><strong style="color:var(--neon-cyan,#00f0ff);">TOTAL</strong></td>
      <td style="color:var(--neon-green,#10b981);font-weight:800;">${fmtPnl(total.gross_profit)}</td>
      <td style="color:var(--neon-red,#ef4444);font-weight:800;">${fmtPnl(total.gross_loss)}</td>
      <td style="color:${pnlColor(totalNetPnl)};font-weight:900;font-size:1.02em;">${fmtPnl(totalNetPnl, true)}</td>
      <td><strong>${total.trade_count || 0}</strong></td>
      <td>
        <span style="color:var(--neon-green,#10b981);">${total.win_count || 0}W</span> /
        <span style="color:var(--neon-red,#ef4444);">${total.loss_count || 0}L</span>
      </td>
    </tr>
  `;

  tbody.innerHTML = html;
}

/** Sets the mode filter for the P&L Summary card. */
function setPnlModeFilter(mode) {
  currentPnlModeFilter = mode;
  ['pnlBtnFilterAll', 'pnlBtnFilterPaper', 'pnlBtnFilterLive'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.remove('active');
  });
  if (mode === 'paper') document.getElementById('pnlBtnFilterPaper')?.classList.add('active');
  else if (mode === 'live') document.getElementById('pnlBtnFilterLive')?.classList.add('active');
  else document.getElementById('pnlBtnFilterAll')?.classList.add('active');
  fetchPnlSummary();
}

/** Handles period dropdown changes for the P&L Summary card. */
function onPnlPeriodChange(value) {
  currentPnlPeriod = value;
  const customInput = document.getElementById('pnlCustomDate');
  if (customInput) {
    customInput.style.display = (value === 'custom') ? 'inline-block' : 'none';
  }
  if (value !== 'custom') {
    fetchPnlSummary();
  }
}

/** Handles custom date input changes for the P&L Summary card. */
function onPnlCustomDateChange(value) {
  currentPnlCustomDate = value;
  if (value) {
    fetchPnlSummary();
  }
}

// ── Initialization on Page Load ─────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const token = getAuthToken();
  if (!token) {
    updateUserBadge();
    showLoginModal(true);
  } else {
    updateUserBadge();
    connectEventStream();
    fetchState();
    fetchPnlSummary();
  }
  syncFormInputs(null, currentCommodity, true);
  updateSpanBadge();
  initAstroDropzone();
  fetchAndApplyAutoOptLtp();
});
