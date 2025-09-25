// server.js — Express proxy/control shim for Amazon listing checks
// Node >= 18 (global fetch available). package.json should have "type":"module".

import express from 'express';
import cors from 'cors';

const app = express();

// ---------- Config via environment ----------
const PORT         = Number(process.env.PORT) || 3000;
const UI_KEY       = process.env.UI_KEY || '';             // optional UI key to protect /control/*
const UPSTREAM_URL = (process.env.UPSTREAM_URL || '').replace(/\/+$/,''); // optional: proxy to real SP-API worker
const MOCK_SPAPI   = process.env.MOCK_SPAPI === '0' ? false : true;       // default true when no UPSTREAM_URL

// ---------- Middleware ----------
app.use(cors());
app.use(express.json({ limit: '2mb' }));

// Simple UI-key check for /control routes (optional)
function checkUiKey(req, res, next) {
  if (!UI_KEY) return next(); // open if not configured
  const k = (req.query.k || req.headers['x-ui-key'] || '').toString();
  if (k === UI_KEY) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

// ---------- Health ----------
app.get('/health', (req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

// ---------- Control (status + test action) ----------
app.get('/control/status', checkUiKey, (req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString(),
    version: process.env.RENDER_GIT_COMMIT || 'dev',
    hasUpstream: !!UPSTREAM_URL,
    mockMode: !UPSTREAM_URL && MOCK_SPAPI
  });
});

app.post('/control/action', checkUiKey, async (req, res) => {
  const { action, payload } = req.body || {};
  if (!action) return res.status(400).json({ ok: false, error: 'missing action' });
  // simple echo/ping
  if (action === 'ping') {
    return res.json({ ok: true, pong: true, at: new Date().toISOString(), payload: payload || null });
  }
  return res.json({ ok: true, received: { action, payload: payload || null } });
});

// ---------- Helpers ----------
function validateBody(req, keys) {
  const missing = [];
  keys.forEach(k => { if (req.body == null || !(k in req.body)) missing.push(k); });
  return missing;
}

async function proxyPost(path, body) {
  if (!UPSTREAM_URL) throw new Error('UPSTREAM_URL not configured');
  const url = `${UPSTREAM_URL}${path.startsWith('/') ? path : `/${path}`}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type':'application/json' },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  let json;
  try { json = JSON.parse(text); } catch {
    throw new Error(`Upstream non-JSON response: ${text.slice(0,200)}`);
  }
  if (!r.ok) {
    const msg = (json && (json.error || json.message)) ? json.error || json.message : `HTTP ${r.status}`;
    const err = new Error(`Upstream error: ${msg}`);
    err.status = r.status;
    err.payload = json;
    throw err;
  }
  return json;
}

// ---------- Mock generators (structure-compatible) ----------
function mockRestrictions(asin, marketplaceIds) {
  const results = {};
  (marketplaceIds || []).forEach(m => {
    // deterministic-ish mock: Open if asin ends with 0–4
    const last = asin.slice(-1);
    const openish = /[0-4]/.test(last);
    results[m] = {
      exists: true,
      status: openish ? 'Open' : 'Restricted',
      reasonCodes: openish ? [] : ['BRAND_RESTRICTED'],
    };
  });
  return { ok: true, results };
}
function mockOffers(asin, marketplaceIds) {
  const results = {};
  (marketplaceIds || []).forEach(m => {
    const last = asin.slice(-1);
    const has = /[02468]/.test(last); // even => has offers
    results[m] = { hasNewOffers: !!has };
  });
  return { ok: true, results };
}

// ---------- Business endpoints ----------
// Single-ASIN restrictions (expects { asin, marketplaceIds, conditionType? })
app.post('/restrictions', async (req, res) => {
  const missing = validateBody(req, ['asin','marketplaceIds']);
  if (missing.length) return res.status(400).json({ ok:false, error:`missing fields: ${missing.join(', ')}` });

  try {
    if (UPSTREAM_URL) {
      const j = await proxyPost('/restrictions', req.body);
      return res.json(j);
    }
    if (!MOCK_SPAPI) return res.status(501).json({ ok:false, error:'Not configured (set UPSTREAM_URL or enable MOCK_SPAPI)' });
    return res.json(mockRestrictions(String(req.body.asin), req.body.marketplaceIds));
  } catch (err) {
    const code = err.status || 502;
    return res.status(code).json({ ok:false, error: err.message || 'upstream error', details: err.payload || null });
  }
});

// Single-ASIN offers (expects { asin, marketplaceIds, itemCondition?, includeSiblings? })
app.post('/offers', async (req, res) => {
  const missing = validateBody(req, ['asin','marketplaceIds']);
  if (missing.length) return res.status(400).json({ ok:false, error:`missing fields: ${missing.join(', ')}` });

  try {
    if (UPSTREAM_URL) {
      const j = await proxyPost('/offers', req.body);
      return res.json(j);
    }
    if (!MOCK_SPAPI) return res.status(501).json({ ok:false, error:'Not configured (set UPSTREAM_URL or enable MOCK_SPAPI)' });
    return res.json(mockOffers(String(req.body.asin), req.body.marketplaceIds));
  } catch (err) {
    const code = err.status || 502;
    return res.status(code).json({ ok:false, error: err.message || 'upstream error', details: err.payload || null });
  }
});

// Optional: batch restrictions (expects { asins:[], marketplaceIds:[] })
app.post('/restrictions/batch', async (req, res) => {
  const missing = validateBody(req, ['asins','marketplaceIds']);
  if (missing.length) return res.status(400).json({ ok:false, error:`missing fields: ${missing.join(', ')}` });
  const { asins, marketplaceIds } = req.body;

  try {
    if (UPSTREAM_URL) {
      const j = await proxyPost('/restrictions/batch', req.body);
      return res.json(j);
    }
    if (!MOCK_SPAPI) return res.status(501).json({ ok:false, error:'Not configured (set UPSTREAM_URL or enable MOCK_SPAPI)' });
    const out = {};
    asins.forEach(a => { out[a] = mockRestrictions(String(a), marketplaceIds).results; });
    return res.json({ ok:true, results: out });
  } catch (err) {
    const code = err.status || 502;
    return res.status(code).json({ ok:false, error: err.message || 'upstream error', details: err.payload || null });
  }
});

// Optional: batch offers (expects { asins:[], marketplaceIds:[] })
app.post('/offers/batch', async (req, res) => {
  const missing = validateBody(req, ['asins','marketplaceIds']);
  if (missing.length) return res.status(400).json({ ok:false, error:`missing fields: ${missing.join(', ')}` });
  const { asins, marketplaceIds } = req.body;

  try {
    if (UPSTREAM_URL) {
      const j = await proxyPost('/offers/batch', req.body);
      return res.json(j);
    }
    if (!MOCK_SPAPI) return res.status(501).json({ ok:false, error:'Not configured (set UPSTREAM_URL or enable MOCK_SPAPI)' });
    const out = {};
    asins.forEach(a => { out[a] = mockOffers(String(a), marketplaceIds).results; });
    return res.json({ ok:true, results: out });
  } catch (err) {
    const code = err.status || 502;
    return res.status(code).json({ ok:false, error: err.message || 'upstream error', details: err.payload || null });
  }
});

// ---------- 404 handler ----------
app.use((req, res) => {
  res.status(404).json({ ok:false, error:'not found', path:req.path });
});

// ---------- Start server (single listen) ----------
app.listen(PORT, () => {
  console.log(`Server listening on ${PORT} ${UPSTREAM_URL ? '(proxy mode)' : (MOCK_SPAPI ? '(mock mode)' : '')}`);
});
