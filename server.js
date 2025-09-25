import express from 'express';
import cors from 'cors';
import SellingPartnerAPI from 'amazon-sp-api';

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ---------- Config via ENV ----------
const {
  PORT = 3000,

  // LWA (Login With Amazon)
  LWA_CLIENT_ID,
  LWA_CLIENT_SECRET,

  // Refresh token for the selling partner
  SPAPI_REFRESH_TOKEN,

  // AWS keys (role is recommended if you have it)
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_SELLING_PARTNER_ROLE // optional role ARN
} = process.env;

// ---------- Helpers ----------
const MP_REGION = {
  // EU
  A1F83G8C2ARO7P: 'eu', // UK
  A13V1IB3VIYZZH: 'eu', // FR
  APJ6JRA9NG5V4: 'eu',  // IT
  A1RKKUPIHCS9HS: 'eu', // ES
  A1805IZSGTT6HS: 'eu', // NL
  A1C3SOZRARQ6R3: 'eu', // PL
  A2NODRKZP88ZB9: 'eu', // SE
  AMEN7PMS3EDWL: 'eu',  // BE
  // NA + FE
  ATVPDKIKX0DER: 'na',  // US
  A1VC38T7YXB528: 'fe', // JP
  A39IBJ37TRP1C6: 'fe'  // AU
};

function getRegionFor(mpId) {
  const r = MP_REGION[mpId];
  if (!r) throw new Error(`Unsupported marketplace: ${mpId}`);
  return r;
}

function newSpClient(region) {
  const opts = {
    region,
    refresh_token: SPAPI_REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: LWA_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: LWA_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY,
      role: AWS_SELLING_PARTNER_ROLE || undefined
    }
  };
  return new SellingPartnerAPI(opts);
}

// simple concurrency limiter without extra deps
async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0, running = 0;

  return new Promise((resolve) => {
    const next = () => {
      if (i >= items.length && running === 0) return resolve(results);
      while (running < limit && i < items.length) {
        const idx = i++;
        running++;
        Promise.resolve(worker(items[idx], idx))
          .then((res) => { results[idx] = res; })
          .catch((err) => { results[idx] = { error: String(err && err.message || err) }; })
          .finally(() => { running--; next(); });
      }
    };
    next();
  });
}

// format restriction => 'Open' | 'Closed'
function normalizeRestriction(resp) {
  try {
    if (!resp || resp.error) return { status: 'Closed' };
    const d = resp.restrictions || resp; // different shapes
    // If there are no reasons or status explicitly 'OPEN'
    if (Array.isArray(d) && d.length === 0) return { status: 'Open' };
    if (Array.isArray(d) && d.some(x => x && x.restriction)) return { status: 'Closed' };

    const st = (resp.status || '').toString().toLowerCase();
    if (st === 'open') return { status: 'Open' };
    return { status: 'Closed' };
  } catch {
    return { status: 'Closed' };
  }
}

// returns boolean “hasNewOffers”
function normalizeOffers(resp) {
  try {
    if (!resp || resp.error) return false;
    const offers = resp.Offers || resp.offers || resp.payload?.Offers || resp.payload?.offers || resp.payload;
    if (Array.isArray(offers)) return offers.length > 0;
    // Some SDKs return { payload: { offers: [...] } }
    const arr = offers?.offers || offers?.Offers;
    return Array.isArray(arr) ? arr.length > 0 : false;
  } catch {
    return false;
  }
}

// ---------- Endpoints ----------

app.get('/health', (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

/**
 * POST /restrictions
 * body: { asin, marketplaceIds: string[], conditionType?: 'new_new'|'used_like_new'... }
 */
app.post('/restrictions', async (req, res) => {
  try {
    const { asin, marketplaceIds = [], conditionType = 'new_new' } = req.body || {};
    if (!asin || !Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
      return res.status(400).json({ error: 'Provide asin and marketplaceIds[]' });
    }

    // call each marketplace (limit concurrency a bit)
    const results = {};
    await mapLimit(marketplaceIds, 5, async (mpId) => {
      const region = getRegionFor(mpId);
      const sp = newSpClient(region);
      try {
        const resp = await sp.callAPI({
          operation: 'getListingsRestrictions',
          endpoint: 'listingsRestrictions',
          path: { asin },
          query: { sellerId: undefined, marketplaceIds: mpId, conditionType }
        });
        results[mpId] = normalizeRestriction(resp);
      } catch (e) {
        results[mpId] = { status: 'Closed', error: String(e?.message || e) };
      }
    });

    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/**
 * POST /offers
 * body: { asin, marketplaceIds: string[], itemCondition?: 'New'|'Used' }
 */
app.post('/offers', async (req, res) => {
  try {
    const { asin, marketplaceIds = [], itemCondition = 'New' } = req.body || {};
    if (!asin || !Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
      return res.status(400).json({ error: 'Provide asin and marketplaceIds[]' });
    }

    const results = {};
    await mapLimit(marketplaceIds, 5, async (mpId) => {
      const region = getRegionFor(mpId);
      const sp = newSpClient(region);
      try {
        // Use Catalog Items or Product Pricing offers; here we use Product Pricing getItemOffers
        const resp = await sp.callAPI({
          operation: 'getItemOffers',
          endpoint: 'productPricing',
          query: {
            MarketplaceId: mpId,
            ItemCondition: itemCondition
          },
          path: { Asin: asin }
        });
        results[mpId] = { hasNewOffers: normalizeOffers(resp) };
      } catch (e) {
        results[mpId] = { hasNewOffers: false, error: String(e?.message || e) };
      }
    });

    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/**
 * POST /restrictionsBatch
 * body: { asins: string[], marketplaceIds: string[], conditionType?: 'new_new' }
 * Note: keep asins <= 100 per request
 */
app.post('/restrictionsBatch', async (req, res) => {
  try {
    const { asins = [], marketplaceIds = [], conditionType = 'new_new' } = req.body || {};
    if (!Array.isArray(asins) || asins.length === 0) return res.status(400).json({ error: 'Provide asins[]' });
    if (!Array.isArray(marketplaceIds) || marketplaceIds.length === 0) return res.status(400).json({ error: 'Provide marketplaceIds[]' });

    const out = {};
    // limit overall concurrency to avoid SP-API throttles
    await mapLimit(asins, 8, async (asin) => {
      const row = {};
      await mapLimit(marketplaceIds, 5, async (mpId) => {
        const region = getRegionFor(mpId);
        const sp = newSpClient(region);
        try {
          const resp = await sp.callAPI({
            operation: 'getListingsRestrictions',
            endpoint: 'listingsRestrictions',
            path: { asin },
            query: { marketplaceIds: mpId, conditionType }
          });
          row[mpId] = normalizeRestriction(resp);
        } catch (e) {
          row[mpId] = { status: 'Closed', error: String(e?.message || e) };
        }
      });
      out[asin] = row;
    });

    res.json({ results: out });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/**
 * POST /offersBatch
 * body: { asins: string[], marketplaceIds: string[], itemCondition?: 'New' }
 */
app.post('/offersBatch', async (req, res) => {
  try {
    const { asins = [], marketplaceIds = [], itemCondition = 'New' } = req.body || {};
    if (!Array.isArray(asins) || asins.length === 0) return res.status(400).json({ error: 'Provide asins[]' });
    if (!Array.isArray(marketplaceIds) || marketplaceIds.length === 0) return res.status(400).json({ error: 'Provide marketplaceIds[]' });

    const out = {};
    await mapLimit(asins, 8, async (asin) => {
      const row = {};
      await mapLimit(marketplaceIds, 5, async (mpId) => {
        const region = getRegionFor(mpId);
        const sp = newSpClient(region);
        try {
          const resp = await sp.callAPI({
            operation: 'getItemOffers',
            endpoint: 'productPricing',
            query: { MarketplaceId: mpId, ItemCondition: itemCondition },
            path: { Asin: asin }
          });
          row[mpId] = { hasNewOffers: normalizeOffers(resp) };
        } catch (e) {
          row[mpId] = { hasNewOffers: false, error: String(e?.message || e) };
        }
      });
      out[asin] = row;
    });

    res.json({ results: out });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// --- Add this block near the bottom of server.js, above app.listen(...) ---

// If you don't already have these:
import express from 'express';
import cors from 'cors';

// If your file already created the app & middlewares, skip these two:
const app = globalThis.app || express();
app.use(cors());
app.use(express.json());

// Optional UI key (leave blank to disable auth)
const UI_KEY = process.env.UI_KEY || '';

function checkUiKey(req, res, next) {
  if (!UI_KEY) return next(); // no key required
  const k = (req.query.k || req.headers['x-ui-key'] || '').toString();
  if (k === UI_KEY) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

// Simple health check
app.get('/healthz', (_req, res) => {
  res.status(200).send('ok');
});

// Control status (optional ?sheetId=... just gets echoed)
app.get('/control/status', checkUiKey, (req, res) => {
  const info = {
    ok: true,
    time: new Date().toISOString(),
    version: process.env.RENDER_GIT_COMMIT || 'dev',
    hasUiKey: !!UI_KEY,
    sheetId: req.query.sheetId || null
  };
  res.json(info);
});

// Optional: POST control/action so you can test POSTs easily
app.post('/control/action', checkUiKey, (req, res) => {
  const { action } = req.body || {};
  if (!action) return res.status(400).json({ ok: false, error: 'missing action' });
  // For now just echo back; wire real actions later
  res.json({ ok: true, action, receivedAt: new Date().toISOString() });
});

// Ensure PORT is a valid number
const PORT = Number(process.env.PORT) || 3000;
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});

// --- ADD: rotte di controllo ---

// Se hai già `app.use(express.json())`, puoi saltare questa riga:
app.use(express.json());

// (Opzionale) chiave per proteggere le rotte di controllo: impostala su Render come UI_KEY
const UI_KEY = process.env.UI_KEY || '';

function checkUiKey(req, res, next) {
  if (!UI_KEY) return next(); // nessuna chiave richiesta
  const k = (req.query.k || req.headers['x-ui-key'] || '').toString();
  if (k === UI_KEY) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

// /health esiste già nel tuo server e torna JSON; lo lasciamo com'è.

// Stato controllo (GET)
app.get('/control/status', checkUiKey, (req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString(),
    version: process.env.RENDER_GIT_COMMIT || 'dev',
    sheetId: req.query.sheetId || null
  });
});

// Azione di controllo (POST) – per testare una POST semplice
app.post('/control/action', checkUiKey, (req, res) => {
  const { action } = req.body || {};
  if (!action) return res.status(400).json({ ok: false, error: 'missing action' });
  res.json({ ok: true, action, receivedAt: new Date().toISOString() });
});

// (Assicurati che sotto ci sia UNA sola app.listen(...))

app.listen(PORT, () => {
  console.log(`SP-API proxy listening on :${PORT}`);
});
