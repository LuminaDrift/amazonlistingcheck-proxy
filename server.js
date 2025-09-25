// server.js — SP-API proxy with single and batch endpoints
// ESM build (package.json has "type":"module")

import express from 'express';
import cors from 'cors';
import pLimit from 'p-limit';
import SellingPartner from 'amazon-sp-api';

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

/* ========== ENV VARS (set these in Render → Environment) ==========

LWA_CLIENT_ID
LWA_CLIENT_SECRET
LWA_REFRESH_TOKEN

AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
ROLE_ARN              // IAM role ARN your app assumes
SPAPI_REGION_HINT     // optional: eu | na | fe (fallback if marketplaceId not mapped)

PORT                  // Render sets this automatically
*/

const {
  LWA_CLIENT_ID,
  LWA_CLIENT_SECRET,
  LWA_REFRESH_TOKEN,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  ROLE_ARN,
  SPAPI_REGION_HINT
} = process.env;

function assertEnv() {
  const missing = [];
  [
    'LWA_CLIENT_ID',
    'LWA_CLIENT_SECRET',
    'LWA_REFRESH_TOKEN',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'ROLE_ARN'
  ].forEach(k => { if (!process.env[k]) missing.push(k); });
  if (missing.length) {
    throw new Error('Missing environment variables: ' + missing.join(', '));
  }
}
assertEnv();

/* ===== Marketplace → SP-API region =====
   Keep only the 11 markets you’re checking.
   eu = Europe, na = North America, fe = Far East
*/
const REGION_BY_MKT = {
  // EU
  A1F83G8C2ARO7P: 'eu', // UK
  A13V1IB3VIYZZH: 'eu', // FR
  APJ6JRA9NG5V4:  'eu', // IT
  A1RKKUPIHCS9HS: 'eu', // ES
  A1805IZSGTT6HS: 'eu', // NL
  A1C3SOZRARQ6R3: 'eu', // PL
  A2NODRKZP88ZB9: 'eu', // SE
  AMEN7PMS3EDWL:  'eu', // BE

  // NA
  ATVPDKIKX0DER:  'na', // US

  // FE
  A1VC38T7YXB528: 'fe', // JP
  A39IBJ37TRP1C6: 'fe'  // AU
};

function regionForMarketplace(marketplaceId) {
  return REGION_BY_MKT[marketplaceId] || SPAPI_REGION_HINT || 'eu';
}

// Simple per-region client cache
const clientCache = new Map();

function getSpClient(region) {
  if (clientCache.has(region)) return clientCache.get(region);

  const sp = new SellingPartner({
    region,                           // 'eu' | 'na' | 'fe'
    refresh_token: LWA_REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: LWA_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: LWA_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY,
      role: ROLE_ARN
    }
  });

  clientCache.set(region, sp);
  return sp;
}

/* ------------- Helpers to call SP-API ------------- */

// GET restrictions for one ASIN/marketplace
async function getRestrictionsOne(asin, marketplaceId, conditionType = 'new_new') {
  const region = regionForMarketplace(marketplaceId);
  const sp = getSpClient(region);

  try {
    const res = await sp.callAPI({
      operation: 'getListingsRestrictions',
      endpoint: 'listingsRestrictions',
      query: {
        asin,
        marketplaceIds: marketplaceId,
        conditionType
      }
    });

    // Per spec: if array empty or all reason codes empty => Open
    // We’ll reduce to a minimal, stable shape for the sheet.
    let status = 'Closed';
    let reasonCodes = [];
    let exists = true;

    if (Array.isArray(res) && res.length) {
      // If any entry has empty reasonCodes -> Open
      const hasOpen = res.some(r => Array.isArray(r.restrictions) && r.restrictions.some(x =>
        !x.reasonCodes || (Array.isArray(x.reasonCodes) && x.reasonCodes.length === 0)
      ));
      status = hasOpen ? 'Open' : 'Closed';

      // collect reason codes to help debugging (optional)
      res.forEach(r => (r.restrictions || []).forEach(x => {
        if (Array.isArray(x.reasonCodes)) reasonCodes.push(...x.reasonCodes);
      }));
    } else {
      // SP-API returns [] for not found sometimes; treat as Closed but exists=false
      exists = false;
      status = 'Closed';
    }

    return { exists, status, reasonCodes: [...new Set(reasonCodes)].slice(0, 10) };
  } catch (err) {
    // On any error, be conservative (Closed)
    return { exists: false, status: 'Closed', error: normalizeErr(err) };
  }
}

// GET offers for one ASIN/marketplace (New condition)
async function getOffersOne(asin, marketplaceId, itemCondition = 'New') {
  const region = regionForMarketplace(marketplaceId);
  const sp = getSpClient(region);

  try {
    const out = await sp.callAPI({
      operation: 'getItemOffers',
      endpoint: 'productPricing',
      path: { Asin: asin },
      query: {
        MarketplaceId: marketplaceId,
        ItemCondition: itemCondition
      }
    });

    const offers = (out && out.Offers) || [];
    return { hasNewOffers: Array.isArray(offers) && offers.length > 0 };
  } catch (err) {
    return { hasNewOffers: false, error: normalizeErr(err) };
  }
}

function normalizeErr(err) {
  if (!err) return 'unknown';
  if (typeof err === 'string') return err;
  try {
    if (err.response && err.response.data) return JSON.stringify(err.response.data).slice(0, 500);
    if (err.message) return err.message;
    return JSON.stringify(err).slice(0, 500);
  } catch {
    return 'unknown';
  }
}

/* ------------- Endpoints ------------- */

// Health
app.get('/health', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// Single ASIN restrictions
app.post('/restrictions', async (req, res) => {
  const { asin, marketplaceIds = [], conditionType = 'new_new' } = req.body || {};
  if (!asin || !Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
    return res.status(400).json({ error: 'asin and marketplaceIds[] required' });
  }

  const limit = pLimit(6);
  const pairs = marketplaceIds.map(m =>
    limit(() => getRestrictionsOne(asin, m, conditionType).then(r => [m, r]))
  );

  const entries = await Promise.all(pairs);
  const results = Object.fromEntries(entries);
  res.json({ asin, results });
});

// Single ASIN offers
app.post('/offers', async (req, res) => {
  const { asin, marketplaceIds = [], itemCondition = 'New' } = req.body || {};
  if (!asin || !Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
    return res.status(400).json({ error: 'asin and marketplaceIds[] required' });
  }

  const limit = pLimit(6);
  const pairs = marketplaceIds.map(m =>
    limit(() => getOffersOne(asin, m, itemCondition).then(r => [m, r]))
  );

  const entries = await Promise.all(pairs);
  const results = Object.fromEntries(entries);
  res.json({ asin, results });
});

// BATCH restrictions: { asins:[], marketplaceIds:[] }
app.post('/restrictions/batch', async (req, res) => {
  const { asins = [], marketplaceIds = [], conditionType = 'new_new' } = req.body || {};
  if (!Array.isArray(asins) || asins.length === 0) {
    return res.status(400).json({ error: 'asins[] required' });
  }
  if (!Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
    return res.status(400).json({ error: 'marketplaceIds[] required' });
  }
  if (asins.length > 100) {
    return res.status(400).json({ error: 'max 100 ASINs per batch' });
  }

  const limit = pLimit(10); // up to 10 concurrent SP-API calls
  const tasks = [];

  const out = {};
  for (const asin of asins) {
    out[asin] = {};
    for (const m of marketplaceIds) {
      tasks.push(
        limit(async () => {
          out[asin][m] = await getRestrictionsOne(asin, m, conditionType);
        })
      );
    }
  }

  await Promise.all(tasks);
  res.json({ results: out });
});

// BATCH offers: { asins:[], marketplaceIds:[] }
app.post('/offers/batch', async (req, res) => {
  const { asins = [], marketplaceIds = [], itemCondition = 'New' } = req.body || {};
  if (!Array.isArray(asins) || asins.length === 0) {
    return res.status(400).json({ error: 'asins[] required' });
  }
  if (!Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
    return res.status(400).json({ error: 'marketplaceIds[] required' });
  }
  if (asins.length > 100) {
    return res.status(400).json({ error: 'max 100 ASINs per batch' });
  }

  const limit = pLimit(10);
  const tasks = [];

  const out = {};
  for (const asin of asins) {
    out[asin] = {};
    for (const m of marketplaceIds) {
      tasks.push(
        limit(async () => {
          out[asin][m] = await getOffersOne(asin, m, itemCondition);
        })
      );
    }
  }

  await Promise.all(tasks);
  res.json({ results: out });
});

/* ------------- Boot ------------- */

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log('Proxy listening on :', port);
});
