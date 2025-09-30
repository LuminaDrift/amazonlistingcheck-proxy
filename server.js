// server.js v1.3 — SP-API proxy (restrictions, offers, title) with strict JSON errors (NO /image)
const express = require('express');
const cors = require('cors');
require('dotenv').config();
const SellingPartnerAPI = require('amazon-sp-api');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

/* ========= ENV / TOKENS / SELLER IDs ========= */
const TOKENS = {
  eu: process.env.SPAPI_REFRESH_TOKEN_EU || '',
  na: process.env.SPAPI_REFRESH_TOKEN_NA || '',
  fe: {
    jp: process.env.SPAPI_REFRESH_TOKEN_FE_JP || '',
    au: process.env.SPAPI_REFRESH_TOKEN_FE_AU || '',
  },
};
const SELLER_IDS = {
  eu: process.env.SELLER_ID_EU || '',
  na: process.env.SELLER_ID_NA || '',
  fe: {
    jp: process.env.SELLER_ID_FE_JP || '',
    au: process.env.SELLER_ID_FE_AU || '',
  },
};

// Marketplace → region mapping  (official marketplace IDs; IE/IN/ZA included)
const REGION_OF = new Map([
  // NA
  ['ATVPDKIKX0DER','na'], // US
  ['A2EUQ1WTGCTBG2','na'], // CA
  ['A1AM78C64UM0Y8','na'], // MX
  ['A2Q3Y263D00KWC','na'], // BR

  // EU (incl. EU-managed markets)
  ['A28R8C7NBKEWEA','eu'], // IE  ✅
  ['A1RKKUPIHCS9HS','eu'], // ES
  ['A1F83G8C2ARO7P','eu'], // UK
  ['A13V1IB3VIYZZH','eu'], // FR
  ['AMEN7PMS3EDWL','eu'], // BE
  ['A1805IZSGTT6HS','eu'], // NL
  ['A1PA6795UKMFR9','eu'], // DE
  ['APJ6JRA9NG5V4','eu'], // IT
  ['A2NODRKZP88ZB9','eu'], // SE
  ['AE08WJ6YKNBMC','eu'], // ZA  ✅
  ['A1C3SOZRARQ6R3','eu'], // PL
  ['ARBP9OOSHTCHU','eu'], // EG
  ['A33AVAJ2PDY3EV','eu'], // TR
  ['A17E79C6D8DWNP','eu'], // SA
  ['A2VIGQ35RCS4UG','eu'], // AE
  ['A21TJRUUN4KGV','eu'], // IN  ✅

  // FE
  ['A19VAU5U5O7RUS','fe'], // SG
  ['A39IBJ37TRP1C6','fe'], // AU
  ['A1VC38T7YXB528','fe'], // JP
]);

// FE profile map (JP vs AU; SG uses JP profile)
const FE_PROFILE_OF = {
  'A1VC38T7YXB528': 'jp', // JP
  'A39IBJ37TRP1C6': 'au', // AU
  'A19VAU5U5O7RUS': 'jp', // SG
  // (IN moved to EU above, so no FE profile for it)
};

const mkClient = (region, refresh_token) =>
  new SellingPartnerAPI({
    region,
    refresh_token,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID:     process.env.LWA_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.LWA_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID:                 process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY:             process.env.AWS_SECRET_ACCESS_KEY,
      AWS_SELLING_PARTNER_ROLE:          process.env.AWS_ROLE_ARN,
    },
  });

const clients = {
  eu: TOKENS.eu ? mkClient('eu', TOKENS.eu) : null,
  na: TOKENS.na ? mkClient('na', TOKENS.na) : null,
  fe: { jp: null, au: null },
};

function getFEClient(profile) {
  const rt = TOKENS.fe[profile];
  if (!rt) throw new Error(`No FE refresh token for profile '${profile}'`);
  if (!clients.fe[profile]) clients.fe[profile] = mkClient('fe', rt);
  return clients.fe[profile];
}
const regionOf = (m) => REGION_OF.get(m);
const feProfileOf = (m) => FE_PROFILE_OF[m] || 'jp';

function spClientForMarketplace(marketplaceId) {
  const r = regionOf(marketplaceId);
  if (r === 'eu') { if (!clients.eu) clients.eu = mkClient('eu', TOKENS.eu); return clients.eu; }
  if (r === 'na') { if (!clients.na) clients.na = mkClient('na', TOKENS.na); return clients.na; }
  if (r === 'fe') { const p = feProfileOf(marketplaceId); return getFEClient(p); }
  throw new Error(`Unknown marketplaceId: ${marketplaceId}`);
}

/* ========= RETRY / HELPERS ========= */
async function callWithRetry(sp, params, tries = 5, base = 300) {
  for (let i = 0; i < tries; i++) {
    try { return await sp.callAPI(params); }
    catch (e) {
      const msg = String(e?.code || e?.message || e);
      const retryable = /429|5\d\d|Throttl|Timeout|EAI_AGAIN|ENETUNREACH/i.test(msg);
      if (!retryable || i === tries - 1) throw e;
      await new Promise(r => setTimeout(r, base * Math.pow(2, i)));
    }
  }
}

function classify(reasons) {
  const ret = { exists: true, status: 'Open', reasonCodes: [] };
  if (!Array.isArray(reasons) || reasons.length === 0) return ret;
  const codes = reasons.map(r => r.reasonCode || '').filter(Boolean);
  if (codes.includes('ASIN_NOT_FOUND'))    return { exists: false, status: 'NA',       reasonCodes: codes };
  if (codes.includes('APPROVAL_REQUIRED')) return { exists: true,  status: 'Approval', reasonCodes: codes };
  return { exists: true, status: 'Blocked', reasonCodes: codes };
}

const pickTitle = (obj) => {
  if (!obj) return '';
  return (
    obj?.attributeSets?.[0]?.title ||
    obj?.summaries?.[0]?.itemName ||
    obj?.summaries?.[0]?.brand ||
    (obj?.attributes?.item_name?.[0]?.value || '') ||
    obj?.itemName || obj?.brand || ''
  ) || '';
};

/* ========= ROUTES ========= */

// --- /restrictions ---
app.post('/restrictions', async (req, res, next) => {
  try {
    const asin = String(req.body.asin || '').trim().toUpperCase();
    const marketplaceIds = (req.body.marketplaceIds || []).map(String).filter(Boolean);
    const conditionType = String(req.body.conditionType || 'new_new');
    if (!asin || !marketplaceIds.length) return res.status(400).json({ error: 'asin + marketplaceIds[] required' });

    const results = {};
    const setClosed = (id, reason) => {
      results[id] = {
        exists: false,
        status: 'NA',
        reasonCodes: reason ? [reason] : ['NO_DATA'],
      };
    };
    marketplaceIds.forEach(m => setClosed(m));

    // Group by region/profile so we can pass correct sellerId/token
    const buckets = { eu: [], na: [], fe: {} };
    for (const m of marketplaceIds) {
      const r = regionOf(m);
      if (r === 'eu') buckets.eu.push(m);
      else if (r === 'na') buckets.na.push(m);
      else if (r === 'fe') { const p = feProfileOf(m); (buckets.fe[p] ||= []).push(m); }
    }

    if (buckets.eu.length) {
      const sp = spClientForMarketplace('A1F83G8C2ARO7P');
      const data = await callWithRetry(sp, {
        operation: 'getListingsRestrictions', endpoint: 'listingsRestrictions',
        query: { asin, conditionType, marketplaceIds: buckets.eu, sellerId: SELLER_IDS.eu },
      });
      const seen = new Set();
      (data?.restrictions || []).forEach(r => {
        if (r?.marketplaceId) {
          seen.add(r.marketplaceId);
          results[r.marketplaceId] = classify(r.reasons || []);
        }
      });
      buckets.eu.forEach(id => { if (!seen.has(id)) setClosed(id, 'NO_RESTRICTIONS'); });
    }
    if (buckets.na.length) {
      const sp = spClientForMarketplace('ATVPDKIKX0DER');
      const data = await callWithRetry(sp, {
        operation: 'getListingsRestrictions', endpoint: 'listingsRestrictions',
        query: { asin, conditionType, marketplaceIds: buckets.na, sellerId: SELLER_IDS.na },
      });
      const seen = new Set();
      (data?.restrictions || []).forEach(r => {
        if (r?.marketplaceId) {
          seen.add(r.marketplaceId);
          results[r.marketplaceId] = classify(r.reasons || []);
        }
      });
      buckets.na.forEach(id => { if (!seen.has(id)) setClosed(id, 'NO_RESTRICTIONS'); });
    }
    for (const [profile, ids] of Object.entries(buckets.fe)) {
      if (!ids.length) continue;
      const sp = getFEClient(profile);
      const data = await callWithRetry(sp, {
        operation: 'getListingsRestrictions', endpoint: 'listingsRestrictions',
        query: { asin, conditionType, marketplaceIds: ids, sellerId: SELLER_IDS.fe[profile] },
      });
      const seen = new Set();
      (data?.restrictions || []).forEach(r => {
        if (r?.marketplaceId) {
          seen.add(r.marketplaceId);
          results[r.marketplaceId] = classify(r.reasons || []);
        }
      });
      ids.forEach(id => { if (!seen.has(id)) setClosed(id, 'NO_RESTRICTIONS'); });
    }

    res.json({ asin, conditionType, results });
  } catch (err) { next(err); }
});

// --- /offers ---
app.post('/offers', async (req, res, next) => {
  try {
    const asin = String(req.body.asin || '').trim().toUpperCase();
    const ids = (req.body.marketplaceIds || []).map(String).filter(Boolean);
    const itemCondition = String(req.body.itemCondition || 'New');
    if (!asin || !ids.length) return res.status(400).json({ error: 'asin + marketplaceIds[] required' });

    const results = {};
    for (const mp of ids) {
      try {
        const sp = spClientForMarketplace(mp);
        const r = await callWithRetry(sp, {
          operation: 'getItemOffers', endpoint: 'productPricing',
          path: { Asin: asin },
          query: { MarketplaceId: mp, ItemCondition: itemCondition, CustomerType: 'Consumer' },
        });
        const offers = r?.payload?.Offers || r?.Offers || [];
        results[mp] = { hasNewOffers: offers.length > 0, newOfferCount: offers.length, status: 200 };
      } catch (err) {
        results[mp] = { hasNewOffers: false, newOfferCount: 0, status: 0, error: String(err?.message || err) };
      }
    }
    res.json({ asin, results });
  } catch (e) { next(e); }
});

// --- /title (optional; not used by your Sheet but kept) ---
async function fetchTitleOnce(asin, marketplaceId) {
  const sp = spClientForMarketplace(marketplaceId);

  try {
    const d = await callWithRetry(sp, {
      operation: 'getCatalogItem', endpoint: 'catalogItems',
      path: { asin }, query: { MarketplaceId: marketplaceId }
    });
    const t = pickTitle(d?.payload || d);
    if (t) return t;
  } catch (_) {}

  try {
    const d = await callWithRetry(sp, {
      operation: 'getCatalogItem', endpoint: 'catalogItems_2022_04_01',
      path: { asin }, query: { marketplaceIds: [marketplaceId], includedData: ['summaries'] }
    });
    const sums = Array.isArray(d?.summaries) ? d.summaries : d?.payload?.summaries || null;
    const s = sums?.find(x => x.marketplaceId === marketplaceId) || sums?.[0];
    const t = pickTitle(s);
    if (t) return t;
  } catch (_) {}

  try {
    const d = await callWithRetry(sp, {
      operation: 'getCatalogItem', endpoint: 'catalogItems_2022_04_01',
      path: { asin }, query: { marketplaceIds: marketplaceId, includedData: 'summaries' }
    });
    const sums = Array.isArray(d?.summaries) ? d.summaries : d?.payload?.summaries || null;
    const s = sums?.find(x => x.marketplaceId === marketplaceId) || sums?.[0];
    const t = pickTitle(s);
    if (t) return t;
  } catch (_) {}

  try {
    const d3 = await callWithRetry(sp, {
      operation: 'searchCatalogItems', endpoint: 'catalogItems',
      query: { MarketplaceId: marketplaceId, Identifiers: [asin], IdentifiersType: 'ASIN' }
    });
    const item = d3?.items?.[0] || d3?.payload?.items?.[0];
    const t = pickTitle(item?.summaries?.[0]) || pickTitle(item);
    if (t) return t;
  } catch (_) {}

  try {
    const d4 = await callWithRetry(sp, {
      operation: 'searchCatalogItems', endpoint: 'catalogItems',
      query: { MarketplaceId: marketplaceId, keywords: asin }
    });
    const item = d4?.items?.[0] || d4?.payload?.items?.[0];
    const t = pickTitle(item?.summaries?.[0]) || pickTitle(item);
    if (t) return t;
  } catch (_) {}

  return '';
}

app.get('/title', async (req, res, next) => {
  try {
    const asin = String(req.query.asin || '').trim().toUpperCase();
    const marketplaceId = String(req.query.marketplaceId || '').trim();
    if (!asin || !marketplaceId) return res.status(400).json({ error: 'asin & marketplaceId required' });
    const title = await fetchTitleOnce(asin, marketplaceId);
    if (!title) return res.status(404).json({ error: 'Title not found' });
    res.json({ asin, marketplaceId, title });
  } catch (e) { next(e); }
});

/* ========= HEALTH / VERSION ========= */
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/version', (_req, res) => res.json({ version: '1.3' }));

/* ========= JSON-ONLY ERROR HANDLERS ========= */
app.use((req, res) => {
  res.status(404).json({ error: 'Not found', path: req.originalUrl });
});
app.use((err, req, res, _next) => {
  const status = typeof err?.status === 'number' ? err.status : 500;
  res.status(status).json({ error: String(err?.message || err), code: err?.code || undefined });
});

process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); });

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`SP-API proxy ready on port ${port} (v1.3)`));
