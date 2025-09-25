// server.js  — add this complete file (drop-in). Keeps existing endpoints working.
// Node >= 18 is required (global fetch).

import express from 'express';
import cors from 'cors';

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// --- Your existing endpoints (keep them as-is) ---
// Example stubs shown here; DO NOT remove your current handlers.
// They must return { results: { [marketplaceId]: <object> } } as they do today.

app.get('/health', (_req, res) => res.json({ ok: true }));

// KEEP your real logic for restrictions here:
app.post('/restrictions', async (req, res) => {
  // ... your current implementation ...
  res.json({ results: {} }); // placeholder to show shape
});

// KEEP your real logic for offers here:
app.post('/offers', async (req, res) => {
  // ... your current implementation ...
  res.json({ results: {} }); // placeholder to show shape
});

// ---------- NEW: batch scan up to 100 ASINs in one call ----------
function clamp(n, min, max){ return Math.max(min, Math.min(max, n)); }

// Basic async pool (no extra deps)
async function mapPool(items, limit, worker){
  const ret = [];
  let i = 0, active = 0, done = 0;
  return new Promise((resolve, reject)=>{
    const kick = () => {
      while (active < limit && i < items.length){
        const idx = i++, it = items[idx];
        active++;
        Promise.resolve(worker(it, idx))
          .then(v => { ret[idx] = v; })
          .catch(reject)
          .finally(() => { active--; done++; (done === items.length) ? resolve(ret) : kick(); });
      }
    };
    if (items.length === 0) resolve([]);
    else kick();
  });
}

app.post('/batch/scan', async (req, res) => {
  try {
    const {
      asins,                         // string[]
      marketplaceIds,                // string[]
      conditionType = 'new_new',
      itemCondition = 'New',
      includeSiblings = true,
      concurrency = 10
    } = req.body || {};

    if (!Array.isArray(asins) || asins.length === 0) {
      return res.status(400).json({ error: 'Provide body.asins = [ ... ]' });
    }
    if (!Array.isArray(marketplaceIds) || marketplaceIds.length === 0) {
      return res.status(400).json({ error: 'Provide body.marketplaceIds = [ ... ]' });
    }

    const PORT = process.env.PORT || 3000;
    const local = `http://127.0.0.1:${PORT}`;

    const list = asins.slice(0, 100);                // hard cap 100 per call
    const conc = clamp(+concurrency || 10, 1, 20);   // be nice to SP-API

    const results = {};
    const errors  = [];

    await mapPool(list, conc, async (asin) => {
      try {
        const [restrRes, offersRes] = await Promise.all([
          fetch(local + '/restrictions', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ asin, marketplaceIds, conditionType })
          }).then(r => r.ok ? r.json() : Promise.reject(new Error('restrictions ' + r.status))),
          fetch(local + '/offers', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ asin, marketplaceIds, itemCondition, includeSiblings })
          }).then(r => r.ok ? r.json() : Promise.reject(new Error('offers ' + r.status)))
        ]);

        results[asin] = {
          restrictions: restrRes?.results || {},
          offers:       offersRes?.results || {}
        };
      } catch (e) {
        errors.push({ asin, message: String(e?.message || e) });
        results[asin] = { restrictions: {}, offers: {} };
      }
    });

    res.json({ results, errors, count: list.length });
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// ---- start ----
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Proxy running on :' + PORT));
