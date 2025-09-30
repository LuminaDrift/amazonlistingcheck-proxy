/***********************
 * ListingChecksAmazon.gs (core)
 *
 * Keys:
 *  - Keepa:  KEY!B1
 *  - Tokens: KEY!B2 (Keepa tokens left; this file updates it)
 *  - Vision: KEY!B3
 ***********************/

/* =================== CONFIG =================== */

/** If you have ONE proxy/app, keep BASE_URL set and leave MULTI_BASE_URLS empty.
 *  If you have MULTIPLE proxies (each with its own SP-API app/keys), put them into MULTI_BASE_URLS.
 *  The batch scanner will round-robin ASINs across these endpoints and call them in parallel. */
const BASE_URL = 'https://amazonlistingcheck-proxy.onrender.com';
const MULTI_BASE_URLS = [
  // 'https://another-proxy.trycloudflare.com'
];

const THUMB_SIZE  = 120;
const ROW_PADDING = 20;

// Minimal Keepa domains to try (fast but robust)
const KEEPA_DOMAINS_TRY = [2, 1, 3, 4]; // UK, US, DE, FR

/** ====== MARKETPLACE SET ======
 * UK, US, FR, IT, ES, JP, NL, BE, SE, PL, AU, MX, IN, SG, AE, SA, EG
 */
const MARKETPLACE_ID_BY_LABEL = {
  UK:'A1F83G8C2ARO7P',
  FR:'A13V1IB3VIYZZH',
  IT:'APJ6JRA9NG5V4',
  ES:'A1RKKUPIHCS9HS',
  NL:'A1805IZSGTT6HS',
  PL:'A1C3SOZRARQ6R3',
  SE:'A2NODRKZP88ZB9',
  BE:'AMEN7PMS3EDWL',
  US:'ATVPDKIKX0DER',
  JP:'A1VC38T7YXB528',
  AU:'A39IBJ37TRP1C6',
  MX:'A1AM78C64UM0Y8',
  IN:'A21TJRUUN4KGV',
  SG:'A19VAU5U5O7RUS',
  AE:'A2VIGQ35RCS4UG',
  SA:'A17E79C6D8DWNP',
  EG:'ARBP9OOSHTCHU'
};

const DEFAULT_MARKET_ORDER = ['UK','FR','IT','ES','NL','PL','SE','BE','US','JP','AU','MX','IN','SG','AE','SA','EG'];

const EXCLUDED_CHECK_MARKETS = [];              // none among the configured markets
const OPEN_SUMMARY_EXCLUDE   = new Set([]);     // none among the configured markets
const NOT_SELLING = new Set([]);                // keep all configured markets

let VISION_API_KEY = '';

/* ---------- Seller Central & Retail ---------- */
const DEFAULT_LIST_BASE = 'https://sellercentral.amazon.co.uk';
const DEFAULT_BUY_BASE  = 'https://www.amazon.co.uk';

const SC_MAP = {
  UK:'https://sellercentral.amazon.co.uk',
  US:'https://sellercentral.amazon.com',
  FR:'https://sellercentral.amazon.fr',
  IT:'https://sellercentral.amazon.it',
  ES:'https://sellercentral.amazon.es',
  JP:'https://sellercentral.amazon.co.jp',
  NL:'https://sellercentral.amazon.nl',
  BE:'https://sellercentral.amazon.be',
  SE:'https://sellercentral.amazon.se',
  PL:'https://sellercentral.amazon.pl',
  AU:'https://sellercentral.amazon.com.au',
  MX:'https://sellercentral.amazon.com.mx',
  IN:'https://sellercentral.amazon.in',
  SG:'https://sellercentral.amazon.sg',
  AE:'https://sellercentral.amazon.ae',
  SA:'https://sellercentral.amazon.sa',
  EG:'https://sellercentral.amazon.eg'
};

const BUY_MAP = {
  UK:'https://www.amazon.co.uk',
  US:'https://www.amazon.com',
  FR:'https://www.amazon.fr',
  IT:'https://www.amazon.it',
  ES:'https://www.amazon.es',
  JP:'https://www.amazon.co.jp',
  NL:'https://www.amazon.nl',
  BE:'https://www.amazon.be',
  SE:'https://www.amazon.se',
  PL:'https://www.amazon.pl',
  AU:'https://www.amazon.com.au',
  MX:'https://www.amazon.com.mx',
  IN:'https://www.amazon.in',
  SG:'https://www.amazon.sg',
  AE:'https://www.amazon.ae',
  SA:'https://www.amazon.sa',
  EG:'https://www.amazon.eg'
};

/* ---------- Main-sheet market groups ---------- */
const GROUPS = [
  { name:'EU', markets:[
    {label:'UK',id:MARKETPLACE_ID_BY_LABEL.UK},
    {label:'FR',id:MARKETPLACE_ID_BY_LABEL.FR},
    {label:'IT',id:MARKETPLACE_ID_BY_LABEL.IT},
    {label:'ES',id:MARKETPLACE_ID_BY_LABEL.ES},
    {label:'NL',id:MARKETPLACE_ID_BY_LABEL.NL},
    {label:'PL',id:MARKETPLACE_ID_BY_LABEL.PL},
    {label:'SE',id:MARKETPLACE_ID_BY_LABEL.SE},
    {label:'BE',id:MARKETPLACE_ID_BY_LABEL.BE}
  ]},
  { name:'Americas', markets:[
    {label:'US',id:MARKETPLACE_ID_BY_LABEL.US},
    {label:'MX',id:MARKETPLACE_ID_BY_LABEL.MX}
  ]},
  { name:'APAC', markets:[
    {label:'JP',id:MARKETPLACE_ID_BY_LABEL.JP},
    {label:'AU',id:MARKETPLACE_ID_BY_LABEL.AU},
    {label:'IN',id:MARKETPLACE_ID_BY_LABEL.IN},
    {label:'SG',id:MARKETPLACE_ID_BY_LABEL.SG}
  ]},
  { name:'MENA', markets:[
    {label:'AE',id:MARKETPLACE_ID_BY_LABEL.AE},
    {label:'SA',id:MARKETPLACE_ID_BY_LABEL.SA},
    {label:'EG',id:MARKETPLACE_ID_BY_LABEL.EG}
  ]}
];

function getDefaultMarkets_() {
  return DEFAULT_MARKET_ORDER
    .map(label => ({label, id: MARKETPLACE_ID_BY_LABEL[label]}))
    .filter(m => m.id && EXCLUDED_CHECK_MARKETS.indexOf(m.label) === -1);
}

function marketsForChecks_() {
  return getDefaultMarkets_();
}
function sellableSet_(){
  const set = new Set();
  Object.keys(SC_MAP).forEach(k=>{ if(!NOT_SELLING.has(k)) set.add(k); });
  return set;
}

/* =================== LOW-LEVEL HELPERS =================== */
function httpGet_(url){ return UrlFetchApp.fetch(url,{method:'get',muteHttpExceptions:true}); }
function httpPostJson_(url,body){
  return UrlFetchApp.fetch(url,{
    method:'post', contentType:'application/json', payload:JSON.stringify(body), muteHttpExceptions:true
  });
}
function safeJson_(res){ try { return JSON.parse(res.getContentText()); } catch(e){ return null; } }
function toast_(msg, title){ SpreadsheetApp.getActive().toast(msg, title || 'Amazon', 4); }
function indexMap_(header){ const m={}; header.forEach((h,i)=>m[h]=i); return m; }

function analyzeHeaderForMarkets_(header){
  const idx = indexMap_(header);
  const openColumns = [];
  const sellerColumns = [];
  const seen = new Set();
  const uniqueIds = [];

  header.forEach((name, i) => {
    if (!name) return;
    const openMatch = name.match(/^(.*?)\s+Open$/i);
    if (openMatch){
      const label = openMatch[1].trim();
      const id = MARKETPLACE_ID_BY_LABEL[label] || '';
      openColumns.push({index:i, label, id});
      if (id && !seen.has(id)){
        seen.add(id);
        uniqueIds.push(id);
      }
      return;
    }
    const sellersMatch = name.match(/^(.*?)\s+Sellers$/i);
    if (sellersMatch){
      const label = sellersMatch[1].trim();
      const id = MARKETPLACE_ID_BY_LABEL[label] || '';
      sellerColumns.push({index:i, label, id});
      if (id && !seen.has(id)){
        seen.add(id);
        uniqueIds.push(id);
      }
    }
  });

  return {
    indexMap: idx,
    openColumns,
    sellerColumns,
    marketplaceIds: uniqueIds
  };
}

/* =================== KEYS =================== */
function getKeepaKey_(){
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('KEY');
  if (!sh) throw new Error('Create sheet "KEY" — put Keepa API key in B1, tokens in B2, Vision key in B3.');
  const key = String(sh.getRange('B1').getValue()||'').trim();
  if (!key) throw new Error('Keepa API key missing in KEY!B1.');
  if (!VISION_API_KEY) {
    const vk = String(sh.getRange('B3').getValue()||'').trim();
    if (vk) VISION_API_KEY = vk;
  }
  return key;
}
function setKeepaTokensLeft_(n){
  try{
    const sh = SpreadsheetApp.getActive().getSheetByName('KEY');
    if (sh && typeof n === 'number' && !isNaN(n)) sh.getRange('B2').setValue(n);
  }catch(_){ }
}
/* Check/refresh tokens: primary = /token, fallback = tiny /product call. */
function getKeepaTokensLeft_(){
  const key = getKeepaKey_();
  try{
    const r = UrlFetchApp.fetch('https://api.keepa.com/token?key='+encodeURIComponent(key), {muteHttpExceptions:true});
    const j = JSON.parse(r.getContentText());
    if (j && typeof j.tokensLeft === 'number'){ setKeepaTokensLeft_(j.tokensLeft); return j.tokensLeft; }
  }catch(_){ }
  try{
    const r2 = UrlFetchApp.fetch('https://api.keepa.com/product?key='+encodeURIComponent(key)+'&domain=2&asin=B00TESTKEEPA&stats=0', {muteHttpExceptions:true});
    const j2 = JSON.parse(r2.getContentText());
    if (j2 && typeof j2.tokensLeft === 'number'){ setKeepaTokensLeft_(j2.tokensLeft); return j2.tokensLeft; }
  }catch(_){ }
  const sh = SpreadsheetApp.getActive().getSheetByName('KEY');
  return Number(sh ? (sh.getRange('B2').getValue()||0) : 0);
}

/* =================== Keepa helpers =================== */
function keepaMinutesToDateString_(km){
  if (typeof km !== 'number' || isNaN(km)) return '';
  const ms = (km + 21564000) * 60000; // minutes since 2011-01-01
  const d = new Date(ms);
  const y = d.getFullYear(), m=('0'+(d.getMonth()+1)).slice(-2), dd=('0'+d.getDate()).slice(-2);
  return y+'-'+m+'-'+dd;
}
function parseKeepaProduct_(p){
  if (!p) return null;
  const s = p.stats || {};
  const cur = Array.isArray(s.current) ? s.current : null;
  const avg = Array.isArray(s.avg90)   ? s.avg90   : null;

  let ratingRaw  = (cur && typeof cur[16]==='number') ? cur[16] : (avg && typeof avg[16]==='number' ? avg[16] : (typeof p.reviewRating==='number' ? p.reviewRating : null));
  let reviewsRaw = (cur && typeof cur[17]==='number') ? cur[17] : (avg && typeof avg[17]==='number' ? avg[17] : (typeof p.reviewCount==='number' ? p.reviewCount : null));
  const rating  = (typeof ratingRaw==='number'  && ratingRaw>0)   ? (ratingRaw/10).toFixed(1) : '';
  const reviews = (typeof reviewsRaw==='number' && reviewsRaw>=0) ? reviewsRaw : '';

  let imageUrl = '';
  const toMedia = key => 'https://m.media-amazon.com/images/I/'+key+'.jpg';
  if (p.imagesCSV){
    const first = String(p.imagesCSV).split(',')[0].trim();
    if (first) imageUrl = toMedia(first);
  } else if (Array.isArray(p.images) && p.images.length){
    const f = String(p.images[0]).trim();
    imageUrl = f.startsWith('http') ? f : toMedia(f);
  }
  if (imageUrl) imageUrl = imageUrl.replace(/\+/g, '%2B');

  let fee = '';
  if (typeof p.referralFeePercent === 'number') fee = p.referralFeePercent;
  else if (s && typeof s.referralFeePercent === 'number') fee = s.referralFeePercent;
  else if (p.fees && typeof p.fees.referralFeePercent === 'number') fee = p.fees.referralFeePercent;
  if (typeof fee === 'number') {
    if (fee > 0 && fee <= 1) fee = fee * 100;
    fee = Math.round(fee * 100) / 100;
  } else fee = '';

  const trackingSince = (typeof p.trackingSince === 'number') ? keepaMinutesToDateString_(p.trackingSince) : '';
  const listedSince   = (typeof p.listedSince   === 'number') ? keepaMinutesToDateString_(p.listedSince)   : '';
  const srCur  = (s && typeof s.salesRankCurrent === 'number') ? s.salesRankCurrent : '';
  const srAvg  = (s && typeof s.salesRank90     === 'number') ? s.salesRank90 : '';
  const salesM = (s && typeof s.salesPerMonth   === 'number') ? s.salesPerMonth : '';
  const lastPC = (typeof p.lastPriceChange === 'number') ? keepaMinutesToDateString_(p.lastPriceChange)
                : (s && typeof s.lastPriceChange === 'number') ? keepaMinutesToDateString_(s.lastPriceChange) : '';

  let catRoot='', catSub='', catTree='';
  if (Array.isArray(p.categoryTree) && p.categoryTree.length){
    const names = p.categoryTree.map(x => (x && x.name) ? x.name : (x || '')).filter(Boolean);
    if (names.length){
      catRoot = names[0] || ''; catSub  = names[1] || ''; catTree = names.join(' > ');
    }
  } else if (Array.isArray(p.categoryTreeNames) && p.categoryTreeNames.length){
    const names = p.categoryTreeNames;
    catRoot = names[0] || ''; catSub = names[1] || ''; catTree = names.join(' > ');
  }

  return {
    asin: p.asin || '',
    title: p.title || '',
    imageUrl,
    rating,
    reviews,
    referralFeePct: fee,
    trackingSince,
    listedSince,
    salesRankCurrent: srCur,
    salesRankAvg90: srAvg,
    boughtLastMonth: salesM,
    reviewsFormatSpecific: (typeof p.reviewCount === 'number') ? p.reviewCount : reviews,
    lastPriceChange: lastPC,
    catRoot, catSub, catTree,
    brand: p.brand || '',
    productGroup: p.productGroup || '',
    model: p.model || '',
    color: p.color || '',
    recommendedUses: Array.isArray(p.features) ? p.features.join('; ') : (p.featureBullets ? p.featureBullets.join('; ') : '')
  };
}
function keepaFetchAllDomain_(domain, asins){
  const key = getKeepaKey_();
  if (!asins.length) return {map:{}, tokensLeft:null};

  const reqs = asins.map(a => ({
    url: 'https://api.keepa.com/product'
       + '?key='+encodeURIComponent(key)
       + '&domain='+domain
       + '&asin='+encodeURIComponent(a)
       + '&stats=1&rating=1',
    method:'get', muteHttpExceptions:true
  }));
  const res = UrlFetchApp.fetchAll(reqs);

  const map = {};
  let minTokens = null;

  res.forEach((r, i)=>{
    const j = safeJson_(r);
    if (j && typeof j.tokensLeft === 'number') {
      minTokens = (minTokens===null)? j.tokensLeft : Math.min(minTokens, j.tokensLeft);
    }
    const p = j && Array.isArray(j.products) ? j.products[0] : null;
    if (p) map[asins[i]] = parseKeepaProduct_(p);
  });

  if (minTokens !== null) setKeepaTokensLeft_(minTokens);
  return {map, tokensLeft:minTokens};
}

/* Try to fill missing critical fields (title/image/rating/reviews) up to maxTries rounds. */
function keepaQuickRetryCritical_(asin, maxTries){
  const key = getKeepaKey_();
  let out = { };
  const need = () => !(out.title && out.imageUrl && out.rating && (out.reviews!=='' && out.reviews!=null));
  let tries = 0;

  outer: while (tries < maxTries && need()){
    for (let d = 0; d < KEEPA_DOMAINS_TRY.length && need(); d++){
      const domain = KEEPA_DOMAINS_TRY[d];
      const url = 'https://api.keepa.com/product?key='+encodeURIComponent(key)+'&domain='+domain+'&asin='+encodeURIComponent(asin)+'&stats=1&rating=1';
      const j = safeJson_(httpGet_(url));
      if (j && typeof j.tokensLeft === 'number') setKeepaTokensLeft_(j.tokensLeft);
      const p = j && Array.isArray(j.products) ? j.products[0] : null;
      if (!p) continue;
      const parsed = parseKeepaProduct_(p);
      // merge only the four criticals
      if (!out.title   && parsed.title)    out.title = parsed.title;
      if (!out.imageUrl&& parsed.imageUrl) out.imageUrl = parsed.imageUrl;
      if (!out.rating  && parsed.rating)   out.rating = parsed.rating;
      if ((out.reviews===''||out.reviews==null) && (parsed.reviews!=='' && parsed.reviews!=null)) out.reviews = parsed.reviews;
      if (!need()) break outer;
    }
    tries++;
  }
  return out;
}

/* =================== SP-API proxy helpers =================== */
function getRestrictions_(asin, marketplaceIds){
  const out={};
  try{
    const data = safeJson_(httpPostJson_(BASE_URL+'/restrictions',{ asin, marketplaceIds, conditionType:'new_new' })) || {};
    const results = data.results || {};
    marketplaceIds.forEach(m=>{
      const r = results[m];
      let val='Closed';
      if (r){
        if (r.exists===false) val='Closed';
        else if (r.status==='Open' || (Array.isArray(r.reasonCodes)&&r.reasonCodes.length===0)) val='Open';
      }
      out[m]=val;
    });
  }catch(e){ marketplaceIds.forEach(m=>out[m]='Closed'); }
  return out;
}
function getOffersFamily_(asin, marketplaceIds){
  const out={};
  try{
    const data = safeJson_(httpPostJson_(BASE_URL+'/offers',{ asin, marketplaceIds, itemCondition:'New', includeSiblings:true })) || {};
    const results = data.results || {};
    marketplaceIds.forEach(m=>{
      const r = results[m];
      out[m] = (r && r.hasNewOffers) ? 'YES SELLERS' : 'NO SELLERS';
    });
  }catch(e){ marketplaceIds.forEach(m=>out[m]='NO SELLERS'); }
  return out;
}

/* =================== MAIN SHEET (Amazon) =================== */
function buildHeader_(){
  const head=['ASIN','Processed','Title','Image','Rating','Reviews'];
  getDefaultMarkets_().forEach(m => { head.push(m.label+' Open', m.label+' Sellers'); });
  return head;
}
function ensureHeaders_(sh){
  const lastCol = sh.getLastColumn();
  const header = lastCol ? sh.getRange(1,1,1,lastCol).getValues()[0] : [];
  const required = ['ASIN','Processed','Title','Image','Rating','Reviews'];
  const hasRequired = required.every(r => header.indexOf(r) !== -1);
  const layout = analyzeHeaderForMarkets_(header);
  const hasMarkets = layout.openColumns.length > 0;
  if (hasRequired && hasMarkets) return;
  setupHeaders();
}
function setupHeaders(){
  const sh = SpreadsheetApp.getActiveSheet();
  const header = buildHeader_();
  if(sh.getMaxColumns() < header.length){
    sh.insertColumnsAfter(sh.getMaxColumns(), header.length - sh.getMaxColumns());
  }
  sh.getRange(1,1,1,header.length).setValues([header]).setFontWeight('bold');
  sh.setFrozenRows(1);
  header.forEach((h,i)=>{
    let w=120;
    if(h==='ASIN') w=145;
    if(h==='Processed') w=100;
    if(h==='Title') w=360;
    if(h==='Image') w=115;
    if(h==='Rating'||h==='Reviews') w=95;
    sh.setColumnWidth(i+1,w);
  });
  toast_('Headers set ✓','Amazon');
}
function buildRow_(header, layout, asin, keepa, restr, sellers){
  const row = new Array(header.length).fill('');
  const idx = layout.indexMap;

  if (idx.ASIN !== undefined) row[idx.ASIN]=asin;
  if (idx.Title !== undefined) row[idx.Title]=keepa.title||'';
  if (idx.Image !== undefined){
    row[idx.Image]=keepa.imageUrl ? '=HYPERLINK("'+keepa.imageUrl+'", IMAGE("'+keepa.imageUrl+'", 4, '+THUMB_SIZE+', '+THUMB_SIZE+'))' : '';
  }
  if (idx.Rating !== undefined) row[idx.Rating]=keepa.rating||'';
  if (idx.Reviews !== undefined) row[idx.Reviews]=keepa.reviews||'';

  layout.openColumns.forEach(info => {
    const val = info.id ? (restr[info.id] || 'Closed') : 'Closed';
    row[info.index] = val;
  });
  layout.sellerColumns.forEach(info => {
    const val = info.id ? (sellers[info.id] || 'NO SELLERS') : 'NO SELLERS';
    row[info.index] = val;
  });

  return row;
}

/* ========== LEGACY single-row scanner (unchanged) ========== */
function checkAsinsAllMarkets(){
  if(!/^https:\/\//i.test(BASE_URL) && MULTI_BASE_URLS.length===0) throw new Error('BASE_URL must be public HTTPS.');

  const sh = SpreadsheetApp.getActiveSheet();
  ensureHeaders_(sh);

  const header = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const layout = analyzeHeaderForMarkets_(header);
  if (!layout.marketplaceIds.length) throw new Error('No "<Market> Open" columns detected.');

  const col = layout.indexMap;
  const lastRow = sh.getLastRow();
  if(lastRow<2) return;

  const cAsin = col.ASIN !== undefined ? col.ASIN + 1 : 1;
  const cProcessed = col.Processed !== undefined ? col.Processed + 1 : 2;

  const asins = sh.getRange(2,cAsin,lastRow-1,1).getValues().map(r=>String(r[0]).trim().toUpperCase());
  const mpIds = layout.marketplaceIds;

  for (var i=0;i<asins.length;i++){
    const asin = asins[i];
    if(!asin) continue;
    const processedCell = sh.getRange(2+i,cProcessed).getValue();
    const processed = String(processedCell||'').toUpperCase()==='Y';
    if(processed) continue;

    const keepa  = getKeepaInfo_(asin);
    const restr  = getRestrictions_(asin, mpIds);
    const sellers= getOffersFamily_(asin, mpIds);

    const row = buildRow_(header, layout, asin, keepa, restr, sellers);
    if (col.Processed !== undefined) row[col.Processed] = 'Y';

    sh.getRange(2+i,1,1,header.length).setValues([row]);
    sh.setRowHeight(2+i, Math.max(THUMB_SIZE+ROW_PADDING,40));
    Utilities.sleep(120);
  }
  toast_('Done ✓','Amazon');
}

/* =================== OPEN SUMMARY =================== */
/*  >>> This whole block is swapped in from your previously-working version <<< */

const SUMMARY_THUMB_SIZE=130;
const SUMMARY_ROW_PADDING=24;

// Countries for checkbox option (sellable only)
const LISTED_COUNTRIES = ['UK','IE','FR','IT','ES','NL','PL','SE','BE','US','JP','AU','MX','IN','SG','AE','SA','EG'];

function ensureSummarySheet_(){
  const ss=SpreadsheetApp.getActive();
  let sh=ss.getSheetByName('Open Summary');
  if(!sh) sh=ss.insertSheet('Open Summary');
  return sh;
}
function ensureSummaryHeader_(dst){
  const HDR=[
    'ASIN','Image','Title','Rating','Reviews','Open Markets',
    'Open Markets (No Sellers)','Choose Market','Link To List','Product Page','Logo',
    'Listed?','Listed Where?','SKU','Listed Date','Amazon Referral Fee (%)','Tracking Since',
    'Sales Rank: Current','Sales Rank: 90 days avg.','Bought in past month','Reviews: Review Count - Format Specific','Last Price Change','Listed since',
    'Categories: Root','Categories: Sub','Categories: Tree','ASIN (Keepa)','Brand','Product Group','Model','Color','Recommended Uses'
  ];
  const hasHeader = dst.getLastRow()>=1 && dst.getRange(1,1).getDisplayValue()==='ASIN';
  if(!hasHeader){
    dst.getRange(1,1,1,HDR.length).setValues([HDR]).setFontWeight('bold');
    dst.setFrozenRows(1); dst.setFrozenColumns(2);
    const widths=[140,130,420,70,90,260,260,140,160,160,90,100,220,160,130,160,140,
                  130,150,150,190,150,130,180,180,300,140,160,160,160,140,260];
    widths.forEach((w,i)=>dst.setColumnWidth(i+1,w));
  }
  ensureListedCheckboxColumns_(dst); // be sure checkboxes sit after "Listed Where?"
}

/* Insert/move checkbox columns immediately after "Listed Where?" and group them */
function ensureListedCheckboxColumns_(sh){
  const getHeader = () => sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  let header = getHeader();
  const base = header.indexOf('Listed Where?') + 1;
  if (base < 1) throw new Error('Column "Listed Where?" not found');

  let desired = base + 1;
  const map = {};
  for (let i=0;i<LISTED_COUNTRIES.length;i++){
    const name = 'Listed ['+LISTED_COUNTRIES[i]+']';
    header = getHeader();
    let idx = header.indexOf(name) + 1;

    if (idx < 1){
      sh.insertColumnBefore(desired);
      sh.getRange(1,desired).setValue(name).setFontWeight('bold');
      sh.setColumnWidth(desired, 90);
      idx = desired;
    } else if (idx !== desired) {
      const maxR = sh.getMaxRows();
      const vals = sh.getRange(1,idx,maxR,1).getValues();
      sh.insertColumnBefore(desired);
      sh.getRange(1,desired,maxR,1).setValues(vals);
      if (idx >= desired) sh.deleteColumn(idx+1); else sh.deleteColumn(idx);
      idx = desired;
      sh.setColumnWidth(idx,90);
    } else {
      sh.setColumnWidth(idx,90);
    }
    map[LISTED_COUNTRIES[i]]=idx;
    desired++;
  }

  try{
    sh.setColumnGroupControlPosition(SpreadsheetApp.GroupControlTogglePosition.BEFORE);
    sh.getRange(1, base+1, sh.getMaxRows(), LISTED_COUNTRIES.length).shiftColumnGroupDepth(1);
  }catch(e){}
  return map;
}

/* Create checkboxes for a row and set the TEXTJOIN formula */
function setListedWhereFormulaForRow_(sh,row,map){
  const h = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const colLW = h.indexOf('Listed Where?') + 1;
  if (colLW < 1) return;

  LISTED_COUNTRIES.forEach(c=>{
    const ccol = map[c];
    const r = sh.getRange(row, ccol, 1, 1);
    r.insertCheckboxes();
    if (!r.getValue()) r.setValue(false);
  });

  const pieces = LISTED_COUNTRIES.map(c=>{
    const a1 = sh.getRange(row, map[c]).getA1Notation();
    return `IF(${a1}=TRUE, "${c}", "")`;
  });
  sh.getRange(row,colLW).setFormula(`=TEXTJOIN(", ", TRUE, ${pieces.join(", ")})`);
}

/* === Two-phase builder (append fast + per-row Keepa fill) === */
function buildOpenSummary(){ return buildOpenSummaryCore_('append'); }
function rebuildOpenSummary(){ const dst=ensureSummarySheet_(); dst.clear(); ensureSummaryHeader_(dst); return buildOpenSummaryCore_('append'); }

function buildOpenSummaryCore_(mode){
  const ss=SpreadsheetApp.getActive();
  const src=ss.getSheets()[0];
  const lastCol=src.getLastColumn();
  if(lastCol<1) throw new Error('Source sheet is empty.');

  const header=src.getRange(1,1,1,lastCol).getValues()[0];
  const col=name=>header.indexOf(name)+1;

  const cAsin=col('ASIN'), cTitle=col('Title'), cImage=col('Image'), cRating=col('Rating'), cReviews=col('Reviews');
  if([cAsin,cTitle,cImage,cRating,cReviews].some(x=>x<1)) throw new Error('Missing headers.');

  const openCols=[];
  header.forEach((h,idx)=>{
    if(/\sOpen$/.test(h)){
      const label=h.replace(/\sOpen$/,'');
      openCols.push({label,index:idx+1});
    }
  });
  if(!openCols.length) throw new Error('No “<Market> Open” columns found.');

  const lastRow=src.getLastRow();
  if(lastRow<2){ toast_('No data rows in source.','Open Summary'); return; }

  // Use the exact values from Sheet1 (as in your working version)
  const vals=src.getRange(2,1,lastRow-1,lastCol).getValues();
  const frms=src.getRange(2,1,lastRow-1,lastCol).getFormulas();

  const dst=ensureSummarySheet_();
  ensureSummaryHeader_(dst);

  const DH = () => dst.getRange(1,1,1,dst.getLastColumn()).getValues()[0];
  let dHeader = DH();
  const dCol = n => (dHeader.indexOf(n)+1);

  const existing = new Set();
  const dstLast = dst.getLastRow();
  if (dstLast >= 2){
    dst.getRange(2,1,dstLast-1,1).getDisplayValues().forEach(r=>{
      const a=String(r[0]||'').trim().toUpperCase();
      if(a) existing.add(a);
    });
  }

  const sellable = sellableSet_();
  const cbMap = ensureListedCheckboxColumns_(dst);
  dHeader = DH();

  const appendedRows = []; // {asin, dstRow, imgFormulaFromSheet}

  /* ---- Phase 1: append all rows quickly ---- */
  for(let i=0;i<vals.length;i++){
    const row=vals[i];
    const asin=String(row[cAsin-1]||'').trim().toUpperCase();
    if(!asin) continue;
    if(mode==='append' && existing.has(asin)) continue;

    const openMkts=[];
    openCols.forEach(oc=>{
      const v=String(row[oc.index-1]||'').trim().toLowerCase();
      const label=oc.label;
      if(OPEN_SUMMARY_EXCLUDE.has(label)) return;
      if(!sellable.has(label)) return;
      if(v==='open') openMkts.push(label);
    });
    if(!openMkts.length) continue;

    const openNoSellers=[];
    GROUPS.forEach(g=>g.markets.forEach(m=>{
      if(OPEN_SUMMARY_EXCLUDE.has(m.label)) return;
      if(!sellable.has(m.label)) return;
      const openIdx=header.indexOf(m.label+' Open');
      const sellersIdx=header.indexOf(m.label+' Sellers');
      if(openIdx>=0 && sellersIdx>=0){
        const vOpen=String(row[openIdx]||'').toLowerCase();
        const vSell=String(row[sellersIdx]||'').toLowerCase();
        if(vOpen==='open' && vSell==='no sellers') openNoSellers.push(m.label);
      }
    }));

    const titleFromSheet   = row[cTitle-1]   || '';
    const ratingFromSheet  = row[cRating-1]  || '';
    const reviewsFromSheet = row[cReviews-1] || '';
    const imgFormulaFromSheet = frms[i][cImage-1] || '';

    const dstRow = dst.getLastRow() + 1;
    const totalCols = dst.getLastColumn();
    const initial = new Array(totalCols).fill('');

    initial[dCol('ASIN')-1]              = asin;
    initial[dCol('Title')-1]             = titleFromSheet;
    initial[dCol('Rating')-1]            = ratingFromSheet;
    initial[dCol('Reviews')-1]           = reviewsFromSheet;
    initial[dCol('Open Markets')-1]      = openMkts.join(', ');
    initial[dCol('Open Markets (No Sellers)')-1] = openNoSellers.join(', ');
    initial[dCol('Listed?')-1]           = 'Not Listed'; // default

    dst.getRange(dstRow,1,1,totalCols).setValues([initial]);

    if (imgFormulaFromSheet){
      dst.getRange(dstRow,dCol('Image')).setFormula(imgFormulaFromSheet);
    }

    const firstMarket=openMkts[0] || 'UK';
    const asinFormula='=HYPERLINK("'+((BUY_MAP[firstMarket]||DEFAULT_BUY_BASE))+'/dp/'+asin+'","'+asin+'")';
    dst.getRange(dstRow,dCol('ASIN')).setFormula(asinFormula);

    const choiceA1=dst.getRange(dstRow,dCol('Choose Market')).getA1Notation();
    const asinA1  =dst.getRange(dstRow,dCol('ASIN')).getA1Notation();
    const scSwitch=Object.keys(SC_MAP).map(k=>'"'+k+'","'+SC_MAP[k]+'"').join(',');
    const buySwitch=Object.keys(BUY_MAP).map(k=>'"'+k+'","'+BUY_MAP[k]+'"').join(',');
    const listFormula='=IF('+choiceA1+'="","",HYPERLINK(SWITCH('+choiceA1+','+scSwitch+',"'+DEFAULT_LIST_BASE+'") & "/abis/listing/syh?asin=" & '+asinA1+', "List in " & '+choiceA1+'))';
    const viewFormula='=IF('+choiceA1+'="","",HYPERLINK(SWITCH('+choiceA1+','+buySwitch+',"'+DEFAULT_BUY_BASE+'") & "/dp/" & '+asinA1+', "View in " & '+choiceA1+'))';
    dst.getRange(dstRow,dCol('Link To List')).setFormula(listFormula);
    dst.getRange(dstRow,dCol('Product Page')).setFormula(viewFormula);

    const ruleMarket = SpreadsheetApp.newDataValidation().requireValueInList(openMkts.length?openMkts:['UK'], true).setAllowInvalid(false).build();
    dst.getRange(dstRow,dCol('Choose Market')).setDataValidation(ruleMarket).setValue(openMkts[0]||'UK');

    const ruleListed=SpreadsheetApp.newDataValidation().requireValueInList(['Not Listed','Listed'], true).setAllowInvalid(false).build();
    dst.getRange(dstRow,dCol('Listed?')).setDataValidation(ruleListed).setValue('Not Listed');

    setListedWhereFormulaForRow_(dst, dstRow, cbMap);

    dst.setRowHeight(dstRow, Math.max(SUMMARY_THUMB_SIZE+SUMMARY_ROW_PADDING,44));
    dst.getRange(dstRow,dCol('Rating'),1,2).setHorizontalAlignment('center');
    dst.getRange(dstRow,dCol('Listed Date')).setNumberFormat('dd-mm-yyyy');
    dst.getRange(dstRow,dCol('Tracking Since')).setNumberFormat('yyyy-mm-dd');
    dst.getRange(dstRow,dCol('Last Price Change')).setNumberFormat('yyyy-mm-dd');
    dst.getRange(dstRow,dCol('Listed since')).setNumberFormat('yyyy-mm-dd');

    appendedRows.push({asin, dstRow, imgFormulaFromSheet});
  }

  if(!appendedRows.length){ toast_('Open Summary: no new rows'); return; }

  /* ---- Phase 2: Keepa fill (sequential updates) ---- */
  for (let j=0;j<appendedRows.length;j++){
    const {asin, dstRow, imgFormulaFromSheet} = appendedRows[j];
    const k = getKeepaInfo_(asin);
    const dHeaderNow = dst.getRange(1,1,1,dst.getLastColumn()).getValues()[0];
    const dColNow = n => (dHeaderNow.indexOf(n)+1);
    const setIf = (name,val) => { if (val!==null && val!==undefined && name) dst.getRange(dstRow,dColNow(name)).setValue(val); };
    setIf('Amazon Referral Fee (%)', (k.referralFeePct!==''? k.referralFeePct : ''));
    setIf('Tracking Since',          k.trackingSince || '');
    setIf('Sales Rank: Current',     k.salesRankCurrent || '');
    setIf('Sales Rank: 90 days avg.',k.salesRankAvg90   || '');
    setIf('Bought in past month',    k.boughtLastMonth  || '');
    setIf('Reviews: Review Count - Format Specific', k.reviewsFormatSpecific || '');
    setIf('Last Price Change',       k.lastPriceChange  || '');
    setIf('Listed since',            k.listedSince      || '');
    setIf('Categories: Root',        k.catRoot          || '');
    setIf('Categories: Sub',         k.catSub           || '');
    setIf('Categories: Tree',        k.catTree          || '');
    setIf('ASIN (Keepa)',            k.asin || asin);
    setIf('Brand',                   k.brand || '');
    setIf('Product Group',           k.productGroup || '');
    setIf('Model',                   k.model || '');
    setIf('Color',                   k.color || '');
    setIf('Recommended Uses',        k.recommendedUses || '');

    if (!imgFormulaFromSheet && k.imageUrl){
      const u = k.imageUrl;
      const f = '=HYPERLINK("'+u+'", IMAGE("'+u+'", 4, '+SUMMARY_THUMB_SIZE+', '+SUMMARY_THUMB_SIZE+'))';
      dst.getRange(dstRow,dColNow('Image')).setFormula(f);
    }
    Utilities.sleep(60);
  }

  toast_('Open Summary: appended '+appendedRows.length+' row(s)','Amazon');
}

/* =================== VISION =================== */
function parseImageUrl_(cellValue, cellFormula){
  const raw = (cellFormula && cellFormula.length) ? cellFormula : (cellValue || '');
  if(!raw) return '';
  let m = /IMAGE\(\s*"([^"]+)/i.exec(raw);
  if(m && m[1]) return String(m[1]);
  if(/^https?:\/\//i.test(raw)) return raw;
  return '';
}
function runVisionLogo_(sheetName, imageColName, logoColName){
  if(!VISION_API_KEY){
    const ss = SpreadsheetApp.getActive();
    const keySh = ss.getSheetByName('KEY');
    if (keySh) {
      const vk = String(keySh.getRange('B3').getValue()||'').trim();
      if (vk) VISION_API_KEY = vk;
    }
  }
  if(!VISION_API_KEY) throw new Error('Set Google Vision API key in VISION_API_KEY or KEY!B3.');

  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(sheetName);
  if(!sh) throw new Error('Sheet "'+sheetName+'" not found');

  const header = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const cImg = header.indexOf(imageColName)+1;
  if(cImg<1) throw new Error('Column "'+imageColName+'" not found on "'+sheetName+'"');

  let cLogo = header.indexOf(logoColName)+1;
  if(cLogo<1){
    sh.insertColumnAfter(sh.getLastColumn());
    cLogo = sh.getLastColumn();
    sh.getRange(1,cLogo).setValue(logoColName).setFontWeight('bold');
  }

  const lastRow = sh.getLastRow();
  if(lastRow<2) return;

  const values   = sh.getRange(2,cImg,lastRow-1,1).getDisplayValues();
  const formulas = sh.getRange(2,cImg,lastRow-1,1).getFormulas();

  const results = [];
  for (let i=0;i<values.length;i++){
    const url = parseImageUrl_(values[i][0], formulas[i][0]);
    if(!url){ results.push(['']); continue; }
    const safe = String(url).replace(/\+/g, '%2B');
    try{
      const payload = {
        requests: [{ image: { source: { imageUri: safe } }, features: [{ type: 'LOGO_DETECTION', maxResults: 1 }] }]
      };
      const data = safeJson_(UrlFetchApp.fetch(
        'https://vision.googleapis.com/v1/images:annotate?key='+VISION_API_KEY,
        { method:'post', contentType:'application/json', payload:JSON.stringify(payload), muteHttpExceptions:true }
      ));
      const anns = data && data.responses && data.responses[0] && data.responses[0].logoAnnotations;
      results.push([ (anns && anns.length) ? anns[0].description : 'No' ]);
    }catch(e){ results.push(['Error']); }
  }
  sh.getRange(2,cLogo,results.length,1).setValues(results);
  toast_('Logo check ('+sheetName+'): done','Vision');
}
function OS_checkLogos(){ runVisionLogo_('Open Summary','Image','Logo'); }

/******************************************************
 * FAST/BATCH SCANNER  (restrictions/offers + Keepa + auto-append)
 * — NOW PIPELINED: keeps pulling more batches while appending/Keepa worker runs.
 ******************************************************/

const SCAN_CHUNK_SIZE   = 40;           // per pass
const SCAN_TRIGGER_MIN  = 1;            // resume every minute
const SCAN_SOFT_LIMITMS = 5 * 60 * 1000;// keep looping up to ~5 minutes per execution

function SCAN_getState_(){
  const p = PropertiesService.getDocumentProperties();
  try { return JSON.parse(p.getProperty('SCAN_STATE') || '{}'); } catch(e){ return {}; }
}
function SCAN_setState_(patch){
  const p = PropertiesService.getDocumentProperties();
  const cur = SCAN_getState_();
  p.setProperty('SCAN_STATE', JSON.stringify(Object.assign({}, cur, patch||{})));
}
function SCAN_clearTriggers_(){
  ScriptApp.getProjectTriggers().forEach(t=>{
    if (t.getHandlerFunction && t.getHandlerFunction()==='SCAN_runner_') ScriptApp.deleteTrigger(t);
  });
}
function SCAN_schedule_(mins){
  SCAN_clearTriggers_();
  ScriptApp.newTrigger('SCAN_runner_').timeBased().everyMinutes(Math.max(1, Math.min(30, Math.floor(mins)))).create();
}
// Menu controls come from Menu.gs

function SCAN_statusSidebar(){
  const st = SCAN_getState_();
  const html = HtmlService.createHtmlOutput(
`<div style="font:13px Arial; padding:10px; width:300px">
  <h3 style="margin:0 0 8px">Batch Scanner — Status</h3>
  <div><b>Active:</b> ${!!st.active}</div>
  <div><b>Processed:</b> ${st.done||0}</div>
  <div><b>Last:</b> ${st.lastMsg||'-'}</div>
  <div id="rt" style="margin-top:8px;color:#555">…</div>
  <script>
    function tick(){ google.script.run.withSuccessHandler(function(s){ document.getElementById('rt').innerHTML=s; setTimeout(tick, 1200); }).SCAN_statusText_(); }
    tick();
  </script>
</div>`
  ).setWidth(320).setHeight(200);
  SpreadsheetApp.getUi().showSidebar(html);
}
function SCAN_statusText_(){
  const sh = SpreadsheetApp.getActiveSheet();
  const hdr = sh.getRange(1,1,1,sh.getLastColumn()).getDisplayValues()[0];
  const layout = analyzeHeaderForMarkets_(hdr);
  const idx = layout.indexMap;
  const cAsin = idx.ASIN !== undefined ? idx.ASIN + 1 : 0;
  const cProc = idx.Processed !== undefined ? idx.Processed + 1 : 0;
  let pending=0, total=0;
  if (cAsin>0 && cProc>0){
    const last = sh.getLastRow();
    if (last>=2){
      const vals = sh.getRange(2,1,last-1,Math.max(cAsin,cProc)).getDisplayValues();
      vals.forEach(r=>{
        const a = (r[cAsin-1]||'').toString().trim(); if (a) total++;
        if (a && (r[cProc-1]||'').toString().trim().toUpperCase()!=='Y') pending++;
      });
    }
  }
  const st = SCAN_getState_();
  return `Pending: <b>${pending}</b> of ${total}<br>Last: <i>${st.lastMsg||'-'}</i>`;
}

/* helpers for multi-proxy fan-out */
function getProxyList_(){
  if (Array.isArray(MULTI_BASE_URLS) && MULTI_BASE_URLS.length) return MULTI_BASE_URLS.slice();
  if (/^https:\/\//i.test(BASE_URL)) return [BASE_URL];
  throw new Error('No valid BASE_URL or MULTI_BASE_URLS configured.');
}
function splitRoundRobin_(arr, buckets){
  const out = Array.from({length:buckets},()=>[]);
  arr.forEach((v,i)=> out[i % buckets].push(v));
  return out;
}

/* === Runner (pipelined) === */
function SCAN_startFast(){ SCAN_setState_({active:true, lastMsg:'Started fast/batch scan', done:0}); SCAN_statusSidebar(); SCAN_runner_(); }
function SCAN_pause(){  SCAN_setState_({active:false, lastMsg:'Paused'});  SCAN_clearTriggers_(); toast_('Batch scanner paused','Amazon'); }
function SCAN_resume(){ SCAN_setState_({active:true,  lastMsg:'Resumed'}); SCAN_statusSidebar(); SCAN_runner_(); }
function SCAN_stop(){   SCAN_setState_({active:false, lastMsg:'Stopped'}); SCAN_clearTriggers_(); toast_('Batch scanner stopped','Amazon'); }

function SCAN_runner_(){
  const started = Date.now();
  let st = SCAN_getState_();
  if (!st.active) return;

  const sh = SpreadsheetApp.getActiveSheet();
  const header = sh.getRange(1,1,1,sh.getLastColumn()).getDisplayValues()[0];
  const layout = analyzeHeaderForMarkets_(header);
  const col = (name)=> layout.indexMap[name] !== undefined ? layout.indexMap[name] + 1 : 0;
  const cAsin = col('ASIN'), cProcessed = col('Processed');
  if (cAsin < 1 || cProcessed < 1) { SCAN_setState_({lastMsg:'Missing headers'}); return; }

  const mpIds = layout.marketplaceIds;
  if (!mpIds.length) { SCAN_setState_({lastMsg:'No marketplaces detected'}); return; }
  const proxies = getProxyList_();

  while (Date.now() - started < SCAN_SOFT_LIMITMS) {
    st = SCAN_getState_();
    if (!st.active) break;

    // Build one batch of pending rows
    const lastRow = sh.getLastRow();
    if (lastRow < 2){ SCAN_setState_({active:false,lastMsg:'Nothing to scan'}); SCAN_clearTriggers_(); return; }

    const rows = sh.getRange(2,1,lastRow-1,sh.getLastColumn()).getValues();
    const pending = [];
    for (let i=0;i<rows.length && pending.length<SCAN_CHUNK_SIZE;i++){
      const asin = String(rows[i][cAsin-1]||'').trim().toUpperCase();
      const proc = String(rows[i][cProcessed-1]||'').trim().toUpperCase()==='Y';
      if (asin && !proc) pending.push({asin, row: 2+i});
    }
    if (!pending.length){ SCAN_setState_({active:false,lastMsg:'Done'}); SCAN_clearTriggers_(); return; }

    const perProxy = splitRoundRobin_(pending, proxies.length);

    // ===== 1) Restrictions first (parallel per proxy) =====
    const restrPackets = [];
    proxies.forEach((base, idx)=>{
      const chunk = perProxy[idx];
      if (!chunk.length) return;
      const restrReqs = chunk.map(p => ({
        url: base + '/restrictions',
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify({ asin: p.asin, marketplaceIds: mpIds, conditionType:'new_new' }),
        muteHttpExceptions: true
      }));
      restrPackets.push({chunk, res: UrlFetchApp.fetchAll(restrReqs), base});
    });

    const restrMap = {};
    restrPackets.forEach(packet=>{
      const {chunk, res} = packet;
      chunk.forEach((p,i)=>{
        let rj=null; try{ rj = JSON.parse(res[i].getContentText()); }catch(_){ }
        const rs = (rj && rj.results) ? rj.results : {};
        const outR={};
        mpIds.forEach(m=>{
          const r = rs[m];
          let val='Closed';
          if (r){
            if (r.exists===false) val='Closed';
            else if (r.status==='Open' || (Array.isArray(r.reasonCodes)&&r.reasonCodes.length===0)) val='Open';
          }
          outR[m]=val;
        });
        restrMap[p.asin]=outR;
      });
    });

    // ===== 2) Offers ONLY where at least one market is Open (cuts UrlFetch calls) =====
    const offersPackets = [];
    const sellMap  = {};
    restrPackets.forEach(({chunk, base})=>{
      const reqs = [];
      const asinsNeedingOffers = [];
      chunk.forEach(p=>{
        const opens = restrMap[p.asin] || {};
        const hasAnyOpen = Object.keys(opens).some(mid => opens[mid]==='Open');
        if (hasAnyOpen){
          reqs.push({
            url: base + '/offers',
            method: 'post',
            contentType: 'application/json',
            payload: JSON.stringify({ asin: p.asin, marketplaceIds: mpIds, itemCondition:'New', includeSiblings:true }),
            muteHttpExceptions: true
          });
          asinsNeedingOffers.push(p.asin);
        } else {
          const o={}; mpIds.forEach(m=>o[m]='NO SELLERS'); sellMap[p.asin]=o;
        }
      });
      if (reqs.length) offersPackets.push({asins:asinsNeedingOffers, res: UrlFetchApp.fetchAll(reqs)});
    });

    offersPackets.forEach(packet=>{
      const {asins, res} = packet;
      asins.forEach((asin, i)=>{
        let oj=null; try{ oj = JSON.parse(res[i].getContentText()); }catch(_){ }
        const os = (oj && oj.results) ? oj.results : {};
        const outS={};
        mpIds.forEach(m=>{
          const s=os[m];
          outS[m]=(s && s.hasNewOffers)?'YES SELLERS':'NO SELLERS';
        });
        sellMap[asin]=outS;
      });
    });

    // ===== 3) Keepa concurrently (domain fallbacks) =====
    const asins = pending.map(p=>p.asin);
    let keepaMap = {};
    let miss = new Set(asins);
    const addBatch = (m)=>{ Object.keys(m).forEach(a=>{ keepaMap[a]=m[a]; miss.delete(a); }); };

    let r = keepaFetchAllDomain_(2, Array.from(miss)); addBatch(r.map);
    if (miss.size){ r = keepaFetchAllDomain_(1, Array.from(miss)); addBatch(r.map); }
    if (miss.size){ r = keepaFetchAllDomain_(3, Array.from(miss)); addBatch(r.map); }
    if (miss.size){ r = keepaFetchAllDomain_(4, Array.from(miss)); addBatch(r.map); }

    // 3b) EXTRA: per-ASIN quick retries (up to 2) for critical fields if still missing
    pending.forEach(p=>{
      const k = keepaMap[p.asin] || {};
      const missingCritical = !(k && k.title && k.imageUrl && k.rating && (k.reviews!=='' && k.reviews!=null));
      if (missingCritical){
        const patch = keepaQuickRetryCritical_(p.asin, 2);
        keepaMap[p.asin] = Object.assign({}, k, patch);
      }
    });

    // ===== 4) Progressive write to the main sheet — batch contiguous writes to reduce Sheet calls =====
    const headerLen = header.length;
    const updates = pending.map(p=>{
      const k = keepaMap[p.asin] || { title:'', imageUrl:'', rating:'', reviews:'' };
      const rowVals = buildRow_(header, layout, p.asin, k, (restrMap[p.asin]||{}), (sellMap[p.asin]||{}));
      if (layout.indexMap.Processed !== undefined) rowVals[layout.indexMap.Processed] = 'Y';
      return {start:p.row, vals:rowVals};
    }).sort((a,b)=>a.start-b.start);

    let segStart = null, segBuf = [];
    let segments = [];
    const flushSeg = ()=>{
      if (segBuf.length){
        segments.push({start:segStart, rows:segBuf.slice()});
      }
      segStart=null; segBuf=[];
    };
    for (let i=0;i<updates.length;i++){
      const u = updates[i];
      if (segStart===null){ segStart=u.start; segBuf=[u.vals]; }
      else {
        const prevRow = segStart + segBuf.length - 1;
        if (u.start === prevRow + 1){
          segBuf.push(u.vals);
        } else {
          flushSeg();
          segStart = u.start; segBuf = [u.vals];
        }
      }
    }
    flushSeg();

    segments.forEach(seg=>{
      sh.getRange(seg.start, 1, seg.rows.length, headerLen).setValues(seg.rows);
      for (let i=0;i<seg.rows.length;i++){
        sh.setRowHeight(seg.start+i, Math.max(THUMB_SIZE+ROW_PADDING,40));
      }
    });

    SCAN_setState_({done:(st.done||0)+pending.length, lastMsg:`Processed ${pending.length} rows (fast)`});
  }

  SCAN_schedule_(SCAN_TRIGGER_MIN);
  SCAN_statusSidebar();
}

/* ======== Single-ASIN Keepa (used in legacy) ======== */
function getKeepaInfo_(asin){
  const best = {
    title:'', imageUrl:'', rating:'', reviews:'', referralFeePct:'',
    trackingSince:'', listedSince:'',
    salesRankCurrent:'', salesRankAvg90:'', boughtLastMonth:'',
    reviewsFormatSpecific:'', lastPriceChange:'',
    catRoot:'', catSub:'', catTree:'',
    asin: asin, brand:'', productGroup:'', model:'', color:'', recommendedUses:''
  };
  const prefer = (a,b)=> a ? a : (b || '');
  for (let di=0; di<KEEPA_DOMAINS_TRY.length; di++){
    const domain = KEEPA_DOMAINS_TRY[di];
    const url = 'https://api.keepa.com/product'
      + '?key='+encodeURIComponent(getKeepaKey_())
      + '&domain='+domain
      + '&asin='+encodeURIComponent(asin)
      + '&stats=1&rating=1';
    const data = safeJson_(httpGet_(url));
    if (!data) continue;
    if (data.tokensLeft !== undefined) setKeepaTokensLeft_(data.tokensLeft);
    if (data.error) continue;
    const p = Array.isArray(data.products) ? data.products[0] : null;
    if (!p) continue;

    const parsed = parseKeepaProduct_(p);
    const assign = (k,v) => { best[k] = prefer(best[k], v); };
    Object.keys(parsed).forEach(k=> assign(k, parsed[k]));
  }
  return best;
}
