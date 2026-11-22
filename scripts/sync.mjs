#!/usr/bin/env node
// =============================================================================
//  phone-catalog sync — copies phone-specs-api into static JSON in this repo
// =============================================================================
//  The mobile_maintenance app reads ONLY these files (through jsDelivr / raw
//  GitHub), never phone-specs-api itself.
//
//  phone-specs-api is a public scraper of GSMArena running on one shared Vercel
//  IP, so GSMArena rate-limits it and it answers `500` (wrapping a `429`) for
//  long stretches — then works again. GSMArena itself blocks GitHub Actions
//  outright (every request 429 in Sept 2026), so going direct is not an option.
//
//  So this job is PATIENT, not persistent:
//    • it runs every hour with a small time budget;
//    • after 3 failures in a row it stops for this run (the API is throttled —
//      hammering it only extends that) and keeps everything it got;
//    • a queue file (data/state.json) remembers what was tried when, so every
//      run continues where the last one stopped, round-robin.
//  It fills the whole catalog over a day or two of open windows, and after that
//  only re-reads brands whose device count changed — a few requests a week.
//
//  Output (data/):
//    brands.json            [{ slug, name, count }]
//    models/<brand>.json    [{ slug, name, image }]   newest first
//    colors.json            { "<model slug>": ["Black", …] }  ([] = none listed)
//    meta.json              counts + timestamps
//    state.json             the work queue's memory (last attempt per brand)
//
//  Usage:
//    node scripts/sync.mjs [--phase=all|brands|colors] [--budget-min=20]
//                          [--limit=N] [--brand=apple-phones-48] [--force]
//
//  Never relax:
//    • Sequential, one request at a time, INTERVAL_MS apart.
//    • Nothing is ever deleted. Model lists are merged; a model one run saw is
//      kept even if a later run doesn't return it.
// =============================================================================

import { mkdir, readFile, writeFile, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DATA = path.join(ROOT, 'data');
const MODELS = path.join(DATA, 'models');

const BASE = process.env.PHONE_SPECS_API_BASE_URL || 'https://phone-specs-api.vercel.app';
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 3000);
const RETRY_AFTER_MS = 20_000;
const STOP_AFTER_FAILURES = 3;
const MAX_PAGES = 60; // Samsung is ~30 pages of 50; a runaway loop stops here.
const COMPLETE_RATIO = 0.9; // a brand file with >= 90% of device_count is done

// Brands whose colors are worth having first (the shop's market).
const COLOR_PRIORITY = [
  'samsung', 'apple', 'xiaomi', 'oppo', 'realme', 'vivo', 'honor', 'huawei',
  'infinix', 'tecno', 'nokia', 'motorola', 'oneplus', 'google', 'itel', 'lenovo',
];

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, '').split('=');
    return [k, v ?? true];
  }),
);
const phase = args.phase || 'all';
const limit = args.limit ? Number(args.limit) : Infinity;
const force = Boolean(args.force);
const onlyBrand = args.brand || null;
const deadline = Date.now() + Number(args['budget-min'] || 20) * 60_000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date().toISOString();
let lastRequestAt = 0;
let requests = 0;
let failuresInARow = 0;
let stopped = null; // reason this run stopped early
let changed = false; // did any data file change?

const outOfTime = () => Date.now() > deadline;
const canContinue = () => !stopped && !outOfTime();

/** One GET with a single retry. Returns `data` or null — never throws. */
async function api(pathAndQuery) {
  for (let attempt = 0; attempt < 2; attempt++) {
    if (!canContinue()) return null;
    const wait = lastRequestAt + INTERVAL_MS - Date.now();
    if (wait > 0) await sleep(wait);
    lastRequestAt = Date.now();
    requests++;
    try {
      const res = await fetch(BASE + pathAndQuery, {
        headers: { 'user-agent': 'phone-catalog-sync (+github actions)' },
        signal: AbortSignal.timeout(30_000),
      });
      if (res.ok) {
        const body = await res.json();
        if (body && body.status === true) {
          failuresInARow = 0;
          return body.data;
        }
      }
      console.warn(`  ${res.status} ${pathAndQuery}`);
    } catch (e) {
      console.warn(`  error ${pathAndQuery}: ${e.message}`);
    }
    if (attempt === 0) await sleep(RETRY_AFTER_MS);
  }
  if (++failuresInARow >= STOP_AFTER_FAILURES) {
    stopped = `phone-specs-api failed ${failuresInARow} times in a row (throttled) — next run continues`;
    console.warn(`  ${stopped}`);
  }
  return null;
}

async function readJson(file, fallback) {
  try {
    return JSON.parse(await readFile(file, 'utf8'));
  } catch {
    return fallback;
  }
}

/** Stable, diff-friendly JSON; only touches the file when the content changed. */
async function writeJson(file, value, { isData = true } = {}) {
  let text;
  if (Array.isArray(value)) {
    text = '[\n' + value.map((v) => '  ' + JSON.stringify(v)).join(',\n') + '\n]\n';
  } else if (value && typeof value === 'object' && file.endsWith('colors.json')) {
    const keys = Object.keys(value).sort();
    text = '{\n' + keys.map((k) => `  ${JSON.stringify(k)}: ${JSON.stringify(value[k])}`).join(',\n') + '\n}\n';
  } else {
    text = JSON.stringify(value, null, 2) + '\n';
  }
  let old = null;
  try { old = await readFile(file, 'utf8'); } catch { /* new file */ }
  if (old === text) return false;
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, text);
  if (isData) changed = true;
  return true;
}

// ── Brands phase ────────────────────────────────────────────────────────────

/** All of one brand's models, or null if any page failed. */
async function fetchBrandModels(slug) {
  const bySlug = new Map();
  for (let page = 1; page <= MAX_PAGES; page++) {
    const data = await api(`/brands/${slug}?page=${page}`);
    if (data == null) return null;
    const phones = Array.isArray(data.phones) ? data.phones : [];
    // The API's `last_page` is wrong (Apple says 6–7, the list ends on page 4),
    // so the end of the list is the first empty page.
    if (phones.length === 0) break;
    for (const p of phones) {
      const name = String(p.phone_name ?? '').trim();
      if (!name || !p.slug) continue;
      bySlug.set(p.slug, { slug: p.slug, name, image: p.image || null });
    }
  }
  return [...bySlug.values()];
}

async function syncBrands(state) {
  const previous = await readJson(path.join(DATA, 'brands.json'), []);
  const prevBySlug = new Map(previous.map((b) => [b.slug, b]));

  let live = await api('/brands');
  if (!Array.isArray(live) || live.length === 0) {
    if (previous.length === 0) {
      console.warn('No brand list yet and /brands unavailable — nothing to do this run.');
      return;
    }
    console.warn(`/brands unavailable — using the previous list (${previous.length} brands)`);
    live = previous.map((b) => ({ brand_slug: b.slug, brand_name: b.name, device_count: b.count }));
    stopped = null; // one failure on /brands should not stop the model pages
    failuresInARow = 0;
  }

  const brands = new Map(prevBySlug);
  for (const b of live) {
    const name = String(b.brand_name ?? '').trim();
    if (!name || !b.brand_slug) continue;
    brands.set(b.brand_slug, { slug: b.brand_slug, name, count: Number(b.device_count) || 0 });
  }
  const sorted = [...brands.values()].sort((a, b) => a.name.localeCompare(b.name));
  await writeJson(path.join(DATA, 'brands.json'), sorted);

  // What needs (re)reading: no file yet, a file well short of the brand's
  // device count, or a count that moved since the last successful read.
  const queue = [];
  for (const b of brands.values()) {
    if (onlyBrand && b.slug !== onlyBrand) continue;
    const file = path.join(MODELS, `${b.slug}.json`);
    const have = existsSync(file) ? (await readJson(file, [])).length : 0;
    const st = state.brands[b.slug] ?? {};
    const reason = force ? 'forced'
      : have === 0 ? 'missing'
      : have < Math.floor(b.count * COMPLETE_RATIO) ? `incomplete ${have}/${b.count}`
      : st.ok_count !== undefined && st.ok_count !== b.count ? `count ${st.ok_count}→${b.count}`
      : null;
    if (reason) queue.push({ brand: b, reason, lastTry: st.last_attempt ?? '' });
  }
  // Least recently tried first, so a brand that keeps failing can't starve the rest.
  queue.sort((a, b) => a.lastTry.localeCompare(b.lastTry) || a.brand.count - b.brand.count);
  console.log(`brands: ${brands.size}, ${queue.length} need reading`);

  let done = 0;
  for (const { brand, reason } of queue) {
    if (!canContinue() || done >= limit) break;
    done++;
    state.brands[brand.slug] = { ...(state.brands[brand.slug] ?? {}), last_attempt: now() };
    process.stdout.write(`${brand.name} (${reason}) … `);
    const models = await fetchBrandModels(brand.slug);
    if (models == null) {
      console.log('failed, will retry');
      continue;
    }
    const file = path.join(MODELS, `${brand.slug}.json`);
    const old = await readJson(file, []);
    const order = new Map(models.map((m, i) => [m.slug, i]));
    const merged = new Map(old.map((m) => [m.slug, m]));
    for (const m of models) merged.set(m.slug, m);
    const list = [...merged.values()].sort(
      (a, b) => (order.get(a.slug) ?? 1e9) - (order.get(b.slug) ?? 1e9),
    );
    await writeJson(file, list);
    state.brands[brand.slug].ok_count = brand.count;
    state.brands[brand.slug].last_ok = now();
    console.log(`${models.length} read, ${list.length} on file`);
  }
}

// ── Colors phase ────────────────────────────────────────────────────────────

function colorsOf(detail) {
  for (const group of detail?.specifications ?? []) {
    for (const spec of group?.specs ?? []) {
      if (String(spec?.key ?? '').toLowerCase() !== 'colors') continue;
      const raw = (Array.isArray(spec.val) ? spec.val : [spec.val]).join(', ');
      const seen = new Set();
      return raw
        .split(/[,/،]/)
        .map((s) => s.trim())
        .filter((s) => s && s.length <= 60 && !seen.has(s.toLowerCase()) && seen.add(s.toLowerCase()));
    }
  }
  return [];
}

async function syncColors() {
  const file = path.join(DATA, 'colors.json');
  const colors = await readJson(file, {});
  const files = existsSync(MODELS) ? (await readdir(MODELS)).sort() : [];
  const rank = (f) => {
    const i = COLOR_PRIORITY.findIndex((p) => f.startsWith(`${p}-phones-`));
    return i === -1 ? COLOR_PRIORITY.length : i;
  };
  files.sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));
  const pending = [];
  for (const f of files) {
    for (const m of await readJson(path.join(MODELS, f), [])) {
      if (!(m.slug in colors)) pending.push(m.slug);
    }
  }
  console.log(`colors: ${Object.keys(colors).length} known, ${pending.length} pending`);

  let done = 0;
  for (const slug of pending) {
    if (!canContinue() || done >= limit) break;
    const detail = await api(`/${slug}`);
    done++;
    if (detail == null) continue;
    colors[slug] = colorsOf(detail);
    if (done % 100 === 0) await writeJson(file, colors);
  }
  await writeJson(file, colors);
  console.log(`colors: tried ${done} this run`);
}

// ── Main ────────────────────────────────────────────────────────────────────

const statePath = path.join(DATA, 'state.json');
const state = await readJson(statePath, { brands: {} });
state.brands ??= {};
const started = Date.now();

if (phase === 'brands' || phase === 'all') await syncBrands(state);
// Colors only once the API has answered this run; after a stop there is no
// point asking for detail pages.
if ((phase === 'colors' || phase === 'all') && !stopped) await syncColors();

await writeJson(statePath, state, { isData: false });

const brands = await readJson(path.join(DATA, 'brands.json'), []);
const colors = await readJson(path.join(DATA, 'colors.json'), {});
let models = 0;
let brandsWithModels = 0;
if (existsSync(MODELS)) {
  for (const f of await readdir(MODELS)) {
    const n = (await readJson(path.join(MODELS, f), [])).length;
    models += n;
    if (n > 0) brandsWithModels++;
  }
}
const meta = await readJson(path.join(DATA, 'meta.json'), {});
// A weekly heartbeat commit even when nothing changed, so GitHub never
// disables the schedule for inactivity (it does after 60 days).
const heartbeatDue = !meta.heartbeat || Date.now() - Date.parse(meta.heartbeat) > 6 * 86_400_000;
if (changed || heartbeatDue) {
  await writeJson(path.join(DATA, 'meta.json'), {
    source: BASE,
    brands: brands.length,
    brands_with_models: brandsWithModels,
    models,
    models_with_colors_checked: Object.keys(colors).length,
    last_change: changed ? now() : meta.last_change ?? null,
    heartbeat: now(),
  });
}
console.log(
  `requests ${requests}, ${Math.round((Date.now() - started) / 1000)}s, ` +
  `${brandsWithModels}/${brands.length} brands, ${models} models, ` +
  `${Object.keys(colors).length} colors checked` + (stopped ? ` — stopped: ${stopped}` : ''),
);
