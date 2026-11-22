#!/usr/bin/env node
// =============================================================================
//  phone-catalog sync — copies phone-specs-api into static JSON in this repo
// =============================================================================
//  The mobile_maintenance app reads ONLY these files (through jsDelivr / raw
//  GitHub), never phone-specs-api itself. phone-specs-api scrapes GSMArena on
//  every request from one shared Vercel IP and answers `500` wrapping a `429`
//  when hit in bursts, so it is only ever called from here: sequentially,
//  throttled, with backoff, on a schedule.
//
//  Output (all under data/):
//    brands.json            [{ slug, name, count }]
//    models/<brand>.json    [{ slug, name, image }]
//    colors.json            { "<model slug>": ["Black", "Blue", …] }
//    meta.json              counts + timestamps (also the scheduler keep-alive)
//
//  Usage:
//    node scripts/sync.mjs --phase=brands [--limit=N] [--force] [--brand=apple-phones-48]
//    node scripts/sync.mjs --phase=colors [--limit=1500]
//    node scripts/sync.mjs --phase=all
//
//  Rules that must not be relaxed:
//    • Sequential, one request at a time, INTERVAL_MS apart. Speed buys nothing.
//    • Nothing is ever deleted. A brand or model missing from one run is kept:
//      a bad scrape must never strip the picker.
//    • A brand's model file is only replaced when the fetch looks complete
//      (>= 90% of the brand's device_count). Otherwise the old file stays.
// =============================================================================

import { mkdir, readFile, writeFile, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DATA = path.join(ROOT, 'data');
const MODELS = path.join(DATA, 'models');

const BASE = process.env.PHONE_SPECS_API_BASE_URL || 'https://phone-specs-api.vercel.app';
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 2000);
// phone-specs-api fails intermittently (GSMArena rate-limits its shared IP), so
// give each request up to ~6 minutes before moving on.
const BACKOFF_MS = [10_000, 30_000, 90_000, 240_000];
const MAX_PAGES = 60; // Samsung is ~30 pages of 50; a runaway loop stops here.

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, '').split('=');
    return [k, v ?? true];
  }),
);
const phase = args.phase || 'brands';
const limit = args.limit ? Number(args.limit) : Infinity;
const force = Boolean(args.force);
const onlyBrand = args.brand || null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let lastRequestAt = 0;
let requests = 0;

/** One GET, throttled and retried. Returns `data` or null — never throws. */
async function api(pathAndQuery) {
  for (let attempt = 0; attempt <= BACKOFF_MS.length; attempt++) {
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
        if (body && body.status === true) return body.data;
      }
      console.warn(`  ${res.status} ${pathAndQuery}`);
    } catch (e) {
      console.warn(`  error ${pathAndQuery}: ${e.message}`);
    }
    if (attempt < BACKOFF_MS.length) {
      console.warn(`  backing off ${BACKOFF_MS[attempt] / 1000}s`);
      await sleep(BACKOFF_MS[attempt]);
    }
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

/** Stable, diff-friendly JSON: one entry per line. */
async function writeJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  let text;
  if (Array.isArray(value)) {
    text = '[\n' + value.map((v) => '  ' + JSON.stringify(v)).join(',\n') + '\n]\n';
  } else if (value && typeof value === 'object' && file.endsWith('colors.json')) {
    const keys = Object.keys(value).sort();
    text = '{\n' + keys.map((k) => `  ${JSON.stringify(k)}: ${JSON.stringify(value[k])}`).join(',\n') + '\n}\n';
  } else {
    text = JSON.stringify(value, null, 2) + '\n';
  }
  await writeFile(file, text);
}

// ── Brands phase ────────────────────────────────────────────────────────────

async function fetchBrandModels(slug) {
  const bySlug = new Map();
  for (let page = 1; page <= MAX_PAGES; page++) {
    const data = await api(`/brands/${slug}?page=${page}`);
    if (data == null) return null; // a page failed outright → not trustworthy
    const phones = Array.isArray(data.phones) ? data.phones : [];
    // `last_page` from the API is unreliable (Apple reports 6–7 when the list
    // ends on page 4), so the end is the first empty page.
    if (phones.length === 0) break;
    for (const p of phones) {
      const name = String(p.phone_name ?? '').trim();
      if (!name || !p.slug) continue;
      bySlug.set(p.slug, { slug: p.slug, name, image: p.image || null });
    }
  }
  return [...bySlug.values()];
}

async function syncBrands() {
  const previous = await readJson(path.join(DATA, 'brands.json'), []);
  const prevBySlug = new Map(previous.map((b) => [b.slug, b]));

  let live = await api('/brands');
  if (!Array.isArray(live) || live.length === 0) {
    if (previous.length === 0) throw new Error('Could not read /brands and no previous list — nothing changed.');
    // Carry on with last run's list: the model pages are separate requests and
    // usually still answer when this one doesn't.
    console.warn(`/brands unavailable — using the previous list (${previous.length} brands)`);
    live = previous.map((b) => ({ brand_slug: b.slug, brand_name: b.name, device_count: b.count }));
  }

  const merged = new Map(prevBySlug);
  for (const b of live) {
    const name = String(b.brand_name ?? '').trim();
    if (!name || !b.brand_slug) continue;
    merged.set(b.brand_slug, { slug: b.brand_slug, name, count: Number(b.device_count) || 0 });
  }

  let done = 0;
  let updated = 0;
  let kept = 0;
  for (const brand of merged.values()) {
    if (onlyBrand && brand.slug !== onlyBrand) continue;
    if (done >= limit) break;
    const file = path.join(MODELS, `${brand.slug}.json`);
    const prev = prevBySlug.get(brand.slug);
    if (!force && prev && prev.count === brand.count && existsSync(file)) continue;

    done++;
    process.stdout.write(`${brand.name} (${brand.count}) … `);
    const models = await fetchBrandModels(brand.slug);
    if (models == null) {
      console.log('FAILED, kept previous file');
      kept++;
      // Remember the old count so the next run tries this brand again.
      merged.set(brand.slug, { ...brand, count: prev?.count ?? -1 });
      continue;
    }
    const enough = brand.count === 0 || models.length >= Math.floor(brand.count * 0.9);
    if (!enough) {
      console.log(`only ${models.length}/${brand.count}, kept previous file`);
      kept++;
      merged.set(brand.slug, { ...brand, count: prev?.count ?? -1 });
      continue;
    }
    // Never drop a model a previous run saw (invoices store the name as text,
    // so a stale row is harmless; a missing one empties the picker).
    const old = await readJson(file, []);
    const bySlug = new Map(old.map((m) => [m.slug, m]));
    for (const m of models) bySlug.set(m.slug, m);
    // Newest first, the way GSMArena lists them; old-only rows go last.
    const order = new Map(models.map((m, i) => [m.slug, i]));
    const list = [...bySlug.values()].sort(
      (a, b) => (order.get(a.slug) ?? 1e9) - (order.get(b.slug) ?? 1e9),
    );
    await writeJson(file, list);
    updated++;
    console.log(`${models.length} models`);
  }

  const brands = [...merged.values()].sort((a, b) => a.name.localeCompare(b.name));
  await writeJson(path.join(DATA, 'brands.json'), brands);
  console.log(`brands: ${brands.length}, refreshed ${updated}, kept ${kept}`);
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
        .filter((s) => s && !seen.has(s.toLowerCase()) && seen.add(s.toLowerCase()));
    }
  }
  return [];
}

async function syncColors() {
  const file = path.join(DATA, 'colors.json');
  const colors = await readJson(file, {});
  const pending = [];
  if (existsSync(MODELS)) {
    for (const f of (await readdir(MODELS)).sort()) {
      for (const m of await readJson(path.join(MODELS, f), [])) {
        if (!(m.slug in colors)) pending.push(m.slug);
      }
    }
  }
  console.log(`colors: ${Object.keys(colors).length} known, ${pending.length} pending`);

  let done = 0;
  for (const slug of pending) {
    if (done >= limit) break;
    const detail = await api(`/${slug}`);
    done++;
    if (detail == null) continue; // stays pending; tried again next run
    colors[slug] = colorsOf(detail); // [] = checked, none listed
    if (done % 100 === 0) {
      await writeJson(file, colors); // a timeout mid-run loses at most 100
      console.log(`  ${done} done`);
    }
  }
  await writeJson(file, colors);
  console.log(`colors: checked ${done} this run`);
}

// ── Meta ────────────────────────────────────────────────────────────────────

async function writeMeta() {
  const brands = await readJson(path.join(DATA, 'brands.json'), []);
  const colors = await readJson(path.join(DATA, 'colors.json'), {});
  let models = 0;
  if (existsSync(MODELS)) {
    for (const f of await readdir(MODELS)) models += (await readJson(path.join(MODELS, f), [])).length;
  }
  const meta = await readJson(path.join(DATA, 'meta.json'), {});
  const now = new Date().toISOString();
  await writeJson(path.join(DATA, 'meta.json'), {
    source: BASE,
    brands: brands.length,
    models,
    models_with_colors_checked: Object.keys(colors).length,
    last_brands_sync: phase === 'colors' ? meta.last_brands_sync ?? null : now,
    last_colors_sync: phase === 'brands' ? meta.last_colors_sync ?? null : now,
    // Changes every run, so the repo always has a fresh commit and GitHub
    // never disables the schedule for inactivity (it does after 60 days).
    last_run: now,
  });
}

const started = Date.now();
if (phase === 'brands' || phase === 'all') await syncBrands();
if (phase === 'colors' || phase === 'all') await syncColors();
await writeMeta();
console.log(`requests: ${requests}, took ${Math.round((Date.now() - started) / 1000)}s`);
