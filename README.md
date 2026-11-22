# phone-catalog

Static mirror of [phone-specs-api](https://phone-specs-api.vercel.app) (GSMArena
data) used by the mobile_maintenance app's device picker.

The app reads these files and **never** calls phone-specs-api directly. That API
scrapes GSMArena on every request from a shared IP and fails with `500`/`429`
under bursts, so it is only called from this repo's GitHub Action: one request
every 2 s, with backoff, on a schedule.

## Files (`data/`)

| file | shape |
|---|---|
| `brands.json` | `[{ "slug", "name", "count" }]` |
| `models/<brand slug>.json` | `[{ "slug", "name", "image" }]`, newest first |
| `colors.json` | `{ "<model slug>": ["Black", …] }` (`[]` = checked, none listed) |
| `meta.json` | counts and last-sync timestamps |

Read them at
`https://cdn.jsdelivr.net/gh/MohamedMahmoud10/phone-catalog@main/data/<file>`
(fallback: `https://raw.githubusercontent.com/MohamedMahmoud10/phone-catalog/main/data/<file>`).

## Schedule

- **Weekly** (Mon 03:00 UTC): brand list, plus model lists of every brand whose
  device count changed.
- **Every 6 h**: up to 1,200 model detail pages for colors. Once every model has
  been checked this finds nothing pending and ends in seconds.
- Every run commits `meta.json`, so the repo is never idle long enough for GitHub
  to disable the schedule (60 days).

Run by hand: **Actions → Sync catalog → Run workflow**, or locally
`node scripts/sync.mjs --phase=brands --brand=apple-phones-48`.

## Rules

- Sequential and throttled. Never parallelise requests.
- Nothing is ever deleted. A model list is only replaced when the fetch got at
  least 90% of the brand's device count; otherwise the previous file is kept.
