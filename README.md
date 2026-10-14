# Workout Data Pipeline

A Python ingestion pipeline that builds a personal training database from
weightlifting data (Fitbod) and marathon training running data (Apple Health / Apple Watch),
feeding an AI coach layer. Built as a portfolio project with a product
mindset — designed so it could serve many users, not just one.

## Architecture

```
User shares Fitbod CSV ──►┐
                           ├─► Ingestion (this code) ──► training.db (SQLite)
User pings after a run ──►┘         │                          │
                                    │                          ▼
                          nightly backstop (cron)      post-workout analysis
```

Two trigger types, one codebase:

- **User-triggered** — the primary path. Sharing a CSV or pinging after a
  workout runs the ingest. Only the user knows when the data is actually
  ready (e.g. waiting ~3 min post-run so heart-rate recovery is captured).
- **Schedule-triggered** — a nightly cron backstop catches anything missed.
  Every operation is idempotent, so re-runs are harmless.

## The interesting problem: deduping full exports

Fitbod exports your *entire* history every time (~9,500 sets), not just new
ones — and it spells the same number differently between exports
(`7.937875915123695` vs `7.94`). So the pipeline fingerprints each set with a
**normalized key** (date + exercise + reps + weight, cleaned and rounded),
stores the key as the SQLite primary key, and lets `INSERT OR IGNORE` do the
diffing. 9,586 rows in, 5,921 unique sets out — 3,665 duplicates were hiding
inside a single export.

Apple Health workouts are simpler: Apple assigns each a unique ID, so the ID
is the primary key and duplicates are impossible.

## Data model

Three tables in `training.db`:

```sql
fitbod_sets     (set_key TEXT PRIMARY KEY,  -- normalized fingerprint
                 workout_date, exercise, reps, weight_kg,
                 duration_s, distance_m, is_warmup, ingested_at)
workouts        (healthkit_id TEXT PRIMARY KEY,  -- Apple's unique workout id
                 workout_type, start_datetime, end_datetime, timezone,
                 distance_m, duration_sec, avg_hr_bpm, max_hr_bpm,
                 energy_kcal, avg_speed_mps, max_speed_mps,
                 elevation_gain_m, is_indoor, ingested_at)
ingestion_state (key TEXT PRIMARY KEY, value TEXT)  -- watermarks
```

## Usage

```bash
python pipeline.py init                            # create training.db
python pipeline.py fitbod path/to/WorkoutExport.csv
python pipeline.py health                           # recent workouts (post-ping)
python pipeline.py health --days 7                  # nightly backstop sweep
python pipeline.py stats                             # counts + watermarks
```

Zero dependencies — Python standard library only (`sqlite3`, `csv`,
`argparse`, `subprocess`).

**Prerequisite:** Apple Health ingestion shells out to an external CLI
(`health-cli`) via `subprocess`. It is a system binary on this machine, not
a pip package — so "zero dependencies" still holds for the Python side.
Without it, only the Fitbod path runs. `fetch_healthkit_workouts()` is the
single seam where an alternative source (Garmin, Strava, mock data) slots in.

Sample output:

```text
$ python pipeline.py stats
Fitbod sets in database : 5,921
Apple Health workouts   : 31
Watermarks:
  last_fitbod_ingest       2026-10-13T13:58:13+00:00
  last_healthkit_ingest    2026-10-14T10:59:29.171470+00:00
```

## Design notes

- **SQLite, not Postgres** — one file, zero ops. Postgres when (if) this
  becomes multi-user.
- **Watermarks** in `ingestion_state` remember the high-water mark, so the
  backstop only ever asks for new data.
- Functions are import-safe (`if __name__ == "__main__"` guard), so a
  FastAPI backend could import the ingest functions directly later.
