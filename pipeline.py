#!/usr/bin/env python3
"""
================================================================================
TRAINING DATA PIPELINE
================================================================================

WHAT THIS IS
-----------
A small Python program that builds and maintains a personal training database.
It takes workout data from two sources -- Fitbod (weightlifting) and Apple
Health (runs / Apple Watch workouts) -- and merges them into a single SQLite
database. Everything downstream (post-workout analysis, the AI coach) reads
from this database instead of from the raw exports.

HOW IT MAPS TO THE FIGJAM DIAGRAM
----------------------------------
Each function below corresponds to a shape on the "Fitness App Data Pipeline"
board:

  Trapezoid "User shares CSV to Muse from Fitbod App"  ->  YOU. This is the
      manual trigger. Nothing in this file runs until a new WorkoutExport.csv
      shows up. (See TRIGGERS below.)
  Document "WorkoutExport.csv"                         ->  the file itself.
  Rectangle "Ingestion job diffs the CSV..."            ->  ingest_fitbod_csv()
  Parallelogram "User records running workouts..."      ->  your Apple Watch
      writing data into the Health app on your iPhone.
  Trapezoid "User pings Muse: 'Just completed a run'"  ->  YOU again. The
      manual trigger for the running side.
  Rectangle "Ingestion job pulls the completed..."      ->  ingest_workouts()
  Cylinder "Training database"                         ->  training.db, created
      by init_db(). Tables: fitbod_sets, workouts, ingestion_state.

TRIGGERS: CRON vs. PING (the two ways this code gets run)
----------------------------------------------------------
This program does not run itself. Something has to invoke it. There are two
patterns, and this pipeline uses both:

  1. USER-TRIGGERED ("your ping"): you do something, and that action causes
     the pipeline to run. Examples: you share a new Fitbod CSV into chat, or
     you message "just completed a run" after a workout. The trigger is an
     event -- your message. This is how the primary ingestion paths work,
     because only you know the moment the data is actually ready (e.g. you
     wait ~3 minutes post-run so the heart-rate-recovery dip is captured).

  2. SCHEDULE-TRIGGERED ("cron"): the computer runs the program on a clock,
     whether or not anything happened. `cron` is the classic Unix scheduler:
     you write a line like "run this every night at 9pm" and the operating
     system does it forever. This pipeline uses cron only as a BACKSTOP --
     a nightly safety net that catches any workout you forgot to ping about.
     It is idempotent (see GLOSSARY), so re-running it when there is nothing
     new is harmless: it just finds zero new rows and exits.

In product terms: user-triggered = "the app has an Analyze button";
schedule-triggered = "the app syncs every night". Good systems use both.

GLOSSARY (words used in the comments below)
-------------------------------------------
  NORMALIZED KEY .... A fingerprint computed from a row's meaningful fields
      after cleaning them up ("normalizing": rounding numbers, stripping
      whitespace, lowercasing names). Two exports of the SAME set can spell
      its numbers differently ("7.937875915123695" vs "7.94"); normalizing
      first means the fingerprint still matches. See make_set_key().
  DEDUPE ............ Removing duplicates: keeping only rows whose key is
      not already in the database.
  PRIMARY KEY ....... A database column guaranteed unique per row. We store
      the normalized key AS the primary key, so the database itself refuses
      duplicates -- dedupe becomes a one-word SQL feature (OR IGNORE).
  WATERMARK ......... A remembered "high-water mark": e.g. the newest workout
      date we have ingested. Next run only asks for data NEWER than the mark,
      so we never re-scan all of history.
  IDEMPOTENT ........ Safe to run twice. Running ingest on the same file ten
      times inserts each set exactly once, because duplicates are ignored.

USAGE
-----
  python pipeline.py init                       # create training.db + tables
  python pipeline.py fitbod path/to/WorkoutExport.csv
  python pipeline.py health                     # pull recent Apple Health workouts
  python pipeline.py health --days 7            # backstop: last 7 days
  python pipeline.py stats                      # row counts + watermarks
"""

import argparse
import csv
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Everything the program needs to know about its environment lives here, at
# the top, so there are no magic strings buried in the logic below.

# The database file. SQLite stores the ENTIRE database -- schema, tables,
# indexes, data -- in this one ordinary file. No server to install, no
# password, no network. You can copy it, back it up, or email it like any
# file. (Postgres, the alternative on the FigJam diamond, is a separate
# server program -- more powerful for many simultaneous users, but overkill
# until this becomes a multi-user product.)
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "training.db"

# The command-line tool that reads Apple Health data synced from the iPhone.
# We call it as a subprocess (see fetch_healthkit_workouts): this Python
# program asks the operating system to run that tool, captures what it
# prints, and parses the JSON. This keeps all Apple-specific plumbing in one
# place instead of scattered through the code.
HEALTH_CLI = "/opt/hatch/bin/health-cli"

# --- Fitbod cardio blocklist ---
# SOURCE-DOMAIN RULE: Fitbod is the strength source; Apple Health is the
# cardio source. The cardio rows in a Fitbod export (running, walking,
# treadmill, stair stepper) are NOT workouts logged in Fitbod -- they are
# Apple Health workouts that Fitbod synced in and re-exported. Importing
# them here would count the same activity twice, once per source.
# Matching is exact and case-insensitive on the exercise name: a substring
# match would be wrong ("Crunches" contains "run" but is core work and
# stays). Timed strength/core work such as planks has a duration but is not
# cardio, so it stays too.
FITBOD_CARDIO_BLOCKLIST = frozenset({
    "walking",
    "walking - treadmill",
    "running",
    "stair stepper",
})


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

def get_connection():
    """Open (or create) the SQLite database file and return the connection.

    A "connection" is your program's open channel to the database file.
    sqlite3.connect() creates the file if it does not exist yet -- the
    tables themselves are created separately by init_db().
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # Return rows as dict-like objects (row["exercise"]) instead of plain
    # tuples (row[1]). Costs nothing, reads much better.
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the three tables that make up the training database.

    Called once via `python pipeline.py init`. Uses CREATE TABLE IF NOT
    EXISTS so it is safe to run again -- it will simply leave existing
    tables (and their data) untouched.
    """
    conn = get_connection()
    # A "cursor" is the object you use to send SQL commands through a
    # connection. Think of the connection as the phone line and the cursor
    # as the conversation happening over it.
    cur = conn.cursor()

    # --- fitbod_sets: one row per weightlifting set, from the CSV export. ---
    # The PRIMARY KEY is the normalized fingerprint PLUS an occurrence index
    # ("<fingerprint>#<n>"), where n counts which time this fingerprint
    # appears within its workout, in file order. The index is what keeps two
    # identical sets in one workout (e.g. 3x10 at the same weight) as two
    # distinct rows -- without it, the database would collapse them and
    # silently delete real training volume (this happened: 3,662 sets were
    # lost to exactly that bug, caught in the Oct 16, 2026 audit).
    # Because re-ingesting the same export reproduces identical keys, the
    # database still refuses true re-imports -- dedupe becomes a one-word
    # SQL feature (OR IGNORE), now at the correct granularity.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fitbod_sets (
            set_key     TEXT PRIMARY KEY,  -- fingerprint + "#occurrence_index"
            workout_date TEXT NOT NULL,    -- ISO timestamp of when it happened
            exercise    TEXT NOT NULL,
            reps        REAL,
            weight_kg   REAL,
            duration_s  REAL,
            distance_m  REAL,
            is_warmup   INTEGER NOT NULL DEFAULT 0,  -- 1 = warmup set, else 0
            ingested_at TEXT NOT NULL      -- when THIS program stored the row
        )
    """)

    # --- workouts: one row per Apple Health workout (runs, etc.). ---
    # Here dedupe is even simpler: Apple assigns every workout a globally
    # unique id (looks like "healthkit_84E66E17-..."). Same workout synced
    # twice has the same id, so the primary key rejects the duplicate.
    # No fingerprinting needed -- the source already did it for us.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS workouts (
            healthkit_id   TEXT PRIMARY KEY,  -- Apple's unique workout id
            workout_type   TEXT,               -- e.g. "Running", "Walking"
            start_datetime TEXT NOT NULL,
            end_datetime   TEXT,
            timezone       TEXT,
            distance_m     REAL,
            duration_sec   REAL,
            avg_hr_bpm     REAL,
            max_hr_bpm     REAL,               -- includes the post-workout window
            energy_kcal    REAL,
            avg_speed_mps  REAL,
            max_speed_mps  REAL,
            elevation_gain_m REAL,
            is_indoor      INTEGER,            -- 1 = indoor, 0 = outdoor
            ingested_at    TEXT NOT NULL
        )
    """)

    # --- ingestion_state: the pipeline's memory. ---
    # A tiny key/value table holding watermarks (see GLOSSARY): e.g.
    # ("last_fitbod_ingest" -> "2026-10-14T06:19:00+00:00"). Lets each run
    # know where the previous run left off, so the nightly backstop only
    # ever asks Apple Health for genuinely new data.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_state (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # commit() makes the changes permanent. Until you commit, everything
    # happens in a private scratch space -- if the program crashed mid-way,
    # the database would be untouched. This all-or-nothing property is
    # called a TRANSACTION, and it is what keeps the DB from ever holding
    # half an import.
    conn.commit()
    conn.close()
    print(f"Database ready at {DB_PATH}")


def get_state(key):
    """Read a watermark from ingestion_state. Returns None if never set."""
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM ingestion_state WHERE key = ?", (key,)
    ).fetchone()
    conn.close()
    # The "?" is a query PARAMETER: the database driver inserts the value
    # safely, so a weird key can never break the SQL or inject commands.
    # Never build SQL with f-strings -- always use parameters.
    return row["value"] if row else None


def set_state(key, value):
    """Write (or overwrite) a watermark in ingestion_state."""
    conn = get_connection()
    # INSERT ... ON CONFLICT(key) DO UPDATE = "upsert": insert the row, but
    # if the key already exists, just update its value instead of failing.
    conn.execute(
        """INSERT INTO ingestion_state (key, value) VALUES (?, ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
        (key, value),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# THE NORMALIZED DEDUPE KEY (Fitbod)
# ---------------------------------------------------------------------------
# THE PROBLEM, IN PLAIN ENGLISH:
#   Every Fitbod export contains your ENTIRE history -- all ~9,586 sets --
#   not just the new ones. So each import must answer: "which of these rows
#   do I already have?" Comparing raw text lines fails, because Fitbod
#   spells the same number differently between exports: one export writes
#   7.937875915123695, the next writes 7.94. Same set, different text.
#
# THE FIX, PART 1 -- A FINGERPRINT PER SET:
#   For each set, build a FINGERPRINT from the fields that identify it --
#   date, exercise, reps, weight, duration, distance -- AFTER cleaning them:
#   strip stray spaces, round numbers to a sensible precision. Two exports
#   of the same set then produce the SAME fingerprint even if the raw text
#   differed. That cleaned-up fingerprint is the "normalized key".
#   "Normalized" = cleaned into a canonical form. "Key" = the thing we use
#   to recognize the row. "Dedupe" = we keep only keys we haven't seen.
#
# WHY THESE FIELDS, AND WHY THIS PRECISION:
#   date+exercise+reps+weight identifies a set in practice. Rounding weight
#   to 2 decimals absorbs Fitbod's float formatting wobble (10-decimal
#   noise) while still distinguishing 20kg from 20.5kg. Duration/distance
#   (1 decimal) are included so timed entries, which have 0 reps and
#   0 weight, still fingerprint uniquely.
#
# THE FIX, PART 2 -- WHY THE FINGERPRINT ALONE IS NOT ENOUGH:
#   (Lesson learned Oct 16, 2026, during the training-data audit.)
#   Fitbod's Date column is WORKOUT-level: every set in one workout shares
#   the same timestamp, to the second. So the fingerprint CANNOT tell apart
#   two identical sets performed in the same workout -- and "3x10 at the
#   same weight" is the most normal thing in lifting. The original code
#   treated same-fingerprint rows as duplicates and silently deleted 3,662
#   genuinely performed sets (46% of all strength volume). The audit caught
#   it by checking the export against the Fitbod app set-for-set: there were
#   never any true duplicates in the source at all.
#
#   The corrected scheme: the stored primary key is the fingerprint plus an
#   occurrence index -- "<fingerprint>#<n>" -- where n counts which time
#   this fingerprint appears within its workout, in file order. Identical
#   sets in one workout get distinct keys (#0, #1, #2...) and are all kept.
#   Re-ingesting the same export reproduces identical keys, so the database
#   itself still refuses re-imports (OR IGNORE): the import is idempotent,
#   and a new export's genuinely new workouts get fresh keys and insert
#   normally. There is deliberately NO other dedupe: six years of exports
#   contain zero true duplicates, and the previous attempt at catching them
#   is what deleted nearly half the history. We err on the side of keeping
#   real training data.
#
# KNOWN LIMITATION (documented, not hidden):
#   If the source ever contained a literally duplicated workout (the same
#   timestamp with the same sets listed twice), both copies would be
#   ingested. This has never occurred in any export to date. If you EDIT an
#   old workout in Fitbod (add/remove a set) and re-export, occurrence
#   indices after the edit point can shift, and the changed sets may insert
#   as new rows. Old workouts are never edited in practice, so both are
#   accepted risks -- far preferable to the alternative, which silently
#   deleted 46% of performed sets.

def make_set_key(date_str, exercise, reps, weight_kg, duration_s, distance_m):
    """Build the normalized fingerprint identifying one Fitbod set.

    Args are the raw CSV strings. Returns a single string like:
        "2026-10-13T13:58:13+00:00|walking|0|0.00|314.1|441.5"

    NOTE: this fingerprint alone is NOT unique per performed set -- two
    identical sets in one workout share it (see the comment block above).
    ingest_fitbod_csv() appends "#<occurrence_index>" to make the stored
    primary key unique per set within its workout.
    """
    # Parse the CSV timestamp ("2026-10-13 13:58:13 +0000") into a real
    # datetime object, then re-emit it in ISO format. This canonicalizes
    # any formatting differences in how the date is written.
    dt = datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S %z")
    canonical_date = dt.isoformat()

    # Exercise names get lowercased/stripped so "Bench Press" and
    # "bench press " fingerprint identically.
    canonical_exercise = exercise.strip().lower()

    # Helper: turn messy numeric strings (" 0", "", "7.937875915123695")
    # into rounded floats. Empty/missing becomes 0.0.
    def num(s, places):
        s = (s or "").strip()
        return round(float(s) if s else 0.0, places)

    parts = [
        canonical_date,
        canonical_exercise,
        str(num(reps, 0)),          # reps are whole numbers
        f"{num(weight_kg, 2):.2f}",   # 2 decimals: absorbs float wobble
        f"{num(duration_s, 1):.1f}",
        f"{num(distance_m, 1):.1f}",
    ]
    # "|" separates fields so "a|bc" can never collide with "ab|c".
    return "|".join(parts)


# ---------------------------------------------------------------------------
# FITBOD INGESTION  (FigJam rectangle: "Ingestion job diffs the CSV...")
# ---------------------------------------------------------------------------

def ingest_fitbod_csv(csv_path):
    """Import a Fitbod WorkoutExport.csv, appending only genuinely new sets.

    HOW THE DIFF WORKS (see the comment block above make_set_key for the
    full story):
      1. Read every row of the CSV.
      2. Skip cardio-type rows (FITBOD_CARDIO_BLOCKLIST) -- those are echoed
         Apple Health workouts, not Fitbod strength sets. Cardio comes from
         the Apple Health path instead.
      3. Group the remaining rows into workouts by their (workout-level)
         timestamp, preserving file order within each group.
      4. Fingerprint each set, then append "#<occurrence_index>" -- which
         time this fingerprint appears within THIS workout, in file order --
         to form the primary key. INSERT OR IGNORE each set: identical sets
         in one workout get distinct keys and are all kept; re-ingesting the
         same export reproduces identical keys, so the database itself skips
         them. There is no separate "compare" step; the primary-key
         constraint does the diffing for us. Elegant and fast.
      5. Count how many rows were actually new, and record a watermark.

    Because skipped rows are simply ignored, this function is IDEMPOTENT:
    run it on the same file ten times and the database ends up identical.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        print(f"ERROR: file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    conn = get_connection()
    cur = conn.cursor()

    # total_changes counts every INSERT/UPDATE/DELETE on this connection.
    # Snapshot it now; the difference afterwards = rows actually added.
    # (We can't trust cursor.rowcount with executemany -- it is unreliable
    # in some Python versions -- so we measure the connection instead.)
    before = conn.total_changes

    rows_seen = 0
    cardio_skipped = 0
    newest_date = None

    # Group rows by workout timestamp, preserving file order within each
    # group. Dicts remember insertion order; each group's list preserves
    # the row order within the workout -- the occurrence index depends on
    # that order being stable across exports (it is: newest-first, always).
    workouts = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        # utf-8-sig: Fitbod's export may start with a BOM (invisible byte
        # order mark). This encoding strips it so the first header isn't
        # read as "\ufeffDate".
        reader = csv.DictReader(f)
        for row in reader:
            rows_seen += 1
            date_str = (row.get("Date") or "").strip()
            if not date_str:
                continue  # skip blank lines

            exercise = (row.get("Exercise") or "").strip()

            # Cardio blocklist (see FITBOD_CARDIO_BLOCKLIST): echoed Apple
            # Health workouts, not strength sets. Counted separately so the
            # summary stays honest.
            if exercise.lower() in FITBOD_CARDIO_BLOCKLIST:
                cardio_skipped += 1
                continue

            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
            canonical_date = dt.isoformat()
            if newest_date is None or dt > newest_date:
                newest_date = dt

            fingerprint = make_set_key(
                date_str,
                exercise,
                row.get("Reps", ""),
                row.get("Weight(kg)", ""),
                row.get("Duration(s)", ""),
                row.get("Distance(m)", ""),
            )
            is_warmup = 1 if (row.get("isWarmup") or "").strip().lower() == "true" else 0
            workouts.setdefault(canonical_date, []).append({
                "fingerprint": fingerprint,
                "workout_date": canonical_date,
                "exercise": exercise,
                "reps": float((row.get("Reps") or "0").strip() or 0),
                "weight_kg": float((row.get("Weight(kg)") or "0").strip() or 0),
                "duration_s": float((row.get("Duration(s)") or "0").strip() or 0),
                "distance_m": float((row.get("Distance(m)") or "0").strip() or 0),
                "is_warmup": is_warmup,
            })

    # Flatten the groups back into one batch, assigning occurrence indices.
    # The 2nd identical set in a workout gets "#1", the 3rd "#2" -- distinct
    # keys, all kept. (A second export of the same workout produces the
    # same indices, so OR IGNORE skips them.)
    now_iso = datetime.now(timezone.utc).isoformat()
    batch = []
    for sets in workouts.values():
        occurrence = {}
        for s in sets:
            n = occurrence.get(s["fingerprint"], 0)
            occurrence[s["fingerprint"]] = n + 1
            batch.append((
                f"{s['fingerprint']}#{n}",
                s["workout_date"],
                s["exercise"],
                s["reps"],
                s["weight_kg"],
                s["duration_s"],
                s["distance_m"],
                s["is_warmup"],
                now_iso,
            ))

    # One executemany() sends all rows in a single round-trip instead of
    # thousands of individual INSERTs -- the difference between seconds and
    # minutes on a 9,586-row file. OR IGNORE = "skip rows whose primary key
    # already exists".
    cur.executemany(
        """INSERT OR IGNORE INTO fitbod_sets
           (set_key, workout_date, exercise, reps, weight_kg,
            duration_s, distance_m, is_warmup, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch,
    )
    conn.commit()

    inserted = conn.total_changes - before
    if newest_date:
        set_state("last_fitbod_ingest", newest_date.isoformat())
    conn.close()

    print(f"Rows in CSV : {rows_seen}")
    print(f"New sets    : {inserted}")
    print(f"Cardio      : {cardio_skipped} (blocklisted exercise, skipped)")
    return {"seen": rows_seen, "inserted": inserted,
            "cardio_skipped": cardio_skipped}


# ---------------------------------------------------------------------------
# APPLE HEALTH INGESTION  (FigJam: input -> ping -> "Ingestion job pulls...")
# ---------------------------------------------------------------------------

def fetch_healthkit_workouts(start_date, end_date):
    """Ask the health-cli tool for workouts in a date range; return records.

    This is a SUBPROCESS call: Python asks the operating system to run the
    health-cli program (the Apple Health connector), waits for it to finish,
    and captures what it printed. We then parse that output as JSON.

    Why a subprocess instead of a library? The Health data lives behind
    Muse's connector infrastructure (device sync, permissions). The CLI is
    the supported doorway in; shelling out keeps all of that complexity out
    of this file. The tradeoff: if the CLI changes its output format, this
    breaks -- acceptable for a personal pipeline, and the JSON parsing is
    defensive (.get with defaults) to soften that risk.
    """
    cmd = [
        HEALTH_CLI, "query", "sessions",
        "--provider", "healthkit",     # the CLI serves multiple providers;
                                      # this flag pins it to Apple Health
        "--category", "workout",      # one record per workout session
        "--start-date", start_date,   # YYYY-MM-DD; end defaults to today
        "--end-date", end_date,
    ]
    # capture_output=True grabs stdout/stderr instead of printing them.
    # check=True raises an exception if the tool exits with an error, so a
    # failed sync can never be mistaken for "zero workouts".
    # text=True gives us strings instead of raw bytes.
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout)
    # The CLI returns {"coverage": {...}, "records": [...]}. .get("records",
    # []) means "missing key -> empty list" instead of crashing.
    return payload.get("records", [])


def ingest_workouts(days=2):
    """Pull recent Apple Health workouts and store any that are new.

    Called in two situations (see TRIGGERS in the header):
      - After YOUR PING ("just completed a run"): days=2 is plenty, because
        we only need workouts since the last check. Fast and cheap.
      - By the NIGHTLY BACKSTOP (cron): pass --days 7 to sweep a wider net
        in case a ping was missed. The primary-key dedupe makes the wider
        net harmless -- already-stored workouts are skipped.

    Dedupe here needs no fingerprinting: each record carries Apple's own
    unique id (healthkit_id), stored as the primary key. Same workout seen
    twice -> same id -> OR IGNORE skips it.
    """
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=days)).isoformat()
    end = today.isoformat()

    records = fetch_healthkit_workouts(start, end)

    conn = get_connection()
    cur = conn.cursor()
    before = conn.total_changes

    for r in records:
        # .get() with a default of None: many fields are absent for some
        # workout types (a lifting session has no distance, an outdoor walk
        # has no max speed). NULL in the database honestly means "not
        # recorded", which beats storing a fake zero.
        cur.execute(
            """INSERT OR IGNORE INTO workouts
               (healthkit_id, workout_type, start_datetime, end_datetime,
                timezone, distance_m, duration_sec, avg_hr_bpm, max_hr_bpm,
                energy_kcal, avg_speed_mps, max_speed_mps, elevation_gain_m,
                is_indoor, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                r.get("id"),
                r.get("workout_type"),
                r.get("start_datetime"),
                r.get("end_datetime"),
                r.get("timezone"),
                r.get("distance_meters"),
                r.get("active_duration_sec"),
                r.get("hr_average_bpm"),
                r.get("hr_max_bpm"),
                r.get("energy_burned_kcal"),
                r.get("average_speed_mps"),
                r.get("max_speed_mps"),
                r.get("elevation_gain_meters"),
                # is_indoor may be True/False/None -> store 1/0/NULL
                (1 if r.get("is_indoor") is True else
                 0 if r.get("is_indoor") is False else None),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    conn.commit()
    inserted = conn.total_changes - before
    set_state("last_healthkit_ingest", datetime.now(timezone.utc).isoformat())
    conn.close()

    print(f"Workouts seen (last {days}d): {len(records)}")
    print(f"New workouts stored        : {inserted}")
    return {"seen": len(records), "inserted": inserted}


# ---------------------------------------------------------------------------
# STATS + COMMAND LINE
# ---------------------------------------------------------------------------

def show_stats():
    """Print a small summary: table counts and watermarks. For humans."""
    conn = get_connection()
    sets = conn.execute("SELECT COUNT(*) AS n FROM fitbod_sets").fetchone()["n"]
    workouts = conn.execute("SELECT COUNT(*) AS n FROM workouts").fetchone()["n"]
    print(f"Fitbod sets in database : {sets:,}")
    print(f"Apple Health workouts   : {workouts:,}")
    print("Watermarks:")
    for row in conn.execute("SELECT key, value FROM ingestion_state"):
        print(f"  {row['key']:22s} {row['value']}")
    conn.close()


def main():
    """Parse the command line and dispatch to the right function.

    argparse turns `python pipeline.py fitbod file.csv` into a tidy command
    structure with --help text, instead of hand-rolled sys.argv parsing.
    Each subcommand maps to one pipeline operation.
    """
    parser = argparse.ArgumentParser(
        description="Training data pipeline: ingest Fitbod + Apple Health into training.db"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Create the database and tables (safe to re-run)")

    p_fitbod = sub.add_parser("fitbod", help="Ingest a Fitbod WorkoutExport.csv")
    p_fitbod.add_argument("csv_path", help="Path to the exported CSV file")

    p_health = sub.add_parser("health", help="Ingest recent Apple Health workouts")
    p_health.add_argument("--days", type=int, default=2,
                          help="How many days back to pull (default: 2; use 7 for the nightly backstop)")

    sub.add_parser("stats", help="Show row counts and watermarks")

    args = parser.parse_args()

    if args.command == "init":
        init_db()
    elif args.command == "fitbod":
        ingest_fitbod_csv(args.csv_path)
    elif args.command == "health":
        ingest_workouts(days=args.days)
    elif args.command == "stats":
        show_stats()


if __name__ == "__main__":
    # This guard means: only run main() when the file is executed directly
    # (`python pipeline.py ...`), NOT when another file imports it. Standard
    # Python practice -- it lets future code reuse these functions as a
    # library (e.g. a FastAPI backend importing ingest_fitbod_csv).
    main()
