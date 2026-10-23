# Workout Data Pipeline

I've been training for marathons this year (Copenhagen May 2026; Philadelphia Nov 2026) and I have a ton of data available to analyze and help me make progress. Unfortunately, that data is siloed across various apps (eg Fitbod for weightlifting; Apple Watch/Health for running, heart rate, other vitals; Google Sheets for my training schedule; various LLMs for nutrition tracking). 

To get feedback from AI, I've been manually sending snapshots of workout data to different LLMs. It's cumbersome and unreliable.

I'm building this pipeline to bring that data together and keep my training data independent of any one AI model. The goal is to give analysis and coaching a consistent history to work from, while letting me change models without rebuilding the data pipeline.

Today, the Python ingestion layer imports Fitbod workout CSV exports and retrieves Apple Health workout records through `health-cli`, storing both in SQLite. It prevents repeat imports without dropping repeated sets I actually performed. Analysis and AI coaching are next. Nutrition tracking to follow. 

## Tools used

- **Fitbod:** the app I lift with. It exports my workout history as a CSV, which is one of the pipeline's two inputs.
- **Apple Health:** where my Apple Watch running workout and heart-rate data live on my phone. It's the other input.
- **health-cli:** a command-line tool that ships with the Muse platform. The Python code calls it to read workouts from Apple Health.
- **Python:** the language the ingestion code is written in. It's standard library only, so there's nothing extra to install.
- **SQLite:** the database. My whole training history lives in a single file.
- **Muse:** Meta's AI assistant. It wrote the code to my design, and I chat with it to run imports and get post-workout analysis.
- **Scriptable:** an iPhone app that runs small scripts. I use it for the macro-tracker widget on my home screen, a separate companion tool.

## Architecture

```text
Fitbod CSV exports ----+
                       |
Apple Health workouts -+
                       |
                       v
               Python ingestion
                       |
                       v
             training.db (SQLite)
                       |
                       v
            Analysis + AI coaching
```

Ingestion runs in two ways:

- **On demand:** Sharing a Fitbod export or sending a message after a run
  triggers an import.
- **Nightly backstop:** A scheduled run checks recent Apple Health workouts
  for anything missed.

Database keys prevent duplicate entries when the same records are imported
again.

## Workflow

I interact with the pipeline through Muse, an AI assistant: I share a Fitbod CSV or send a message after a run (which prompts Muse to pull Apple Health workout data), then receive post-workout analysis in the same conversation.

The Python ingestion code is separate from the chat interface. That lets me change how I interact with the pipeline without changing the underlying import logic.

I'm planning to try Muse as my day-to-day coach (October 2026). That would run separately from the pipeline in this repo. The plan: when I send Muse a Fitbod export, it adds new workouts to a private Google Sheet, writes a summary of each one, and proposes plan changes for me to approve. Muse would also save my Apple Health cardio workouts to the same Sheet. The Sheet is private and isn't part of this repo.

## Handling duplicate workout data

Fitbod exports my entire workout history each time, not just the latest session. That means importing a new file would add the same sets again unless the pipeline checks what's already there. Comparing the raw rows isn't enough: the same weight can appear as `7.937875915123695` in one export and `7.94` in another.

The first version cleaned up the values, rounded the numbers, and built a key from the date, exercise, reps, weight, duration, and distance. But identical sets within a workout could share all those values, so the pipeline dropped real sets as duplicates.

I caught this while reviewing an audit sheet of flagged rows. An October 2020 workout had two sets of Cable Rope Tricep Extensions at 10 reps and 42.5 lb. Fitbod showed both; the database had one.

Muse corrected the key by adding an occurrence number, so matching sets get separate keys. This preserves repeated sets while still preventing the same export from being imported twice. I confirmed the fix by reconciling the export against my actual in-app log in Fitbod.

Apple Health is simpler: its workout records already have an ID. Using that as the primary key means importing the same record again won't add another entry.

## Data model

The SQLite database (`training.db`) has three tables:
- `fitbod_sets`: One row per imported Fitbod set, including the exercise, reps, weight, and workout date. Its key combines the cleaned set values with an occurrence number.
- `workouts`: One row per Apple Health workout, with fields for distance, duration, heart rate, and other workout metrics. Apple's workout ID is used to prevent repeat imports of the same record.
- `ingestion_state`: Tracks import progress, including the latest Fitbod workout date imported and when Apple Health ingestion last ran.

Keeping Fitbod sets and Apple Health workouts separate preserves the detail from each source while making both available for analysis in one database.

The Sheet and the SQLite database wouldn't share data. The Sheet's Apple Health rows come from Muse's own connector, not from health-cli, so the two can hold different workouts. Fitbod's export has no set order or set ID, so the Sheet keeps one row per set and avoids duplicates by comparing how many identical sets each workout has. If a past workout changes in the export, it gets flagged for me to review. Fitbod also writes my strength workouts into Health as duration and calories only, so I leave those out of the cardio tab.

## Usage

The Python code uses only the standard library; no pip packages are required. That keeps the Python setup simple, with fewer dependencies to install, update, and maintain.

Apple Health imports also require an external tool (`health-cli`) available in my current setup. Without it, the Fitbod import still works.

### Quick demo

From the repository folder, try the included synthetic data on a fresh database:

```bash
python pipeline.py init
python pipeline.py fitbod sample-workout-export.csv
python pipeline.py stats
```

The first import adds all 13 sets. The sample includes repeated sets and numeric formatting differences on purpose; each row still represents a performed set. Importing the same file again adds 0 sets, so the database stays at 13. This demo doesn't require the Apple Health tool.

### Commands

Run these commands from the repository folder:

```bash
# Create the database and tables
python pipeline.py init

# Import a Fitbod export (replace the path with your file)
python pipeline.py fitbod path/to/WorkoutExport.csv

# Import recent Apple Health workouts
python pipeline.py health

# Check the past seven days of Apple Health workouts
python pipeline.py health --days 7

# Show record counts and import progress
python pipeline.py stats
```

Example from my personal training database after the rebuild:

```text
$ python pipeline.py stats
Fitbod sets in database : 7,940
Apple Health workouts   : 31
Watermarks:
  last_fitbod_ingest       2026-10-12T14:39:02+00:00
  last_healthkit_ingest    2026-10-14T10:59:29.171470+00:00
```

## Design notes

- **Start simple:** SQLite keeps the database in one file, without a separate database server to manage. That suits a personal project. I'd revisit the database choice if usage grows and the requirements change.
- **Track import progress:** `ingestion_state` records the latest Fitbod workout date imported and when Apple Health ingestion last ran. The Apple Health backstop checks a recent date range, with database keys preventing repeat entries.
- **Check against the source:** Reproducing a database result only proves that the code is consistent with itself. The duplicate-set bug became clear only after I compared the flagged rows with the Fitbod app.
- **Keep the import logic reusable:** The ingestion functions can be imported without running the command-line interface. That gives me a way to reuse them if I add a web app later.

## V1 trade-offs

For this version, I'm using Fitbod for strength training and Apple Health for cardio, rather than trying to match the same workout across both sources.

I record my runs on Apple Watch, not in Fitbod. But my Fitbod export still contains 1,600+ cardio rows across six years of history, including running, walking, treadmill, and stair-stepper workouts synced from Apple Health. Importing those alongside the Apple Health records would count the same activity twice.

I considered matching workouts by type and time to catch that overlap. For my current workflow, it's simpler to use one source for each type of exercise.

The Fitbod import excludes specific cardio exercise types. Timed core work, like planks, stays: an exercise having a duration doesn't make it cardio. I keep the original CSV exports locally, so excluded rows can be recovered if I revisit that decision.

Imports add records but don't sync edits or deletions to completed workouts. I've never gone back and edited a completed workout, so I'm leaving that as a known limit. If it ever matters, I can rebuild from a current full export.

## How I built it

I defined the problem and worked through the design with Muse, an AI assistant. I drew the pipeline diagram and decided which source to use for each type of training, what data points are important, how to handle duplicate records, and why SQLite was enough for this version.

Muse wrote the Python code and SQL schema. I reviewed the code and asked for changes where it didn't match the design. I also caught instances where it supplied data that wasn't supported by the source, so checking against the original records became part of the process.

## What's next

The next step is to make the combined training history useful in two ways:

- **Analysis:** I'm testing Muse writing a per-workout summary and comparison for lifting and running into the Sheet. I haven't decided whether that should also be saved in the database or replace it.
- **Coaching:** Use those summaries and my training history to suggest what to do next, including possible changes to upcoming workouts and when to prioritize recovery. Nutrition tracking and fueling feedback would follow.

I haven't settled on the best way to present the recommendations yet: through the assistant, a dashboard, or a regular recap.

The goal is to keep the training data separate from the AI model, so I can try different models for analysis and coaching without rebuilding the ingestion pipeline.

## Related tool: macro tracker

I report meals and snacks to Muse, which tracks daily totals against my calorie and nutrient targets. A Scriptable home-screen widget shows what I've eaten and what's left without opening the chat. Daily nutrition totals are also going into the same private Sheet, not the SQLite database. Nutrition still isn't part of the pipeline.
