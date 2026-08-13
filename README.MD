Wind Turbine Data Pipeline (PoC)

PySpark pipeline that cleans raw turbine CSVs, calculates daily stats, flags anomalies, and stores it all in SQLite. Built as a proof-of-concept — see "what I'd change for production" at the bottom.

Quick start

Needs Python 3.10+ and Java 17.

bash
python -m venv env-wind-turbine-pipeline
source env-wind-turbine-pipeline/Scripts/activate
pip install pyspark pytest pyyaml pandas

# drop your data_group_N.csv files into data/, then:
pytest tests/ -v
python main.py

Results land in turbine_data.db.

What it does
Auto-discovers files in data/ matching data_group_*.csv — no hardcoded filenames
Flags missing/impossible readings (negative power, wind speed out of range), preserves the original bad values in a quality_errors table, and separately imputes them (forward-fill) in the main dataset
Calculates min/max/avg power per turbine, per day
Flags anomalies — a turbine is anomalous if its daily average is 2+ std devs from what the other 14 turbines averaged that day
Writes everything to SQLite
Key design calls

Cross-turbine anomaly comparison, not each turbine vs. its own history. The brief calls this a "wind farm" — doesn't say turbines share wind, but that's what a wind farm is. So a genuinely windy day shouldn't flag every turbine at once; only a turbine that's out of step with its neighbors that day should. This is my inference, not something the brief states outright.

Small catch: with too few turbines in the comparison (tried 5), one extreme outlier inflates the group's own std dev enough that it can never cross the 2-std-dev line, no matter how extreme. Needed the real 15-turbine group. Test for it's in test_anomalies.py.

Idempotent storage, not full-replace. Each table has a real primary key (timestamp + turbine_id, or turbine_id + day), writes use INSERT OR IGNORE. Run the pipeline twice on the same data, database ends up identical — no dupes. Trade-off: if I fix a bug and re-run, corrected values for already-processed rows get silently ignored, not applied. Upsert would fix that; went with insert-only since accidental duplication felt like the bigger everyday risk for a PoC.

A real bug I hit

The data has a literal 2022-03-13 02:00:00 row — the exact date the US DST "springs forward," so 2am doesn't exist that day locally. Spark was parsing timestamps in my machine's local timezone, silently collided that row with 3am, lost 5 rows with zero error. Fixed by forcing the Spark session to UTC.

Layout
config/pipeline_config.yaml   thresholds, file pattern — nothing hardcoded in .py
src/
  ingestion/ingest.py         discover + read CSVs
  processing/
    clean.py                  flag / split / impute
    stats.py                  daily min/max/avg
    anomalies.py              cross-turbine z-score
  storage/store.py            idempotent SQLite writes
tests/                        one file per module + one integration test
main.py                       wires it together
Other assumptions
Missing values and out-of-range values treated the same way for cleaning
Imputation = forward-fill (last known good reading) — simple, not the only valid choice
Cleaning thresholds (10.0 MW, 30 m/s max) are placeholders with headroom above the sample data, not real turbine specs
Sample data has zero bad values — tested cleaning logic with injected fake bad data instead
What I'd change for production
SQLite → real warehouse/Postgres, once concurrent writes matter
File-driven → event-driven (S3 → Lambda), instead of running main.py by hand
Add incremental/CDC processing — right now every run reprocesses the whole file from scratch, fine for a monthly snapshot, not fine for real daily appends
A data quality alert if quality_errors spikes, instead of it sitting quietly in a table