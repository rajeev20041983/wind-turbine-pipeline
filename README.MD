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
Writes everything to SQLite — 4 tables: cleaned_readings, quality_errors, daily_stats, anomalies
Current solution diagram
1. data_group_*.csv (3 files) -> read_group_file() -> one Spark DataFrame
2. flag_quality_issues() tags every row good or bad
3. BRANCH A: split_errors() -> quality_errors table (original bad value, audit trail)
4. BRANCH B: impute_bad_values() -> forward-fills bad rows, keeps good rows as-is
5. (continuing branch B) calculate_daily_stats() -> min/max/avg per turbine per day
6. detect_anomalies() -> cross-turbine z-score
7. write_to_sqlite() writes 4 tables: cleaned_readings, quality_errors, daily_stats, anomalies

Bad rows get written twice, in two different forms: once untouched (quality_errors, for audit), once imputed (flowing into cleaned_readings, then daily_stats and anomalies).

Key design calls

Cross-turbine anomaly comparison, not each turbine vs. its own history. The brief calls this a "wind farm" — doesn't say turbines share wind, but that's what a wind farm is. So a genuinely windy day shouldn't flag every turbine at once; only a turbine out of step with its neighbors that day should. This is my inference, not something the brief states outright.

Small catch: with too few turbines in the comparison (tried 5), one extreme outlier inflates the group's own std dev enough that it can never cross the 2-std-dev line, no matter how extreme. Needed the real 15-turbine group. Test for it's in test_anomalies.py.

Idempotent storage, not full-replace. Each table has a real primary key (timestamp + turbine_id, or turbine_id + day), writes use INSERT OR IGNORE. Run the pipeline twice on the same data, database ends up identical — no dupes. Trade-off: if I fix a bug and re-run, corrected values for already-processed rows get silently ignored, not applied. Upsert would fix that; went with insert-only since accidental duplication felt like the bigger everyday risk for a PoC.

A real bug I hit

The data has a literal 2022-03-13 02:00:00 row — the exact date the US DST "springs forward," so 2am doesn't exist that day locally. Spark was parsing timestamps in my machine's local timezone, silently collided that row with 3am, lost 5 rows with zero error. Fixed by forcing the Spark session to UTC.

Layout
File	What it does
config/pipeline_config.yaml	thresholds, file pattern — nothing hardcoded in .py
src/ingestion/ingest.py	discover + read CSVs
src/processing/clean.py	flag / split / impute
src/processing/stats.py	daily min/max/avg
src/processing/anomalies.py	cross-turbine z-score
src/storage/store.py	idempotent SQLite writes
tests/	one file per module + one integration test
main.py	wires it all together
Other assumptions
Missing values and out-of-range values treated the same way for cleaning
Imputation = forward-fill (last known good reading) — simple, not the only valid choice
Cleaning thresholds (10.0 MW, 30 m/s max) are placeholders with headroom above the sample data, not real turbine specs
Sample data has zero bad values — tested cleaning logic with injected fake bad data instead
What I'd change for production

(mapped to the stack I actually work with day-to-day, not generic advice)

Backfill safety: keep an immutable raw-archive S3 bucket, separate from the working bucket that triggers processing — if cleaning logic or anomaly thresholds change later, the original untouched files are still there to reprocess from, rather than relying on whatever's left in the processing path.
Storage: SQLite → Apache Iceberg tables on S3, partitioned by turbine_id and day — atomic commits for safe re-runs instead of INSERT OR IGNORE, schema evolution if new sensor fields get added later, and time travel to compare a re-run against what was there before.
Orchestration: file-driven discovery → EventBridge + Lambda, triggered by the file actually landing in S3, instead of someone running main.py by hand.
Multi-stage pipeline: right now, one Python function calls the next in a straight line — if any step fails, the whole run stops and has to restart from the very beginning, even the parts that already worked. I'd wire ingest → clean → stats → anomalies as a Step Functions state machine instead: each step becomes its own tracked stage, so a single failed step can retry on its own without redoing the earlier ones, and there's a visual diagram showing which stage passed or failed on every run.
Scale: local PySpark (master("local[*]")) → EMR on EKS, once turbine count or history genuinely outgrows one machine.
Schema management: register the turbine reading schema in Glue Schema Registry, so a sensor vendor adding/renaming a field is a schema evolution event, not a pipeline crash.
Querying the output: Starburst/Trino on top of the Iceberg tables, instead of connecting to a SQLite file directly.
Infrastructure: all of the above provisioned via Terraform, not manual setup.
Alerting: a data quality alert if quality_errors spikes, instead of it sitting quietly in a table.
Production solution diagram
1. Turbine sensor data -> S3 raw-archive bucket (immutable, backfill source)
2. Same files also land in S3 working bucket (triggers processing)
3. EventBridge fires when a new file lands in the working bucket
4. Lambda (thin trigger) starts a Step Functions execution
5. Step Functions: ingest -> clean -> stats -> anomalies (each stage retries independently)
6. Output written to Apache Iceberg tables on S3, partitioned by turbine_id + day
   - cleaned_readings, quality_errors, daily_stats, anomalies
   - schema tracked in Glue Schema Registry
7. Starburst/Trino queries the Iceberg tables
8. CloudWatch alerts if quality_errors spikes
9. All infrastructure provisioned via Terraform