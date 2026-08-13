This is a PySpark pipeline that takes raw wind turbine CSVs, cleans them,
calculates daily stats per turbine, flags anomalies, and stores everything
in SQLite. Built as a proof-of-concept, not production code - see the
"what I'd change for production" section at the bottom.

## Quick start

You'll need Python 3.10+ and Java 17 (Spark needs a JVM under the hood).

```bash
python -m venv env-wind-turbine-pipeline
source env-wind-turbine-pipeline/Scripts/activate   # Windows Git Bash
# .\env-wind-turbine-pipeline\Scripts\Activate.ps1   # Windows PowerShell

pip install pyspark pytest pyyaml pandas

# drop your 3 data_group_N.csv files into data/, then:
pytest tests/ -v
python main.py
```

Check `turbine_data.db` afterward - that's where everything lands.

## What it actually does

1. Reads all 3 CSVs (5 turbines each, 15 total)
2. Flags rows with missing or physically impossible readings (negative
   power, wind speed outside a realistic range) - the bad values get
   preserved in their own table (`quality_errors`), not thrown away.
   Separately, the main dataset gets those values imputed (forward-filled
   from the turbine's last known good reading) so downstream stats have a
   complete grid to work with.
3. Calculates min/max/avg power per turbine, per day
4. Flags anomalies - a turbine is anomalous if its daily average is more
   than 2 standard deviations from what the other 14 turbines averaged
   that same day (see "why cross-turbine" below)
5. Writes cleaned data, error log, daily stats, and anomalies to SQLite

## Why cross-turbine, not "compare each turbine to its own history"

All 15 turbines sit on the same farm and see the same wind. If one turbine
compares itself only to its own past, a genuinely windy day makes every
turbine's output jump together - nothing looks "wrong," it's just weather.
Comparing turbines to each other on the same day cancels that out: if
everyone's up together, nobody gets flagged. If one turbine is way off
from its 14 neighbors under the same conditions, that's an actual signal
worth looking at - probably a fault, not weather.

One thing worth knowing if you dig into the anomaly code: with a small
group (tried it with just 5), a single extreme outlier can inflate the
group's own standard deviation enough that it never crosses the 2-std-dev
line, no matter how extreme it is. Needed the full 15-turbine group for
the z-score to actually work. Test for it is in
`tests/processing/test_anomalies.py`.

## A real bug I ran into: timestamps and daylight saving

The raw CSVs have a literal `2022-03-13 02:00:00` row for every turbine -
that's the exact date the US "springs forward," so 2am doesn't really
exist that day in a DST-observing local timezone. Spark was parsing
timestamps using my machine's local timezone by default, which silently
collided that row with the 3am row, losing 5 rows (one per turbine) with
no error, just a quietly wrong row count. Fixed by forcing the Spark
session to UTC (`spark.sql.session.timeZone`), so timestamps get read
literally instead of being reinterpreted against local DST rules.