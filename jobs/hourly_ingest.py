# Databricks notebook source
# MAGIC %md
# MAGIC # EIA hourly ingest
# MAGIC
# MAGIC Runs every hour as a scheduled Databricks job.
# MAGIC
# MAGIC 1. **Fetch** a recent window of hourly grid data from the EIA API v2 (`region-data` and `fuel-type-data`).
# MAGIC 2. **Land** the raw API responses in the Volume (`/Volumes/workspace/eia/raw/api/...`) for replay and debugging.
# MAGIC 3. **MERGE** the rows into the Delta tables (upsert on the natural key, so re-fetching overlap is harmless).
# MAGIC 4. **Log** the run to `workspace.eia.ingest_runs` (the continuity ledger used to prove the pipeline is alive).

# COMMAND ----------

dbutils.widgets.text("lag_window_hours", "36", "Re-fetch this far behind the last good run (EIA generation lags up to ~30h)")
dbutils.widgets.text("max_backfill_days", "14", "Never reach back further than this, even after a long outage")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Fetch + land only; no MERGE, no ledger row")
LAG_WINDOW_HOURS = int(dbutils.widgets.get("lag_window_hours"))
MAX_BACKFILL_DAYS = int(dbutils.widgets.get("max_backfill_days"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

spark.conf.set("spark.sql.session.timeZone", "UTC")  # every timestamp in this pipeline is UTC

# COMMAND ----------

import datetime as dt
import json
import time
import uuid

import requests
from pyspark.sql import functions as F

API_KEY = dbutils.secrets.get("eia", "api_key")  # never printed; Databricks shows [REDACTED] if it is
BASE = "https://api.eia.gov/v2/electricity/rto"
RAW_DIR = "/Volumes/workspace/eia/raw/api"
PAGE = 5000  # EIA API v2 returns at most 5,000 rows per request

RUN_ID = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
STARTED_AT = dt.datetime.now(dt.timezone.utc)

# COMMAND ----------

def get_page(route: str, start: str, offset: int) -> dict:
    """One page of results, with retries. The key goes in a header, not the URL, so it never lands in logs."""
    params = {
        "frequency": "hourly",
        "data[0]": "value",
        "start": start,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": offset,
        "length": PAGE,
    }
    last_error = None
    for attempt in range(4):  # EIA is often slow; back off 5s, 10s, 20s
        try:
            r = requests.get(f"{BASE}/{route}/data/", params=params,
                             headers={"X-Api-Key": API_KEY}, timeout=120)
            if r.status_code == 200:
                return r.json()["response"]
            last_error = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
        time.sleep(5 * 2 ** attempt)
    raise RuntimeError(f"EIA {route} offset={offset} failed after retries: {last_error}")


def fetch_all(route: str, start: str) -> list[dict]:
    """Page through everything from `start` onward."""
    rows, offset = [], 0
    while True:
        resp = get_page(route, start, offset)
        batch = resp.get("data", [])
        rows.extend(batch)
        offset += len(batch)
        if not batch or offset >= int(resp.get("total", 0)):
            return rows


def land_raw(route: str, rows: list[dict]) -> str:
    """Save exactly what the API returned (bronze layer): lets us replay a run or debug a bad value later."""
    path = f"{RAW_DIR}/{route}/{RUN_ID}.json"
    dbutils.fs.mkdirs(f"{RAW_DIR}/{route}")
    with open(path, "w") as f:
        json.dump(rows, f)
    return path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch window: backfill from where we left off
# MAGIC EIA posts demand ~1h late, but generation arrives once a day per operator, 15-30h late.
# MAGIC So each run re-fetches `lag_window_hours` behind the **last successful run** (not behind "now"):
# MAGIC - normal hour: last good run was 1h ago, so the window is ~37h
# MAGIC - after an outage: the window stretches back to cover the whole gap (capped at `max_backfill_days`)
# MAGIC - very first run: no runs yet, so it starts from the end of the seed (bridges the seed/trickle gap)
# MAGIC
# MAGIC Re-fetching overlap is safe because the MERGE below is idempotent.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.eia.ingest_runs (
  run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, window_start STRING, status STRING,
  region_fetched BIGINT, fuel_fetched BIGINT,
  region_inserted BIGINT, region_updated BIGINT, fuel_inserted BIGINT, fuel_updated BIGINT,
  error STRING)
""")

window_start = spark.sql(f"""
  WITH covered AS (
    SELECT coalesce(
             (SELECT max(started_at) FROM workspace.eia.ingest_runs WHERE status = 'ok'),
             (SELECT max(period)     FROM workspace.eia.fuel_type_data)        -- first run: end of seed
           ) AS covered_until
  )
  SELECT date_format(
           greatest(covered_until        - INTERVAL {LAG_WINDOW_HOURS} HOURS,
                    current_timestamp()  - INTERVAL {MAX_BACKFILL_DAYS} DAYS),
           "yyyy-MM-dd'T'HH")
  FROM covered
""").first()[0]
print("fetching from", window_start, "UTC")

# COMMAND ----------

status, error = "ok", None
counts = {}
try:
    region_raw = fetch_all("region-data", window_start)
    fuel_raw = fetch_all("fuel-type-data", window_start)
    land_raw("region-data", region_raw)
    land_raw("fuel-type-data", fuel_raw)
    counts["region_fetched"], counts["fuel_fetched"] = len(region_raw), len(fuel_raw)
    print(f"fetched {len(region_raw):,} region rows, {len(fuel_raw):,} fuel rows")
except Exception as e:
    status, error = "fetch_failed", str(e)[:1000]
    print("FETCH FAILED:", error)

# COMMAND ----------

def to_rows(raw: list[dict], kind_col: str):
    """API JSON -> same columns as the seed tables. Drops null values (same rule as the seed)."""
    df = spark.createDataFrame(
        [(r["period"], r["respondent"], r[kind_col], r.get("value")) for r in raw],
        f"period string, respondent string, {kind_col} string, value string",
    )
    return (df
            .withColumn("period", F.to_timestamp("period", "yyyy-MM-dd'T'HH"))   # UTC hour
            .withColumn("value", F.col("value").cast("double"))
            .where(F.col("value").isNotNull())
            # MERGE fails if two source rows hit the same target row, so make the source unique per key
            .dropDuplicates(["period", "respondent", kind_col]))

if status == "ok":
    to_rows(region_raw, "type").createOrReplaceTempView("region_new")
    to_rows(fuel_raw, "fueltype").createOrReplaceTempView("fuel_new")

if DRY_RUN:
    summary = {
        "status": status, "error": error, "window_start": window_start, **counts,
        "region_new": spark.sql("SELECT count(*) n, min(period) lo, max(period) hi, count(DISTINCT type) types FROM region_new").collect()[0].asDict() if status == "ok" else None,
        "fuel_new": spark.sql("SELECT count(*) n, min(period) lo, max(period) hi, collect_set(fueltype) fuels FROM fuel_new").collect()[0].asDict() if status == "ok" else None,
    }
    dbutils.notebook.exit(json.dumps(summary, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into the Delta tables
# MAGIC Source views: `region_new` (period, respondent, type, value) and `fuel_new` (period, respondent, fueltype, value).

# COMMAND ----------

# Upsert on the natural key. Only rewrite a row when EIA actually revised its value:
# Delta rewrites whole files for every updated row, so "update everything we re-fetched" would
# rewrite ~10k unchanged rows every hour for nothing.
REGION_MERGE = """
MERGE INTO workspace.eia.region_data AS t
USING region_new AS s
ON  t.period = s.period AND t.respondent = s.respondent AND t.type = s.type
WHEN MATCHED AND t.value <> s.value THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT (period, respondent, type, value)
                      VALUES (s.period, s.respondent, s.type, s.value)
"""

FUEL_MERGE = """
MERGE INTO workspace.eia.fuel_type_data AS t
USING fuel_new AS s
ON  t.period = s.period AND t.respondent = s.respondent AND t.fueltype = s.fueltype
WHEN MATCHED AND t.value <> s.value THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT (period, respondent, fueltype, value)
                      VALUES (s.period, s.respondent, s.fueltype, s.value)
"""

def run_merge(sql: str) -> tuple[int, int]:
    """Returns (inserted, updated) from Delta's MERGE metrics."""
    m = spark.sql(sql).collect()[0].asDict()
    return int(m.get("num_inserted_rows", 0)), int(m.get("num_updated_rows", 0))

if status == "ok":
    try:
        counts["region_inserted"], counts["region_updated"] = run_merge(REGION_MERGE)
        counts["fuel_inserted"], counts["fuel_updated"] = run_merge(FUEL_MERGE)
        print(counts)
    except Exception as e:
        status, error = "merge_failed", str(e)[:1000]
        print("MERGE FAILED:", error)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the run (continuity ledger)
# MAGIC One row per run, success or failure. This is how we prove the pipeline ran unattended,
# MAGIC and where to look first if it dies.

# COMMAND ----------

spark.createDataFrame([(
    RUN_ID, STARTED_AT, dt.datetime.now(dt.timezone.utc), window_start, status,
    counts.get("region_fetched"), counts.get("fuel_fetched"),
    counts.get("region_inserted"), counts.get("region_updated"),
    counts.get("fuel_inserted"), counts.get("fuel_updated"),
    error,
)], spark.table("workspace.eia.ingest_runs").schema).write.mode("append").saveAsTable("workspace.eia.ingest_runs")

# Fail the job loudly (so the scheduler marks it red and emails) AFTER the failure is logged.
if status != "ok":
    raise RuntimeError(f"run {RUN_ID} {status}: {error}")
print(f"run {RUN_ID} ok")
