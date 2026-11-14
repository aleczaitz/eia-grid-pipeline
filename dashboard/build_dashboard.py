"""Build (or rebuild) the Databricks AI/BI dashboard for the EIA pipeline.

    python3 dashboard/build_dashboard.py

1. Runs every dataset query against the SQL warehouse (fails fast on a bad query).
2. Writes the dashboard definition to dashboard/eia_pipeline.lvdash.json (versioned in git).
3. Creates the dashboard the first time, updates it after that, and publishes it.

Uses the `databricks` CLI profile `eia` for auth; no credentials live in this file.
"""

import json
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
WAREHOUSE_ID = "375f6c92eaa1f625"
PARENT_PATH = "/Users/aleczaitz@gmail.com/eia-pipeline"
DISPLAY_NAME = "EIA Grid Pipeline"
DEF_FILE = HERE / "eia_pipeline.lvdash.json"
ID_FILE = HERE / ".dashboard_id"

T = "workspace.eia"

# EIA's 16 fuel codes -> 8 readable groups. The *NB codes are solar/wind paired with a battery; BAT, PS (pumped
# hydro), OES and UES are storage, which goes negative while charging. Only on the API side of the seam.
FUEL = """CASE f.fueltype WHEN 'COL' THEN 'Coal'  WHEN 'NG' THEN 'Natural gas'  WHEN 'NUC' THEN 'Nuclear'
                       WHEN 'WAT' THEN 'Hydro' WHEN 'SUN' THEN 'Solar' WHEN 'SNB' THEN 'Solar'
                       WHEN 'WND' THEN 'Wind'  WHEN 'WNB' THEN 'Wind'
                       WHEN 'BAT' THEN 'Storage' WHEN 'PS' THEN 'Storage' WHEN 'OES' THEN 'Storage'
                       WHEN 'UES' THEN 'Storage'
                       ELSE 'Other' END"""

# Balancing authorities only: regions (US48, CAL, ...) are sums of these, so mixing the two double-counts.
BA_FUEL_7D = f"""
          SELECT f.period, f.respondent, r.respondent_name, {FUEL} AS fuel, f.value
          FROM {T}.fuel_type_data f JOIN {T}.respondents r USING (respondent)
          WHERE r.respondent_kind = 'balancing_authority' AND f.period >= current_timestamp() - INTERVAL 7 DAYS"""

DATASETS = {
    # ---- Is it alive? -------------------------------------------------------------------------
    # One row per (run, metric): fetched is ~constant (each run re-pulls its window), inserted/updated are
    # what the MERGE actually wrote. Fetched >> inserted is idempotency, visible; a gap is a missed run.
    "runs": f"""
        SELECT started_at, '1. Fetched (re-pulled window)' AS metric,
               coalesce(region_fetched, 0) + coalesce(fuel_fetched, 0) AS row_count
        FROM {T}.ingest_runs
        UNION ALL
        SELECT started_at, '2. Inserted (new hours)',
               coalesce(region_inserted, 0) + coalesce(fuel_inserted, 0)
        FROM {T}.ingest_runs
        UNION ALL
        SELECT started_at, '3. Updated (EIA revisions)',
               coalesce(region_updated, 0) + coalesce(fuel_updated, 0)
        FROM {T}.ingest_runs""",
    "health": f"""
        SELECT timestampdiff(MINUTE, max(started_at) FILTER (WHERE status = 'ok'), current_timestamp())
                                                                       AS minutes_since_last_ok_run,
               count_if(status = 'ok' AND started_at >= current_timestamp() - INTERVAL 24 HOURS)
                                                                       AS ok_runs_last_24h,
               count_if(status <> 'ok')                                AS failed_runs,
               (SELECT count(*) FROM {T}.region_data) + (SELECT count(*) FROM {T}.fuel_type_data)
                                                                       AS total_rows
        FROM {T}.ingest_runs""",
    "freshness": f"""
        SELECT series,
               round(timestampdiff(MINUTE, latest_hour, current_timestamp()) / 60.0, 1) AS hours_behind
        FROM (
          SELECT '1. Demand (D)'           AS series, max(period) AS latest_hour FROM {T}.region_data WHERE type = 'D'
          UNION ALL
          SELECT '2. Net generation (NG)',            max(period)                FROM {T}.region_data WHERE type = 'NG'
          UNION ALL
          SELECT '3. Generation by fuel',             max(period)                FROM {T}.fuel_type_data
        )""",
    # ---- Live vs. history -----------------------------------------------------------------------
    # The live trickle (last 7 days) joined to the seed (2019-2025) on calendar day + hour.
    "us_demand": f"""
        WITH recent AS (
          SELECT period, value FROM {T}.region_data
          WHERE respondent = 'US48' AND type = 'D' AND period >= current_timestamp() - INTERVAL 7 DAYS
            -- newest hour is partial (not every operator has reported yet), so it always dips: drop it
            AND period < (SELECT max(period) FROM {T}.region_data WHERE respondent = 'US48' AND type = 'D')
        ),
        hist AS (
          SELECT dayofyear(period) AS doy, hour(period) AS hr, avg(value) AS avg_mwh
          FROM {T}.region_data
          WHERE respondent = 'US48' AND type = 'D' AND year(period) BETWEEN 2019 AND 2025
          GROUP BY 1, 2
        )
        SELECT period, 'Last 7 days' AS series, value AS mwh FROM recent
        UNION ALL
        SELECT r.period, '2019-2025 average (same date & hour)', round(h.avg_mwh)
        FROM recent r JOIN hist h ON h.doy = dayofyear(r.period) AND h.hr = hour(r.period)""",
    # National solar for the most recent full week vs. the same week of every year.
    # Only balancing authorities (regions are sums of them), and only hours where >=90% of them reported.
    "solar_by_year": f"""
        WITH sun AS (
          SELECT f.period, sum(f.value) AS mwh, count(*) AS n
          FROM {T}.fuel_type_data f JOIN {T}.respondents r USING (respondent)
          WHERE r.respondent_kind = 'balancing_authority' AND f.fueltype = 'SUN'
          GROUP BY f.period
        ),
        complete AS (
          SELECT * FROM sun
          WHERE n >= 0.9 * (SELECT max(n) FROM sun WHERE period >= current_timestamp() - INTERVAL 30 DAYS)
        ),
        last_full_day AS (SELECT date(max(period)) - INTERVAL 1 DAY AS d FROM complete)
        SELECT cast(year(period) AS STRING) AS year, round(sum(mwh) / 1000) AS solar_gwh
        FROM complete, last_full_day
        WHERE dayofyear(period) BETWEEN dayofyear(d) - 6 AND dayofyear(d)
        GROUP BY 1""",
    # Whole country by fuel: add up every balancing authority, but only hours where >=90% have reported
    # (the newest ~day is partial because generation is posted one day at a time).
    "us_mix": f"""
        WITH ba AS ({BA_FUEL_7D}
        ),
        reporting AS (SELECT period, count(DISTINCT respondent) AS n FROM ba GROUP BY period),
        complete  AS (SELECT period FROM reporting WHERE n >= 0.9 * (SELECT max(n) FROM reporting))
        SELECT period, fuel, round(sum(value) / 1000, 1) AS gwh
        FROM ba JOIN complete USING (period)
        GROUP BY 1, 2""",
    # One operator at a time. The dataset holds all 68; the dashboard's dropdown filter adds the WHERE.
    "operator_mix": f"""
        SELECT period, concat(coalesce(nullif(respondent_name, ''), respondent), ' (', respondent, ')') AS operator,
               fuel, sum(value) AS mwh
        FROM ({BA_FUEL_7D}
        )
        GROUP BY 1, 2, 3""",
}
DEFAULT_OPERATOR = "PacifiCorp East (PACE)"


def cli(*args: str, body: dict | None = None) -> dict:
    cmd = ["databricks", *args, "-o", "json"]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"databricks {' '.join(args)} failed:\n{out.stderr}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def check_queries() -> None:
    for name, sql in DATASETS.items():
        res = cli("api", "post", "/api/2.0/sql/statements",
                  body={"warehouse_id": WAREHOUSE_ID, "wait_timeout": "50s", "statement": sql})
        state = res["status"]["state"]
        rows = res.get("manifest", {}).get("total_row_count")
        print(f"  {name:14s} {state:9s} rows={rows}")
        if state != "SUCCEEDED":
            sys.exit(f"query '{name}' failed: {res['status']}")


# ---------------------------------------------------------------------------------------------
# Widget helpers (AI/BI dashboard JSON format)

def field(name: str) -> dict:
    return {"name": name, "expression": f"`{name}`"}


def query(dataset: str, *names: str) -> list:
    return [{"name": "main_query",
             "query": {"datasetName": dataset, "fields": [field(n) for n in names], "disaggregated": True}}]


def text(name: str, lines: list[str], x, y, w, h) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(name: str, dataset: str, col: str, title: str, x, y, w=2, h=3) -> dict:
    return {"widget": {"name": name, "queries": query(dataset, col),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": col, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(name: str, kind: str, dataset: str, title: str, x_enc: dict, y_enc: dict, pos: tuple,
          color: dict | None = None, description: str | None = None) -> dict:
    names = [x_enc["fieldName"], y_enc["fieldName"]] + ([color["fieldName"]] if color else [])
    enc = {"x": x_enc, "y": y_enc}
    if color:
        enc["color"] = color
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    x, y, w, h = pos
    return {"widget": {"name": name, "queries": query(dataset, *names),
                       "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def dropdown(name: str, dataset: str, col: str, title: str, default: str, pos: tuple) -> dict:
    """Single-select filter. Filters every widget on the page that reads `dataset`, by column `col`."""
    qname = f"{name}_values"
    x, y, w, h = pos
    return {"widget": {"name": name,
                       "queries": [{"name": qname,
                                    "query": {"datasetName": dataset, "fields": [field(col)],
                                              "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "filter-single-select",
                                "encodings": {"fields": [{"fieldName": col, "displayName": title,
                                                          "queryName": qname}]},
                                "selection": {"defaultSelection": {"values": {
                                    "dataType": "STRING", "values": [{"value": default}]}}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def enc(field_name: str, scale: str, label: str) -> dict:
    return {"fieldName": field_name, "scale": {"type": scale}, "displayName": label}


def build() -> dict:
    layout = [
        text("hdr", ["## Is it alive?\n", "\n",
                     "Hourly Databricks job pulls EIA's hourly grid data and MERGEs it into Delta tables. "
                     "Every run (success or failure) is logged to `ingest_runs`. Times are UTC.\n"], 0, 0, 6, 2),
        counter("c_last", "health", "minutes_since_last_ok_run", "Minutes since last successful run", 0, 2),
        counter("c_runs", "health", "ok_runs_last_24h", "Successful runs, last 24h", 2, 2),
        counter("c_rows", "health", "total_rows", "Total rows (seed + live)", 4, 2),
        chart("w_runs", "line", "runs", "Every run: rows fetched vs. rows actually written",
              enc("started_at", "temporal", "Run started (UTC)"),
              enc("row_count", "quantitative", "Rows"), (0, 5, 4, 6),
              color=enc("metric", "categorical", ""),
              description="Each run re-fetches ~36h of data; the MERGE only writes new hours and revisions. "
                          "A gap in the top line is a missed run."),
        chart("w_fresh", "bar", "freshness", "How far behind is each series? (hours)",
              enc("series", "categorical", "Series"),
              enc("hours_behind", "quantitative", "Hours behind now"), (4, 5, 2, 6),
              description="Generation is posted a day at a time, so it trails demand by 15-30h."),
        text("hdr2", ["## Live vs. history\n", "\n",
                      "Live hourly data (EIA API) joined to the 2019-2025 seed (EIA bulk file).\n"], 0, 11, 6, 2),
        chart("w_demand", "line", "us_demand", "US Lower 48 electricity demand vs. history",
              enc("period", "temporal", "Hour (UTC)"), enc("mwh", "quantitative", "MWh"), (0, 13, 6, 7),
              color=enc("series", "categorical", "")),
        chart("w_solar", "bar", "solar_by_year", "US solar generation, same week each year (GWh)",
              enc("year", "categorical", "Year"), enc("solar_gwh", "quantitative", "GWh"), (0, 20, 2, 7),
              description="Most recent complete week vs. the same calendar week in past years."),
        chart("w_us_mix", "area", "us_mix", "US generation by fuel, last 7 days (all operators added up)",
              enc("period", "temporal", "Hour (UTC)"), enc("gwh", "quantitative", "GWh"), (2, 20, 4, 7),
              color=enc("fuel", "categorical", "Fuel"),
              description="Complete hours only; the newest day is still being reported."),
        dropdown("f_operator", "operator_mix", "operator", "Operator", DEFAULT_OPERATOR, (0, 27, 2, 2)),
        text("hdr3", ["**One operator at a time.** Pick any of the 68 balancing authorities;"
                      "the chart below redraws for it.\n"], 2, 27, 4, 2),
        chart("w_operator", "area", "operator_mix", "Generation by fuel for the selected operator, last 7 days",
              enc("period", "temporal", "Hour (UTC)"), enc("mwh", "quantitative", "MWh"), (0, 29, 6, 7),
              color=enc("fuel", "categorical", "Fuel")),
    ]
    return {
        "datasets": [{"name": n, "displayName": n, "queryLines": [l + "\n" for l in sql.strip().splitlines()]}
                     for n, sql in DATASETS.items()],
        "pages": [{"name": "main", "displayName": "EIA grid pipeline", "layout": layout}],
    }


def main() -> None:
    print("checking queries ...")
    check_queries()

    definition = build()
    DEF_FILE.write_text(json.dumps(definition, indent=2))
    body = {"display_name": DISPLAY_NAME, "warehouse_id": WAREHOUSE_ID,
            "serialized_dashboard": json.dumps(definition)}

    if ID_FILE.exists():
        dash_id = ID_FILE.read_text().strip()
        cli("lakeview", "update", dash_id, body=body)
        print("updated dashboard", dash_id)
    else:
        res = cli("lakeview", "create", body={**body, "parent_path": PARENT_PATH})
        dash_id = res["dashboard_id"]
        ID_FILE.write_text(dash_id)
        print("created dashboard", dash_id)

    cli("lakeview", "publish", dash_id, body={"warehouse_id": WAREHOUSE_ID, "embed_credentials": True})
    print("published")


if __name__ == "__main__":
    main()
