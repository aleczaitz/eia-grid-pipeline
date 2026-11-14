"""One-time seed conversion: EIA bulk file (EBA.zip) -> Parquet.

Run once from the project root:
    .venv/bin/python seed/convert_bulk.py

Input:  data/raw/EBA.zip (downloaded once from https://api.eia.gov/bulk/EBA.zip)
Output: data/parquet/region_data.parquet      (period, respondent, type, value)
        data/parquet/fuel_type_data.parquet   (period, respondent, fueltype, value)
        data/parquet/respondents.parquet      (respondent, respondent_name)

Column names match the EIA API v2 routes (electricity/rto/region-data and
electricity/rto/fuel-type-data) so the seed and the hourly trickle share one schema.
"""

import pathlib
import zipfile

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_ZIP = ROOT / "data" / "raw" / "EBA.zip"
TMP_TXT = ROOT / "data" / "raw" / "EBA.txt"
OUT = ROOT / "data" / "parquet"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    if not TMP_TXT.exists():
        print(f"unzipping {RAW_ZIP.name} (4.4 GB) ...")
        with zipfile.ZipFile(RAW_ZIP) as z:
            z.extract("EBA.txt", TMP_TXT.parent)

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")  # lets DuckDB parallelize freely
    con.execute("SET threads = 4")                         # each thread buffers JSON; keeps memory bounded
    con.execute(f"SET temp_directory = '{ROOT / 'data' / 'tmp'}'")  # spill to disk instead of OOM

    # Each line is one series: {"series_id": "EBA.BANC-ALL.NG.SUN.H", "name": ..., "data": [["20261112T07","0"], ...]}
    # Keep UTC hourly series (".H") only; ".HL" are the same numbers in local time.
    con.execute(f"""
        CREATE TEMP VIEW series AS
        SELECT series_id,
               name,
               string_split(series_id, '.') AS parts,
               data
        FROM read_json('{TMP_TXT}',
                       format = 'newline_delimited',
                       columns = {{series_id: 'VARCHAR', name: 'VARCHAR', data: 'VARCHAR[][]'}})
        WHERE series_id LIKE 'EBA.%-ALL.%.H'
    """)

    # parts = ['EBA', 'BANC-ALL', 'NG', 'SUN', 'H']  (1-indexed in DuckDB)
    con.execute("""
        CREATE TEMP VIEW points AS
        SELECT split_part(parts[2], '-', 1)              AS respondent,
               parts                                     AS parts,
               len(parts)                                AS n_parts,
               name,
               unnest(data)                              AS pt
        FROM series
    """)

    print("writing region_data.parquet ...")
    con.execute(f"""
        COPY (
            SELECT strptime(pt[1], '%Y%m%dT%H')::TIMESTAMP AS period,   -- UTC
                   respondent,
                   parts[3]                                AS type,     -- D, DF, NG, TI
                   TRY_CAST(pt[2] AS DOUBLE)               AS value     -- megawatthours
            FROM points
            WHERE n_parts = 4 AND parts[3] IN ('D', 'DF', 'NG', 'TI')
              AND TRY_CAST(pt[2] AS DOUBLE) IS NOT NULL
        ) TO '{OUT / "region_data.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)

    print("writing fuel_type_data.parquet ...")
    con.execute(f"""
        COPY (
            SELECT strptime(pt[1], '%Y%m%dT%H')::TIMESTAMP AS period,
                   respondent,
                   parts[4]                                AS fueltype, -- COL, NG, NUC, SUN, WND, WAT, OIL, OTH, UNK
                   TRY_CAST(pt[2] AS DOUBLE)               AS value
            FROM points
            WHERE n_parts = 5 AND parts[3] = 'NG'
              AND TRY_CAST(pt[2] AS DOUBLE) IS NOT NULL
        ) TO '{OUT / "fuel_type_data.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)

    # Names come from text like
    #   "Demand for Balancing Authority of Northern California (BANC), hourly - UTC time"  -> balancing authority
    #   "Demand for California (region), hourly - UTC time"                                -> rollup region
    # Regions (US48, CAL, TEX, ...) are sums of balancing authorities: don't add them together.
    # Classify by EIA's fixed region list, not the name: single-BA regions (NE = ISNE, NY = NYIS, TEN = TVA,
    # TEX = ERCO) are named like BAs ("Demand for New York (NY)"), and treating them as BAs double-counts them.
    regions = "'US48','CAL','CAR','CENT','FLA','MIDA','MIDW','NE','NW','NY','SE','SW','TEN','TEX'"
    print("writing respondents.parquet ...")
    con.execute(f"""
        COPY (
            SELECT respondent,
                   any_value(coalesce(nullif(regexp_extract(name, ' for (.*) \\(' || respondent || '\\)', 1), ''),
                                      regexp_extract(name, ' for (.*) \\(region\\)', 1)))           AS respondent_name,
                   CASE WHEN respondent IN ({regions}) THEN 'region'
                        ELSE 'balancing_authority' END                                              AS respondent_kind
            FROM series, LATERAL (SELECT split_part(parts[2], '-', 1) AS respondent)
            WHERE len(parts) = 4 AND parts[3] IN ('D', 'NG')
            GROUP BY respondent
        ) TO '{OUT / "respondents.parquet"}' (FORMAT parquet)
    """)

    for f in ("region_data", "fuel_type_data", "respondents"):
        path = OUT / f"{f}.parquet"
        rows, lo, hi = con.execute(
            f"SELECT count(*), {'min(period), max(period)' if f != 'respondents' else 'NULL, NULL'} FROM '{path}'"
        ).fetchone()
        print(f"{f:16s} rows={rows:>12,}  {lo} -> {hi}  ({path.stat().st_size / 1e6:.0f} MB)")

    TMP_TXT.unlink()
    print("deleted temporary EBA.txt (EBA.zip is kept as the raw copy)")


if __name__ == "__main__":
    main()
