"""
Extract France roads with routing_class from Orbis MCR (read-only).
Saves to data/france_roads.parquet locally.

Uses the Databricks SQL REST API directly — no SDK required.
Paginates through all result chunks automatically.

Confirmed filter:
  license_zone = 'FRA'       — mainland France (Corsica = 'OCN;FRA', excluded)
  product = 'nexventura_26150.000'

Usage:
    cp .env.example .env      # fill in credentials
    pip install -r requirements.txt
    python extract.py
"""

import os
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT = DATA_DIR / "france_roads.parquet"

QUERY = """
SELECT
    osm_identifier,
    CAST(mcr_tags['routing_class'] AS INT) AS routing_class,
    highway,
    oneway,
    name,
    geometry AS geometry_wkt
FROM pu_orbis_platform_prod_catalog.map_central_repository.lines_nexventura_26150_000
WHERE product      = 'nexventura_26150.000'
  AND license_zone = 'FRA'
  AND mcr_tags['routing_class'] IS NOT NULL
"""


def api(host: str, token: str, method: str, path: str, **kwargs):
    url = f"{host.rstrip('/')}{path}"
    resp = requests.request(
        method, url,
        headers={"Authorization": f"Bearer {token}"},
        **kwargs
    )
    resp.raise_for_status()
    return resp.json()


def main():
    host         = os.environ["DATABRICKS_HOST"]
    token        = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    print("Submitting query to Databricks MCR (read-only)...")
    result = api(host, token, "POST", "/api/2.0/sql/statements", json={
        "statement":      QUERY,
        "warehouse_id":   warehouse_id,
        "wait_timeout":   "30s",
        "on_wait_timeout": "CONTINUE",
        "format":         "JSON_ARRAY",
        "disposition":    "INLINE",
    })

    stmt_id = result["statement_id"]

    # Poll until complete
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        print(f"  Status: {result['status']['state']} — waiting...")
        time.sleep(5)
        result = api(host, token, "GET", f"/api/2.0/sql/statements/{stmt_id}")

    if result["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"Query failed: {result['status']}")

    manifest  = result["manifest"]
    columns   = [c["name"] for c in manifest["schema"]["columns"]]
    n_chunks  = manifest["total_chunk_count"]
    n_rows    = manifest["total_row_count"]
    print(f"Query succeeded — {n_rows:,} rows across {n_chunks} chunk(s)")

    writer = None
    rows_written = 0

    for chunk_idx in range(n_chunks):
        if chunk_idx == 0:
            rows = result["result"].get("data_array", [])
        else:
            chunk = api(host, token, "GET",
                        f"/api/2.0/sql/statements/{stmt_id}/result/chunks/{chunk_idx}")
            rows = chunk.get("data_array", [])

        if not rows:
            continue

        df = pd.DataFrame(rows, columns=columns)
        df["routing_class"] = pd.to_numeric(df["routing_class"], errors="coerce").astype("Int8")

        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = pq.ParquetWriter(OUTPUT, table.schema)
        writer.write_table(table)

        rows_written += len(df)
        print(f"  Chunk {chunk_idx + 1}/{n_chunks} — {rows_written:,} rows written", end="\r")

    if writer:
        writer.close()

    print(f"\nDone. Saved {rows_written:,} rows → {OUTPUT}")

    # Quick sanity check
    df = pd.read_parquet(OUTPUT, columns=["routing_class"])
    print("\nRouting class distribution:")
    print(df["routing_class"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
