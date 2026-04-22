"""
Extract France roads with routing_class from Orbis MCR (read-only).
Saves to data/france_roads.parquet locally.

Usage:
    pip install -r requirements.txt
    cp .env.example .env  # fill in your credentials
    python extract.py
"""

import os
import pandas as pd
from pathlib import Path
from databricks import sql
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT = DATA_DIR / "france_roads.parquet"

# Metropolitan France bbox — excludes Corsica (starts ~8.5°E)
BBOX = (-5.5, 42.3, 8.3, 51.2)  # min_lon, min_lat, max_lon, max_lat

QUERY = f"""
SELECT
    osm_identifier,
    CAST(mcr_tags['routing_class'] AS INT)  AS routing_class,
    highway,
    oneway,
    name,
    ST_AsText(geometry)                      AS geometry_wkt
FROM pu_orbis_platform_prod_catalog.map_central_repository.lines_nexventura_26150_000
WHERE mcr_tags['routing_class'] IS NOT NULL
  AND ST_Intersects(
        geometry,
        ST_MakeEnvelope({BBOX[0]}, {BBOX[1]}, {BBOX[2]}, {BBOX[3]})
      )
"""


def main():
    host     = os.environ["DATABRICKS_HOST"]
    token    = os.environ["DATABRICKS_TOKEN"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]

    print("Connecting to Databricks MCR (read-only)...")
    with sql.connect(
        server_hostname=host.replace("https://", ""),
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            print("Running query for metropolitan France roads...")
            cur.execute(QUERY)

            print("Fetching results in batches...")
            rows, columns = [], None
            batch_size = 10_000

            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                if columns is None:
                    columns = [d[0] for d in cur.description]
                rows.extend(batch)
                print(f"  {len(rows):,} rows fetched", end="\r")

    print(f"\nTotal rows: {len(rows):,}")
    df = pd.DataFrame(rows, columns=columns)

    # Sanity-check routing_class values
    print("\nRouting class distribution:")
    print(df["routing_class"].value_counts().sort_index().to_string())

    df.to_parquet(OUTPUT, index=False)
    print(f"\nSaved → {OUTPUT}")


if __name__ == "__main__":
    main()
