"""
Export france_roads.parquet → data/france_roads.osm (OSM XML).
Convert to PBF with: osmium cat data/france_roads.osm -o data/france_roads.pbf

routing_class is written as a plain OSM tag — style by value (1–5) in QGIS.

Usage:
    python export_osm.py
    osmium cat data/france_roads.osm -o data/france_roads.pbf
"""

import pandas as pd
from pathlib import Path
from lxml import etree
from shapely import wkt
from tqdm import tqdm

DATA   = Path("data") / "france_roads.parquet"
OUTPUT = Path("data") / "france_roads.osm"

# OSM node IDs are synthetic — negative to avoid clash with real OSM IDs
NODE_ID_START = -1
WAY_ID_START  = -1


def main():
    print(f"Loading {DATA}...")
    df = pd.read_parquet(DATA)
    print(f"  {len(df):,} roads")

    root = etree.Element("osm", version="0.6", generator="routing-class-france")

    node_map: dict[tuple, int] = {}
    node_id = NODE_ID_START
    way_id  = WAY_ID_START

    print("Building OSM XML...")
    ways = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        try:
            line = wkt.loads(row["geometry_wkt"])
        except Exception:
            continue
        coords = list(line.coords)
        if len(coords) < 2:
            continue

        nd_refs = []
        for lon, lat in coords:
            pt = (round(lon, 7), round(lat, 7))
            if pt not in node_map:
                node_map[pt] = node_id
                node_id -= 1
            nd_refs.append(node_map[pt])

        tags = {
            "routing_class": str(int(row["routing_class"])),
            "highway":       str(row.get("highway") or ""),
        }
        if row.get("oneway"):
            tags["oneway"] = str(row["oneway"])
        if row.get("name"):
            tags["name"] = str(row["name"])
        if row.get("osm_identifier"):
            tags["osm:way:id"] = str(row["osm_identifier"])

        ways.append((way_id, nd_refs, tags))
        way_id -= 1

    # Write nodes first (OSM XML ordering requirement)
    print("Writing nodes...")
    coord_by_id = {v: k for k, v in node_map.items()}
    for nid, (lon, lat) in tqdm(coord_by_id.items()):
        etree.SubElement(root, "node", id=str(nid), lat=str(lat), lon=str(lon),
                         version="1", visible="true")

    # Write ways
    print("Writing ways...")
    for wid, nd_refs, tags in tqdm(ways):
        way_el = etree.SubElement(root, "way", id=str(wid), version="1", visible="true")
        for ref in nd_refs:
            etree.SubElement(way_el, "nd", ref=str(ref))
        for k, v in tags.items():
            if v:
                etree.SubElement(way_el, "tag", k=k, v=v)

    tree = etree.ElementTree(root)
    tree.write(str(OUTPUT), pretty_print=True, xml_declaration=True, encoding="UTF-8")
    print(f"\nSaved → {OUTPUT}")
    print("\nNext step:")
    print(f"  osmium cat {OUTPUT} -o data/france_roads.pbf")


if __name__ == "__main__":
    main()
