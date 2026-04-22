"""
Quality checks per routing-class-spec.md §8.
Reads data/france_roads.parquet (local, no Databricks writes).

Checks:
  1. Connected components per RC subgraph (RC1-only, RC1+2, …, RC1-5)
  2. Closure: lower-class roads acting as mandatory connectors for higher classes
  3. Dead-end detection per RC level

Usage:
    python quality_checks.py
"""

import pandas as pd
import networkx as nx
from shapely import wkt
from pathlib import Path

DATA = Path("data") / "france_roads.parquet"
REPORT = Path("data") / "quality_report.txt"


def build_graph(df: pd.DataFrame, max_rc: int) -> nx.DiGraph:
    """Directed graph for roads with routing_class <= max_rc."""
    G = nx.DiGraph()
    subset = df[df["routing_class"] <= max_rc]
    for _, row in subset.iterrows():
        try:
            line = wkt.loads(row["geometry_wkt"])
        except Exception:
            continue
        coords = list(line.coords)
        if len(coords) < 2:
            continue
        start = coords[0]
        end   = coords[-1]
        oneway = str(row.get("oneway", "")).lower()
        G.add_edge(start, end, rc=row["routing_class"], osm_id=row["osm_identifier"])
        if oneway not in ("yes", "1", "true", "-1"):
            G.add_edge(end, start, rc=row["routing_class"], osm_id=row["osm_identifier"])
    return G


def check_connectivity(df: pd.DataFrame, lines: list) -> None:
    """§8.1: count strongly connected components per RC subgraph."""
    lines.append("=== §8.1 Connectivity (strongly connected components) ===")
    for max_rc in range(1, 6):
        G = build_graph(df, max_rc)
        sccs = list(nx.strongly_connected_components(G))
        main = max(sccs, key=len) if sccs else set()
        isolated = [s for s in sccs if len(s) == 1]
        lines.append(
            f"  RC1–RC{max_rc}: {len(sccs)} components | "
            f"largest={len(main):,} nodes | isolated nodes={len(isolated):,}"
        )
    lines.append("")


def check_closure(df: pd.DataFrame, lines: list) -> None:
    """§8.1: find lower-class roads that act as mandatory bridges for higher-class subgraphs."""
    lines.append("=== §8.1 Closure (lower-class mandatory connectors) ===")
    for rc in range(1, 5):
        G_high   = build_graph(df, rc)
        G_all    = build_graph(df, rc + 1)
        # nodes reachable only via rc+1 roads between rc-subgraph components
        high_nodes = {n for n, d in G_high.degree() if d > 0}
        sccs_high  = [c for c in nx.strongly_connected_components(G_high) if len(c) > 1]
        if len(sccs_high) <= 1:
            lines.append(f"  RC1–RC{rc} already connected — no lower-class bridges needed")
            continue
        # count lower-class edges that sit between components
        lower_edges = [
            (u, v) for u, v, d in G_all.edges(data=True)
            if d["rc"] == rc + 1
            and any(u in c for c in sccs_high) != any(v in c for c in sccs_high)
        ]
        lines.append(
            f"  RC{rc+1} roads bridging disconnected RC1–RC{rc} components: {len(lower_edges):,}"
        )
    lines.append("")


def check_dead_ends(df: pd.DataFrame, lines: list) -> None:
    """§8.1: dead ends — nodes with out-degree 0 in RC subgraph (excluding true termini)."""
    lines.append("=== §8.1 Dead-end detection ===")
    for max_rc in range(1, 6):
        G = build_graph(df, max_rc)
        dead_ends = [n for n in G.nodes() if G.out_degree(n) == 0 and G.in_degree(n) > 0]
        lines.append(f"  RC1–RC{max_rc}: {len(dead_ends):,} dead-end nodes")
    lines.append("")


def main():
    print(f"Loading {DATA}...")
    df = pd.read_parquet(DATA)
    print(f"  {len(df):,} roads loaded | RC range: {df['routing_class'].min()}–{df['routing_class'].max()}")

    lines = [f"Quality Report — Metropolitan France | Orbis 26150\n{'='*60}\n"]
    check_connectivity(df, lines)
    check_closure(df, lines)
    check_dead_ends(df, lines)

    report = "\n".join(lines)
    print("\n" + report)
    REPORT.write_text(report)
    print(f"Report saved → {REPORT}")


if __name__ == "__main__":
    main()
