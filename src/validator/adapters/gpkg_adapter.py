from __future__ import annotations
import duckdb
import sqlite3
from typing import List, Tuple, Optional
from validator.core.geom_alias import _duckdb_cols, _pick_first_present

def gpkg_integrity_errors(path: str) -> list[str]:
    """Return non-'ok' messages from PRAGMA quick_check (fast integrity screen)."""
    try:
        with sqlite3.connect(path) as db:
            rows = db.execute("PRAGMA quick_check;").fetchall()
        return [r[0] for r in rows if r[0] != "ok"]
    except Exception as e:
        return [f"quick_check_failed: {e}"]


def gpkg_list_layers(path: str) -> list[str]:
    """Return feature layers (tables) in the GeoPackage."""
    try:
        with sqlite3.connect(path) as db:
            rows = db.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name"
            ).fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception:
        pass
    try:
        import pyogrio

        layers = pyogrio.list_layers(path)
        return [t[0] if isinstance(t, (list, tuple)) else t for t in layers]
    except Exception:
        return []


def gpkg_table_columns(path: str, layer: str) -> list[str]:
    """List column names for a given layer/table."""
    with sqlite3.connect(path) as db:
        rows = db.execute(f'PRAGMA table_info("{layer}")').fetchall()
    return [r[1] for r in rows]


def _guess_canonical(present_cols: dict) -> Optional[str]:
    """Choose which canonical we should expose based on what looks present."""
    # Prefer explicit canonical names if present
    for cand in ("lpis_geom", "gsa_geom"):
        if cand in present_cols:
            return cand
    # Otherwise, guess by generic aliases found:
    # If we see 'gem' it strongly suggests GSA
    if "gem" in present_cols:
        return "gsa_geom"
    # If we see 'geom' or 'geometry' and 'gsa_*' columns exist, prefer gsa
    if any(k.startswith("gsa_") for k in present_cols):
        return "gsa_geom"
    if any(k.startswith("lpis_") for k in present_cols):
        return "lpis_geom"
    # Fallback: None → we won't add a canonical column
    return None


def _normalize_geometry_view(
    con,
    src_view: str,
    dst_view: str,
    explicit_canonical: Optional[str] = None,
):
    """
    Ensure the final view (dst_view) exposes a canonical geometry column (lpis_geom or gsa_geom)
    constructed from common aliases and cast to GEOMETRY.
    """
    cols = _duckdb_cols(con, src_view)

    # Decide the canonical geometry name we want to expose
    canonical = explicit_canonical or _guess_canonical(cols)
    if not canonical:
        # Nothing to normalize; pass-through
        con.execute(f"CREATE OR REPLACE VIEW {dst_view} AS SELECT * FROM {src_view}")
        return {
            "normalized": False,
            "canonical": None,
            "source": None,
            "source_type": None,
        }

    # Aliases per canonical
    aliases = {
        "lpis_geom": ["lpis_geom", "geom", "geometry"],
        "gsa_geom": ["gsa_geom", "gem", "geometry"],
    }[canonical]

    # If canonical already exists, just pass-through
    if canonical.lower() in cols:
        con.execute(f"CREATE OR REPLACE VIEW {dst_view} AS SELECT * FROM {src_view}")
        return {
            "normalized": False,
            "canonical": canonical,
            "source": canonical,
            "source_type": cols[canonical.lower()],
        }

    # Otherwise pick the first present alias
    src_name, src_type = _pick_first_present(cols, aliases)
    if not src_name:
        # No alias → pass-through
        con.execute(f"CREATE OR REPLACE VIEW {dst_view} AS SELECT * FROM {src_view}")
        return {
            "normalized": False,
            "canonical": canonical,
            "source": None,
            "source_type": None,
        }

    # Build a robust expression:
    # sqlite_scanner often returns everything as VARCHAR when sqlite_all_varchar=true.
    # Try WKB (BLOB) cast first, then WKT as a fallback.
    src_lc = src_name
    expr = f"COALESCE(ST_GeomFromWKB(TRY_CAST({src_lc} AS BLOB)), ST_GeomFromText({src_lc}))"

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {dst_view} AS
        SELECT
          *,
          {expr} AS {canonical}
        FROM {src_view}
        """
    )
    return {
        "normalized": True,
        "canonical": canonical,
        "source": src_name,
        "source_type": src_type,
    }


def load_gpkg_view(
    con: duckdb.DuckDBPyConnection,
    path: str,
    layer: str,
    needed_cols: Optional[list[str]] = None,
    view_name: str = "v",
    # Optional hint (if you know which table you're loading); leave None to auto-detect:
    canonical_geometry: Optional[str] = None,  # "lpis_geom" | "gsa_geom" | None
):
    """
    Creates a normalized view with a canonical geometry column (lpis_geom or gsa_geom).
    Accepts geometry aliases:
      LPIS: lpis_geom, geom, geometry
      GSA : gsa_geom, gem,  geometry
    Ensures the canonical column is a true DuckDB GEOMETRY (via WKB/WKT coercion).
    """
    raw_view = f"{view_name}_raw"

    # 1) Try sqlite_scanner (zero-copy)
    diag = {}
    try:
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")
        # Keep this for stable text typing across diverse GPKGs:
        con.execute("SET sqlite_all_varchar=true;")

        con.execute(f"ATTACH '{path}' AS gpkg (TYPE SQLITE)")

        present = gpkg_table_columns(path, layer)
        cols = needed_cols or present
        cols = [c for c in cols if c in present]
        select_list = "*" if not cols else ", ".join([f'"{c}"' for c in cols])

        con.execute(
            f'CREATE OR REPLACE VIEW {raw_view} AS SELECT {select_list} FROM gpkg."{layer}"'
        )
        norm_info = _normalize_geometry_view(
            con, raw_view, view_name, explicit_canonical=canonical_geometry
        )
        return view_name, {
            "mode": "sqlite_scanner",
            "selected_columns": cols or ["*"],
            "sqlite_all_varchar": True,
            **{"geometry_normalization": norm_info},
        }
    except Exception as e:
        diag = {"mode": "sqlite_scanner_failed", "error": str(e)}

    # 2) Fallback: chunk copy from SQLite → DuckDB temp table (bounded memory)
    present = gpkg_table_columns(path, layer)
    cols = needed_cols or present
    cols = [c for c in cols if c in present]
    if not cols:
        cols = [present[0]] if present else []

    with sqlite3.connect(path) as db:
        try:
            total = db.execute(f'SELECT COUNT(*) FROM "{layer}"').fetchone()[0]
        except Exception:
            total = None
        step = 200_000
        offset = 0
        created = False
        table_name = "_gpkg_tmp"

        while True:
            q = f'SELECT {", ".join([f""" "{c}" """ for c in cols])} FROM "{layer}" LIMIT {step} OFFSET {offset}'
            rows = db.execute(q).fetchmany(step)
            if not rows:
                break
            import pandas as pd

            chunk = pd.DataFrame(rows, columns=cols)
            if not created:
                cols_def = ", ".join([f'"{c}" VARCHAR' for c in chunk.columns])
                con.execute(f"CREATE TEMP TABLE {table_name} ({cols_def})")
                created = True
            con.register("_gpkg_chunk", chunk)
            con.execute(
                f'INSERT INTO {table_name} SELECT {", ".join([f""" "{c}" """ for c in cols])} FROM _gpkg_chunk'
            )
            con.unregister("_gpkg_chunk")
            offset += len(chunk)
            if total is not None and offset >= total:
                break

        if not created:
            con.execute("CREATE TEMP TABLE _gpkg_tmp (_dummy VARCHAR)")
            con.execute("DELETE FROM _gpkg_tmp")

        con.execute(f"CREATE OR REPLACE VIEW {raw_view} AS SELECT * FROM {table_name}")
        norm_info = _normalize_geometry_view(
            con, raw_view, view_name, explicit_canonical=canonical_geometry
        )
        return view_name, {
            **diag,
            "fallback_loaded_rows": offset,
            "selected_columns": cols,
            "geometry_normalization": norm_info,
        }
