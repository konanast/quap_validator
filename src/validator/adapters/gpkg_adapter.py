from __future__ import annotations
import sqlite3
from typing import List, Tuple, Optional
import duckdb

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
    # Fallback via pyogrio if gpkg tables are unusual
    try:
        import pyogrio
        layers = pyogrio.list_layers(path)
        # pyogrio returns list of (name, geom_type, encoding) tuples
        return [t[0] if isinstance(t, (list, tuple)) else t for t in layers]
    except Exception:
        return []

def gpkg_table_columns(path: str, layer: str) -> list[str]:
    """List column names for a given layer/table."""
    with sqlite3.connect(path) as db:
        rows = db.execute(f'PRAGMA table_info("{layer}")').fetchall()
    # rows: cid, name, type, notnull, dflt_value, pk
    return [r[1] for r in rows]

def load_gpkg_view(
    con: duckdb.DuckDBPyConnection,
    path: str,
    layer: str,
    needed_cols: Optional[list[str]] = None,
    view_name: str = "v",
) -> Tuple[str, dict]:
    """
    Prefer DuckDB's sqlite_scanner extension. If not available, fallback to chunk copy
    via sqlite3 into a DuckDB temp table (VARCHAR columns), then expose view 'v'.
    Returns (view_name, diagnostics).
    """
    # 1) Try sqlite_scanner (fast, zero-copy)
    try:
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")
        con.execute(f"ATTACH '{path}' AS gpkg (TYPE SQLITE)")
        present = gpkg_table_columns(path, layer)
        cols = needed_cols or present
        cols = [c for c in cols if c in present]
        select_list = "*" if not cols else ", ".join([f'"{c}"' for c in cols])
        con.execute(f'CREATE OR REPLACE VIEW {view_name} AS SELECT {select_list} FROM gpkg."{layer}"')
        return view_name, {"mode": "sqlite_scanner", "selected_columns": cols or ["*"]}
    except Exception as e:
        diag = {"mode": "sqlite_scanner_failed", "error": str(e)}

    # 2) Fallback: chunk copy from SQLite → DuckDB temp table (bounded memory)
    present = gpkg_table_columns(path, layer)
    cols = needed_cols or present
    cols = [c for c in cols if c in present]
    if not cols:
        # ensure at least one column to materialize
        cols = [present[0]] if present else []

    with sqlite3.connect(path) as db:
        # Determine chunking
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
            # fetch column names and make a DataFrame chunk
            import pandas as pd
            chunk = pd.DataFrame(rows, columns=cols)
            # Normalize to strings for type-safety in validation (types enforced later by template)
            # We'll leave values as-is; DuckDB TRY_CAST will validate.
            if not created:
                cols_def = ", ".join([f'"{c}" VARCHAR' for c in chunk.columns])
                con.execute(f"CREATE TEMP TABLE {table_name} ({cols_def})")
                created = True
            con.register("_gpkg_chunk", chunk)
            con.execute(f'INSERT INTO {table_name} SELECT {", ".join([f""" "{c}" """ for c in cols])} FROM _gpkg_chunk')
            con.unregister("_gpkg_chunk")
            offset += len(chunk)
            if total is not None and offset >= total:
                break

        if not created:
            # empty table
            con.execute('CREATE TEMP TABLE _gpkg_tmp (_dummy VARCHAR)')
            con.execute("DELETE FROM _gpkg_tmp")
        con.execute(f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name}')
        return view_name, {**diag, "fallback_loaded_rows": offset, "selected_columns": cols}
