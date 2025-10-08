from __future__ import annotations
import os
from typing import List, Tuple
import duckdb

def shapefile_integrity_errors(path: str) -> list[str]:
    """
    Basic checks: sidecar presence (.shp, .shx, .dbf) and GDAL header read via pyogrio.
    """
    errs = []
    root, ext = os.path.splitext(path)
    for side in (".shp", ".shx", ".dbf"):
        if not os.path.exists(root + side):
            errs.append(f"missing_sidecar:{side}")
    try:
        import pyogrio
        _ = pyogrio.read_info(path)   # raises if header corrupt
    except Exception as e:
        errs.append(f"read_info_failed:{e}")
    return errs

def load_shapefile_into_duck(
    con: duckdb.DuckDBPyConnection,
    path: str,
    needed_cols: list[str],
    table_name: str = "_shape_tmp",
    view_name: str = "v",
) -> Tuple[str, dict]:
    """
    Paged read with pyogrio.read_arrow → insert into DuckDB temp table (VARCHAR columns),
    then create view 'v'. Keeps memory bounded.
    """
    import pyogrio
    step = 200_000
    offset = 0
    created = False
    selected_cols = needed_cols[:] if needed_cols else None

    # Intersect with actual fields to avoid loading non-existent columns
    try:
        info = pyogrio.read_info(path)
        present = [f["name"] for f in info["fields"]]
        if selected_cols:
            selected_cols = [c for c in selected_cols if c in present]
        else:
            selected_cols = present
    except Exception:
        # hard fallback: let GDAL choose columns in first chunk
        selected_cols = needed_cols[:] if needed_cols else None

    total_rows = 0
    while True:
        arr = pyogrio.read_arrow(
            path,
            columns=selected_cols,
            skip_features=offset,
            max_features=step,
        )
        if arr.num_rows == 0:
            break
        df = arr.to_pandas()
        if not created:
            cols_def = ", ".join([f'"{c}" VARCHAR' for c in df.columns])
            con.execute(f"CREATE TEMP TABLE {table_name} ({cols_def})")
            created = True
        con.register("_shape_chunk", df)
        con.execute(f'INSERT INTO {table_name} SELECT {", ".join([f""" "{c}" """ for c in df.columns])} FROM _shape_chunk')
        con.unregister("_shape_chunk")
        offset += df.shape[0]
        total_rows += df.shape[0]

    if not created:
        con.execute('CREATE TEMP TABLE _shape_tmp (_dummy VARCHAR)')
        con.execute("DELETE FROM _shape_tmp")

    con.execute(f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name}')
    return view_name, {"selected_columns": selected_cols, "loaded_rows": total_rows}
