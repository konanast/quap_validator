# validator/adapters/csv_adapter.py
from __future__ import annotations
from typing import Optional, Dict, Any
import duckdb

from validator.core.geom_alias import normalize_geometry_view


def load_csv_view(
    con: duckdb.DuckDBPyConnection,
    path: str,
    view_name: str = "v",
    *,
    canonical_geometry: Optional[str] = None,
    delimiter: Optional[str] = None,
    **read_opts: Dict[str, Any],
):
    """
    Create a DuckDB view over a CSV with out-of-core reading and normalize geometry:
      - Accepts geometry aliases:
          LPIS: lpis_geom, geom, geometry
          GSA : gsa_geom, gem,  geometry
      - Exposes a canonical column (lpis_geom or gsa_geom) as a true GEOMETRY.

    Notes:
      - We keep ALL_VARCHAR=TRUE; logical typing is enforced later by the validator via TRY_CAST rules.
      - We build a raw view from read_csv_auto(), then create the final normalized view.
    """
    opts = ["AUTO_DETECT=TRUE", "ALL_VARCHAR=TRUE"]
    if delimiter:
        opts.append(f"DELIM='{delimiter}'")

    for k, v in read_opts.items():
        if isinstance(v, bool):
            opts.append(f"{k}={'TRUE' if v else 'FALSE'}")
        elif v is None:
            continue
        elif isinstance(v, (int, float)):
            opts.append(f"{k}={v}")
        else:
            opts.append(f"{k}='{v}'")

    opts_sql = ", ".join(opts)

    raw_view = f"{view_name}_raw"
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {raw_view} AS
        SELECT * FROM read_csv_auto('{path}', {opts_sql})
        """
    )

    norm = normalize_geometry_view(
        con,
        src_view=raw_view,
        dst_view=view_name,
        explicit_canonical=canonical_geometry,
    )

    return view_name, {
        "mode": "csv",
        "read_options": {"options_sql": opts_sql},
        "geometry_normalization": norm,
    }
