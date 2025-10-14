import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from validator.core.geom_alias import normalize_geometry_view


def test_normalize_geometry_view_without_geometry_columns():
    con = duckdb.connect()
    try:
        con.execute("CREATE OR REPLACE VIEW src_view AS SELECT 1 AS id")
        result = normalize_geometry_view(con, "src_view", "dst_view")
        assert result["normalized"] is False
        assert result["canonical"] is None
    finally:
        con.close()
