from typing import Optional, Tuple


def _duckdb_cols(con, table_or_view: str) -> dict[str, str]:
    rows = con.execute(
        """
        SELECT lower(column_name) AS name, upper(data_type) AS ctype
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
        """,
        [table_or_view],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _pick_first_present(
    cols: dict[str, str], candidates: list[str]
) -> Tuple[Optional[str], Optional[str]]:
    for c in candidates:
        lc = c.lower()
        if lc in cols:
            return c, cols[lc]
    return None, None


def guess_canonical_by_columns(present_cols: dict[str, str]) -> Optional[str]:
    if "lpis_geom" in present_cols:
        return "lpis_geom"
    if "gsa_geom" in present_cols:
        return "gsa_geom"
    if "gem" in present_cols:
        return "gsa_geom"
    if any(k.startswith("gsa_") for k in present_cols):
        return "gsa_geom"
    if any(k.startswith("lpis_") for k in present_cols):
        return "lpis_geom"
    return None


def normalize_geometry_view(
    con,
    src_view: str,
    dst_view: str,
    explicit_canonical: Optional[str] = None,
):
    cols = _duckdb_cols(con, src_view)
    canonical = explicit_canonical or guess_canonical_by_columns(cols)
    if not canonical:
        con.execute(
            f'CREATE OR REPLACE VIEW "{dst_view}" AS SELECT * FROM "{src_view}"'
        )
        return {
            "normalized": False,
            "canonical": None,
            "source": None,
            "source_type": None,
        }

    alias_map = {
        "lpis_geom": ["lpis_geom", "geom", "geometry"],
        "gsa_geom": ["gsa_geom", "gem", "geometry"],
    }
    aliases = alias_map[canonical]
    can_lc = canonical.lower()

    def _geom_expr(id_quoted: str) -> str:
        return f"COALESCE(ST_GeomFromWKB(TRY_CAST({id_quoted} AS BLOB)), ST_GeomFromText({id_quoted}))"

    if can_lc in cols:
        ctype = cols[can_lc]
        if ctype == "GEOMETRY":
            con.execute(
                f'CREATE OR REPLACE VIEW "{dst_view}" AS SELECT * FROM "{src_view}"'
            )
            return {
                "normalized": False,
                "canonical": canonical,
                "source": canonical,
                "source_type": ctype,
            }
        src_id = f'"{canonical}"'
        expr = _geom_expr(src_id)
        con.execute(
            f"""
            CREATE OR REPLACE VIEW "{dst_view}" AS
            SELECT
              * EXCLUDE ("{canonical}"),
              {expr} AS "{canonical}"
            FROM "{src_view}"
            """
        )
        return {
            "normalized": True,
            "canonical": canonical,
            "source": canonical,
            "source_type": ctype,
        }

    src_name = None
    src_type = None
    for a in aliases:
        if a.lower() in cols:
            src_name = a
            src_type = cols[a.lower()]
            break

    if not src_name:
        con.execute(
            f'CREATE OR REPLACE VIEW "{dst_view}" AS SELECT * FROM "{src_view}"'
        )
        return {
            "normalized": False,
            "canonical": canonical,
            "source": None,
            "source_type": None,
        }

    src_id = f'"{src_name}"'
    expr = _geom_expr(src_id)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW "{dst_view}" AS
        SELECT
          *,
          {expr} AS "{canonical}"
        FROM "{src_view}"
        """
    )
    return {
        "normalized": True,
        "canonical": canonical,
        "source": src_name,
        "source_type": src_type,
    }
