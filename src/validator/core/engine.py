from __future__ import annotations
import duckdb, sqlite3, time
from typing import Iterable

from validator.core.duck import ensure_spatial_extension
DUCK_TYPES = {
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "string": "VARCHAR",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _null_predicate(
    col: str, duck_type: str | None, null_equivalents: list | None
) -> str:
    base = f"{_qi(col)} IS NULL"
    if not null_equivalents or (duck_type and duck_type.upper() != "VARCHAR"):
        return base
    str_vals = [v for v in null_equivalents if isinstance(v, str)]
    if not str_vals:
        return base
    eqs = " OR ".join(f"{_qi(col)}={_sql_quote(v)}" for v in str_vals)
    return f"{base} OR {eqs}"


def _nonnull_predicate(
    col: str, duck_type: str | None, null_equivalents: list | None
) -> str:
    return f"NOT ({_null_predicate(col, duck_type, null_equivalents)})"


def _bad_cast_count(
    con, view, col, logical_type, duck_type, null_equivalents: list[str] | None
):
    if logical_type == "geometry":
        return 0
    t = DUCK_TYPES.get(logical_type)
    if not t:
        return 0
    nonnull = _nonnull_predicate(col, duck_type, null_equivalents)
    q = f"""
        SELECT COUNT(*) FROM {_qi(view)}
        WHERE {nonnull}
          AND TRY_CAST({_qi(col)} AS {t}) IS NULL
    """
    return con.execute(q).fetchone()[0]


def _dup_examples(con, view, keys: list[str], limit: int):
    k_list = ", ".join(_qi(k) for k in keys)
    q = f"SELECT {k_list}, COUNT(*) AS count FROM {_qi(view)} GROUP BY {k_list} HAVING count>1 LIMIT {limit}"
    rows = con.execute(q).fetchall()
    return [dict(zip(keys + ["count"], row)) for row in rows]


def validate_with_duckdb(
    table_view: str, template: dict, con: duckdb.DuckDBPyConnection
):
    t0 = time.time()
    result = {"ok": True, "errors": [], "warnings": [], "metrics": {}}

    cols = con.execute(f"DESCRIBE {_qi(table_view)}").fetchall()
    present = [c[0] for c in cols]
    duck_types_by_col = {c[0]: (c[1] or "").upper() for c in cols}

    expected = [c["name"] for c in template["columns"]]
    missing = [c for c in expected if c not in present]
    extra = (
        [c for c in present if c not in expected]
        if not template.get("allow_extra_columns", True)
        else []
    )
    if missing:
        result["errors"].append({"code": "MISSING_COLUMNS", "columns": missing})
    if extra:
        result["warnings"].append({"code": "EXTRA_COLUMNS", "columns": extra})

    null_equiv = template.get("null_equivalents", [])
    mism = []
    nulls = {}

    unknown_dtype_issues = []

    for col_spec in template["columns"]:
        col = col_spec["name"]
        if col not in present:
            continue
        duck_type = duck_types_by_col.get(col)

        null_pred = _null_predicate(col, duck_type, null_equiv)
        n_null = con.execute(
            f"SELECT COUNT(*) FROM {_qi(table_view)} WHERE {null_pred}"
        ).fetchone()[0]
        nulls[col] = n_null

        logical = col_spec["dtype"]
        if logical not in DUCK_TYPES:
            unknown_dtype_issues.append({"column": col, "dtype": logical})
        else:
            bad = _bad_cast_count(con, table_view, col, logical, duck_type, null_equiv)
            if bad > 0:
                mism.append({"column": col, "expected": logical, "invalid_rows": bad})

        if "enum" in col_spec and col in present:
            enum_vals_sql = ", ".join(
                [
                    _sql_quote(v) if isinstance(v, str) else str(v)
                    for v in col_spec["enum"]
                ]
            )
            nonnull = _nonnull_predicate(col, duck_type, null_equiv)
            q = f"SELECT COUNT(*) FROM {_qi(table_view)} WHERE {nonnull} AND {_qi(col)} NOT IN ({enum_vals_sql})"
            bad_enum = con.execute(q).fetchone()[0]
            if bad_enum > 0:
                result["errors"].append(
                    {"code": "ENUM_VIOLATION", "column": col, "invalid_rows": bad_enum}
                )

        if "range" in col_spec and col in present and logical in DUCK_TYPES:
            t = DUCK_TYPES[logical]
            rng = col_spec["range"]
            nonnull = _nonnull_predicate(col, duck_type, null_equiv)
            min_clause = (
                f"CAST({_qi(col)} AS {t}) < {rng['min']}" if "min" in rng else "FALSE"
            )
            max_clause = (
                f"CAST({_qi(col)} AS {t}) > {rng['max']}" if "max" in rng else "FALSE"
            )
            bad_range = con.execute(
                f"SELECT COUNT(*) FROM {_qi(table_view)} WHERE {nonnull} AND (({min_clause}) OR ({max_clause}))"
            ).fetchone()[0]
            if bad_range > 0:
                result["errors"].append(
                    {
                        "code": "RANGE_VIOLATION",
                        "column": col,
                        "invalid_rows": bad_range,
                    }
                )

    if unknown_dtype_issues:
        result["errors"].append(
            {"code": "DTYPE_MISMATCH", "details": unknown_dtype_issues}
        )

    if mism:
        result["errors"].append({"code": "DTYPE_MISMATCH", "details": mism})

    result["metrics"]["nulls"] = nulls

    for col_spec in template["columns"]:
        if col_spec.get("required") and col_spec["name"] in present:
            if nulls.get(col_spec["name"], 0) > 0:
                result["errors"].append(
                    {
                        "code": "NULL_REQUIRED",
                        "column": col_spec["name"],
                        "count": nulls[col_spec["name"]],
                    }
                )

    for d in template.get("duplicate_checks", []):
        keys = d["keys"]
        if all(k in present for k in keys):
            examples = _dup_examples(con, table_view, keys, d.get("sample_limit", 1000))
            if examples:
                sev = d.get("severity", "error")
                (result["errors"] if sev == "error" else result["warnings"]).append(
                    {"code": "DUPLICATES", "keys": keys, "examples": examples}
                )

    result["timing_sec"] = round(time.time() - t0, 3)
    result["ok"] = len(result["errors"]) == 0
    return result


def _pick_existing_column(
    con, table_name: str, candidates: list[str]
) -> tuple[str | None, str | None]:
    rows = con.execute(
        """
        SELECT lower(column_name) AS name, column_type
        FROM duckdb_columns()
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchall()
    cols = {r[0]: r[1] for r in rows}
    for c in candidates:
        lc = c.lower()
        if lc in cols:
            return c, cols[lc]
    return None, None


def ensure_geometry_alias(
    con,
    in_view: str,
    out_view: str,
    canonical_name: str,
    aliases: list[str],
):
    name, ctype = _pick_existing_column(con, in_view, [canonical_name])
    if name is not None:
        con.execute(f"CREATE OR REPLACE VIEW {out_view} AS SELECT * FROM {in_view}")
        return

    src, src_type = _pick_existing_column(con, in_view, aliases)
    if src is None:
        con.execute(f"CREATE OR REPLACE VIEW {out_view} AS SELECT * FROM {in_view}")
        return

    src_lc = src.lower()
    geom_expr = src_lc
    t = (src_type or "").upper()
    if t in ("BLOB", "VARBINARY"):
        geom_expr = f"ST_GeomFromWKB({src_lc})"
    elif t in ("VARCHAR", "TEXT"):
        geom_expr = f"ST_GeomFromText({src_lc})"

    if geom_expr != src_lc:
        ensure_spatial_extension(con)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {out_view} AS
        SELECT
          *,
          {geom_expr} AS {canonical_name}
        FROM {in_view}
        """
    )
