from __future__ import annotations
import duckdb, sqlite3, time
from typing import Iterable

DUCK_TYPES = {
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "string": "VARCHAR",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}

def _bad_cast_count(con, view, col, logical_type, null_equivalents: list[str] | None):
    if logical_type == "geometry":
        return 0
    t = DUCK_TYPES[logical_type]
    null_pred = " OR ".join([f"{col}='{v}'" for v in (null_equivalents or [])])
    null_clause = f" OR {null_pred}" if null_pred else ""
    q = f"""
        SELECT COUNT(*) FROM {view}
        WHERE ({col} IS NOT NULL{null_clause})
          AND TRY_CAST({col} AS {t}) IS NULL
    """
    return con.execute(q).fetchone()[0]

def _dup_examples(con, view, keys: list[str], limit: int):
    k = ", ".join(keys)
    q = f"SELECT {k}, COUNT(*) c FROM {view} GROUP BY {k} HAVING c>1 LIMIT {limit}"
    return [dict(zip(keys+["count"], row)) for row in con.execute(q).fetchall()]

def validate_with_duckdb(table_view: str, template: dict, con: duckdb.DuckDBPyConnection):
    t0 = time.time()
    result = {"ok": True, "errors": [], "warnings": [], "metrics": {}}

    # schema discovery
    cols = con.execute(f"DESCRIBE {table_view}").fetchall()
    present = [c[0] for c in cols]
    # required/missing/extra
    expected = [c["name"] for c in template["columns"]]
    missing = [c for c in expected if c not in present]
    extra = [c for c in present if c not in expected] if not template.get("allow_extra_columns", True) else []
    if missing:
        result["errors"].append({"code":"MISSING_COLUMNS","columns":missing})
    if extra:
        result["warnings"].append({"code":"EXTRA_COLUMNS","columns":extra})

    # dtype checks
    null_equiv = template.get("null_equivalents", [])
    mism = []
    nulls = {}
    for col_spec in template["columns"]:
        col = col_spec["name"]
        if col not in present:
            continue
        # nulls
        null_pred = " OR ".join([f"{col}='{v}'" for v in null_equiv])
        null_clause = f" OR {null_pred}" if null_pred else ""
        n_null = con.execute(f"SELECT COUNT(*) FROM {table_view} WHERE {col} IS NULL{null_clause}").fetchone()[0]
        nulls[col] = n_null
        # type
        bad = _bad_cast_count(con, table_view, col, col_spec["dtype"], null_equiv)
        if bad > 0:
            mism.append({"column": col, "expected": col_spec["dtype"], "invalid_rows": bad})
        # enums
        if "enum" in col_spec:
            enum_vals = ", ".join([f"'{v}'" if isinstance(v,str) else str(v) for v in col_spec["enum"]])
            bad_enum = con.execute(
                f"SELECT COUNT(*) FROM {table_view} WHERE {col} IS NOT NULL AND {col} NOT IN ({enum_vals})"
            ).fetchone()[0]
            if bad_enum > 0:
                result["errors"].append({"code":"ENUM_VIOLATION","column":col,"invalid_rows":bad_enum})
        # range
        if "range" in col_spec:
            t = DUCK_TYPES[col_spec["dtype"]]
            rng = col_spec["range"]
            min_clause = f"CAST({col} AS {t}) < {rng['min']}" if "min" in rng else "FALSE"
            max_clause = f"CAST({col} AS {t}) > {rng['max']}" if "max" in rng else "FALSE"
            bad_range = con.execute(
                f"SELECT COUNT(*) FROM {table_view} WHERE ({min_clause}) OR ({max_clause})"
            ).fetchone()[0]
            if bad_range > 0:
                result["errors"].append({"code":"RANGE_VIOLATION","column":col,"invalid_rows":bad_range})

    if mism:
        result["errors"].append({"code":"DTYPE_MISMATCH","details":mism})
    result["metrics"]["nulls"] = nulls

    # required columns null check
    for col_spec in template["columns"]:
        if col_spec.get("required") and col_spec["name"] in present:
            if nulls[col_spec["name"]] > 0:
                result["errors"].append({"code":"NULL_REQUIRED","column":col_spec["name"],"count":nulls[col_spec["name"]]})

    # duplicates
    for d in template.get("duplicate_checks", []):
        keys = d["keys"]
        if all(k in present for k in keys):
            examples = _dup_examples(con, table_view, keys, d.get("sample_limit",1000))
            if examples:
                sev = d.get("severity","error")
                result["errors" if sev=="error" else "warnings"].append({"code":"DUPLICATES","keys":keys,"examples":examples})

    result["timing_sec"] = round(time.time()-t0, 3)
    result["ok"] = len([e for e in result["errors"] if True]) == 0
    return result
