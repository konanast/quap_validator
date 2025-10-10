import duckdb

def load_csv_view(con: duckdb.DuckDBPyConnection, path: str, view_name: str = "v", **read_opts):
    # Probe with ALL_VARCHAR=TRUE; strict typing is handled by template validation
    opts = "AUTO_DETECT=TRUE, ALL_VARCHAR=TRUE"
    if read_opts.get("delimiter"): opts += f", DELIM='{read_opts['delimiter']}'"
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_csv_auto('{path}', {opts})")
    return view_name
