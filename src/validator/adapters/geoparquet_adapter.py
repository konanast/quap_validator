import duckdb

def load_parquet_view(con: duckdb.DuckDBPyConnection, path: str, view_name: str = "v"):
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{path}')")
    return view_name
