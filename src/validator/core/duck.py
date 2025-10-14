"""Utilities for working with DuckDB connections."""

from __future__ import annotations

import duckdb


def ensure_spatial_extension(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure the DuckDB ``spatial`` extension is installed and loaded.

    DuckDB registers geometry helper functions such as ``ST_GeomFromWKB`` in the
    optional ``spatial`` extension. Some environments ship without the extension
    pre-loaded which causes ``Catalog Error`` failures when the functions are
    referenced. This helper installs (if needed) and loads the extension once
    per connection so subsequent calls are free.
    """

    if getattr(con, "_validator_spatial_loaded", False):
        return

    try:
        con.load_extension("spatial")
    except duckdb.Error as load_err:
        # ``LOAD`` can fail if the extension has not been installed yet. Try to
        # install it and then attempt to load again, surfacing the original
        # error if we still cannot load the extension afterwards.
        con.install_extension("spatial")
        try:
            con.load_extension("spatial")
        except duckdb.Error as second_err:  # pragma: no cover - defensive branch
            raise second_err from load_err

    setattr(con, "_validator_spatial_loaded", True)

