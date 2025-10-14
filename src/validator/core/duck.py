"""Utilities for working with DuckDB connections."""

from __future__ import annotations

import weakref

import duckdb

_SPATIAL_EXTENSION_LOADED: "weakref.WeakSet[duckdb.DuckDBPyConnection]" = weakref.WeakSet()


def ensure_spatial_extension(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure the DuckDB ``spatial`` extension is installed and loaded.

    DuckDB registers geometry helper functions such as ``ST_GeomFromWKB`` in the
    optional ``spatial`` extension. Some environments ship without the extension
    pre-loaded which causes ``Catalog Error`` failures when the functions are
    referenced. This helper installs (if needed) and loads the extension once
    per connection so subsequent calls are free.
    """

    if _has_loaded_spatial_extension(con):
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

    _mark_spatial_extension_loaded(con)


def _has_loaded_spatial_extension(con: duckdb.DuckDBPyConnection) -> bool:
    """Return ``True`` if ``ensure_spatial_extension`` has been run for ``con``."""

    try:
        if con in _SPATIAL_EXTENSION_LOADED:
            return True
    except TypeError:
        # Some connection-like objects may not support hashing/weak references.
        pass

    return getattr(con, "_validator_spatial_loaded", False)


def _mark_spatial_extension_loaded(con: duckdb.DuckDBPyConnection) -> None:
    """Record that the DuckDB ``spatial`` extension has been loaded for ``con``."""

    try:
        _SPATIAL_EXTENSION_LOADED.add(con)
    except TypeError:
        pass

    try:
        setattr(con, "_validator_spatial_loaded", True)
    except AttributeError:
        # ``DuckDBPyConnection`` objects do not allow setting custom attributes.
        pass

