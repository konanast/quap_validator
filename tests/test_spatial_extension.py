import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from validator.core.duck import ensure_spatial_extension


class DummyConnection:
    def __init__(self):
        self.install_calls = 0
        self.load_calls = 0
        self._should_fail = True

    def install_extension(self, name: str) -> None:
        assert name == "spatial"
        self.install_calls += 1

    def load_extension(self, name: str) -> None:
        assert name == "spatial"
        self.load_calls += 1
        if self._should_fail:
            self._should_fail = False
            raise duckdb.IOException("Extension 'spatial' not installed")


def test_ensure_spatial_extension_installs_and_caches():
    con = DummyConnection()

    ensure_spatial_extension(con)

    assert con.install_calls == 1
    assert con.load_calls == 2  # initial failure + retry after install
    assert getattr(con, "_validator_spatial_loaded") is True

    ensure_spatial_extension(con)
    assert con.install_calls == 1
    assert con.load_calls == 2


class SlotsConnection:
    __slots__ = ("install_calls", "load_calls", "_should_fail", "__weakref__")

    def __init__(self):
        self.install_calls = 0
        self.load_calls = 0
        self._should_fail = True

    def install_extension(self, name: str) -> None:
        assert name == "spatial"
        self.install_calls += 1

    def load_extension(self, name: str) -> None:
        assert name == "spatial"
        self.load_calls += 1
        if self._should_fail:
            self._should_fail = False
            raise duckdb.IOException("Extension 'spatial' not installed")


def test_ensure_spatial_extension_handles_attribute_errors():
    con = SlotsConnection()

    ensure_spatial_extension(con)
    assert con.install_calls == 1
    assert con.load_calls == 2
    assert not hasattr(con, "_validator_spatial_loaded")

    ensure_spatial_extension(con)
    assert con.install_calls == 1
    assert con.load_calls == 2
