"""
Global fixtures shared by all tests.
"""
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


# ----------  sample raw CSV used by several tests ----------
@pytest.fixture
def raw_badc_csv() -> str:
    return (
        "ignored header line\n"
        "DATA\n"
        "timestamp , value\n"
        "2025-01-01 00:00,1.1\n"
        "2025-01-02 00:00,2.2\n"
        "end data\n"
    )


# ----------  a tmp cache directory ----------
@pytest.fixture
def tmp_cache(tmp_path) -> Path:
    p = tmp_path / "cache"
    p.mkdir()
    return p


# ----------  a minimal replacement for settings ----------
@pytest.fixture(autouse=True)
def _patch_settings(monkeypatch, tmp_cache):
    dummy = SimpleNamespace(
        cache_format="csv",
        cache_dir=tmp_cache,
        midas=SimpleNamespace(
            version="202407",
            tables={"TD": []},
        ),
    )
    import src.midas as midas_mod

    monkeypatch.setattr(midas_mod, "settings", dummy, raising=False)


# ----------  stub BallTree so we don't need sklearn ----------
class _DummyTree:
    def __init__(self, *_, **__):
        pass

    def query(self, _pts, k=1):
        idx = np.zeros((_pts.shape[0], k), dtype=int)
        return None, idx


@pytest.fixture(autouse=True)
def _patch_balltree(monkeypatch):
    import src.midas as midas_mod

    monkeypatch.setattr(midas_mod, "BallTree", _DummyTree, raising=False)
