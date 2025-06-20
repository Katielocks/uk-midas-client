import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def raw_badc_csv() -> str:
    return (
        "ignored header line\n"
        "DATA\n"
        "timestamp , value\n"
        "2020-01-01 00:00,1.1\n"
        "2020-01-02 00:00,2.2\n"
        "end data\n"
    )


@pytest.fixture
def tmp_cache(tmp_path) -> Path:
    p = tmp_path / "cache"
    p.mkdir()
    return p

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
    import midas_client.midas as midas_mod

    monkeypatch.setattr(midas_mod, "settings", dummy, raising=False)


class _DummyTree:
    def __init__(self, *_, **__):
        pass

    def query(self, _pts, k=1):
        idx = np.zeros((_pts.shape[0], k), dtype=int)
        return None, idx


@pytest.fixture(autouse=True)
def _patch_balltree(monkeypatch):
    import midas_client.midas as midas_mod

    monkeypatch.setattr(midas_mod, "BallTree", _DummyTree, raising=False)
