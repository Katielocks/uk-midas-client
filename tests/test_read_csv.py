import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
import pytest

from midas_client.session import _read_badc_csv


def test_parses_simple_csv(raw_badc_csv):
    df = _read_badc_csv(raw_badc_csv, parse_dates=["timestamp"])
    assert df.shape == (2, 2)
    assert list(df.columns) == ["timestamp", "value"]
    assert df["timestamp"].tolist() == [
        "2020-01-01 00:00",
        "2020-01-02 00:00",
    ]


def test_raises_if_marker_missing():
    with pytest.raises(ValueError):
        _read_badc_csv("no marker here")
