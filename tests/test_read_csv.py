import pandas as pd
import pytest

from src.session import _read_badc_csv


def test_parses_simple_csv(raw_badc_csv):
    df = _read_badc_csv(raw_badc_csv, parse_dates=["timestamp"])
    assert df.shape == (2, 2)
    assert list(df.columns) == ["timestamp", "value"]
    assert df["timestamp"].tolist() == [
        "2025-01-01 00:00",
        "2025-01-02 00:00",
    ]


def test_raises_if_marker_missing():
    with pytest.raises(ValueError):
        _read_badc_csv("no marker here")
