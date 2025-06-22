import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
import types

import midas_client.midas as midas_mod


def _fake_get_csv(url: str, *_, **__):
    if url.endswith("station-metadata.csv"):
        return pd.DataFrame.from_records([
            {
                "src_id": "123".zfill(5),
                "historic_county": "dummy",
                "station_file_name": "foo",
                "station_latitude": 51.0,
                "station_longitude": -1.0,
                "first_year": 1900,
                "last_year": 2100,
            }
        ])
    return pd.DataFrame({
        "meto_stmp_time": pd.date_range("2025-01-01", periods=2, freq="D"),
        "obs": [1.1, 2.2],
    })


def _dummy_session(monkeypatch):
    import midas_client.session as sess_mod
    monkeypatch.setattr(
        sess_mod.MidasSession,
        "_refresh_token",
        lambda self: "dummy-token",
        raising=False,
    )
    s = sess_mod.MidasSession(username="x", password="y")
    monkeypatch.setattr(s, "get_csv", _fake_get_csv, raising=True)
    return s


def test_download_by_counties(monkeypatch, tmp_cache):
    session = _dummy_session(monkeypatch)

    midas_mod.download_by_counties(
        {"dummy": []},
        years=[2020],
        tables={"TD": []},
        session=session,
        out_dir=tmp_cache,
        out_fmt="csv",
    )

    assert Path(tmp_cache / "dummy" / "TD_2020.csv").exists()
    assert Path(tmp_cache / "dummy" / "station_map.json").exists()