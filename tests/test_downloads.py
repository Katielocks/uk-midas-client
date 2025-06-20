import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import types
import pandas as pd

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


def test_download_station_year(monkeypatch, tmp_cache):
    session = _dummy_session(monkeypatch)

    df = midas_mod.download_station_year(
        "TD", station_id="00123", year=2020, session=session
    )
    assert list(df.columns) == ["meto_stmp_time", "obs"]
    assert len(df) == 2


def test_download_locations(monkeypatch, tmp_cache):
    session = _dummy_session(monkeypatch)

    locs = pd.DataFrame({
        "loc_id": ["here"],
        "lat": [51.001],
        "long": [-1.001],
    })

    out = midas_mod.download_locations(
        locs,
        years=range(2020, 2021),
        tables={"TD":[]},
        k=1,
        session=session,
        out_dir=tmp_cache,
        out_fmt="csv"
    )

    assert len(out) == 1
    assert out.loc_id.iloc[0] == "here"
    assert out.year.iloc[0] == 2020
    assert Path(tmp_cache/ "TD_2020.csv").exists()
    assert Path(tmp_cache/ "station_map.json").exists()
