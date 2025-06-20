import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import types
import pandas as pd
import pytest
import requests

from midas_client.session import MidasSession, _read_badc_csv


class _StubResponse(requests.Response):
    def __init__(self, status: int, text: str | None = None):
        super().__init__()
        self.status_code = status
        if text is not None:
            self._content = text.encode()

    def raise_for_status(self):
        if 400 <= self.status_code < 600 and self.status_code not in (404, 500):
            raise requests.HTTPError(f"{self.status_code} Error")


@pytest.fixture
def session(monkeypatch):
    import midas_client.session as sess_mod
    monkeypatch.setattr(
        sess_mod.MidasSession,
        "_refresh_token",
        lambda self: "dummy-token",
        raising=False,
    )
    s = MidasSession(username="x", password="y")
    s._session = types.SimpleNamespace(get=lambda *args, **kwargs: None)
    return s


def test_get_csv_returns_dataframe(session, raw_badc_csv):
    stub = _StubResponse(200, raw_badc_csv)
    session._session.get = lambda *_, **__: stub

    df = session.get_csv("http://dummy")
    assert not df.empty
    assert df.equals(_read_badc_csv(raw_badc_csv))


def test_get_csv_swallows_404(session):
    session._session.get = lambda *_, **__: _StubResponse(404)

    df = session.get_csv("http://dummy")
    assert df.empty


def test_get_csv_retries_then_raises(monkeypatch, session):
    calls = {"n": 0}

    def _always_fail(*_, **__):
        calls["n"] += 1
        raise requests.ConnectionError

    session._session.get = _always_fail
    monkeypatch.setattr("time.sleep", lambda *_: None)

    with pytest.raises(requests.ConnectionError):
        session.get_csv("http://dummy", max_retries=3)

    assert calls["n"] == 3
