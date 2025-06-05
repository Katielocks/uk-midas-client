from __future__ import annotations
import os, time, pandas as pd, requests
from base64 import b64encode

_CEDA_AUTH_URL = "https://services-beta.ceda.ac.uk/api/token/create/"

class MidasSession:
    def __init__(self, email: str | None = None, password: str | None = None):
        self.email = email or os.getenv("CEDA_USER")
        self.password = password or os.getenv("CEDA_PASS")
        self._token: str | None = os.getenv("CEDA_TOKEN")

        if not self.token and (not self.user or not self.password):
            raise RuntimeError("EMAIL or CEDA_PWD missing")

        self._session = requests.Session()

    def _refresh_token(self) -> str:
        cred = b64encode(f"{self.email}:{self.password}".encode()).decode()
        r = requests.post(_CEDA_AUTH_URL, headers={"Authorization": f"Basic {cred}"})
        r.raise_for_status()
        self._token = r.json()["access_token"]
        os.environ["CEDA_TOKEN"] = self._token   
        return self._token

    @property
    def token(self) -> str:
        return self._token or self._refresh_token()

def get_csv(
        self,
        url: str,
        *,
        sep: str = ",",
        parse_dates: list[str] | None = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> pd.DataFrame:
        headers = {"Authorization": f"Bearer {self.token}"}
        attempt = 0

        while attempt < max_retries:
            try:
                response = self._session.get(url, headers=headers, timeout=60)
                if response.status_code in (404, 500):
                    return pd.DataFrame()
                response.raise_for_status()
                return _read_badc_csv(response.text, sep=sep, parse_dates=parse_dates)

            except requests.exceptions.RequestException as exc:
                attempt += 1
                if attempt >= max_retries:
                    raise
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_time)

        return pd.DataFrame()

from io import StringIO
def _read_badc_csv(raw: str, *, sep=",", parse_dates=None) -> pd.DataFrame:
    buf = StringIO(raw)
    for n, line in enumerate(buf):
        if line.strip().lower() == "data":
            header = next(buf).rstrip("\n")
            names  = [c.strip().lower() for c in header.split(sep)]
            start  = n + 2
            break
    else:
        raise ValueError("'data' marker not found")

    buf.seek(0)
    return (
        pd.read_csv(buf, engine="python", sep=sep, names=names,
                    skiprows=start, parse_dates=parse_dates,
                    on_bad_lines="warn")
        .iloc[:-1]   
    )
