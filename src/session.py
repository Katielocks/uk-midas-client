from __future__ import annotations
import os, time, pandas as pd, requests
from base64 import b64encode
from io import StringIO
import logging

_CEDA_AUTH_URL = "https://services-beta.ceda.ac.uk/api/token/create/"

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

class MidasSession:
    def __init__(self, username: str | None = None, password: str | None = None):
        logger.info("Initializing Midas Session...")
        self.username = username or os.getenv("CEDA_USER")
        self.password = password or os.getenv("CEDA_PASS")
        self._token: str | None = os.getenv("CEDA_TOKEN")
        logger.debug(f"Username set to {self.username}, token initialized: {bool(self._token)}")
        if not self.token and (not self.username or not self.password):
            logger.error("CEDA_USER or CEDA_PASS missing")
            raise RuntimeError("CEDA_USER or CEDA_PASS missing")
        self._session = requests.Session()

    def _refresh_token(self) -> str:
        logger.info("Refreshing CEDA Token")
        cred = b64encode(f"{self.username}:{self.password}".encode()).decode()
        logger.debug("Encoded credentials for token refresh")
        r = requests.post(_CEDA_AUTH_URL, headers={"Authorization": f"Basic {cred}"})
        r.raise_for_status()
        self._token = r.json()["access_token"]
        os.environ["CEDA_TOKEN"] = self._token   
        logger.debug(f"New token obtained, length {len(self._token)}")
        return self._token

    @property
    def token(self) -> str:
        if self._token:
            logger.debug("Using existing token")
            return self._token
        logger.debug("No existing token, calling _refresh_token")
        return self._refresh_token()

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
            attempt += 1
            logger.debug(f"Attempt {attempt} of {max_retries} for URL {url}")
            try:
                response = self._session.get(url, headers=headers, timeout=60)
                if response.status_code in (404, 500):
                    logger.error(f"get returned {response.status_code} from {url}, retrying...")
                    return pd.DataFrame()
                response.raise_for_status()
                df = _read_badc_csv(response.text, sep=sep, parse_dates=parse_dates)
                return df
            except requests.exceptions.RequestException as exc:
                logger.warning(f"RequestException on attempt {attempt}: {exc}")
                if attempt >= max_retries:
                    logger.error(f"Max retries reached for {url}, raising exception")
                    raise
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                logger.debug(f"Sleeping for {sleep_time} seconds before retry")
                time.sleep(sleep_time)
        logger.error(f"Failed to fetch CSV from {url} after {max_retries} attempts")
        return pd.DataFrame()

def _read_badc_csv(raw: str, *, sep=",", parse_dates=None) -> pd.DataFrame:
    buf = StringIO(raw)
    for n, line in enumerate(buf):
        if line.strip().lower() == "data":
            header = next(buf).rstrip("\n")
            logger.debug(f"Header line found at {n+1}: {header}")
            names  = [c.strip().lower() for c in header.split(sep)]
            start  = n + 2
            break
    else:
        logger.error("'data' marker not found in CSV content")
        raise ValueError("'data' marker not found")

    buf.seek(0)
    df = (
        pd.read_csv(buf, engine="python", sep=sep, names=names,
                    skiprows=start, parse_dates=parse_dates,
                    on_bad_lines="warn")
        .iloc[:-1]
    )
    return df
