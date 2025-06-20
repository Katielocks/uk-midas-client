from __future__ import annotations

import json
import logging
import os
import time
from base64 import b64encode
from io import StringIO

import pandas as pd
import requests

_CEDA_AUTH_URL = "https://services-beta.ceda.ac.uk/api/token/create/"

log = logging.getLogger(__name__)

def _read_badc_csv(raw: str, *, sep: str = ",", parse_dates: list[str] | None = None) -> pd.DataFrame:
    buf = StringIO(raw)
    for n, line in enumerate(buf):
        if line.strip().lower() == "data":
            header = next(buf).rstrip("\n")
            names = [c.strip().lower() for c in header.split(sep)]
            start = n + 2  
            break
    else: 
        raise ValueError("'data' marker not found in CSV content")

    buf.seek(0)
    return (
        pd.read_csv(
            buf,
            engine="python",
            sep=sep,
            names=names,
            skiprows=start,
            parse_dates=parse_dates,
            on_bad_lines="warn",
        ).iloc[:-1]
    )


class MidasSession:
    """A :pyclass:`requests.Session` wrapper that handles CEDA authentication."""

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
    ) -> None:
        log.info("Initialising MidasSession …")


        self.username: str | None = None
        self.password: str | None = None
        self._token: str | None = None 

        self._raw_username = username
        self._raw_password = password
        self._raw_token = token

        self._select_credentials()

        self._session = requests.Session()

    def _select_credentials(self) -> None:


        if self._raw_username and self._raw_password:
            self.username = self._raw_username
            self.password = self._raw_password
            return

        if self._raw_token:
            self._token = self._raw_token
            self.username = os.getenv("CEDA_USER")
            self.password = os.getenv("CEDA_PASS")
            return

        env_token = os.getenv("CEDA_TOKEN")
        if env_token:
            self._token = env_token
            self.username = os.getenv("CEDA_USER")
            self.password = os.getenv("CEDA_PASS")
            return

        env_user = os.getenv("CEDA_USER")
        env_pass = os.getenv("CEDA_PASS")
        if env_user and env_pass:
            self.username = env_user
            self.password = env_pass
            return

        raise RuntimeError(
            "No credentials supplied. Provide either a token (arg or $CEDA_TOKEN) "
            "or username+password (args or $CEDA_USER/$CEDA_PASS)."
        )

    # ...........................................................................................
    def _refresh_token(self) -> str:
        if not (self.username and self.password):
            raise RuntimeError("Cannot refresh token – missing username/password.")

        log.info("Refreshing CEDA token …")
        cred = b64encode(f"{self.username}:{self.password}".encode()).decode()
        resp = requests.post(_CEDA_AUTH_URL, headers={"Authorization": f"Basic {cred}"}, timeout=30)

        alternative_service = alternative_service = """
        To manually obtain an API token:
        1. Visit: https://services-beta.ceda.ac.uk/account/token/
        2. Copy your new token.

        Then either:
        • Set it as an environment variable:
            export CEDA_TOKEN=<your_token_here>
        • Or pass it directly when constructing your session:
            session = MidasSession(token="<your_token_here>")
        """

        if resp.status_code in (401,403):
            raise RuntimeError(
                f"Credentials rejected at token service (HTTP {resp.status_code})). \n {alternative_service}"
            )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Server unavailable while refreshing token (HTTP {resp.status_code}). \n {alternative_service}"
            )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Server error while refreshing token (HTTP {resp.status_code}). \n {alternative_service}"
            )

        resp.raise_for_status() 

        if "application/json" not in resp.headers.get("Content-Type", "").lower():
            snippet = resp.text[:200].replace("\n", " ")
            raise RuntimeError(
                "Unexpected non-JSON response while refreshing token; "
                f"Content-Type={resp.headers.get('Content-Type')!r}. First bytes: {snippet!r}"
            )

        self._token = json.loads(resp.text)["access_token"]
        os.environ["CEDA_TOKEN"] = self._token  
        log.debug("Obtained new token (len=%d)", len(self._token))
        return self._token


    @property
    def token(self) -> str:
        if self._token:
            return self._token
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
        """Download and parse a CSV file that requires CEDA auth."""

        headers = {"Authorization": f"Bearer {self.token}"}
        resp = None

        for attempt in range(1, max_retries + 1):
            try:
                log.debug("GET %s (attempt %d/%d)", url, attempt, max_retries)
                resp = self._session.get(url, headers=headers, timeout=60)

                if resp.status_code in (404, 500):
                    log.error("GET %s returned %d", url, resp.status_code)
                    return pd.DataFrame()

                resp.raise_for_status()

                text = resp.text
                is_html = (
                "text/html" in resp.headers.get("Content-Type", "").lower()
                or text.lstrip().lower().startswith("<!doctype html")
                or text.lstrip().lower().startswith("<html")
                )
                if is_html:
                    log.error(
                        "GET %s returned HTML instead of CSV (likely expired token) "
                        "- attempt %d",
                        url, attempt,
                    )
                    if attempt < max_retries:
                        headers["Authorization"] = f"Bearer {self._refresh_token()}"
                        continue
                    else:
                        raise RuntimeError(
                            f"Exceeded retries: still receiving HTML from {url}"
                        )
                
                return _read_badc_csv(resp.text, sep=sep, parse_dates=parse_dates)

            except requests.exceptions.RequestException as exc:
                log.warning("RequestException on attempt %d: %s", attempt, exc)

                status = getattr(resp, "status_code", None)

                if status in (401, 403):
                    log.info("Token rejected - refreshing …")
                    headers["Authorization"] = f"Bearer {self._refresh_token()}"
                    continue  

                if attempt == max_retries:
                    log.error("Exceeded maximum retries for %s", url)
                    raise

                sleep_time = backoff_factor * (2 ** (attempt - 1))
                log.debug("Sleeping %.1fs before retry", sleep_time)
                time.sleep(sleep_time)

        return pd.DataFrame()
