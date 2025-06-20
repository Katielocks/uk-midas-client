from .logging_config import setup_logging
from .config import settings

setup_logging()

from .session import MidasSession
from .midas import download_station_year, download_locations


__all__ = [
    "settings",
    "MidasSession",
    "download_station_year",
    "download_locations",
]