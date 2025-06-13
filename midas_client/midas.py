from __future__ import annotations
import logging
from pathlib import Path
from collections import defaultdict
from typing import Callable,Union,Dict
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .config import settings
from .session import MidasSession

logger = logging.getLogger(__name__)
cfg = settings.midas


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

_BASE_URL = "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data"
_META_FMT = "midas-open_{db}_dv-{ver}_station-metadata.csv"
_META_CACHE: dict[str, pd.DataFrame] = {}
_TABLE_CODES : Dict[str,str] = {
      "RH": "uk-hourly-rain-obs",
      "RD": "uk-daily-rain-obs",
      "TD": "uk-daily-temperature-obs",
      "WH": "uk-hourly-weather-obs",
      "WD": "uk-daily-weather-obs",
      "WM": "uk-mean-wind-obs",
      "RY": "uk-radiation-obs",
      "SH": "uk-soil-temperature-obs"
    }

_OUTPUT_WRITERS: dict[str, Callable[[pd.DataFrame, Path], None]] = {
    "csv":     lambda df, p: df.to_csv(p, index=False),
    "parquet": lambda df, p: df.to_parquet(p, index=False),
    "json":    lambda df, p: df.to_json(p, orient="records"),
}

def write_cache(cache_path: Union[str, Path], df: pd.DataFrame, mdir: bool = True) -> None:
    cache_path = Path(cache_path)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"Expected a pandas DataFrame to write, got {type(df)}."
        )
    parent = cache_path.parent
    if mdir:
        parent.mkdir(parents=True, exist_ok=True)
    else:
        if not parent.exists():
            raise FileNotFoundError(f"Directory '{parent}' does not exist.")
        if not parent.is_dir():
            raise ValueError(f"Parent path '{parent}' is not a directory.")
    cache_fmt = cache_path.suffix.lstrip(".").lower()
    if not cache_fmt:
        raise ValueError(
            f"No file extension found for '{cache_path}'."
            f" Choose one of {list(_OUTPUT_WRITERS.keys())}."
        )
    if cache_fmt not in _OUTPUT_WRITERS:
        raise ValueError(
            f"Unsupported output format '{cache_fmt}'."
            f" Supported formats are: {list(_OUTPUT_WRITERS.keys())}."
        )
    try:
        _OUTPUT_WRITERS[cache_fmt](df, cache_path)
    except Exception as e:
        raise IOError(
            f"Failed to write DataFrame to '{cache_path}' as {cache_fmt}: {e}"
        ) from e



def _fetch_meta(session: MidasSession, tbl: str) -> pd.DataFrame:
    """Download station metadata for *tbl*, with an in-memory cache.

    Parameters
    ----------
    session
        Active ``MidasSession`` used for HTTP requests.
    tbl
        table key looked up in
        ``_TABLE_CODES``.

    Returns
    -------
    pandas.DataFrame
        The midas station metadata contains date of service and location of all stations, given tbl code.
    """
    db_slug = _TABLE_CODES[tbl]
    version = cfg.version
    meta_url = (
        f"{_BASE_URL}/{db_slug}/dataset-version-{version}/"
        f"{_META_FMT.format(db=db_slug, ver=version)}"
    )

    if meta_url in _META_CACHE:
        logger.debug("Using cached metadata for %s", tbl)
        return _META_CACHE[meta_url]

    logger.info("Fetching metadata for %s from %s", tbl, meta_url)
    meta_df = session.get_csv(meta_url)

    if meta_df.empty:
        logger.error("Received empty metadata for table '%s' – aborting", tbl)
        raise RuntimeError(f"Could not download station metadata for table '{tbl}'")

    _META_CACHE[meta_url] = meta_df
    logger.debug("Cached metadata for %s (rows=%d)", tbl, len(meta_df))
    return meta_df

def download_station_year(
    table: str,
    station_id: str,
    year: int,
    *,
    columns: list[str] | None = None,
    session: MidasSession | None = None,
) -> pd.DataFrame:
    """
    Download data for a single station and year, returning a trimmed DataFrame.

    Parameters
    ----------
    table : str
        Key of the MIDAS table to download (must exist in _TABLE_CODES).
    station_id : str
        Identifier of the station to download data for.
    year : int
        Year of the station data to fetch.
    columns : list[str], optional
        Specific columns to retain; defaults to columns from settings.
    session : MidasSession, optional
        Active session for HTTP requests; a new session is created if not provided.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the requested station-year data, limited to specified columns.
    """
    if table not in _TABLE_CODES:
        logger.error("Unknown MIDAS table %s", table)
        raise KeyError(f"Unknown MIDAS table '{table}'")

    session = session or MidasSession()
    version = cfg.midas.version

    meta = _fetch_meta(session, table)
    if meta.empty:
        raise RuntimeError("Could not download station metadata")

    row = meta.set_index("src_id").loc[int(station_id)]
    county = row.historic_county
    fname = row.station_file_name

    data_url = (
        f"{_BASE_URL}/{_TABLE_CODES[table]}/dataset-version-{version}/"
        f"{county}/{station_id}_{fname}/qc-version-1/"
        f"midas-open_{_TABLE_CODES[table]}_dv-{version}_{county}_"
        f"{station_id}_{fname}_qcv-1_{year}.csv"
    )

    df = session.get_csv(data_url, parse_dates=["meto_stmp_time"])

    if df.empty:
        logger.warning(
            "No data for table=%s, station=%s, year=%d", table, station_id, year
        )
        return df
    if columns:
        df = df[columns]
    return df

def download_locations(
    locations: pd.DataFrame | dict[str, tuple[float, float]],
    *,
    years: range,
    tables: dict[str, list[str] ]  = cfg.tables,
    k: int = 3,
    session: MidasSession | None = None,
    out_dir: str | Path | None = settings.cache_dir,
) -> pd.DataFrame:
    """
    Bulk-download data for multiple locations and years by finding nearest stations.

    For each location and year, the k nearest active stations for each table are identified,
    data is downloaded, and a consolidated station map is returned.

    Parameters
    ----------
    locations : pd.DataFrame or dict
        DataFrame with columns ['loc_id', 'lat', 'long'] or dict mapping loc_id to (lat, long).
    years : range
        Range of years for which to download data.
    tables : list[str], optional
        Subset of table keys to process; defaults to all tables in settings.
    columns_per_table : dict, optional
        Mapping from table keys to lists of columns to retain; defaults to settings.
    k : int
        Number of nearest stations to consider per location (default is 3).
    session : MidasSession, optional
        Active session for HTTP requests; created if not provided.
    out_dir : str or Path, optional
        Directory to save downloaded data and metadata; defaults to settings.cache_dir.

    Returns
    -------
    pd.DataFrame
        Consolidated mapping of locations and years to nearest station IDs.
        Columns include 'loc_id', 'year', and one 'src_id_<table>' per table.
    """
    logger.info("Starting bulk download for %d years and %d tables",
                len(years), len(tables or _TABLE_CODES))

    session = session or MidasSession()

    out_dir = Path(out_dir or settings.cache_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(locations, dict):
        loc_df = pd.DataFrame(
            {
                "loc_id": list(locations.keys()),
                "lat": [coords[0] for coords in locations.values()],
                "long": [coords[1] for coords in locations.values()],
            }
        )
    else:
        if not isinstance(locations, pd.DataFrame):
            raise TypeError(
                "`locations` must be a pandas DataFrame or a dict; got "
                f"{type(locations).__name__}."
            )
        if locations.shape[1] < 3:
            raise ValueError(
                "`locations` DataFrame must have at least 3 columns representing "
                "loc_id, lat and long respectively."
            )
        loc_df = locations.iloc[:, :3].copy()
        loc_df.columns = ["loc_id", "lat", "long"]

    if loc_df.empty:
        logger.error("`locations` is empty – nothing to download.")
        raise ValueError("`locations` is empty – nothing to download.")

    logger.debug("Locations to process: %s", loc_df.loc_id.tolist())
    locs_rad = np.deg2rad(loc_df[["lat", "long"]].values)

    rows: dict[tuple[str, int], dict[str, object]] = defaultdict(dict)

    for tbl in tables:
        logger.info("Processing table '%s'", tbl)
        db_slug = _TABLE_CODES[tbl]
        version = cfg.version
        meta_url = (
            f"{_BASE_URL}/{db_slug}/dataset-version-{version}/"
            f"{_META_FMT.format(db=db_slug, ver=version)}"
        )

        meta = session.get_csv(meta_url)
        if meta.empty:
            logger.warning("Empty metadata for %s – skipping", tbl)
            continue

        meta_num = meta[
            ["src_id", "station_latitude", "station_longitude", "first_year", "last_year"]
        ].apply(pd.to_numeric, errors="coerce").dropna()

        sub_tree = BallTree(
            np.deg2rad(meta_num[["station_latitude", "station_longitude"]].values),
            metric="haversine",
        )

        for yr in years:
            logger.debug("Finding nearest stations for year %d (table=%s)", yr, tbl)
            good_mask = (meta_num.first_year <= yr) & (meta_num.last_year >= yr)
            if not good_mask.any():
                logger.debug("No active stations for %s in %d", tbl, yr)
                continue

            sub_meta = meta_num[good_mask]
            sub_tree = BallTree(
                np.deg2rad(sub_meta[["station_latitude", "station_longitude"]].values),
                metric="haversine",
            )
            _, idxs = sub_tree.query(locs_rad, k=k)

            for loc_idx, loc_id in enumerate(loc_df.loc_id):
                key = (loc_id, yr)
                if "loc_id" not in rows[key]:
                    rows[key]["loc_id"] = loc_id
                    rows[key]["year"] = yr

                nearest_station = str(sub_meta.iloc[idxs[loc_idx, 0]]["src_id"])
                rows[key][f"src_id_{tbl}"] = nearest_station
            logger.debug("Mapped nearest stations for %d locations (yr=%d, tbl=%s)",
                         len(loc_df), yr, tbl)

            frames = []
            nearest_srcs = {str(sub_meta.iloc[idx, 0]) for idx in idxs[:, 0]}
            logger.info("Downloading %d station-years for %s in %d", len(nearest_srcs), tbl, yr)

            for src_id in nearest_srcs:
                df = download_station_year(
                    tbl,
                    src_id,
                    yr,
                    columns=tables[tbl],
                    session=session,
                )
                if not df.empty:
                    frames.append(df)

            if frames:
                df_out = pd.concat(frames, ignore_index=True)
                format = settings.cache_format
                if out_dir:
                    write_cache(out_dir / f"{tbl}_{yr}.{format}",df_out)

    consolidated = pd.DataFrame(rows.values()).sort_values(["loc_id", "year"]).reset_index(drop=True)
    if not consolidated.empty():
        json_path = out_dir / "station_map.json"
        logger.info("Writing consolidated station map to %s", json_path)
        write_cache(json_path,consolidated)
    return consolidated
