from __future__ import annotations
import logging
from pathlib import Path
from collections import defaultdict
from typing import Callable,Union,Dict,Sequence
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .config import settings
from .session import MidasSession
from .io import write_cache

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


log = logging.getLogger(__name__)


def _validate_years(years: range | list[str], version: str | None = None) -> list[str] | None:
    """
    Validate that the requested years exist for the current MIDAS version.
    Parameters
    ----------
    years : range | list[str]
        Range or list of years to check.
    version : str, optional
        MIDAS version in ``YYYYMM`` format. If ``None`` the version from
        :data:`settings` is used.

   Returns
    -------
    list[str] | None
        Filtered list of valid years or ``None`` if none are within range.
    """
    if  settings and settings.midas:
        version = version or settings.midas.version
    max_year = int(version[:4]) - 1

    if any(int(yr) > max_year for yr in years):
        logging.warning(
            "Requested years exceed the dataset limit for MIDAS version %s; "
            "the latest available year is %d.",
            version, max_year
        )

        if all(int(yr) > max_year for yr in years):
            logging.error(
                "All requested years are beyond %d; returning no data.",
                max_year
            )
            return None
        
        filtered = [yr for yr in years if int(yr) <= max_year]
        logging.info(
            "Truncated years to available range: %s",
            ", ".join(filtered)
        )
        return filtered
    
    return list(years)

def _fetch_meta(tbl: str, *, session: MidasSession = None, version: str | None = None) -> pd.DataFrame:
    """Download station metadata for ``tbl`` with caching.

    Parameters
    ----------
    tbl : str
        Table key looked up in :data:`_TABLE_CODES`.
    session : MidasSession, optional
        Active session used for the HTTP requests. A new one is created when
        omitted.
    version : str, optional
        Data version identifier. Defaults to the value from :data:`settings`.

    Returns
    -------
    pandas.DataFrame
        Metadata for all stations in the specified table.
    """

    session = session or MidasSession()
    if settings and settings.midas:
        version = version or settings.midas.version

    db_slug = _TABLE_CODES[tbl]

    meta_url = (
        f"{_BASE_URL}/{db_slug}/dataset-version-{version}/"
        f"{_META_FMT.format(db=db_slug, ver=version)}"
    )

    if meta_url in _META_CACHE:
        log.debug("Using cached metadata for %s", tbl)
        return _META_CACHE[meta_url]

    meta_df = session.get_csv(meta_url)

    if meta_df.empty:
        log.error("Received empty metadata for table '%s' – aborting", tbl)
        raise RuntimeError(f"Could not download station metadata for table '{tbl}'")

    _META_CACHE[meta_url] = meta_df
    log.debug("Cached metadata for %s (rows=%d)", tbl, len(meta_df))
    return meta_df

def download_station_year(
    table: str,
    station_id: str,
    year: int,
    *,
    columns: list[str] | None = None,
    session: MidasSession | None = None,
    version: str | None = None
) -> pd.DataFrame:
    """Download data for a single station and year.

    Parameters
    ----------
    table : str
        Key of the MIDAS table to download.
    station_id : str
        Identifier of the station.
    year : int
        Year of observations to fetch.
    columns : list[str] | None, optional
        Specific columns to retain. If ``None``, columns defined in
        ``settings.midas.tables`` for the given table will be used if
        available. Pass an empty list to keep all columns.

    session : MidasSession, optional
        Session to use for HTTP requests. A new one is created when ``None``.
    version : str, optional
        Data version identifier.

    Returns
    -------
        pd.DataFrame
        DataFrame containing the requested station-year data limited to the
        specified columns.
    """

    year = _validate_years([year])[0]
    if not year:
        logging.error("Valid year provided; returning empty DataFrame.")
        return pd.DataFrame()

        
    if table not in _TABLE_CODES:
        log.error("Unknown MIDAS table %s", table)
        raise KeyError(f"Unknown MIDAS table '{table}'")

    session = session or MidasSession()
    if settings.midas and settings.midas.version:
        version = version or settings.midas.version

    meta = _fetch_meta(table,session=session)
    if meta.empty:
        raise RuntimeError("Could not download station metadata")
    station_id = station_id.zfill(5)
    row = meta.set_index("src_id").loc[station_id]
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
        log.warning(
            "No data for table=%s, station=%s, year=%d", table, station_id, year
        )
        return df
    if columns is None and settings and settings.midas:
        columns = settings.midas.tables.get(table)

    if columns:
        for idx, col in enumerate(("src_id", "meto_stmp_time")):
            if col not in columns:
                columns.insert(idx, col)
        df = df[columns]
    if "src_id" in df.columns:
        df["src_id"] = df["src_id"].astype("Int64").astype(int).astype(str).str.zfill(5)
    return df


def _years_for_row(row, allowed: set[int]) -> list[int]:
    """Return the list of years to download for a metadata row.

    Parameters
    ----------
    row : pandas.Series
        Station metadata row containing ``first_year`` and ``last_year``.
    allowed : set[int]
        Years explicitly requested; an empty set allows all available years.

    Returns
    -------
    list[int]
        Years that should be downloaded for the row.
    """

    span = range(int(row.first_year), int(row.last_year) + 1)
    return [y for y in span if not allowed or y in allowed]


def download_by_counties(
    counties: dict[str, list[str]],
    years: Union[range, Sequence[int]] = None,
    tables: Sequence[str] = settings.midas.tables,
    *,
    out_dir: Union[str, Path] = settings.cache_dir,
    out_fmt: str | None = settings.cache_format,
    session: MidasSession | None = None,
    version: str | None = None
) -> dict[str, dict[str, pd.DataFrame]]:
    """Download data for stations grouped by county.

    Parameters
    ----------
    counties : dict[str, list[str]]
        Mapping of county name to a list of ``src_id`` codes. An empty list
        downloads all stations in the county.
    years : range | Sequence[int] | None, optional
        Years to download. ``None`` downloads all available years per station.
    tables : Sequence[str], optional
        Observation table keys to process.
    out_dir : str | Path, optional
        Directory in which cached files are written.
    out_fmt : str, optional
        Cache file format (e.g. ``'csv'`` or ``'parquet'``).
    session : MidasSession, optional
        Session used for HTTP requests.
    version : str, optional
        Data version identifier.

    Returns
    -------
    dict[str, dict[str, pandas.DataFrame]]
        Mapping of table and county to the filtered station metadata used. """
    if not counties:
        raise ValueError("The 'counties' dictionary must not be empty.")

    out_base = Path(out_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    session = session or MidasSession()

    if years is None:
        years_set = None
    elif isinstance(years, range):
        years_set = set(years)
    elif isinstance(years, (list, tuple, set)):
        years_set = set(int(y) for y in years)
    elif isinstance(years,str):
        years_set = [years]
    else:
        raise ValueError("`years` must be a range, sequence of ints, or None.")



    for tbl in tables:
        try:
            meta = _fetch_meta(tbl, session=session, version=version)
        except Exception as e:
            log.error(f"Failed to fetch metadata for table '{tbl}': {e}")
            continue

        if "historic_county" not in meta.columns:
            log.error(f"Table '{tbl}' metadata missing required column 'historic_county'.")
            continue


        available = set(meta["historic_county"].unique())
        wanted = set(counties.keys()) & available
        if not wanted:
            log.warning(f"No requested counties found in table '{tbl}'. Skipping.")
            continue

        df_filtered = meta[meta["historic_county"].isin(wanted)].copy()
        grouped = df_filtered.groupby("historic_county")


        for county, codes in counties.items():
            out_county_dir = out_base / county
            out_county_dir.mkdir(parents=True, exist_ok=True)

            if county not in grouped.groups:
                log.warning(f"County '{county}' not present in metadata for table '{tbl}'.")
                continue

            df_county = grouped.get_group(county).copy()

            if codes:
                if "src_id" not in df_county.columns:
                    log.error(f"Table '{tbl}' metadata missing 'src_id' column for county '{county}'.")
                    continue

                df_county = df_county[df_county["src_id"].isin(codes)].copy()
                if df_county.empty:
                    log.warning(f"No matching src_id codes {codes} for county '{county}' in table '{tbl}'.")
                    continue

            if {"src_id", "station_latitude", "station_longitude"}.issubset(df_county.columns):
                station_map = df_county[["src_id","station_longitude", "station_latitude"]].reset_index(drop=True)
                map_path = out_county_dir / "station_map.json"
                write_cache(map_path, station_map)
            else:
                log.error(f"Metadata for table '{tbl}', county '{county}' missing latitude/longitude columns.")


            if {"first_year", "last_year"}.issubset(df_county.columns):
                allowed = set(map(int, years_set)) if years_set else None

                df_years = (
                    df_county
                    .assign(year=df_county.apply(_years_for_row, axis=1, allowed=allowed))
                    .explode("year")
                    .dropna(subset=["year"])
                    .astype({"year": int})
                    .loc[:, ["src_id", "year"]]
                    .sort_values(["src_id", "year"], ignore_index=True)
                )


                for year, group in df_years.groupby("year"):

                    cols = tables[tbl] if isinstance(tables, dict) else None

                    frames = [
                        download_station_year(
                            tbl,
                            src_id,
                            year,
                            columns=cols,
                            session=session,
                            version=version,
                        )
                        for src_id in group["src_id"]
                    ]
                    if not frames:
                        continue

                    out_df = pd.concat(frames, ignore_index=True)


                    file_path = out_county_dir / f"{tbl}_{year}.{out_fmt}"
                    write_cache(str(file_path), out_df)



def download_locations(
    locations: pd.DataFrame | dict[str, tuple[float, float]],
    years: range,
    tables: dict[str, list[str] ]  = settings.midas.tables,
    *,
    k: int = 3,
    out_dir: str | Path | None = settings.cache_dir,
    out_fmt: str | None = settings.cache_format,
    session: MidasSession | None = None,
    version: str | None = None
) -> pd.DataFrame | list[pd.DataFrame,list[pd.DataFrame,pd.DataFrame]]:
    """Bulk-download data for multiple locations.

    Parameters
    ----------
    locations : pandas.DataFrame or dict[str, tuple[float, float]]
        Table of ``loc_id``/latitude/longitude or a mapping from ``loc_id`` to
        coordinates.
    years : range
        Years for which to retrieve observations.
    tables : dict[str, list[str]], optional
        Observation tables to query and their column selections.
    k : int, optional
        Number of nearest stations considered for each location. Defaults to ``3``.
    out_dir : str or Path, optional
        Directory used for cached outputs.
    out_fmt : str, optional
        Cache file format.
    session : MidasSession, optional
        Session for HTTP requests.
    version : str, optional
        Data version identifier.

    Returns
    -------
    pandas.DataFrame
        Mapping of location/year pairs to the chosen station IDs. When
        ``out_dir`` is ``None`` the downloaded data frames are also returned.
    """
    years = _validate_years(years)

    if not years:
        logging.error("No valid years provided; returning empty DataFrame.")
        return pd.DataFrame()



    log.info("Starting bulk download for %d years and %d tables",
                len(years), len(tables or _TABLE_CODES))

    session = session or MidasSession()
    if  settings and settings.midas:
        version = version or settings.midas.version

    if out_dir :
        out_dir = Path(out_dir).expanduser()
        out_dir.mkdir(parents=True, exist_ok=True)
        if not out_fmt:
            raise ValueError(f"No file extension found for '{out_dir}'.")

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
        log.error("`locations` is empty - nothing to download.")
        raise ValueError("`locations` is empty - nothing to download.")

    log.debug("Locations to process: %s", loc_df.loc_id.tolist())
    locs_rad = np.deg2rad(loc_df[["lat", "long"]].values)

    rows: dict[tuple[str, int], dict[str, object]] = defaultdict(dict)
    outputs = []
    for tbl in tables:
        log.info("Processing table '%s'", tbl)
        meta = _fetch_meta(tbl,session=session)
        if meta.empty:
            log.warning("Empty metadata for %s - skipping", tbl)
            continue

        meta_num = meta[
            ["src_id", "station_latitude", "station_longitude", "first_year", "last_year"]
        ].apply(pd.to_numeric, errors="coerce").dropna()

        sub_tree = BallTree(
            np.deg2rad(meta_num[["station_latitude", "station_longitude"]].values),
            metric="haversine",
        )

        for yr in years:
            yr = int(yr)
            log.debug("Finding nearest stations for year %d (table=%s)", yr, tbl)
            good_mask = (meta_num.first_year <= yr) & (meta_num.last_year >= yr)
            if not good_mask.any():
                log.debug("No active stations for %s in %d", tbl, yr)
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

                nearest_station = int(sub_meta.iloc[idxs[loc_idx, 0]]["src_id"])
                rows[key][f"src_id_{tbl}"] = str(nearest_station).zfill(5)
            log.debug("Mapped nearest stations for %d locations (yr=%d, tbl=%s)",
                         len(loc_df), yr, tbl)

            frames = []
            nearest_srcs = {str(sub_meta.iloc[idx, 0]) for idx in idxs[:, 0]}
            log.info("Downloading %d station-years for %s in %d", len(nearest_srcs), tbl, yr)
            cols = None
            if isinstance(tables,dict):
                cols = tables[tbl]
            for src_id in nearest_srcs:
                df = download_station_year(
                    tbl,
                    src_id,
                    yr,
                    columns=cols,
                    session=session,
                    version=version
                )
                if not df.empty:
                    frames.append(df)
            if frames:
                df_out = pd.concat(frames, ignore_index=True)
                if out_dir:
                    write_cache(out_dir / f"{tbl}_{yr}.{out_fmt}",df_out)
                else:
                    outputs.append(df_out) 

    consolidated = pd.DataFrame(rows.values()).sort_values(["loc_id", "year"]).reset_index(drop=True)
    if not consolidated.empty and out_dir:
        json_path = out_dir / "station_map.json"
        log.info("Writing station map to %s", json_path)
        write_cache(json_path,consolidated)
        return consolidated
    else:
        return consolidated,outputs
