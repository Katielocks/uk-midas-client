from __future__ import annotations
from pathlib import Path
from collections import defaultdict
from typing import Callable
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from config import settings
from session import MidasSession

_BASE_URL = "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data"
_META_FMT = "midas-open_{db}_dv-{ver}_station-metadata.csv"
_META_CACHE: dict[str, pd.DataFrame] = {}

_OUTPUT_FUNC: dict[str, Callable] = {
    "csv":    pd.DataFrame.to_csv,
    "parquet":pd.DataFrame.to_parquet,
    "json":   pd.DataFrame.to_json,
    "excel":  pd.DataFrame.to_excel
}

def _fetch_meta(session: MidasSession, tbl: str) -> pd.DataFrame:
    db_slug = settings.midas.tables[tbl]
    version = settings.midas.version
    meta_url = (
        f"{_BASE_URL}/{db_slug}/dataset-version-{version}/"
        f"{_META_FMT.format(db=db_slug, ver=version)}"
    )
    if meta_url in _META_CACHE:
        return _META_CACHE[meta_url]

    meta_df = session.get_csv(meta_url)
    if meta_df.empty:
        raise RuntimeError(f"Could not download station metadata for table '{tbl}'")

    _META_CACHE[meta_url] = meta_df
    return meta_df


def download_station_year(
    table: str,
    station_id: str,
    year: int,
    *,
    columns: list[str] | None = None,
    session: MidasSession | None = None,
) -> pd.DataFrame:
    if table not in settings.midas.tables:
        raise KeyError(f"Unknown MIDAS table '{table}'")

    session = session or MidasSession()
    version = settings.midas.version
    cols = columns or settings.midas.columns[table]

    meta = _fetch_meta(session, table)
    if meta.empty:
        raise RuntimeError("Could not download station metadata")

    row = meta.set_index("src_id").loc[int(station_id)]
    county = row.historic_county
    fname = row.station_file_name

    data_url = (
        f"{_BASE_URL}/{settings.midas.tables[table]}/dataset-version-{version}/"
        f"{county}/{station_id}_{fname}/qc-version-1/"
        f"midas-open_{settings.midas.tables[table]}_dv-{version}_{county}_"
        f"{station_id}_{fname}_qcv-1_{year}.csv"
    )

    df = session.get_csv(data_url, parse_dates=["meto_stmp_time"])
    if df.empty:
        return df

    return df[cols]


def download_locations(
    locations: pd.DataFrame | dict[str, tuple[float, float]],
    *,
    years: range,
    tables: list[str] | None = None,
    columns_per_table: dict[str, list[str]] | None = None,
    k: int = 3,
    session: MidasSession | None = None,
    out_dir: str | Path | None = None,
) -> pd.DataFrame:
    
    session = session or MidasSession()
    tables = tables or list(settings.midas.tables)
    cols_cfg = columns_per_table or settings.midas.columns

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
        loc_df = locations.copy()

    if loc_df.empty:
        raise ValueError("`locations` is empty – nothing to download.")

    locs_rad = np.deg2rad(loc_df[["lat", "long"]].values)

    rows: dict[tuple[str, int], dict[str, object]] = defaultdict(dict)

    for tbl in tables:
        db_slug = settings.midas.tables[tbl]
        version = settings.midas.version
        meta_url = (
            f"{_BASE_URL}/{db_slug}/dataset-version-{version}/"
            f"{_META_FMT.format(db=db_slug, ver=version)}"
        )
        meta = session.get_csv(meta_url)
        meta_num = meta[
            ["src_id", "station_latitude", "station_longitude", "first_year", "last_year"]
        ].apply(pd.to_numeric, errors="coerce").dropna()
        for yr in years:
            good_mask = (meta_num.first_year <= yr) & (meta_num.last_year >= yr)
            if not good_mask.any():
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
            frames = []
            nearest_srcs = set(
                str(sub_meta.iloc[idx, 0]) for idx in idxs[:, 0]
            )
            for src_id in nearest_srcs:
                df = download_station_year(
                    tbl,
                    src_id,
                    yr,
                    columns=cols_cfg[tbl],
                    session=session,
                )
                if not df.empty:
                    frames.append(df)

            if frames:
                df_out = pd.concat(frames, ignore_index=True)
                format = settings.cache_format
                if out_dir:
                    _OUTPUT_FUNC[format](df, out_dir / f"{tbl}_{yr}.{format}")

    consolidated = pd.DataFrame(rows.values()).sort_values(["loc_id", "year"]).reset_index(drop=True)

    json_path = out_dir / "station_map.json"
    consolidated.to_json(json_path, orient="records", indent=2)

    return consolidated