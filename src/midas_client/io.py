from __future__ import annotations
import logging
from pathlib import Path
from typing import Callable, Union
import pandas as pd

log = logging.getLogger(__name__)

_OUTPUT_WRITERS: dict[str, Callable[..., None]] = {
    "csv":     lambda df, p, **kwargs: df.to_csv(p, **kwargs),
    "parquet": lambda df, p, **kwargs: df.to_parquet(p, **kwargs),
    "json":    lambda df, p, **kwargs: df.to_json(p,orient="records", indent=2,**kwargs),
}

def _get_fmt(path: Union[str,Path]) -> str:
    suffixes = path.suffixes
    if len(suffixes) == 1:                               
        fmt = suffixes[0].lstrip(".").lower()
    elif len(suffixes) > 1:                               
        fmt = suffixes[-2].lstrip(".").lower()
    return fmt


def write_cache(cache_path: Union[str, Path], df: pd.DataFrame, mdir: bool = True, **kwargs) -> None:
    """Write *df* to *cache_path* in the appropriate format.

    Parameters
    ----------
    cache_path : str | Path
        Destination file path for the cache.
    df : pandas.DataFrame
        Data to be cached.
    mdir : bool, optional
        Create parent directories when ``True`` (default).
    """

    cache_path = Path(cache_path)
    log.info("Writing cache to %s", cache_path)
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
    cache_fmt = _get_fmt(cache_path)
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
        _OUTPUT_WRITERS[cache_fmt](df, cache_path, **kwargs)
    except Exception as e:
        raise IOError(
            f"Failed to write DataFrame to '{cache_path}' as {cache_fmt}: {e}"
        ) from e

