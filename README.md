# MIDAS Client
## Features
* **MIDAS tables** – supports all hourly / daily rain, temperature, weather, wind, radiation & soil temp tables (`RH`, `RD`, `TD`, `WH`, `WD`, `WM`, `RY`, `SH`).  
* **Helper functions** – `download_station_year()` for a single station/year, or `download_locations()` to bulk-grab multiple nearest stations for many locations.  
* **Cache Location** – current config defaults to `data/raw/weather/` (edit in settings).
* **Cache Format** - customizable cashe format; supports `csv`, `parquet`, `json` . Defaults to csv.
* **Config: JSON** – tweak dataset version, default columns, cache directory, etc. in `settings.json`.  
* **CEDA auth** – automatically gets a bearer token using your `CEDA_USER` and `CEDA_PASS` env vars. 

---

## Quick start
```bash
pip install uk-midas-client
```
```python
from midas_client import download_station_year, download_locations
```

Set your CEDA credentials using either username/password:

```bash
export CEDA_USER="me@example.com"
export CEDA_PASS="••••••••"
```

**and/or** use a token:

```bash
export CEDA_TOKEN="••••••••..."
```

### Fetch a single station-year

```python
df = download_station_year("TD", station_id="03743", year=2020)
print(df.head())
```

### Bulk download nearest stations

Given a DataFrame containing observation IDs with associated latitude and longitude coordinates, the algorithm:

1. Identifies the **k-nearest MIDAS stations** for each observation location that support a specified observation type (e.g., Rain Hourly —`RH`).

2. Attempts to download the corresponding datasets (by station and year), prioritizing proximity.

3. Implements a fallback mechanism:
   - If a dataset for the nearest observation-station–year combination is unavailable, it sequentially attempts downloads from the next-nearest stations until either:
     - A valid dataset is retrieved, or
     - All **k** nearest stations have been attempted unsuccessfully.

returns cache files as `obs`_`year` in cache_dir, with a `station_map.json` mapping `src_ids` for each obs to each location code

```python
import pandas as pd

locs = pd.DataFrame({
    "loc_id": ["here"],
    "lat": [51.5],
    "long": [-0.1],
})

station_map = download_locations(
    locs,
    years=range(2021, 2022),
    tables={"TD": ["max_air_temp","min_air_temp"]}
)
```
## Status
This project is currently in a pre-1.0 prototype stage and may change without notice.

## License
Released under the [MIT License](LICENSE). You are free to use, modify and distribute this software.
