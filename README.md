# MIDAS Client
## Features
* **MIDAS tables** – supports all hourly / daily rain, temperature, weather, wind, radiation & soil temp tables (`RH`, `RD`, `TD`, `WH`, `WD`, `WM`, `RY`, `SH`).  
* **Helper functions** – `download_station_year()` for a single station/year, `download_locations()` to bulk-grab multiple nearest stations for many locations, and `download_by_counties()` for downloading groups of counties.
 
* **Cache Location** – current config defaults to `data/raw/weather/` (edit in settings).
* **Cache Format** - customizable cache format; supports `csv`, `parquet`, `json`. Defaults to csv.
* **Config: JSON** – tweak dataset version, default columns, cache directory, etc. in `settings.json`.  
* **CEDA auth** – automatically gets a bearer token using your `CEDA_USER` and `CEDA_PASS` env vars. 

---
### Finding a station ID (`src_id`)

The Met Office assigns every MIDAS station a **five-digit source-ID** (e.g. `03743`).  
You can look it up in three quick ways:

1. **CEDA MIDAS Station Search (recommended)**  
   * Open <https://archive.ceda.ac.uk/tools/midas_stations> – no login required.
   * In **“Search for station name”** type part of the name (e.g. *Oxford*) and press **Search**.  
     The first column of the results table is the `src_id`.  
   * Use the check-box **“Show only stations with MIDAS Open data”** or restrict the **year range** / **current stations only** options to narrow results.  
   * If you do not know the station name, switch to the **postcode** or **county** tabs on the same page. 
2. **Interactive maps**  
   * CEDA also publishes KML layers for **Google Earth** – each marker popup shows the `src_id`. Download the layers from the same tools section. 



## Quick start
```bash
pip install uk-midas-client
```
```python
from midas_client import (
    download_station_year,
    download_locations,
    download_by_counties,
)
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
df = download_station_year(
    table="TD",
    station_id="03743",
    year=2020,
)
print(df.head())
```

### Bulk Download Nearest Stations

Given a DataFrame containing observation locations with associated latitude and longitude coordinates, the algorithm:

1. **Identifies the nearest MIDAS stations**:

   * Finds the **k-nearest MIDAS stations** for each observation location that support a specified observation type (e.g., Rain Hourly — `RH`).

2. **Downloads datasets**:

   * Attempts to download datasets for each station-year combination, prioritizing nearest stations.

3. **Fallback mechanism**:

   * If a dataset for the closest station-year combination is unavailable, the algorithm sequentially attempts downloads from the next nearest stations until either:

     * A valid dataset is successfully retrieved, or
     * All **k** nearest stations have been attempted unsuccessfully.

### Caching and Output Structure

The resulting datasets are stored in a specified cache directory (`cache_dir`), following the naming convention:

```
{obs}_{year}.{fmt}
```

Additionally, the process generates a JSON mapping file (`station_map.json`) within `cache_dir`. This file maps each observation location's input `loc_id` to the corresponding downloaded station identifiers.

### Example Usage

```python
import pandas as pd

locs = pd.DataFrame({
    "loc_id": ["here"],
    "lat": [51.5],
    "long": [-0.1],
})

station_map = download_locations(
    locs,
    locations=locs,
    years=range(2021, 2022),
    tables={"TD": ["max_air_temp", "min_air_temp"]},
)

results = download_by_counties(
    counties={"Hampshire": []},
   tables={"TD": ["max_air_temp", "min_air_temp"]},
```
## Status
This project is currently in a pre-1.0 prototype stage and may change without notice.

## License
Released under the [MIT License](LICENSE). You are free to use, modify and distribute this software.
