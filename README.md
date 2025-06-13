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
from midas_client import download_station_year
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
## Status
This project is currently in a pre-1.0 prototype stage and may change without notice.

## License
Released under the [MIT License](LICENSE). You are free to use, modify and distribute this software.
