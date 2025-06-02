## Features
* **MIDAS tables** – supports all hourly / daily rain, temperature, weather, wind, radiation & soil temp tables (`RH`, `RD`, `TD`, `WH`, `WD`, `WM`, `RY`, `SH`).  
* **High-level helpers** – `download_station_year()` for a single station/year, or `download_locations()` to bulk-grab multiple nearest stations for many locations.  
* **Smart caching** – saves Parquet copies to `data/raw/weather/`.  
* **Config as YAML** – tweak dataset version, default columns, cache directory, etc. in `weather/settings.yaml`.  
* **CEDA authentication** – automatically refreshes a bearer token using your `EMAIL` and `CEDA_PWD` env vars.  

---

## Quick start

```bash
pip install git+https://github.com/katielocks/uk-midas-client@v0.1

export CEDA_EMAIL="me@example.com"
export CEDA_PWD="••••••••"