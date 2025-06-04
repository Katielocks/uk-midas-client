## Features
* **MIDAS tables** – supports all hourly / daily rain, temperature, weather, wind, radiation & soil temp tables (`RH`, `RD`, `TD`, `WH`, `WD`, `WM`, `RY`, `SH`).  
* **Helper functions** – `download_station_year()` for a single station/year, or `download_locations()` to bulk-grab multiple nearest stations for many locations.  
* **Cashe Location** – current parquet defaults to `data/raw/weather/` (edit in settings).
* **Cashe Format** - custom cashe format, csv,parquet etc. Defaults to parquet
* **Config: JSON** – tweak dataset version, default columns, cache directory, etc. in `settings.json`.  
* **CEDA auth** – automatically gets a bearer token using your `EMAIL` and `CEDA_PWD` env vars.  

---

## Quick start

```bash
pip install git+https://github.com/Katielocks/uk-midas-client.git

export CEDA_EMAIL="me@example.com"
export CEDA_PWD="••••••••"
