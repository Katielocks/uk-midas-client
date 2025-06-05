## Features
* **MIDAS tables** – supports all hourly / daily rain, temperature, weather, wind, radiation & soil temp tables (`RH`, `RD`, `TD`, `WH`, `WD`, `WM`, `RY`, `SH`).  
* **Helper functions** – `download_station_year()` for a single station/year, or `download_locations()` to bulk-grab multiple nearest stations for many locations.  
* **Cashe Location** – current config defaults to `data/raw/weather/` (edit in settings).
* **Cashe Format** - customizable cashe format; supports `csv`, `parquet`, `json`, `excel` . Defaults to parquet.
* **Config: JSON** – tweak dataset version, default columns, cache directory, etc. in `settings.json`.  
* **CEDA auth** – automatically gets a bearer token using your `CEDA_USER` and `CEDA_PASS` env vars. 

---

## Quick start
```bash
pip install git+https://github.com/Katielocks/uk-midas-client.git
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
