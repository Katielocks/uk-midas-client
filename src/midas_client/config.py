from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any
import json

@dataclass(frozen=True)
class MidasCfg:
    version: str
    tables: Dict[str, List[str]]

@dataclass(frozen=True)
class Settings:
    cache_format:str
    cache_dir: Path
    midas: MidasCfg

def _loadSettings() -> Settings:

    base_dir = Path(__file__).parent
    settings_path = base_dir / "settings.json"
    if not settings_path.exists():
        raise FileNotFoundError(f"Cannot find settings.json at {settings_path}")

    raw: dict[str, Any] = json.loads(settings_path.read_text())

    cache_dir_str = raw.get("cache_dir")
    if not cache_dir_str or not isinstance(cache_dir_str, str):
        raise RuntimeError("Missing or invalid 'cache_dir' in settings.json")
    
    cache_format_str = raw.get("cache_format")
    if not cache_format_str or not isinstance(cache_format_str, str):
        raise RuntimeError("Missing or invalid 'cache_format' in settings.json")
    
    midas_dict = raw.get("midas")
    if not isinstance(midas_dict, dict):
        raise RuntimeError("Missing or invalid 'midas' section in settings.json")

    version = midas_dict.get("version")
    tables  = midas_dict.get("tables")

    if not isinstance(version, str):
        raise RuntimeError("Missing or invalid 'version' under midas in settings.json")
    if not isinstance(tables, dict) \
    or not all(
        isinstance(k, str)                          
        and isinstance(v, list)                    
        and all(isinstance(col, str) for col in v)  
        for k, v in tables.items()
    ):
        raise RuntimeError("Missing or invalid 'tables' mapping under midas in settings.json")

    midas_cfg = MidasCfg(
        version=version,
        tables=tables
    )

    return Settings(
        cache_format= cache_format_str,
        cache_dir=Path(cache_dir_str),
        midas=midas_cfg,
    )


settings: Settings = _loadSettings()
