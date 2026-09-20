from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="UDP_")

    database_url: str
    sources_dir: Path = Path("sources")
    # Comma-separated; empty means the API is open, which it says once at startup.
    api_keys: str = ""
