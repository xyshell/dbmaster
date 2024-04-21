import os
from typing import Optional, Sequence, Tuple, Type
from pathlib import Path

from pydantic import BaseModel, AfterValidator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, TomlConfigSettingsSource


def _check_path(path: str) -> str:
    parent = str(Path(path).parent)
    assert os.path.exists(parent), f"{parent} not exist"
    return path


class CatalogTypeConfig(BaseModel):
    path: str


class KlineCatalogTypeConfig(CatalogTypeConfig):
    path: str = AfterValidator(_check_path)


class CatalogConfig(BaseModel):
    kline: KlineCatalogTypeConfig


class BinanceConfig(BaseModel):
    api_key: str
    api_secret: str
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None


class VendorConfig(BaseModel):
    binance: Optional[BinanceConfig]


class LoggingConfig(BaseModel):
    version: int = 1
    disable_existing_loggers: bool = False

    formatters: dict[str, dict[str, str]]
    handlers: dict[str, dict[str, str]]
    loggers: dict[str, dict[str, str | Sequence[str] | bool]]
    root: dict[str, str | Sequence[str]]


class Config(BaseSettings):
    catalog: CatalogConfig
    vendor: VendorConfig
    logging: LoggingConfig

    MAX_WORKERS: int = max(int(os.environ.get("DBMASTER_MAX_WORKERS", os.cpu_count())), 1)

    model_config = SettingsConfigDict(
        toml_file=Path(os.environ.get("DBMASTER_CONFIG_PATH", Path(__file__).parent / "config.toml")),
        env_prefix="DBMASTER_",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)


config = Config()
