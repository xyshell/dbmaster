import datetime as dt
from enum import Enum
import functools
from typing import Any, Sequence
from typing_extensions import Annotated
import re
import abc

import sqlalchemy as sa
from pydantic import AfterValidator, validate_call

import pandas as pd


BINANCE_KLINE_FREQ = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}

validate = validate_call(config={"arbitrary_types_allowed": True})


def to_list(x: Any):
    if x is None:
        return []
    return list(x) if isinstance(x, (list, set, tuple)) else [x]


def to_camel_str(snake_str: str):
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


def to_snake_str(camel_str: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", camel_str).lower()


def get_subclasses(cls: type):
    for subclass in cls.__subclasses__():
        yield from get_subclasses(subclass)
        yield subclass


def _check_binance_freq(freq: str):
    assert freq in BINANCE_KLINE_FREQ
    return freq


def _check_binance_symbol(symbol: str):
    symbol = symbol.replace("/", "").replace("-", "")
    return symbol.upper()


def _check_binance_currency(currency: str):
    if currency.endswith("USDT") and "/" not in currency:
        currency = currency.replace("USDT", "/USDT")
    elif currency.endswith("BTC") and "/" not in currency:
        currency = currency.replace("BTC", "/BTC")
    assert "/" in currency and len(currency.split("/")) == 2, f"Invalid {currency=}"
    return currency


class VendorPurposeType(Enum):
    RAW = "raw"
    Catalog = "catalog"
    Derived = "derived"


class IfRowExistsType(Enum):
    RAISE = "raise"
    IGNORE = "ignore"
    INSERT = "insert"
    DROP = "drop"


def _check_datetime(datetime: str | dt.date | dt.datetime | pd.Timestamp | None):
    if datetime is None:
        datetime = pd.to_datetime("now")
    return pd.to_datetime(datetime).round("s")


def _check_date(date: str | dt.date | dt.datetime | pd.Timestamp | None):
    if date is None:
        date = pd.to_datetime("now")
    return pd.to_datetime(date).normalize()


def _check_period(period: str):
    if not period.startswith("-") and not period.startswith("+"):
        period = f"+{period}"
    assert period.startswith("+") or period.startswith("-"), "period must start with + or -"
    return period


BinanceSymbolType = Annotated[str, AfterValidator(_check_binance_symbol)]
BinanceCurrencyType = Annotated[str, AfterValidator(_check_binance_currency)]
BinanceFreqType = Annotated[str, AfterValidator(_check_binance_freq)]
DateTimeType = Annotated[str | dt.date | dt.datetime | pd.Timestamp | None, AfterValidator(_check_datetime)]
DateType = Annotated[str | dt.date | dt.datetime | pd.Timestamp | None, AfterValidator(_check_date)]
PeriodType = Annotated[str, AfterValidator(_check_period)]


class DatasetBase(abc.ABC):
    name: str = NotImplemented  # dataset name. e.g. kline
    vendor: str = NotImplemented  # vendor that implemented the dataset. e.g. binance
    table: sa.Table | Sequence[sa.Table] = NotImplemented  # table(s) in the dataset

    names = set()

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        bypass = {"CatalogBase", "DerivedBase"}
        if cls.__name__ in bypass:
            return

        catalog, vendor = to_snake_str(cls.__name__).split("_", maxsplit=1)

        if cls.name is NotImplemented:
            cls.name = catalog
        if cls.vendor is NotImplemented:
            cls.vendor = vendor
        if cls.table is NotImplemented:
            raise NotImplementedError("table is not implemented")

        assert cls.name not in cls.names, f"{cls.name} already exists"
        cls.names.add(cls.name)

        cls.__initialize__()

        for table in to_list(cls.table):
            table.metadata = cls.metadata
        cls.metadata.create_all(cls.engine, checkfirst=True)

    @classmethod
    @abc.abstractmethod
    def __initialize__(cls, engine: sa.engine.Engine) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def get(cls, **kwargs) -> pd.DataFrame:
        pass

    @classmethod
    @abc.abstractmethod
    def set(cls, df: pd.DataFrame, **kwargs) -> Any:
        pass

    @classmethod
    @functools.cache
    def get_table(cls, name: str) -> sa.Table:
        cls.metadata.reflect(cls.engine)
        return cls.metadata.tables[name]


class DatasetFactory:
    @classmethod
    def get(cls, name: str, vendor: str) -> DatasetBase:
        from dbmaster.vendor import VendorFactory

        vendor_name = VendorFactory.get(vendor).name
        for sub_cls in get_subclasses(DatasetBase):
            if sub_cls.name == name and sub_cls.vendor == vendor_name:
                return sub_cls
        raise ValueError(f"Dataset={name} not implemented for {vendor_name}")
