import datetime as dt
from enum import Enum
from typing import Any, Optional
from typing_extensions import Annotated

from pydantic import AfterValidator, validate_call

import pandas as pd


BINANCE_KLINE_FREQ = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}

validate = validate_call(config={"arbitrary_types_allowed": True})


def to_list(x: Any):
    if x is None:
        return []
    return list(x) if isinstance(x, (list, set, tuple)) else [x]


def check_binance_freq(freq: str):
    assert freq in BINANCE_KLINE_FREQ
    return freq


def check_binance_symbol(symbol: str):
    symbol = symbol.replace("/", "").replace("-", "")
    return symbol.upper()


class IfRowExistsType(Enum):
    RAISE = "raise"
    IGNORE = "ignore"
    INSERT = "insert"
    DROP = "drop"


def check_datetime(datetime: Optional[str | dt.date | dt.datetime | pd.Timestamp]):
    if datetime is None:
        datetime = pd.to_datetime("now")
    return pd.to_datetime(datetime).round("s")


def check_date(date: Optional[str | dt.date | dt.datetime | pd.Timestamp]):
    if date is None:
        date = pd.to_datetime("now")
    return pd.to_datetime(date).normalize()


BinanceSymbolType = Annotated[str, AfterValidator(check_binance_symbol)]
BinanceFreqType = Annotated[str, AfterValidator(check_binance_freq)]
DateTimeType = Annotated[Optional[str | dt.date | dt.datetime | pd.Timestamp], AfterValidator(check_datetime)]
DateType = Annotated[Optional[str | dt.date | dt.datetime | pd.Timestamp], AfterValidator(check_date)]
