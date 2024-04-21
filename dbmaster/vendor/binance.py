import functools
from collections import defaultdict
from typing import Optional
import logging

import pandas as pd
import requests
from retry import retry

from dbmaster import config
from dbmaster.util import BinanceFreqType, BinanceSymbolType, DateTimeType, validate
from dbmaster.vendor.base import KlineVendorBase

from binance import Client
from binance.enums import HistoricalKlinesType


logger = logging.getLogger(__name__)


class Binance(KlineVendorBase):
    """Binance API Wrapper"""

    @classmethod
    @property
    @functools.cache
    def client(cls):
        http_proxy = config.vendor.binance.http_proxy
        https_proxy = config.vendor.binance.https_proxy
        param = defaultdict(dict)
        if http_proxy:
            param.update({"http": http_proxy})
        if https_proxy:
            param.update({"https": https_proxy})

        return Client(config.vendor.binance.api_key, config.vendor.binance.api_secret, {"proxies": param})

    @classmethod
    @property
    @functools.cache
    def universe(cls) -> tuple[BinanceSymbolType, ...]:
        all_usdt_symbols = [
            symbol["symbol"]
            for symbol in cls.client.get_exchange_info()["symbols"]
            if symbol["symbol"].endswith("USDT")
        ]
        return all_usdt_symbols

    @classmethod
    @validate
    @retry(
        (requests.exceptions.ReadTimeout, requests.exceptions.ProxyError, requests.exceptions.ConnectionError), tries=3
    )
    def get_kline(
        cls,
        symbol: BinanceSymbolType,
        freq: BinanceFreqType,
        datefrom: Optional[DateTimeType] = None,
        dateto: Optional[DateTimeType] = None,
        closed_only: bool = True,
        klines_type: str | HistoricalKlinesType = HistoricalKlinesType.SPOT,
        **kwargs,
    ):
        """Get Kline data from Binance API.
        Args:
            symbol (str): Symbol name e.g. BTC/USDT.
            freq (str): Kline interval e.g. 1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M.
            datefrom (Optional[DateTimeType]): Date from. Defaults to None.
            dateto (Optional[DateTimeType]): Date to. Defaults to None.
            closed_only (bool): If True, only closed klines will be returned.
            klines_type (str | HistoricalKlinesType): Defaults to HistoricalKlinesType.SPOT
                SPOT = 1; FUTURES = 2; FUTURES_COIN = 3
            **kwargs: igored
        Returns:
            pd.DataFrame: DataFrame of Kline data.
        """
        start_str = datefrom.strftime("%Y-%m-%d %H:%M:%S") if datefrom else None
        end_str = dateto.strftime("%Y-%m-%d %H:%M:%S") if dateto else None
        kline = cls.client.get_historical_klines(symbol, freq, start_str, end_str, klines_type=klines_type)

        df = pd.DataFrame(
            kline,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )
        df = df[["timestamp", "close_time", "open", "high", "low", "close", "volume", "quote_asset_volume"]]
        df["timestamp"] = pd.to_datetime(df["timestamp"] // 1000, unit="s")
        df["close_time"] = pd.to_datetime(df["close_time"] // 1000, unit="s")
        df[["open", "high", "low", "close", "volume", "quote_asset_volume"]] = df[
            ["open", "high", "low", "close", "volume", "quote_asset_volume"]
        ].astype("float")
        df = df.rename(
            columns={
                "timestamp": "OpenTime",
                "close_time": "CloseTime",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "BaseVolume",
                "quote_asset_volume": "QuoteVolume",
            }
        )
        df["Symbol"] = symbol
        df = df.sort_values(["Symbol", "OpenTime"], ignore_index=True)
        df = df.iloc[:-1] if closed_only else df
        logger.debug(
            f"Got {freq=} {df['Symbol'].iloc[0]}({df['OpenTime'].iloc[0]} - {df['CloseTime'].iloc[-1]}), {df.shape[0]} rows."
        )
        return df


__all__ = ["Binance"]
