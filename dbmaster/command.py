import threading
from typing import Sequence
from concurrent.futures import ThreadPoolExecutor
import logging

import pandas as pd

from dbmaster import config
from dbmaster.catalog import CatalogFactory
from dbmaster.derived import DerivedFactory
from dbmaster.util import validate, DateTimeType, to_list
from dbmaster.vendor.base import VendorFactory


logger = logging.getLogger(__name__)


class Update:
    """Update data catalog from vendor.

    Available Data Catalog:
        - kline: candlestick data (i.e. OHLCV)
    """

    @validate
    def kline(
        self,
        vendor: str,
        *,
        symbol: str | Sequence[str] | None,
        freq: str,
        datefrom: DateTimeType | None = None,
        dateto: DateTimeType | None = None,
        **kwargs,
    ):
        """Update kline from vendor."""
        logger.info(f"Updating Kline for {symbol=}, from {vendor=}, with: {freq=}, {datefrom=}, {dateto=}, {kwargs=}")
        catalog_cls = CatalogFactory.get("kline", vendor)
        vendor_cls = VendorFactory.get(vendor)
        symbol = to_list(symbol or vendor_cls.universe)

        lock = threading.Lock()

        def task(symbol, freq=freq, datefrom=datefrom, dateto=dateto, kwargs=kwargs):
            data = vendor_cls.get_kline(symbol=symbol, freq=freq, datefrom=datefrom, dateto=dateto, **kwargs)
            with lock:
                return catalog_cls.set(data, symbol=symbol, freq=freq, **kwargs)

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = []
            for sym in symbol:
                future = executor.submit(task, symbol=sym)
                futures.append(future)

            res = [future.result() for future in futures]
        print(f"Done. Returned: {res}")


class Compute:
    """Compute derived data from catalog.

    Available Derived data:
        - pmom: price momentum, price percentage change for BTCUSDT and the rest relative to BTC.
    """

    @validate
    def pmom(
        self,
        vendor: str,
        *,
        symbol: Sequence[str] | None,
        period: Sequence[str],
        step: str,
        datefrom: DateTimeType | None = None,
        dateto: DateTimeType | None = None,
        **kwargs,
    ):
        """Compute price momentum from catalog."""
        logger.info(
            f"Computing Price Momentum for {symbol=}, from {vendor=}, with: {period=}, {step=}, {datefrom=}, {dateto=}, {kwargs=}"
        )
        catalog_cls = CatalogFactory.get("kline", vendor)
        derived_cls = DerivedFactory.get("pmom", vendor)
        symbol = symbol or VendorFactory.get(vendor).universe

        period = to_list(period)
        period_td = [pd.Timedelta(prd) for prd in period]
        period_min, period_max = min(period_td), max(period_td)
        fetch_datefrom = datefrom + period_min
        step_td = pd.Timedelta(step)

        df = catalog_cls.get(symbol=symbol, freq=step, datefrom=fetch_datefrom, dateto=dateto)
        df["Symbol"] = df["Symbol"].str.replace("USDT", "/USDT")
        asof_start = df["OpenTime"].min() - pd.Timedelta(period_min)
        asof_end = df["OpenTime"].max() - pd.Timedelta(period_max)
        assert asof_start <= asof_end, f"{asof_start=} > {asof_end=}"
        asofs = pd.date_range(asof_start, asof_end, freq=step_td)

        lock = threading.Lock()

        def task(asof, df=df):
            logger.debug(f"Computing pmom {asof=} for {symbol=}, across {period=}")
            window = df.loc[(df["OpenTime"] >= asof + period_min) & (df["OpenTime"] <= asof + period_max)]
            df = derived_cls.compute(window, period=period, asof=asof)
            with lock:
                return derived_cls.set(df, **kwargs)

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = []
            for asof in asofs:
                future = executor.submit(task, asof=asof)
                futures.append(future)

            res = [future.result() for future in futures]
        print(f"Done. Returned: {res}")
