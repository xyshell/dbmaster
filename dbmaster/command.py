import threading
from typing import Optional, Sequence
from concurrent.futures import ThreadPoolExecutor
import logging

from dbmaster import config
from dbmaster.catalog import CatalogFactory
from dbmaster.vendor import VendorFactory
from dbmaster.util import validate, DateTimeType, to_list


logger = logging.getLogger(__name__)


class Update:
    """Update data catalog from vendor.

    Available Data Catalog:
        - kline: candlestick data (i.e. OHLCV)
    """

    @validate
    def kline(
        self,
        symbol: Optional[str | Sequence[str]],
        vendor: str,
        freq: str,
        datefrom: Optional[DateTimeType] = None,
        dateto: Optional[DateTimeType] = None,
        **kwargs,
    ):
        """Update kline from vendor."""
        logger.info(f"Updating kline for {symbol=}, from {vendor=}, with: {freq=}, {datefrom=}, {dateto=}, {kwargs=}")
        catalog_cls = CatalogFactory.get("kline", vendor)
        vendor_cls = VendorFactory.get(vendor)
        vendor_hanlder = VendorFactory.get_handler(vendor, "kline")
        symbol = to_list(symbol or vendor_cls.universe)

        lock = threading.Lock()

        def task(symbol, freq=freq, datefrom=datefrom, dateto=dateto, kwargs=kwargs):
            data = vendor_hanlder(symbol=symbol, freq=freq, datefrom=datefrom, dateto=dateto, **kwargs)
            with lock:
                return catalog_cls.set(data, symbol=symbol, freq=freq, **kwargs)

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = []
            for sym in symbol:
                future = executor.submit(task, symbol=sym)
                futures.append(future)

            res = [future.result() for future in futures]
        print(f"Done. Returned: {res}")
