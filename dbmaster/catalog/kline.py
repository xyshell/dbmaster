import logging
import time
from typing import Sequence

import pandas as pd
from retry import retry
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Float, String

from dbmaster import config
from dbmaster.util import (
    validate,
    to_list,
    DateTimeType,
    BinanceSymbolType,
    BinanceFreqType,
    BINANCE_KLINE_FREQ,
    IfRowExistsType,
)
from dbmaster.catalog.base import CatalogBase

logger = logging.getLogger(__name__)

engine = sa.create_engine(f"sqlite:///{config.catalog.kline.path}")
metadata = sa.MetaData()


class KlineBinance(CatalogBase):
    table = [
        sa.Table(
            f"kline_binance_{freq}",
            metadata,
            Column("Symbol", String, primary_key=True),
            Column("OpenTime", DateTime, primary_key=True),
            Column("CloseTime", DateTime),
            Column("Open", Float),
            Column("High", Float),
            Column("Low", Float),
            Column("Close", Float),
            Column("BaseVolume", Float),
            Column("QuoteVolume", Float),
        )
        for freq in BINANCE_KLINE_FREQ
    ]

    @classmethod
    def __initialize__(cls, engine=engine) -> None:
        cls.engine = engine

    @classmethod
    @validate
    def get(
        cls,
        symbol: BinanceSymbolType | Sequence[BinanceSymbolType],
        freq: BinanceFreqType,
        datefrom: DateTimeType | None = None,
        dateto: DateTimeType | None = None,
        column: str | list[str] = None,
    ) -> pd.DataFrame:
        table = cls.get_table(f"kline_binance_{freq}")

        column = to_list(column) or [col.name for col in table.columns]
        sql = sa.select(*[table.c[col] for col in column])
        sql = sql.where(table.c.Symbol.in_(to_list(symbol))) if symbol else sql
        sql = sql.where(table.c.OpenTime >= datefrom.to_pydatetime()) if datefrom else sql
        sql = sql.where(table.c.OpenTime <= dateto.to_pydatetime()) if dateto else sql
        df = pd.read_sql(sql, con=engine)
        return df

    @classmethod
    @validate
    @retry(sa.exc.OperationalError, tries=3, logger=logger)
    def set(
        cls,
        symbol: BinanceSymbolType,
        freq: BinanceFreqType,
        df: pd.DataFrame,
        if_row_exists: IfRowExistsType = IfRowExistsType.INSERT,
        **kwargs,
    ) -> None:
        logger.debug(f"KlineBinance.set({symbol=}, {freq=}, {df.shape=}, {if_row_exists=})")
        table_name = f"kline_binance_{freq}"
        try:
            res = df.to_sql(table_name, con=engine, if_exists="append", index=False)
        except sa.exc.IntegrityError as e:
            logger.debug(f"{type(e)}({e})"[:80] + "...")
            if if_row_exists is IfRowExistsType.RAISE:
                raise
            elif if_row_exists is IfRowExistsType.IGNORE:
                return
            elif if_row_exists is IfRowExistsType.INSERT:
                table = cls.get_table(table_name)
                sql = (
                    sa.select(table.c["OpenTime"])
                    .where(table.c["Symbol"] == symbol)
                    .where(table.c["OpenTime"] >= df["OpenTime"].min())
                )
                existed = pd.read_sql(sql, con=engine)

                df = df[~df["OpenTime"].isin(existed["OpenTime"])]
                res = df.to_sql(table_name, con=engine, if_exists="append", index=False)
                logger.info(f"Inserted {res} rows to {table_name}.")
            elif if_row_exists is IfRowExistsType.DROP:
                table = cls.get_table(table_name)
                sql = table.delete().where(table.c["Symbol"] == symbol).where(table.c["OpenTime"].in_(df["OpenTime"]))
                with cls.engine.connect() as conn:
                    res_del = conn.execute(sql)
                    conn.commit()
                res_inc = df.to_sql(table_name, con=engine, if_exists="append", index=False)
                logger.info(f"Dropped {res_del.rowcount} rows from {table_name}, before inserting {res_inc} rows.")
        except sa.exc.OperationalError as e:
            time.sleep(5)
            raise Exception(str(e)[:80] + " ...") from e
        else:
            logger.info(f"Inserted {res} rows to {table_name}.")


__all__ = ["KlineBinance"]
