"""
Price Momentum Implementation
"""

import logging
import time
from typing import Sequence

import pandas as pd
from retry import retry
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Float, String

from dbmaster import config
from dbmaster.derived.base import DerivedBase
from dbmaster.util import (
    BinanceFreqType,
    BinanceCurrencyType,
    PeriodType,
    validate,
    DateTimeType,
    IfRowExistsType,
    to_list,
)


logger = logging.getLogger(__name__)

engine = sa.create_engine(f"sqlite:///{config.derived.pmom.path}")
metadata = sa.MetaData()


@validate
def normalize(
    df: pd.DataFrame,
    value_col: str = "Open",
    date_col: str = "OpenTime",
    symbol_col: str = "Symbol",
    to_symbol: str = "BTC/USDT",
) -> pd.DataFrame:
    """Normalize the values in the DataFrame by a reference symbol. i.e. compute ETH/BTC from BTC/USDT and ETH/USDT
    Args:
        value_col (str): the column to be normalized. Default is "Open".
        date_col (str): date column to align. Default is "OpenTime".
        symbol_col (str): column to group by. Default is "Symbol".
        to_symbol (str): the group in the symbol_col column to normalize to. Default is "BTC/USDT".
    Returns:
        pd.DataFrame: The normalized DataFrame.
    """
    base_value = df.loc[df[symbol_col] == to_symbol].set_index(date_col)
    other_value = df.loc[df[symbol_col] != to_symbol].set_index(date_col)
    other_value = other_value.join(base_value[value_col].rename("demo"))
    other_value["norm"] = other_value[value_col] / other_value["demo"]
    other_value["Symbol"] = other_value["Symbol"].str.split("/").str[0] + "/" + to_symbol.split("/")[0]
    other_value = other_value.drop(columns=["demo", value_col]).rename(columns={"norm": value_col})
    df = pd.concat([base_value, other_value]).reset_index()
    return df


@validate
def pct_change(
    df: pd.DataFrame, value_col: str = "Open", date_col: str = "OpenTime", symbol_col: str = "Symbol"
) -> pd.Series:
    """Calculate the percentage change for each group, between the first and last value.
    Args:
        value_col (str): value column to compute pct change on.  Default is "Open".
        date_col (str): date column to sort by.  Default is "OpenTime".
        symbol_col (str): column to group by. Default is "Symbol".
    Returns:
        pd.Series: The percentage change for each group.
    """
    df = df.sort_values(by=date_col, kind="stable", ignore_index=True)
    value_first = df.groupby(symbol_col, sort=False)[value_col].first()
    value_last = df.groupby(symbol_col, sort=False)[value_col].last()
    value_chg = value_last / value_first - 1
    return value_chg


@validate
def momentum(
    df: pd.DataFrame,
    period: str | list[str],
    asof: DateTimeType | None = None,
    date_col: str = "OpenTime",
    value_col: str = "Open",
    symbol_col: str = "Symbol",
) -> pd.DataFrame:
    df = df.copy()

    df[date_col] = pd.to_datetime(df[date_col])
    if asof is None:
        asof = df[date_col].max()

    periods = to_list(period)
    tds = [pd.Timedelta(p) for p in periods]
    cutoffs = [asof + t for t in tds]

    assert (
        df[date_col].min() <= min(cutoffs) <= max(cutoffs) <= df[date_col].max()
    ), "cutoff must be bounded by the data range"

    momentums = []
    for period, cutoff in zip(periods, cutoffs):
        if cutoff <= asof:
            window = df.loc[(df[date_col] >= cutoff) & (df[date_col] <= asof)]
        else:
            window = df.loc[(df[date_col] <= cutoff) & (df[date_col] >= asof)]
        if window.shape[0] <= df[symbol_col].nunique():
            raise ValueError(f"no data found for {period} asof={asof} to calculate momentum")
        momentum = pct_change(window, value_col, date_col, symbol_col).rename(period)
        momentums.append(momentum)
    momentum = pd.concat(momentums, axis=1)
    return momentum


class PmomBinance(DerivedBase):
    table = sa.Table(
        "pmom_binance",
        metadata,
        Column("Timestamp", DateTime, primary_key=True),
        Column("Symbol", String, primary_key=True),
        Column("Period", String, primary_key=True),
        Column("Pmom", Float),
    )

    @classmethod
    def __initialize__(cls, engine=engine, metadata=metadata) -> None:
        cls.engine = engine
        cls.metadata = metadata

    @classmethod
    @validate
    def get(
        cls,
        symbol: BinanceCurrencyType | Sequence[BinanceCurrencyType] | None = None,
        period: PeriodType | Sequence[PeriodType] | None = None,
        datefrom: DateTimeType | None = None,
        dateto: DateTimeType | None = None,
    ) -> pd.DataFrame:
        table = cls.get_table("pmom_binance")

        sql = sa.select(*table.columns)
        sql = sql.where(table.c.Symbol.in_(to_list(symbol))) if symbol else sql
        sql = sql.where(table.c.Timestamp >= datefrom.to_pydatetime()) if datefrom else sql
        sql = sql.where(table.c.Timestamp <= dateto.to_pydatetime()) if dateto else sql
        sql = sql.where(table.c.Period.in_(to_list(period))) if period else sql
        df = pd.read_sql(sql, con=engine)
        return df

    @classmethod
    @validate
    @retry(sa.exc.OperationalError, tries=3, logger=logger)
    def set(cls, df: pd.DataFrame, if_row_exists: IfRowExistsType = IfRowExistsType.INSERT, **kwargs) -> None:
        logger.debug(f"{cls.__name__}.set({df.shape=}, {if_row_exists=})")
        table_name = "pmom_binance"
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
                sql = sa.select(table.c["Timestamp"], table.c["Symbol"], table.c["Period"]).where(
                    table.c["Timestamp"] == df["Timestamp"].iloc[0]
                )
                existed = pd.read_sql(sql, con=engine)
                existed["existed"] = True

                df = df.merge(existed, how="left", on=["Timestamp", "Symbol", "Period"])
                df = df[df["existed"].isna()].drop(columns="existed")
                res = df.to_sql(table_name, con=engine, if_exists="append", index=False)
                logger.info(f"Inserted {res} rows to {table_name}. Timestamp={df["Timestamp"].iloc[0]}")
            elif if_row_exists is IfRowExistsType.DROP:
                table = cls.get_table(table_name)
                sql = (
                    table.delete()
                    .where(table.c["Timestamp"] == df["Timestamp"].iloc[0])
                    .where(table.c["Period"].in_(df["Period"].unique()))
                    .where(table.c["Symbol"].in_(df["Symbol"].unique()))
                )
                with cls.engine.connect() as conn:
                    res_del = conn.execute(sql)
                    conn.commit()
                res_inc = df.to_sql(table_name, con=engine, if_exists="append", index=False)
                logger.info(
                    f"Dropped {res_del.rowcount} rows from {table_name}, before inserting {res_inc} rows. Timestamp={df["Timestamp"].iloc[0]}"
                )
            else:
                raise ValueError(f"Invalid {if_row_exists=}.")
        except sa.exc.OperationalError as e:
            time.sleep(5)
            raise Exception(str(e)[:80] + " ...") from e
        else:
            logger.info(f"Inserted {res} rows to {table_name}. Timestamp={df["Timestamp"].iloc[0]}")

    @classmethod
    @validate
    def compute(cls, df: pd.DataFrame, period: list[str], asof: DateTimeType) -> pd.DataFrame:
        norm = normalize(df)
        pmom = momentum(norm, period, asof=asof)
        pmom = pmom.melt(ignore_index=False, var_name="Period", value_name="Pmom")
        pmom["Timestamp"] = asof
        pmom = pmom.reset_index()
        return pmom


class Pmom:
    @classmethod
    @validate
    def get(
        cls,
        vendor: str,
        *,
        symbol: str | Sequence[str],
        period: str | Sequence[str],
        datefrom: DateTimeType | None = None,
        dateto: DateTimeType | None = None,
    ) -> pd.DataFrame:
        if vendor == "binance":
            df = PmomBinance.get(symbol=symbol, period=period, datefrom=datefrom, dateto=dateto)
        else:
            raise NotImplementedError(f"{vendor} is not implemented")
        return df


__all__ = ["Pmom", "PmomBinance"]
