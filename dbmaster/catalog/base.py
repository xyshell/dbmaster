import abc
import functools
from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa

from dbmaster.util import to_list


class CatalogBase(abc.ABC):
    table: sa.Table | Sequence[sa.Table] = NotImplemented

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        cls.metadata = sa.MetaData()
        if cls.table is NotImplemented:
            raise NotImplementedError("table is not implemented")

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


class CatalogFactory:
    @classmethod
    def get(cls, name: str, vendor: str) -> CatalogBase:
        import dbmaster.catalog as catalog

        catalog_cls_name = f"{name.capitalize()}{vendor.capitalize()}"
        catalog_cls = getattr(catalog, catalog_cls_name)
        return catalog_cls
