import abc
from typing import Callable

import pandas as pd

from dbmaster.util import get_subclasses, to_snake_str


class VendorBase:
    name: str = NotImplemented

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        cls.has = set()
        if "name" not in cls.__dict__:
            cls.name = to_snake_str(cls.__name__)

    @classmethod
    def get_catalog(cls, catalog: str) -> Callable:
        func_name = f"get_{catalog}"
        return getattr(cls.get(catalog), func_name)


class KlineVendorBase(VendorBase, abc.ABC):
    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        cls.has.add("kline")

        if "universe" not in cls.__dict__:  # abstract class property
            raise NotImplementedError("universe is not implemented")

    @classmethod
    @abc.abstractmethod
    def get_kline(cls, *args, **kwargs) -> pd.DataFrame:
        pass


class VendorFactory:
    @classmethod
    def get(cls, name: str) -> Callable:
        for sub_cls in get_subclasses(VendorBase):
            if sub_cls.name == name:
                return sub_cls


__all__ = ["VendorBase", "KlineVendorBase", "VendorFactory"]
