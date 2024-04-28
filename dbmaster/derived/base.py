import abc

import pandas as pd

from dbmaster.util import DatasetBase, DatasetFactory


class DerivedBase(DatasetBase):
    @classmethod
    @abc.abstractmethod
    def compute(cls, **kwargs) -> pd.DataFrame:
        pass


class DerivedFactory(DatasetFactory):
    pass
