# Python Imports
import logging
from typing import List, Optional

import pandas as pd

# Project Imports
from src.analysis.utils import path_utils

logger = logging.getLogger(__name__)


class DataHandler:
    def __init__(self, ignore_columns: Optional[List] = None):
        self._ignore_columns = ignore_columns
        self._dataframe = pd.DataFrame()

    def dump_dataframe(self, dump_path: str):
        result = path_utils.prepare_path_for_file(dump_path)
        if result.is_err():
            raise OSError(result.err_value)

        self._dataframe.to_csv(result.ok_value)
        logger.debug(f"{dump_path} data dumped")

    def concat_data_as_mean(
        self, target_df: pd.DataFrame, data_df: pd.DataFrame, column_name: str
    ) -> pd.DataFrame:
        if self._ignore_columns:
            columns_to_drop = [
                col
                for col in data_df.columns
                if any(col.startswith(prefix) for prefix in self._ignore_columns)
            ]
            logger.info(f"Dropping {len(columns_to_drop)} columns: {columns_to_drop}")
            data_df = data_df.drop(columns=columns_to_drop)

        df_mean = data_df.mean()
        df_mean = pd.DataFrame(df_mean, columns=[column_name])
        target_df = pd.concat([target_df, df_mean], axis=1)

        return target_df

    @property
    def dataframe(self) -> pd.DataFrame:
        return self._dataframe

    @property
    def ignore_columns(self) -> List:
        return self._ignore_columns

    @staticmethod
    def prepare_dataframe_for_boxplot(
        dataframe: pd.DataFrame, class_name: str = "class"
    ) -> pd.DataFrame:
        return pd.melt(dataframe, id_vars=[class_name], var_name="variable", value_name="value")
