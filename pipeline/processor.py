import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from datetime import datetime
import logging

from pipeline.constants import (
    LONGITUDE_CONSTANT,
    LATITUDE_CONSTANT,
    TIME_CONSTANT,
    H3_INDEX_CONSTANT,
    TP_MM_CONSTANT,
    DATE_CONSTANT,
    TOTAL_DAILY_PRECIPITATIONS_MM_CONSTANT,
)


logger = logging.getLogger()


class Processor:
    """
    A class to process precipitation data and compute total daily precipitation by location.

    Attributes:
        df: Input DataFrame containing precipitation data.
        npartitions: Number of partitions to use for Dask processing.
    """
    def __init__(self, df: pd.DataFrame, npartitions: int):
        self.df = df
        self.npartitions = npartitions

    @staticmethod
    def sum_total_precipitations_by_day(df_chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the total precipitation by day for each unique combination of latitude and longitude.

        Args:
            df_chunk: Chunk of the input DataFrame containing columns 'time', 'latitude', 'longitude', and 'tp_mm'.

        Returns: a pandas DataFrame containing the total precipitation summed by day for
        each unique combination of latitude and longitude.
        """
        df_chunk[TIME_CONSTANT] = pd.to_datetime(df_chunk[TIME_CONSTANT])
        df_chunk[DATE_CONSTANT] = df_chunk[TIME_CONSTANT].dt.date
        result = df_chunk.groupby(
            [DATE_CONSTANT, LATITUDE_CONSTANT, LONGITUDE_CONSTANT, H3_INDEX_CONSTANT]
        )[TP_MM_CONSTANT].sum().reset_index()
        result.columns = [
            DATE_CONSTANT, LATITUDE_CONSTANT, LONGITUDE_CONSTANT,
            H3_INDEX_CONSTANT, TOTAL_DAILY_PRECIPITATIONS_MM_CONSTANT
        ]

        return result

    def compute_daily_total_precipitations(self):
        """
        Compute the total precipitation (tp) by day for each unique combination of latitude and longitude
        using Dask for parallel processing.

        Returns: a pandas DataFrame containing the total precipitation (in mm) summed by day
        for each unique combination of latitude and longitude.
        """
        start_time = datetime.now()
        cluster = LocalCluster()
        client = Client(cluster)

        ddf = dd.from_pandas(self.df, npartitions=self.npartitions)
        result = ddf.map_partitions(self.sum_total_precipitations_by_day).compute()

        client.close()
        cluster.close()

        total_time = datetime.now() - start_time
        total_seconds = total_time.total_seconds()
        logger.info(f'Data Aggregation Step completed in: {total_seconds}')

        return result

    def transformer(self):
        """
        Wrapper of the compute_daily_total_precipitations which transform the input DataFrame
        to compute total daily precipitation by location.
        """
        return self.compute_daily_total_precipitations()
