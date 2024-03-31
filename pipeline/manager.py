import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pipeline.constants import (
    TIME_CONSTANT,
    H3_INDEX_CONSTANT,
)
from pipeline.processor import Processor

from datetime import datetime


class DataManager:
    """
    A class to post-process data and store it as Parquet files.

    Attributes:
        df: Input DataFrame containing data to be processed.
    """
    def __init__(self, df: pd.DataFrame):
        self.data = df

    def transform_data(
            self,
            timestamp_filter: datetime = None,
            h3_index_filter: str = None,
            filter_by_date: bool = False,
    ):
        """
        Transform the input data based on optional filters.

        Args:
            timestamp_filter: Timestamp filter to apply.
            h3_index_filter: H3 index filter to apply.
            filter_by_date: Flag to indicate whether to filter by date only.

        """
        
        if not isinstance(timestamp_filter, datetime) and timestamp_filter is not None:
            raise ValueError('Timestamp filter must be of type datetime')
        
        self.data[TIME_CONSTANT] = pd.to_datetime(self.data[TIME_CONSTANT])
        
        if filter_by_date:
            if timestamp_filter is None:
                raise ValueError('Please Provide a valid datetime')
            self.data = self.data[self.data[TIME_CONSTANT].dt.date == timestamp_filter.date()]
        
        if timestamp_filter and not filter_by_date:
            self.data = self.data[self.data[TIME_CONSTANT] == timestamp_filter]

        if h3_index_filter:
            self.data = self.data[self.data[H3_INDEX_CONSTANT] == h3_index_filter]
        
    def process_data(self):
        """
        Process the transformed data using the Processor class.
        """
        processor = Processor(self.data, 10)
        self.transformed_data = processor.transformer()

    @staticmethod
    def store_as_parquet(df: pd.DataFrame, parquet_schema: pa.Schema, output_file: str):
        """
        Store DataFrame as a Parquet file.

        Args:
            df: DataFrame to store.
            parquet_schema: Schema of the Parquet file.
            output_file: Output file path for the Parquet file.
        """
        table = pa.Table.from_pandas(df, schema=parquet_schema)
        pq.write_table(table, output_file)
