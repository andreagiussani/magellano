import gc
from datetime import datetime
from typing import Union

import pandas as pd

from pipeline.constants import RAW_ERA5_PARQUET_SCHEMA_CONSTANT, PROCESSED_ERA5_PARQUET_SCHEMA_CONSTANT
from pipeline.loader import DataLoader
from pipeline.manager import DataManager

import logging

logger = logging.getLogger()


class DataOrchestrator:
    """
    Orchestrates the data loading, processing, and storing pipeline.

    Attributes:
        h3_indexes_df: DataFrame containing H3 indexes.
        start_day: Start day for data loading.
        end_day: End day for data loading.
        batch_size: Size of each data loading batch.
        datetime_to_filter: Datetime to use for filtering data.
        h3_index_filter: H3 index to use for filtering data.
        filter_by_date: Flag indicating whether to filter data by date. If True, filtering is done by date.
        process_data: Flag indicating whether to process the data. If True, the Processor is instantiated and run.
        output_file: Output file path for storing data.
    """

    def __init__(
            self,
            h3_indexes_df: pd.DataFrame,
            start_day: int,
            end_day: int,
            batch_size: int,
            datetime_to_filter: Union[None, datetime],
            h3_index_filter: Union[None, str],
            filter_by_date: bool,
            process_data: bool,
            output_file: str
    ):
        self.h3_indexes_df = h3_indexes_df
        self.start_day = start_day
        self.end_day = end_day
        self.batch_size = batch_size
        self.datetime_to_filter = datetime_to_filter
        self.h3_index_filter = h3_index_filter
        self.filter_by_date = filter_by_date
        self.process_data = process_data
        self.output_file = output_file

    def pipeline(self):
        """
        Executes the data loading, processing, and storing pipeline.
        """
        logger.info("Start data pipeline.")
        start_time = datetime.now()

        logger.info("Start Loading ERA5 dataset.")
        start_time_loader = datetime.now()
        results = []
        for batch_start in range(self.start_day, self.end_day + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size, self.end_day + 1)
            result_batch = DataLoader.process_data_batch(batch_start, batch_end, self.h3_indexes_df)
            results.append(result_batch)
            del result_batch
            gc.collect()

        self.data = pd.concat(results)
        del results
        gc.collect()
        total_time_loader = datetime.now() - start_time_loader
        total_seconds_loader = total_time_loader.total_seconds()
        logger.info(f'Data Loader Step completed in: {total_seconds_loader}')

        logger.info("Start Filtering and Storage of the raw ERA5 dataset.")
        start_time_manager = datetime.now()
        self.pipeline = DataManager(self.data)
        self.pipeline.transform_data(
            timestamp_filter=self.datetime_to_filter,
            h3_index_filter=self.h3_index_filter,
            filter_by_date= self.filter_by_date,
        )

        self.pipeline.store_as_parquet(
            self.pipeline.data,
            parquet_schema=RAW_ERA5_PARQUET_SCHEMA_CONSTANT,
            output_file=self.output_file
        )

        total_time_manager = datetime.now() - start_time_manager
        total_seconds_manager = total_time_manager.total_seconds()
        logger.info(f'Data Manager Step completed in: {total_seconds_manager}')

        if self.process_data:
            logger.info("Start Processing ERA5 dataset.")
            start_time_processor = datetime.now()

            processed_filename = 'processed_ ' + self.output_file
            self.pipeline.process_data()
            self.pipeline.store_as_parquet(
                self.pipeline.transformed_data,
                parquet_schema=PROCESSED_ERA5_PARQUET_SCHEMA_CONSTANT,
                output_file=processed_filename,
            )

            total_time_processor = datetime.now() - start_time_processor
            total_seconds_processor = total_time_processor.total_seconds()
            logger.info(f'Data Process Step completed in: {total_seconds_processor}')

        total_time = datetime.now() - start_time
        total_seconds = total_time.total_seconds()
        logger.info(f'End data pipeline in: {total_seconds}')
