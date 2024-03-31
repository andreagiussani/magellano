import h3
import pandas as pd
import xarray as xr
from datetime import datetime

from pipeline.constants import (
    LONGITUDE_CONSTANT,
    LATITUDE_CONSTANT,
    TIME_CONSTANT,
    H3_INDEX_CONSTANT,
    TP_MM_CONSTANT,
    TP_CONSTANT,
    RESOLUTION_CONSTANT,
)


import logging

logger = logging.getLogger()


class DataLoader:
    """
    A class to load and process data in batches from NetCDF files.
    It provides methods to load data for a specified range of days, verify coordinate ranges, and add H3 indexes.
    """

    @staticmethod
    def verify_coordinate_ranges(df: pd.DataFrame) -> None:
        """
        Verifies the ranges of latitude and longitude in the given dataframe representation
        of the loaded xarray dataset.
        """

        min_latitude = df[LATITUDE_CONSTANT].min()
        max_latitude = df[LATITUDE_CONSTANT].max()
        min_longitude = df[LONGITUDE_CONSTANT].min()
        max_longitude = df[LONGITUDE_CONSTANT].max()

        latitude_range_correct = (min_latitude >= -90) and (max_latitude <= 90)
        longitude_range_correct = (min_longitude >= -180) and (max_longitude <= 180)

        if not latitude_range_correct:
            raise ValueError('The range of Latitude is not between [-90,90]')

        if not longitude_range_correct:
            raise ValueError('The range of Latitude is not between [-180,180]')

    @classmethod
    def process_data_batch(cls, start_day: int, end_day: int, h3_indexes_df: pd.DataFrame):
        """
        Process data in batches and compute the total precipitation by day for each
        unique combination of latitude and longitude.

        Args:
            start_day: Start day of the batch (inclusive).
            end_day: End day of the batch (exclusive).
            h3_indexes_df: DataFrame containing H3 indexes.

        Returns: a pandas DataFrame containing the total precipitation summed by day
        for each unique combination of latitude and longitude.
        """
        start_time = datetime.now()
        data_list = []
        
        logger.info(f'Start Loading Data for Days between {start_day} and {end_day}')
        for day in range(start_day, end_day):
            day_str = f"{day:02d}"
            logger.debug(f'Loading Data for Day: {day_str}')
            nc_file_path = f"05/{day_str}/total_precipitation/surface.nc"
            data = xr.open_dataset(nc_file_path)

            longitude = data[LONGITUDE_CONSTANT].data
            latitude = data[LATITUDE_CONSTANT].data
            time = pd.to_datetime(data[TIME_CONSTANT].data)

            new_data = xr.Dataset({
                LONGITUDE_CONSTANT: (LONGITUDE_CONSTANT, longitude),
                LATITUDE_CONSTANT: (LATITUDE_CONSTANT, latitude),
                TIME_CONSTANT: (TIME_CONSTANT, time),
                TP_CONSTANT: ([TIME_CONSTANT, LATITUDE_CONSTANT, LONGITUDE_CONSTANT], data[TP_CONSTANT].data),
            })

            new_data[TP_MM_CONSTANT] = new_data[TP_CONSTANT] * 1000
            new_data = new_data.assign_coords(
                longitude=(((new_data.longitude + 180) % 360) - 180)
            ).sortby(LONGITUDE_CONSTANT)

            data_list.append(new_data)
        
        logger.info(f'Combining Data for Days between {start_day} and {end_day}')
        combined_ds = xr.concat(data_list, dim=TIME_CONSTANT)
                        
        logger.info('Converting to Pandas Dataframe')
        df = combined_ds.to_dataframe().reset_index()
        
        logger.info('Verify Coordinates on Latitude and Longitude')
        cls.verify_coordinate_ranges(df)
        
        logger.info(f'Add H3 Indexes to the current Data for Days between {start_day} and {end_day}')
        df = pd.merge(
            df,
            h3_indexes_df[[LATITUDE_CONSTANT, LONGITUDE_CONSTANT, H3_INDEX_CONSTANT]],
            on=[LATITUDE_CONSTANT, LONGITUDE_CONSTANT]
        )
        
        total_time = datetime.now() - start_time
        total_seconds = total_time.total_seconds()
        logger.info(f'Raw Data Loaded for Days between {start_day} and {end_day} in: {total_seconds}')
        return df

    @classmethod
    def compute_h3_index_from_file(cls, nc_file_path: str, resolution: int = RESOLUTION_CONSTANT) -> pd.DataFrame:
        """
        Compute H3 index for each unique latitude and longitude pair in the data from a NetCDF file.

        Args:
            nc_file_path: Path to the NetCDF file containing latitude and longitude data.
            resolution: H3 resolution level. Default is 10.

        Returns: a DataFrame containing unique latitude, longitude pairs along with their corresponding H3 index.
        """
        data = xr.open_dataset(nc_file_path)
        data = data.assign_coords(longitude=(((data.longitude + 180) % 360) - 180)).sortby(LONGITUDE_CONSTANT)

        if TP_CONSTANT in data:
            data = data.drop_vars(TP_CONSTANT)
        if TIME_CONSTANT in data.coords:
            data = data.drop_dims(TIME_CONSTANT)

        df = data.to_dataframe().reset_index()

        unique_coords_df = df[[LATITUDE_CONSTANT, LONGITUDE_CONSTANT]].drop_duplicates()

        cls.verify_coordinate_ranges(unique_coords_df)

        unique_coords_df[H3_INDEX_CONSTANT] = unique_coords_df.apply(
            lambda row: h3.geo_to_h3(lat=row[LATITUDE_CONSTANT], lng=row[LONGITUDE_CONSTANT], resolution=resolution),
            axis=1
        )

        return unique_coords_df
