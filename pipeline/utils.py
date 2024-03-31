import logging
import sys

from math import radians, sin, cos, sqrt, atan2

from pipeline.constants import EARTH_RADIUS_KM_CONSTANT

logger = logging.getLogger()


def setup_logger():
    """
    Setup logger to output logs to the notebook's output cell.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)  # outputs logs to the notebook's output cell
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on the Earth's surface
    using the Haversine formula.

    Args:
        lat1: Latitude of the first point in degrees.
        lon1: Longitude of the first point in degrees.
        lat2: Latitude of the second point in degrees.
        lon2: Longitude of the second point in degrees.

    Returns: The distance between the two points in kilometers.

    The Haversine formula determines the shortest distance between two points
    on a sphere given their longitudes and latitudes.
    Please refer to Wikipedia for more details: https://en.wikipedia.org/wiki/Haversine_formula


    How to use: this method is useful to retrieve a particular location from the h3_indexes_df
        ```
        target_lat = 41.8719
        target_lon = 12.5674
        h3_indexes_df['distance'] = h3_indexes_df.apply(
        lambda row: haversine(target_lat, target_lon, row['latitude'], row['longitude']),
        axis=1
        )
        closest_point = h3_indexes_df.loc[h3_indexes_df['distance'].idxmin()]
        print("Closest Latitude:", closest_point['latitude'])
        print("Closest Longitude:", closest_point['longitude'])
        print("Closest H3 Index:", closest_point['h3_index'])
        ```
    """

    logger.debug('Convert latitude and longitude from degrees to radians')
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    logger.debug('Calculate the differences between the latitudes and longitudes')
    difference_latitude = lat2 - lat1
    difference_longitude = lon2 - lon1

    logger.info('Calculate the distance using the Haversine formula')
    a = sin(difference_latitude / 2)**2 + cos(lat1) * cos(lat2) * sin(difference_longitude / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = EARTH_RADIUS_KM_CONSTANT * c

    return distance
