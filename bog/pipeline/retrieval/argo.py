"""
argo.py
"""

import pandas as pd
import requests
import time
from argopy import DataFetcher as ArgoDataFetcher, IndexFetcher
from datetime import datetime, timedelta, timezone
from retrieval.measurements import MeasurementRetrieval
from typing import List
from utilities.logger import logger


class ArgoDrifter(MeasurementRetrieval):
    """
    The data scraper for Argo drifter measurements.
    """

    SOURCE_NAME = 'French Research Institute for Exploitation of the Sea(IFREMER), Argo'
    SOURCE_WEBSITE = 'https://www.weather.gov/sti/coastalact_ww3'
    NORTH_ATLANTIC = (-98.0539, 12.0059, -0.936, 68.6387)
    NORTH_PACIFIC_WEST = (117.5162, 180,  0, 66.5629)
    NORTH_PACIFIC_EAST = (-180, -76.9854,  0, 66.5629)
    DEFAULT_REGIONS = [NORTH_ATLANTIC, NORTH_PACIFIC_WEST, NORTH_PACIFIC_EAST]


    def __init__(self, origin: datetime) -> None:
        """
        The class constructor. Initializes an Argo
        Drifter ERDDAP dataset through a TABLEDAP
        server hosted by NOAA, as well as default
        measurement types and data columns.

        Parameters:
            None

        Returns:
            None
        """
        self.origin = origin.replace(tzinfo=timezone.utc)
        self._logger = logger
        self.standardized_columns = {
            "PLATFORM_NUMBER" : "sensor_id",
            "TIME" : "datetime",
            "LATITUDE": "latitude",
            "LONGITUDE" : "longitude",
            "PSAL" : "salinity",
            "TEMP" : "sea_temperature",
            "PRES" : "sea_pressure"
        }
        self.col_names = [
            "sensor_id",
            "datetime",
            "latitude",
            "longitude", 
            "salinity",
            "sea_temperature",
            "sea_pressure"
        ]


    def get_sensor_ids(self) -> List[str]:
        """ 
        Retrieves Argo drifter ids from the North Atlantic and
        North Pacific oceans since the `ORIGIN` date specified
        in configuration settings.
       
        Parameters:
            None

        Returns:
            (list of str): The drifter ids.
        """
        # Determine start and end dates of sensor id query
        time_format = "%Y-%m-%d"
        start_time = self.origin.strftime(time_format)
        end_time = datetime.utcnow().strftime(time_format)

        # Query for drifters in the configured regions
        drifter_ids = []
        for region in self.DEFAULT_REGIONS:
            lon_min, lon_max, lat_min, lat_max = region
            params = [
                lon_min,
                lon_max,
                lat_min, 
                lat_max,
                start_time,
                end_time
            ]
            index_fetcher = IndexFetcher().region(params)
            ids = index_fetcher.to_dataframe()["wmo"].unique().tolist()
            drifter_ids.extend(ids)
        
        # Return unique drifter ids
        return list(set(drifter_ids))


    def get_most_recent_measurements(
        self,
        sensor_id: str,
        measurement_types: List[str]=None) -> pd.DataFrame:
        """
        Retrieves recent ten-day measurements for a specified
        Argo drifter.

        Parameters:
            sensor_id (str): A drifter identifier/"wmo" (e.g.,1902300).
            
            measurement_types (list of str): A list of measurement types.
                Defaults to `None`.

        Returns:
            (pd.DataFrame): A DataFrame contianing the most recent
                status of the drifter including time, longitude,
                latitude, salinity, sea temperature and sea pressure.
        """
        # Determine start and end dates of sensor id query
        time_format = "%Y-%m-%d"
        now = datetime.utcnow()
        ten_days_prior = now - timedelta(days=10)
        start_time = ten_days_prior.strftime(time_format)
        end_time = now.strftime(time_format)
        
        # Get data
        return self.get_single_sensor_historical_measurements(
            sensor_id,
            measurement_types,
            start_time,
            end_time
        )

    
    def get_single_sensor_historical_measurements(
        self,
        sensor_id: str,
        measurement_types: List[str]=None,
        start_time: datetime=None,
        end_time: datetime=None):
        """
        Retrieves all measurements available in the dataset
        for the specified global drifter.

        Parameters:
            sensor_id (string): The unique identifier for a
                given sensor.

            measurement_types (list of str): The measurement
                products to store. Defaults to all if not
                provided.

            start_time (datetime): The inclusive start date for which
                to retrieve data. Defaults to the start of the
                dataset if not provided.

            end_time (datetime): The inclusive end date for which
                to retrieve data. Defaults to the end of the
                dataset if not provided.

        Returns:
            (pd.DataFrame): DataFrame with columns for location,
                datetime, location id, and other measurements
        """
        return self.get_historical_measurements(
            sensor_ids=[sensor_id],
            measurement_types=measurement_types,
            start_time=start_time,
            end_time=end_time
        )


    def get_historical_measurements(
        self,
        sensor_ids: List[str]=None,
        measurement_types: List[str]=None,
        start_time: datetime=None,
        end_time: datetime=None) -> pd.DataFrame:
        """
        Retrieve all Argo drifter measurements within
        the given time window.

        Parameters:
            sensor_ids (list of str): The unique identifiers
                of the global drifters to query.

            measurement_types (list of str): The measurement
                products to store. Defaults to all if not
                provided.

            start_time (datetime): The inclusive start date for which
                to retrieve data. Defaults to the start of the
                dataset if not provided.

            end_time (datetime): The inclusive end date for which
                to retrieve data. Defaults to the end of the
                dataset if not provided.

        Returns:
            (pd.DataFrame): DataFrame with columns for latitude,
                longitude, date, and other measurements.
        """
        # Validate requested measurement types
        measurement_types = list(self.standardized_columns.keys())
        invalid_types = list(set(measurement_types) - set(self.measurement_types))
        if invalid_types:
            raise ValueError("Measurement types should be one or more of "
                f"{', '.join(measurement_types)}. Received one or more "
                f"invalid entries: {', '.join(invalid_types)}.")

        # Validate requested start and end times
        start_time = start_time if start_time else self.origin
        end_time = end_time if end_time else datetime.utcnow()
        if end_time < start_time:
            raise ValueError("Start datetime must precede end datetime.")

        # Convert sensor ids to integer type
        sensor_ids = [int(id) for id in sensor_ids]

        # Fetch Argo data using library's built-in parallelization
        num_attempts = 0
        max_num_attempts = 1
        try:
            while num_attempts <= max_num_attempts:
                try:
                    df = ArgoDataFetcher(parallel=True).float(sensor_ids).to_dataframe()
                    break
                except requests.exceptions.HTTPError:
                    time.sleep(0.5)
                    num_attempts += 1
        except Exception as e:
            self._logger.error(e)
            return None

        # Filter and standardize data columns
        df = df[measurement_types]
        df = df.rename(columns=self.standardized_columns)
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df = df[(df["datetime"] <= end_time) & (df["datetime"] >= start_time)]

        # Take measurements closest to the surface
        group_by_cols = ['sensor_id', 'datetime', 'latitude', 'longitude']
        min_sea_pressures = df.groupby(group_by_cols).first()
        final_df = min_sea_pressures.reset_index()
        return final_df


if __name__ == "__main__":
    origin = datetime(year=2022, month=5, day=1)
    argo = ArgoDrifter(origin)
    sensor_ids = argo.get_sensor_ids()
    print(sensor_ids)
    measurements_df = argo.get_single_sensor_historical_measurements(1902300)
    measurements_df.to_csv("argo_measurements.csv")
    print(argo.get_single_sensor_historical_measurements(sensor_id=[1902300], start_time="2022-01-01" ))