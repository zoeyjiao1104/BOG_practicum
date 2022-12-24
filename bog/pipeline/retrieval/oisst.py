import pandas as pd
from datetime import datetime
from erddapy import ERDDAP
from retrieval.measurements import MeasurementRetrieval
from typing import List


class OISST(MeasurementRetrieval):
    """
    The data scraper for OISST measurements.
    """

    SOURCE_NAME = 'Optimum Interpolation Sea Surface Temperature (OISST)'
    SOURCE_WEBSITE = 'https://www.ncei.noaa.gov/products/optimum-interpolation-sst'


    def __init__(self):
        """
        The class constructor. Initializes an OISST ERDDAP
        dataset through a GRIDDAP server hosted by NOAA,
        as well as default measurement types and data columns.

        Parameters:
            None

        Returns:
            None
        """
        # Initialize dataset
        data = ERDDAP(
            server='https://www.ncei.noaa.gov/erddap',
            protocol='griddap'
        )
        data.dataset_id = 'ncdc_oisst_v2_avhrr_by_time_zlev_lat_lon'
        data.griddap_initialize()

        # Set instance fields
        self.erddap = data
        self.measurement_types = [
            "ice",
            "err",
            "anom",
            "sst"
        ]
        self.col_names = [
            'time (UTC)',
            'latitude (degrees_north)', 
            'longitude (degrees_east)',
            'ice (%)', 
            'err (Celsius)',
            'anom (Celsius)',
            'sst (Celsius)'
        ]
        self.default_variables = self.erddap.variables.copy()
        self.default_start_time = self.erddap.constraints['time>=']
        self.default_end_time = self.erddap.constraints['time<=']
        self.default_min_lat = self.erddap.constraints['latitude>=']
        self.default_max_lat = self.erddap.constraints['latitude<=']
        self.default_min_lon = self.erddap.constraints['longitude>=']
        self.default_max_lon = self.erddap.constraints['longitude<=']


    def get_sensor_ids(self) -> List[str]:
        """
        Retrieves the sensor ids associated with the dataset.
        An empty list is returned in this case due to the
        omnipresent nature of the measurement events.

        Parameters:
            None

        Returns:
            (list of str): The sensor ids.
        """
        return []


    def get_most_recent_measurements(
        self,
        sensor_id: str,
        measurement_types: List[str]= None):
        """
        Retrieves the most recent measurements from a given sensor/location.

        Parameters:
            sensor_id (str): The unique identifier for a given sensor.
                Because the dataset is omnipresent, just `None`.

            measurement_types (list of str): The measurement types or
                products to collect for the sensor/station.
        
        Returns:
            (pd.DataFrame): DataFrame with columns for location, datetime, 
                location id, and measurements.
        """
        start, end = self.get_default_times()
        return self._get_measurements(start, end, measurement_types)
    

    def get_single_sensor_historical_measurements(
        self,
        sensor_id: str,
        measurement_types: List[str]=None,
        start_time: datetime=None,
        end_time: datetime=None):
        """
        Retrieves all OISST measurements available in the dataset.

        Parameters:
            sensor_id (str): Unique identifier for a given sensor.
                (Empty array/None for this dataset).

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
                datetime, location id, and other measurements.
        """
        # Validate requested start and end datetimes         
        dataset_start = datetime.strptime(self.default_start_time, "%Y-%m-%dT%H:%M:%SZ")       
        dataset_end = datetime.strptime(self.default_end_time, "%Y-%m-%dT%H:%M:%SZ")
        start_time = dataset_start if not start_time else start_time
        end_time = dataset_end if not end_time else end_time
        if end_time < start_time:
            raise ValueError("Start datetime must precede end datetime.")

        # Return empty DataFrame if dataset doesn't include requested dates
        if dataset_end < start_time or dataset_start > end_time:
            return pd.DataFrame(columns=self.col_names)
        
        # Validate requested measurements
        invalid_types = list(set(measurement_types) - set(self.measurement_types))
        if invalid_types:
            raise ValueError("Measurement types should be one or more of "
                f"{', '.join(self.measurement_types)}. Received one or "
                f" more invalid entries: {', '.join(invalid_types)}.")

        # Specify variables to return
        self.erddap.variables = [
            "time",
            "latitude", 
            "longitude"
        ]
        if measurement_types:
            self.erddap.variables += measurement_types

        # Specify dataset constraints
        self.erddap.constraints = {
            "time>=": start_time, 
            "time<=": end_time,
        }

        # Fetch data
        df = self.erddap.to_pandas()
        df = df.drop("depth (m)", axis=1, errors='ignore')
        return df
