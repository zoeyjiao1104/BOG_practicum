"""
wavewatch.py
"""

import pandas as pd
from datetime import datetime
from erddapy import ERDDAP
from retrieval.measurements import MeasurementRetrieval
from typing import List, Union


class WaveWatchIII(MeasurementRetrieval):
    """
    The data scraper for WaveWatch III measurements.
    """

    SOURCE_NAME = 'NOAA Weather Service WaveWatch III'
    SOURCE_WEBSITE = 'https://www.weather.gov/sti/coastalact_ww3'


    def __init__(self) -> None:
        """
        The class constructor. Initializes a WaveWatch III
        ERDDAP dataset through a GRIDDAP server hosted by NOAA,
        as well as default measurement types and data columns.

        The default variables in the dataset are:

            Tdir - peak wave direction in degrees
            Tper - peak wave period in seconds
            Thgt - significant wave height in meters
            sdir - swell peak wave direction in degrees
            sper - swell peak wave period in seconds
            shgt - swell significant wave height in meters
            wdir - wind peak wave direction in degrees
            wper - wind peak wave period in seconds
            whgt - wind significant wave height in meters

        The default constraints used when filtering the
        dataset are:

            'time>=': ISO timestamp 6 days ahead (e.g., '2022-03-29T18:00:00Z')
            'time<=': ISO timestamp 6 days ahead (e.g., '2022-03-29T18:00:00Z')
            'time_step': 1
            'depth>=': 0.0
            'depth<=': 0.0
            'depth_step': 1
            'latitude>=': -77.5
            'latitude<=': 77.5
            'latitude_step': 1
            'longitude>=': 0.0
            'longitude<=': 359.5
            'longitude_step': 1

        References:
            - https://ioos.github.io/erddapy/erddapy.html

        Parameters:
            None

        Returns:
            None
        """
        # Initialize dataset
        data = ERDDAP(
            server='https://coastwatch.pfeg.noaa.gov/erddap',
            protocol="griddap"
        )
        data.dataset_id = 'NWW3_Global_Best'
        data.griddap_initialize()

        # Set instance fields
        self.erddap = data
        self.measurement_types = [
            "Tdir",
            "Tper",
            "Thgt",
            "sdir",
            "sper",
            "shgt",
            "wdir",
            "wper",
            "whgt"
        ]
        self.col_names = [
            'time (UTC)',
            'latitude (degrees_north)',
            'longitude (degrees_east)',
            'Tdir (degrees)',
            'Tper (second)',
            'Thgt (meters)',
            'sdir (degrees)',
            'sper (seconds)',
            'shgt (meters)',
            'wdir (degrees)',
            'wper (seconds)',
            'whgt (meters)'
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
        measurement_types:List[str]=None) -> pd.DataFrame:
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
        Retrieves all WaveWatch III measurements available in the dataset.

        Parameters:
            sensor_id (string): Unique identifier for a given sensor.
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
                datetime, location id, and other measurements
        """
        return self._get_measurements(measurement_types=measurement_types)
        

    def _get_measurements(
        self,
        start_time: datetime=None,
        end_time: datetime=None,
        min_latitude: float=None,
        max_latitude: float=None,
        min_longitude: float=None,
        max_longitude: float=None,
        measurement_types: List[str]=None) -> pd.DataFrame:
        """
        Queries the ERDDAP server for WaveWatch III measurements
        corresponding to the requested start datetime, end
        datetime, and measurement types.

        Parameters:
            start_time (datetime): The inclusive start date for which
                to retrieve data. Defaults to the start of the
                dataset if not provided.

            end_time (datetime): The inclusive end date for which
                to retrieve data. Defaults to the end of the
                dataset if not provided.

            measurement_types (list of str): The measurement
                products to store. Defaults to all if not
                provided.

            min_latitude (float): The minimum latitude to
                consider when filtering measurements. Valid
                within the range of -77.5 to 77.5.
                Defaults to -77.5 if not provided.

            max_latitude (float): The maximum latitude to
                consider when filtering measurements. Valid
                within the range of -77.5 to 77.5.
                Defaults to 77.5 if not provided.

            min_longitude (float): The minimum longitude to
                consider when filtering measurements. Valid
                within the range of 0 to 359.5. Defaults to
                0 if not provided.

            max_longitude (float): The maximum longitude to
                consider when filtering measurements. Valid
                within the range of 0 to 359.5. Defaults to
                359.5 if not provided.

        Returns:
            (pd.DataFrame): The filtered dataset.
        """
        # Parse start and end datetimes for ERDDAP dataset
        dataset_start = datetime.strptime(self.default_start_time, "%Y-%m-%dT%H:%M:%SZ")       
        dataset_end = datetime.strptime(self.default_end_time, "%Y-%m-%dT%H:%M:%SZ")

        # Validate input start and end datetimes         
        if start_time and end_time:
            if end_time < start_time:
                raise ValueError("Start datetime must precede end datetime.")
        else:
            start_time = dataset_start
            end_time = dataset_end

        # Return empty DataFrame if dataset doesn't include requested dates
        if dataset_end < start_time or dataset_start > end_time:
            return pd.DataFrame(columns=self.col_names)

        # Validate input measurement types
        if measurement_types:
            invalid_types = [m for m in measurement_types if m not in self.measurement_types]
            raise ValueError("Measurement types should be of format "
                "'Tdir', 'Tper', 'Thgt', 'sdir', 'sper', 'shgt', "
                "'wdir', 'wper', or 'whgt'. Received one or more invalid "
                f"entries: {', '.join(invalid_types)}.")
        else:
            measurement_types = self.measurement_types

        # Validate input latitude
        valid_min_lat = self._get_valid_coord(
            val=min_latitude,
            default=self.default_min_lat,
            min_val=self.default_min_lat,
            max_val=self.default_max_lat,
            val_name='latitude'
        )
        valid_max_lat = self._get_valid_coord(
            val=max_latitude,
            default=self.default_max_lat,
            min_val=self.default_min_lat,
            max_val=self.default_max_lat,
            val_name='latitude'
        )
        if valid_min_lat > valid_max_lat:
            raise ValueError("The minimum latitude must be less than or "
                f"equal to the maximum. Recieved {min_latitude} as the "
                f"minimum (default {self.default_min_lat}) and {max_latitude} "
                f"as the maximum (default {self.default_max_lat}).")

        # Validate input longitude
        valid_min_lon = self._get_valid_coord(
            val=min_longitude,
            default=self.default_min_lon,
            min_val=self.default_min_lon,
            max_val=self.default_max_lon,
            val_name='longitude'
        )
        valid_max_lon = self._get_valid_coord(
            val=max_longitude,
            default=self.default_max_lon,
            min_val=self.default_min_lon,
            max_val=self.default_max_lon,
            val_name='longitude'
        )
        if valid_min_lon > valid_max_lon:
            raise ValueError("The minimum longitude must be less than or "
                f"equal to the maximum. Recieved {min_longitude} as the "
                f"minimum (default {self.default_min_lon}) and {max_longitude} "
                f"as the maximum (default {self.default_max_lon}).")

        # Generate filtered dataset
        self.erddap.variables = measurement_types
        self.erddap.constraints['time>='] = start_time
        self.erddap.constraints['time<='] = end_time
        self.erddap.constraints['latitude>='] = valid_min_lat
        self.erddap.constraints['latitude<='] = valid_max_lat
        self.erddap.constraints['longitude>='] = valid_min_lon
        self.erddap.constraints['longitude<='] = valid_max_lon
        df = self.erddap.to_pandas()
        
        return df


    def _get_valid_coord(
        self,
        val: Union[float, None],
        default: float,
        min_val: float,
        max_val: float,
        val_name: str):
        """
        Rounds the given coordinate to the nearest 0.5
        value and ensures that it falls within the
        expected range of the minimum and maximum value, 
        inclusive. Uses the default coordinate value 
        instead if one is not provided.

        Parameters:
            val (float or None): The value to round.

            default (float): The default value set
                by the ERDDAP dataset.

            min_val (float): The inclusive lower
                bound for the coordinate value.

            max_val (float): The inclusive upper
                bound for the coordinate value.

            val_name (str): The name of the coordinate.

        Returns:
            (float): The rounded value.
        """
        original_val = val
        val = val if val else default
        val = round(val * 2) / 2

        if val < min_val or val > max_val:
            raise ValueError(f"Invalid {val_name} "
                f"received: {original_val}. Expected "
                f"value between {min_val} and {max_val} "
                "inclusive.")

        return val


if __name__ == "__main__":
    ww = WaveWatchIII()
    sensor_id = None
    # df = ww.get_most_recent_measurements(sensor_id)
    # df.head()
