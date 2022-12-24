import requests
import pandas as pd
import requests
from datetime import datetime
from erddapy import ERDDAP
from retrieval.measurements import MeasurementRetrieval
from typing import List
from utilities.logger import logger


class GlobalDrifter(MeasurementRetrieval):
    """
    The data scraper for Global Drifter measurements.
    """

    SOURCE_NAME = 'NOAA Global Drifter'
    SOURCE_WEBSITE = 'https://www.aoml.noaa.gov/global-drifter-program/'


    def __init__(self) -> None:
        """
        The class constructor. Initializes a Global
        Drifter ERDDAP dataset through a TABLEDAP
        server hosted by NOAA, as well as default
        measurement types and data columns.

        Parameters:
            None

        Returns:
            None
        """
        # Retrieve current list of global drifters from website
        try:
            response = requests.get("https://www.aoml.noaa.gov/phod/gdp/buoys_countries.geojson")
            data = response.json()
            drifter_wmo_ids = [f['properties']['WMO'] for f in data['features']]
        except KeyError as e:
            raise Exception('Drifter ids not found in response JSON. '
                f'Missing expected key \'{e}\'.')
        except Exception as e:
            raise Exception(f'Failed to retrieve and parse drifter '
                f'ids from webpage. {e}')

        # Initialize dataset
        data = ERDDAP(
            server='http://osmc.noaa.gov/erddap/',
            protocol='tabledap',
            response='csv')
        data.dataset_id = 'OSMC_30day'

        # Set instance fields
        self._logger = logger
        self.erddap = data
        self.measurement_types = [
            'observation_depth',
            'sst',
            'atmp',
            'precip',
            'ztmp',
            'zsal',
            'slp',
            'windspd',
            'winddir',
            'wvht',
            'waterlevel',
            'clouds',
            'dewpoint',
            'uo',
            'vo',
            'wo',
            'rainfall_rate',
            'hur',
            'sea_water_elec_conductivity',
            'sea_water_pressure',
            'rlds',
            'rsds',
            'waterlevel_met_res',
            'waterlevel_wrt_lcd',
            'water_col_ht',
            'wind_to_direction',
            'lon360'
        ]
        self.col_names = [
            'platform_code',
            'platform_type',
            'country',
            'time',
            'latitude',
            'longitude',
            'observation_depth',
            'sst',
            'atmp',
            'precip',
            'ztmp',
            'zsal',
            'slp',
            'windspd',
            'winddir',
            'wvht',
            'waterlevel',
            'clouds',
            'dewpoint',
            'uo',
            'vo',
            'wo',
            'rainfall_rate',
            'hur',
            'sea_water_elec_conductivity',
            'sea_water_pressure',
            'rlds',
            'rsds',
            'waterlevel_met_res',
            'waterlevel_wrt_lcd',
            'water_col_ht',
            'wind_to_direction',
            'lon360'
        ]
        self.drifter_wmo_ids = drifter_wmo_ids
        self.default_variables = data.variables.copy()
        self.default_start_time = data.constraints['time>=']
        self.default_end_time = data.constraints['time<=']
        self.default_min_lat = self.erddap.constraints['latitude>=']
        self.default_max_lat = self.erddap.constraints['latitude<=']
        self.default_min_lon = self.erddap.constraints['longitude>=']
        self.default_max_lon = self.erddap.constraints['longitude<=']


    def get_sensor_ids(self) -> List[str]:
        """
        Retrieves the list of available global drifter
        ids. NOTE: The `platform_code` is a unique
        identifier that can refer to either to the WMO
        id or the ship call sign; we filter to get only
        the global drifters that are buoys, as given by
        the scraped WMO numbers.

        Parameters:
            None

        Returns:
            (list of str): The sensor ids.
        """       
        self.erddap.variables = ["platform_code"]
        df = self.erddap.to_pandas()
        return [
            id
            for id in df['platform_code'].unique()
            if id in self.drifter_wmo_ids
        ]


    def get_most_recent_measurements(
        self,
        sensor_id: str,
        measurement_types:List[str]=None) -> pd.DataFrame:
        """
        Retrieve the most recent measurements from the
        given sensor/location.

        Parameters:
            sensor_id (str): The unique identifier for a
                given sensor.

            measurement_types (list of str): The measurement
                types/products to collect for the sensor/station.

        Returns:
            (pd.DataFrame): DataFrame with columns for
                location, datetime, location id, and measurements.
        """
        start_time, end_time = self.get_default_times()
        return self.get_historical_measurements(
            sensor_ids=[sensor_id],
            measurement_types=measurement_types,
            start_time=start_time,
            end_time=end_time)
        
    
    def get_single_sensor_historical_measurements(
        self,
        sensor_id: str,
        measurement_types:List[str]=None,
        start_time:datetime=None,
        end_time:datetime=None):
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
        sensor_ids: List[str],
        measurement_types: List[str]=None,
        start_time: datetime=None,
        end_time: datetime=None) -> pd.DataFrame:
        """
        Retrieve all global drifter measurements within
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
        # Validate requested sensor ids
        invalid_sensor_ids = set(sensor_ids) - set(self.drifter_wmo_ids)
        if invalid_sensor_ids:
            self._logger.error(f"The following sensor ids are invalid: " +
                ', '.join(str(s) for s in invalid_sensor_ids))
        unique_sensor_ids = list(set(sensor_ids))

        # Validate requested measurements
        invalid_types = list(set(measurement_types) - set(self.measurement_types))
        if invalid_types:
            raise ValueError("Measurement types should be one or more of "
                f"{', '.join(self.measurement_types)}. Received "
                f"one or more invalid entries: {', '.join(invalid_types)}.")

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

        # Specify variables to return
        self.erddap.variables = [
            "platform_code",
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
        return df[df["platform_code"].isin(unique_sensor_ids)]

      
if __name__ == "__main__":
    drifter_measure = GlobalDrifter()
    drifter_measure.get_most_recent_measurements('300234066339760', ["lon360", "rsds"])
    drifter_measure.get_single_sensor_historical_measurements('KAIT', ["lon360", "rsds"])

