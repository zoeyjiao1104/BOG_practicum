import requests
import time
import pandas as pd
from datetime import datetime, timedelta

class MeasurementRetrieval():
    """
    Base class defining methods for aquiring and working with data from
    various sources. Methods informed by predicted use cases on web app.

    How we use data:

     - Want app to be close to real time and avoid redundant api calls so we
       want to be able to call the most recent measurements of each data
       source. 
     - Want to be able to populate with historical data and be able to query
       historical data for anomaly detection tasks.
     - Quickly be able to retrieve nearest measurements to a given buoy for
       display on map and for data analysis. 
    """
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    source_time_format = "%Y-%m-%dT%H:%M:%SZ"
    id_vars = ["sensor_id", "datetime", "latitude", "longitude"]
    

    def get_sensor_ids(self):
        """ Returan a list of sensor ids available. """
        raise NotImplementedError()

    def get_most_recent_measurements(self,
            sensor_id,
            measurement_types = None
    ):
        """
        Retrieve the most recent measurements from the given sensor/location

        Args:
            sensor_id (string): Unique identifier for a given sensor.
                For location based measurements is {lat}_{long}
            measurement_types (list of strings): Measurement type or
                product to collect for the sensor/station.
        Returns:
            pd.DataFrame containing columns for location, datetime, 
            location id, measurements
        """
        raise NotImplementedError()

    def get_historical_measurements(self,
            sensor_ids = None,
            measurement_types = None,
            start_time = None,
            end_time = None,
    ):
        """
        Retrieve all measurements for sensor_ids within time window.

        Args:
            sensor_ids ( list of string): Unique sensor ids. For location
                based measurements is {lat}_{long}. Defaults to all available
                for source.
            measurement_types (list of strings): Measurement type or
                product to collect for the sensor/station.
            start_time (str): Earliest time from which measurements will be
                included. Defaults to 3 days ago
            end_time (str): Latest time from which measurements will be
                included. Defaults to now.
        Returns:
            pd.DataFrame containing columns for location, datetime, 
            location id, measurements
        """
        if sensor_ids == None:
            sensor_ids = self.get_sensor_ids()
        sensor_dfs = []
        for sensor_id in sensor_ids:
            if sensor_id  in ['222']:
                continue
            sensor_df = self.get_single_sensor_historical_measurements(
                str(sensor_id), start_time=start_time, end_time=end_time)
            if sensor_df is not None:
                sensor_dfs.append(sensor_df)

        return pd.concat(sensor_dfs)

    def get_single_sensor_historical_measurements(self,
            sensor_id,
            measurement_types = None,
            start_time = None,
            end_time = None,
    ):
        """
        Retrieve all measurements with in the given area and time window.

        Args:
            sensor_id (string): Unique identifier for a given sensor.
                For location based measurements is {lat}_{long}
            measurement_types (list of strings): Measurement type or
                product to collect for the sensor/station.
            start_time (str): Earliest time from which measurements will be
                included. Defaults to 3 days ago
            end_time (str): Latest time from which measurements will be
                included. Defaults to now.
        Returns:
            pd.DataFrame containing columns for location, datetime, 
            location id, measurements
        """
        raise NotImplementedError()

    def make_request(self, request_str, headers = None):
        """ Query the api using a request url """
        request_out = requests.get(request_str, headers=headers)
        # 429 means we've exceeded rate limit -- wait and try again
        # until it passes
        while request_out.status_code == 429:
            time.sleep(10)
            request_out = requests.get(request_str, headers=headers)
        if request_out.status_code == 200:
            return request_out
        else:
            error_message = (
                "Received status code {} for {}\nReason: {}\n{}".format(
                    request_out.status_code, request_str, request_out.reason,
                    request_out.text)
            )
            raise RuntimeError(error_message)

    def format_time(self, datetime_string):
        """ Makes datetime object from 'yyyy-mm-ddThh:mm:ssZ' or 'yyyy-mm-dd' """
        if len(datetime_string) == 20:
            dt = datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%SZ")
        elif len(datetime_string) == 10:
            dt = datetime.strptime(datetime_string, "%Y-%m-%d")
        else:
            error_message = ("datetime should be of format "
                             "yyyy-mm-ddThh:mm:ssZ' or 'yyyy-mm-dd'. "
                             "received {}".format(datetime_string))
            raise ValueError(error_message)
        return dt

    def get_default_times(self, start_time, end_time):
        if end_time is None:
            end_time = datetime.now()
        else:
            end_time = self.format_time(end_time)
        if start_time is None:
            start_time = datetime.now() - timedelta(days=3)
        else:
            start_time = self.format_time(start_time)
        return start_time, end_time