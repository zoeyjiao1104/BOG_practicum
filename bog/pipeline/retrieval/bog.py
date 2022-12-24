"""
filename: bog_api.py
author: Valeria Blaza
description: Retrieves data from Blue Ocean Gear API
"""
import os
import pandas as pd
import requests
from retrieval.measurements import MeasurementRetrieval
from utilities import files


MAX_API_CALLS = 3


class BOG(MeasurementRetrieval):
    """
    Class to retrieve data from Blue Ocean Gear (BOG) API.
    """

    SOURCE_NAME = 'Blue Ocean Gear(BOG)'
    SOURCE_WEBSITE = 'https://www.blueoceangear.com/'

    source_time_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    standardized_columns = {
        "time" : "datetime",
        "position_latitude" : "latitude",
        "position_longitude": "longitude",
        "prev_position_latitude" : "previous_latitude",
        "prev_position_longitude" : "previous_longitude",
        "velocity" : "speed"
    }

    def __init__(self):
        """ Constructor for the BOG API Class, handles authentication """
        try:
            endpoint = os.environ['BOG_API_BASE_URL']
            username = os.environ['BOG_API_USERNAME']
            password = os.environ['BOG_API_PASSWORD']
        except KeyError as e:
            raise Exception("Cannot authenticate to the BOG API. "
                f"Missing required environment variable '{e}'.")

        self.endpoint = endpoint
        self._username = username
        self._password = password
        
        self._get_token()
        self._header = {'Authorization': 'Bearer ' + str(self._token)}
        self.get_sensor_ids()

    def _get_token(self):
        """ Retrieves authentication token for API access """
        attempt = 0
        while attempt <= MAX_API_CALLS:
            attempt += 1
            r = requests.post(self.endpoint + "/auth", data={"type": "login",
                                "username": self._username,
                                "password": self._password})
            if r.ok:
                self._token = r.json()["token"]
                return

        self._token = None
        raise Exception("""User authentication failed: \n HTTP {} - {},
                        Message {}""".format(r.status_code, r.reason, r.text))

    def get_sensor_ids(self):
        """ Retrieves list of buoy's for which the user has access to """
        response = self.make_request(
            self.endpoint + "/user", headers=self._header
        )
        self.buoy_ids = response.json()["buoys"]
        return [id for id in self.buoy_ids]

    def logout(self):
        """ Logs the authenticated user out of the API """
        try:
            requests.post(self.endpoint + "/auth", data={"type": "logout"})
        except:
            raise Exception("Logout Failed")


    def get_current_status(self, buoy_id):
        """
        Retrieves current status for a specified buoy_id
        Input(s): 
            - buoy_id (int): Buoy identification number (e.g., 133)
        Output(s): JSON file containing information from the 
            buoy's last update, including timestamp, latitude,
            longitude, battery, system status, and available variables
            for historical data retrieval
        """
        req_str = "/buoy/{}/details".format(str(buoy_id))
        req_url = self.endpoint + req_str
        response = self.make_request(req_url, headers=self._header)
        return response.json()

    def get_single_sensor_historical_measurements(self,
            sensor_id,
            measurement_types = None,
            start_time = None,
            end_time = None,
    ):
        """
        Get sensor historical measurments for a single source.
        Input: Sensor Id, Measurement Types (optional), 
               Start Time (optional), End Time(optional)
        """
        
        start_time, end_time = self.get_default_times(start_time, end_time)
        default_vars = [
            'last_updated',
            'next_update',
            'system_status',
            'battery_soc',
            'cloud_battery_soc',
            'long_life',
            'fast_update'
        ]
        available_vars = self.get_current_status(sensor_id)["series"]
        buoy_vars = available_vars + default_vars

        if measurement_types is None:
            measurement_types = buoy_vars

        req_str = "/buoy/{}/reports?series={}".format(
            str(sensor_id), ','.join(measurement_types)
        )
        response = self.make_request(
            self.endpoint + req_str, headers=self._header
        )
        buoy_df = self._normalize_json(response.json(), sensor_id)
        # filter for times
        buoy_df["datetime"] = pd.to_datetime(
            buoy_df["datetime"], format=self.source_time_format
        )
        time_mask = ((buoy_df["datetime"] >= start_time) & 
                     (buoy_df["datetime"] <= end_time))
        buoy_df = buoy_df[time_mask]
        return buoy_df


    def _normalize_json(self, json_output, sensor_id):
        """ Flatten the JSON output into pandas dataframe. 
        
        Args:
            json_output (dict): Expects format like:
                {"series" : {"series" : {
                    "<measurement_type>": {
                        "momsn" : int,
                        "time" : self.time_format,
                        "value" : float
                    }
                }
        """
        results = json_output
        # output is sometimes {"series" : {"series" : {results}}}
        while "series" in results:
            results = results["series"]
        dfs = []
        for parameter in results:
            df = pd.DataFrame(results[parameter], columns=["time", "value", "momsn"])
            df = df.set_index(['momsn', 'time'])
            df = df.rename({'value' : parameter}, axis='columns')
            dfs.append(df)
        wide_df = pd.concat(dfs, axis='columns')
        wide_df = wide_df.reset_index()
        wide_df = wide_df.rename(columns=self.standardized_columns)
        wide_df["sensor_id"] = sensor_id
        long_df = pd.melt(
            wide_df,
            id_vars=self.id_vars,
            var_name="parameter"
        )
        return long_df

    def build_historical_df(self, buoy_ids, series=None):
        """
        Retrieves and concatenates dataframes for a list of buoys
        Input(s):
            - buoy_ids (lst): A list of buoy ids (e.g., [72, 76, 77])
            - series (lst): List of variables to be retrieved. If no
            variables are specified, retrieve all available variables
        Output(s): 
            - dataframe containing the specified information for 
            the given buoy
        """

        assert buoy_ids, "Please include a list of buoy ids"
        dfs = [self.create_buoy_df(buoy_id, series) for buoy_id in buoy_ids]
        final_df = pd.concat(dfs, ignore_index=True)
        return final_df


    def create_buoy_df(self, buoy_id, series=None):
        '''
        Creates dataframe containing timeseries data for buoys
        Input(s):
            - buoy_id (int): Buoy identification number (e.g., 133)
            - series (lst): List of variables to be retrieved. If no
            variables are specified, retrieve all available variables
        Output(s):
            - final_df (pandas dataframe): dataframe containing the
            specified information for the given buoy
        '''
    
        assert buoy_id in self.buoy_ids, "Buoy {} does not exist".format(buoy_id)
        available_vars = self.get_current_status(buoy_id)["series"]

        if not series:
            series = available_vars

        assert set(series).issubset(available_vars), \
            "Variable(s) specified ({}) not available".format(", ".join(series))

        df = self.get_single_sensor_historical_measurements(
            sensor_id=buoy_id,
            measurement_types=None,
            start_time="2020-01-01",
            end_time=None
        )

        pivoted_df = pd.pivot(
            df,
            index=['sensor_id', 'datetime', 'latitude', 'longitude'],
            columns=['parameter'],
            values=['value'])

        pivoted_df.columns = [col for _, col in pivoted_df.columns.values]
        pivoted_df = pivoted_df.reset_index()
    
        return pivoted_df

    def build_current_df(self):
        """
        Retrieves and concatenates dataframes for all the available
        buoys with each buoy's most recent location (lat/lon)
        Output(s): 
            - dataframe containing each buoy's most recent location
        """

        data = pd.DataFrame([self.get_current_status(buoy_id, check=False) for buoy_id in \
               self.buoy_ids], columns=["buoy_id", "summary"])
        current_buoys = pd.concat([data.drop(['summary'], axis=1), data['summary'].apply(pd.Series)], \
                                   axis=1).rename(columns={"latitude" : "buoy_lat", 
                                                           "longitude" : "buoy_lon"})

        filename = "buoys/current_buoys_{}.tsv".format(current_buoys.last_updated.max())

        files.save_df(current_buoys, filename, index=False)
        self.logout()

        return current_buoys


    def get_single_buoy_messages(
        self,
        buoy_id,
        from_date:str=None,
        to_date:str=None,
        sort:str="asc",
        nc=''):
        """
        Queries the BOG API for measurement records for the given buoy,
        over the specified date range. Records are returned in date
        order in IOS8601 format.
        """
        # Parse given datetime strings or retrieve defaults if not provided
        from_date, to_date = self.get_default_times(from_date, to_date)
        
        # Validate sort parameter
        if sort not in ("asc", "desc"):
            raise ValueError("The argument 'sort' must have "
                f"the value 'asc' or 'desc', but '{sort}' "
                "was given.")
        
        # Build URL query
        # NOTE: <buoyId>/messages?from=<date>&to=<date>&sort=<asc|desc>           
        req_str = "/buoy/{}/messages?from={}&to={}&sort={}&next={}".format(
            str(buoy_id), 
            str(from_date).replace(" ", "T"), 
            str(to_date).replace(" ","T"),
            sort,
            str(nc)
        )

        response = self.make_request(
            self.endpoint + req_str, headers=self._header
            )

        page += 1   
        r_df = {}

        r_df[str(page)] = response.json()

        #If there is a next_cursor value:
        if 'next_cursor' in r_df[str(page)]:
            d = self.get_single_buoy_messages(buoy_id, from_date, to_date,sort, r_df[str(page)]['next_cursor'],page)
            r_df.update(d)
        

        #Return list of all messages
        messages = []
        count = page
        if count == 1:
            while str(count) in r_df:
                messages.append(r_df[str(count)])
                count += 1
                
        if page == 1:
            return messages
        else:
            return r_df
    
    def get_since_messages(self,since=None):
        """
        Given a since value, retrieves all messages since given time:

        Returns messages for all buoys assigned to the authenticated user provied.
        Sorted in date order, most recent first.

        By default, returns all known data since stamp.

        Passing in a since parameter allows the user to recieve only data after that point.

        """
        # /updates?since=<since_cursor>
        req_str = "/updates?since={}".format(since)

        response = self.make_request(
            self.endpoint + req_str, headers=self._header
            )

        return response.json()



if __name__ == "__main__":
    bog = BOG()
    buoy_ids = bog.get_sensor_ids()
    df = bog.build_historical_df(buoy_ids)
    df.to_csv('combined_buoys.tsv', sep='\t', index=False)
    bog.logout()
