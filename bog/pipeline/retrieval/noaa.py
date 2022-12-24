"""
filename: noaa_station.py
author: Sabina Hartnett
email: shartnett@uchicago.edu
description: Class to hold noaa API queries.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from utilities import files
from retrieval.measurements import MeasurementRetrieval

NOAA_STATION_DIR = "stations"
NOAA_STATIONS = files.load_df(f'{NOAA_STATION_DIR}/noaa_stations.tsv')
CURRENT_STATIONS = files.load_df(f'{NOAA_STATION_DIR}/noaa-activecurrentstations.tsv').rename(columns = {'Station ID': 'station_id', ' Station Name': 'station_name', ' Latitude': 'lat', ' Longitude': 'lon'}) 

class NOAA(MeasurementRetrieval):

    SOURCE_NAME = 'National Oceanic and Atmospheric Administration(NOAA) Tides and Currents'
    SOURCE_WEBSITE = 'https://tidesandcurrents.noaa.gov/'

    frequency = 6
    products = []
    datum = "MLLW"
    source_time_format = "%Y%m%d"
    max_time_range = 31

    def standardize_columns(self, product_name):
        """ Create map of noaa api names to standardized colum names """
        # Source: https://api.tidesandcurrents.noaa.gov/api/prod/responseHelp.html
        standardize_columns = {
            "t" : "datetime",
            "Time" : "datetime",
            "v" : product_name if product_name != 'predictions' else 'tide_prediction',
            "s" : "{}_speed".format(product_name),
            "d" : "{}_direction".format(product_name),
            "dr" : "{}_cardinal_direction".format(product_name),
            "g" : "{}_gust_speed".format(product_name),
            "f" : "{}_quartod_flags".format(product_name),
            "q" : "{}_quality".format(product_name),
            "Speed" : "{}_speed".format(product_name),
            "Direction" : "{}_direction".format(product_name),
            "b" : "{}_bin".format(product_name),
            "Bin" : "{}_bin".format(product_name),
            "Depth" : "{}_depth".format(product_name),
            
            "Velocity_Major" : "{}_velocity_major".format(product_name),
            "meanEbbDir" : "{}_mean_ebb_direction".format(product_name),
            "meanFloodDir" : "{}_mean_flood_direction".format(product_name),
        }
        # 's' is reused by NOAA api to mean both speed and std
        if product_name == "water_level":
            standardize_columns["s"] = "water_level_standard_deviation"
        return standardize_columns

    def get_station_metadata(self):
        station_dir = Path("stations") / "stations.tsv"
        station_df = files.load_df(station_dir)
        station_df = station_df[station_df["source"] == "noaa"]
        station_df = station_df.set_index("station_id")
        return station_df

    def get_sensor_ids(self):
        station_df = self.get_station_metadata()
        return station_df.index

    def get_most_recent_measurements(self,
        sensor_id,
        measurement_types = None
    ):
        now = datetime.now().strftime(self.time_format)
        last_measure = datetime.now() - timedelta(minutes=self.frequency)
        last_measure = last_measure.strftime(self.time_format)
        return self.get_historical_measurements(
            sensor_id, measurement_types, last_measure, now)

    def get_single_sensor_historical_measurements(self,
            sensor_id,
            measurement_types = None,
            start_time = None,
            end_time = None,
    ):
        if measurement_types is None:
            station_type = self._determine_station_type(sensor_id)
            if station_type == "noaa_std":
                products = ['water_level', 'air_temperature',
                            'water_temperature', 'wind', 'predictions']
            elif station_type == "current":
                products = ['currents', 'currents_predictions']
            else:
                print(sensor_id)
            measurement_types = products
        # divide time into allowable chunks. NOAA api allows a max of 31 days
        start_time, end_time = self.get_default_times(start_time, end_time)
        time_pairs = []
        chunked_start_time = end_time - timedelta(days=self.max_time_range)
        chunked_start_time = max(start_time, chunked_start_time)
        chunked_end_time = end_time
        while chunked_start_time >= start_time:
            chunked_start_time = max(start_time, chunked_start_time)
            time_pairs.append((chunked_start_time, chunked_end_time))
            chunked_end_time = chunked_start_time
            chunked_start_time -= timedelta(days=self.max_time_range)


        product_dfs = []
        for chunked_start, chunked_end in time_pairs:
            chunked_start = chunked_start.strftime(self.source_time_format)
            chunked_end = chunked_end.strftime(self.source_time_format)
            for product in measurement_types:
                product_df = self._collect_product(
                    sensor_id, product, chunked_start, chunked_end
                )
                product_dfs.append(product_df)
            all_products = pd.concat(product_dfs) if product_dfs else []
        return all_products

    def _collect_product(self,
            station_id,
            product_name,
            start_time,
            end_time):
        """
        Collect a single product into a dataframe, set to self.product_name
        
        Inputs:
            product_name (str): Product name to extract
        Output:
            result_df (pd.DataFrame): resulting data. Also saved as attribute
        """
        input_rqst_str = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date={}&end_date={}&station={}&product={}&datum={}&units=metric&time_zone=gmt&application=UCResearch&format=json".format(
            start_time, 
            end_time,
            station_id,
            product_name,
            self.datum
        )
        rqst_output = self.make_request(input_rqst_str)
        result_df = self._normalize_json(rqst_output.json())
        result_df = result_df.rename(columns=self.standardize_columns(product_name))
        result_df = pd.melt(result_df, id_vars=["datetime"], var_name='parameter')
        result_df["sensor_id"] = station_id
        stations = self.get_station_metadata()
        result_df["latitude"] = stations.loc[station_id, "latitude"]
        result_df["longitude"] = stations.loc[station_id, "longitude"]
        return result_df

    def _normalize_json(self, json_output):
        """ Flatten the JSON output into a pandas dataframe. """
        if 'data' in list(json_output) and 'metadata' in list(json_output):
            normalized_df = pd.json_normalize(json_output, record_path =['data'])
        elif list(json_output)[0] == 'predictions':
            normalized_df = pd.json_normalize(json_output, record_path =['predictions'])
        elif list(json_output)[0] == 'current_predictions':
            normalized_df = pd.json_normalize(json_output['current_predictions'], record_path =['cp'])
        elif ('error' in json_output and
              json_output['error']['message'].startswith('No data was found.')):
            return pd.DataFrame(columns=['datetime', 'parameter', 'value', 'sensor_id'])
        else:
            print(json_output)
            error_message = ("json output is an unexpected format such that"
                             " it has not been normalized")
            raise ValueError(error_message)
        return normalized_df

    def _determine_station_type(self, station_id):
        """
        Determine if the station id provided is in the proper format for 
        a standard noaa station or a current station.
        Returns:
            (str) 'noaa_std' or 'current'
        """
        if len(station_id) == 7:
            try: 
                int(station_id)
                return 'noaa_std'
            except: # there is likely a character in the id
                error_message = ("7 character NOAA station id's are expected 0"
                                 " to be numerical. Supplied id: {}".format(
                                     station_id)
                )
                raise ValueError(error_message)
        elif len(station_id) == 6:
            return 'current'
        return 'unknown'

def collect_all_stations(start_time=None, 
                         end_time=None,
                         products=None,
                         datum="MLLW"):
    """
    Collects all data for all stations in given time period

    Args: 
        station_id: (str) station id number (7 digits for water/air
            requests or 2 letters and 4 digits for current requests)
        start_time: (str) the date to start collecting from (can
            be the format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:mm'). Defaults
            to 3 days ago.
        end_time: (str) the date to end collection (can be the format
            'YYYY-MM-DD' or 'YYYY-MM-DD HH:mm'). Defaults to today.
        products: (list of str) the desired products/information
            to collect.
        datum: (str) for water-level information. Defaults to MLLW
    """
    station_ids = NOAA_STATIONS["station_id"].to_list()[:10]
    station_ids += CURRENT_STATIONS["station_id"].to_list()[:10]
    station_dfs = []
    noaa = NOAA()
    for station_id in station_ids:
        try:
            station_df = noaa.get_historical_measurements(
                str(station_id), start_time=start_time, end_time=end_time)
            station_dfs.append(station_df)
        except Exception as e:
            print(e)
    
    return pd.concat(station_dfs)

if __name__ == "__main__":
    station_products_df = collect_all_stations("2021-10-25", "2021-10-27", None)
    files.save_df(station_products_df, "test.tsv")
