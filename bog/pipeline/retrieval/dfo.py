"""
filename: dfo.py
author: Wenshi Wang
email: wenshi@uchicago.edu
description: Functions to query dfo API.
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
from pathlib import Path
from retrieval.measurements import MeasurementRetrieval
from utilities import files

COLUMNS = ['id', 'officialName', 'provinceCode','longitude', 'latitude', 'isTidal','waterBody','establishedYear','Lower Low Water Mean Tide']
ERRANT_STATIONS = ['5cebf1e43d0f4a073c4bc3be','5cebf1e43d0f4a073c4bc3c2']

class DFO(MeasurementRetrieval):

    SOURCE_NAME = 'Fisheries and Oceans Canada(DFO)'
    SOURCE_WEBSITE = 'https://www.dfo-mpo.gc.ca/index-eng.html'

    frequency = 6
    source_time_format = "%Y-%m-%dT%H:%M:%SZ"
    standardized_columns = {
            "wlo" : "water_level_reading",
            "wlp" : "water_level_prediction",
            "wlp-hilo" : "water_level_prediction_highs_lows",
            "wlf" : "water_level_forecasts",
            "wlf-spine" : "water_level_forecasts",
            "eventDate": "datetime",
    }
    products = ['wlo','wlp','wlf','wlp-hilo','wlf-spine',
                'dvcf-spine','wlp-bores','wcp-slack']
    max_time_range = 7

    def get_station_metadata(self):
        station_dir = Path("stations") / "stations.tsv"
        station_df = files.load_df(station_dir)
        station_df = station_df[station_df["source"] == "dfo"]
        station_df = station_df.set_index("station_id")
        return station_df

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
        if sensor_id in ERRANT_STATIONS:
            raise ValueError("Sensor Id is an errant station")
        if measurement_types is None:
            measurement_types = self.products
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
                if len(product_df):
                    product_dfs.append(product_df)
            
            all_products = pd.concat(product_dfs) if product_dfs else []
        return all_products

    def _collect_product(self,
            station_id,
            product_name = "wlo",
            start_time = None,
            end_time = None):
        """
        Collect a single product into a dataframe, set to self.product_name
        
        Inputs
            product: (str) the desired product/information
                collect. Defaults to station's available products.
            start_time: (str) the time to start collecting from. Defaults to
                today. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Defaults to 3 days
                ago. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
        Outputs
            result_df (pd.DataFrame): resulting data. Also saved as attribute

        sample input: request_time_series("wlo","2020-10-01T00:00:00Z","2020-10-02T00:00:00Z")
        """
        request_string = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/{}/data?time-series-code={}&from={}&to={}".format(
            station_id, 
            product_name, 
            start_time, 
            end_time
        )
        response = self.make_request(request_string)
        product_df = self._normalize_json(response.json())
        product_df["parameter"] = self.standardized_columns.get(
            product_name, product_name
        )
        product_df = product_df.rename(columns=self.standardized_columns)
        product_df["sensor_id"] = station_id
        stations = self.get_station_metadata()
        product_df["latitude"] = stations.loc[station_id, "latitude"]
        product_df["longitude"] = stations.loc[station_id, "longitude"]
        return product_df

    def _normalize_json(self, json_output):
        """ Flatten the JSON output into pandas dataframe.
        
        Args:
            json_output (dict): Response for specific product format:
                [
                    {
                        "eventDate": self.time_format,
                        "qcFlagCode" int,
                        "value": object,
                        "timeSeriesId": str
                    },
                ] 
        """
        station_timeseries = pd.DataFrame(json_output)
        if (station_timeseries.empty):
            return station_timeseries
        else:
            station_timeseries = station_timeseries[['eventDate','value']]
        return station_timeseries

    def get_sensor_ids(self):
        """ Returns list of ids of all stations available """
        rqst_str = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations"
        stations = pd.DataFrame()
        all_response = requests.get(rqst_str).json()
        all_station = pd.DataFrame(all_response)[['id']]
        stations = stations.append(all_station, ignore_index=True)
        stations = stations['id'].tolist()
        stations = [station for station in stations if station not in ERRANT_STATIONS]
        return stations
    
    def metadata_handler(self, station_id):
        """
        This function will query all time-invariant information of a station given
        the API parameter provided and will append and return all the 
        returned metadata into a single pandas dataframe.

        Inputs
            station_id : (str) the id of a station 
    
        Outputs
            station_information: a one-row data frame with metadata given the station, including height data and characteristics (id, officialName, provinceCode, longitude, latitude, isTidal)                 

        sample input: process_station("5cebf1e23d0f4a073c4bc0f6")
        """
        station_information = pd.DataFrame(columns=COLUMNS)

        rqst_str = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/{}/metadata".format(station_id)
        single_station_request = self.make_request(rqst_str)

        checkHeight = len(single_station_request.json()['heights'])!=0

        # different data processing workflows for different kinds of missing data
        if checkHeight:
            single_station = pd.json_normalize(single_station_request.json(), 
                                                record_path=['heights'],
                                                meta=['id','officialName','provinceCode','longitude','latitude','isTidal','waterBody','establishedYear'],
                                                errors='ignore')
            single_station = single_station[['id','officialName','provinceCode','longitude','latitude','isTidal','waterBody','establishedYear','heightTypeId','value']]
            single_station = single_station.pivot(index=['id','officialName','provinceCode','longitude','latitude','isTidal','waterBody','establishedYear'],columns='heightTypeId',values='value').reset_index()
            single_station = single_station.rename(columns={'5cec2eba3d0f4a04cc64d5d5':'Lower Low Water Mean Tide'})

        elif not checkHeight:
            single_station = single_station_request.json()
            cols_for_json = ['id','officialName','longitude','latitude'] 
            for key in ['provinceCode','isTidal', 'waterBody','establishedYear']:
                if key in single_station:
                    cols_for_json.append(key)
            single_station = {key: single_station[key] for key in cols_for_json}
            single_station = pd.DataFrame(single_station,index=[0])

        station_information = station_information.append(single_station, ignore_index=True)
        station_information = station_information[COLUMNS]

        return station_information

    def multiple_metadata_handler(self, stations):
        """
        This function requests the metadata of all stations in Nova Scotia and Ontario 
        and put into a dataframe
    
        Outputs
            stations_info: a dataframe with metadata of all stations in Nova Scotia and Ontario               
        """
        stations_info = pd.DataFrame(columns=COLUMNS)    
        for station_id in stations:
            try:
                station_metadata = self.metadata_handler(station_id)
                stations_info = stations_info.append(station_metadata)
            except Exception as e:
                print("Error in DfoStationCollection.multiple_metadata_handler: ", e)
                print("Station Id: ", station_id)

        stations_info = stations_info[stations_info['provinceCode'].isin(['NS','ON'])]
        stations_info = stations_info[stations_info['isTidal']]
        stations_info = stations_info.rename(columns={'id':'station_id',
                                                      'officialName':'station_name',
                                                      'provinceCode':'state',
                                                      'establishedYear':'established',
                                                      'Lower Low Water Mean Tide':'MLLW'})
        return stations_info


if __name__ == "__main__":
    #station_products_df = collect_all_stations("2021-10-25", "2021-10-27", None)
    #print(station_products_df)
    #files.save_df(station_products_df, "dfo.tsv")

    dfo = DFO()
    print(dfo.get_historical_measurements("5cebf1df3d0f4a073c4bbc70"))