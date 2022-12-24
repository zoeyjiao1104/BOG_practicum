"""
loading_job.py
"""

import os
import os
import pandas as pd
import requests
from abc import ABC
from datetime import datetime, timezone
from datetime import timedelta
from retrieval.argo import ArgoDrifter
from retrieval.bog import BOG
from retrieval.dfo import DFO
from retrieval.drifter import GlobalDrifter
from retrieval.noaa import NOAA
from typing import Dict, List
from utilities import files
from utilities import distance
from utilities.logger import logger



PRODUCT_NAME_MAP = {
    'acceleration': 'a',
    'air_temperature': 'at',
    'battery_temperature': 'bt',
    'currents_bin': 'cb',
    'currents_direction': 'cd',
    'currents_predictions_depth': 'cdpth',
    'currents_speed': 'cs',
    'depth': 'depth',
    'ebb_direction': 'ced',
    'flood_direction': 'cfd',
    'momsn': 'momsn',
    'position_delta': 'pd',
    'previous_latitude': 'plat',
    'previous_longitude': 'plon',
    'tide': 't',
    'uc_temperature': 'uct',
    'velocity_major': 'cvm',
    'water_level': 'wl',
    'water_temperature': 'wt',
    'wind_speed': 'ws',
    'wind_cardinal_direction': 'wcd',
    'wind_direction': 'wd',
    'wind_gust_speed': 'wgs',
    'wind_quartod_flags': 'w',
    'wlp': 'wl',
}

PRODUCT_TYPE_MAP = {
    'mean': 'm',
    'prediction': 'pr',
    'predictions_mean':'prm',
    'quality': 'q',
    'quartod_flags': 'qf',
    'q1': 'q1',
    'q2': 'q2',
    'q3': 'q3',
    'standard_deviation': 'sd',
    'water_level_prediction_highs_lows': 'prhl',
    'wlp-bores': 'prb',
}

class LoadingClient(ABC):
    """
    """

    def __init__(self) -> None:
        """
        The class constructor.
        """
        try:
            self.base_api_url = os.environ['BASE_API_URL']
        except KeyError:
            raise Exception("Missing configuration for "
                "base API url.")

        self.product_map = PRODUCT_NAME_MAP
        self.type_map = PRODUCT_TYPE_MAP
        super().__init__()
    

    def map_row_values(self, row, dic):
        """
        """
        for k,v in dic.items():
            if k in row.parameter:
                return v 
        if dic == self.type_map:
            return 'r'
        return None
    

    def get_api_data(
        self,
        url: str,
        timeout:int=3) -> List[Dict]:
        """
        Retrieves data from the API through a GET request.

        Parameters:
            url (str): The API endpoint.

            timeout (int): The number of seconds to wait
                for the API request to complete.

        Returns:
            (list of dict): A representation of the
                newly-created or upserted records.
        """
        try:
            r = requests.get(url=url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"HTTP error: {e}. {r.json()}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error: {e}")
        except requests.exceptions.Timeout as e:
            raise Exception(f"Timeout error: {e}")
        except requests.exceptions.JSONDecodeError as e:
            raise Exception(f"Response is invalid JSON. {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(e)


    def patch_api_data(
        self,
        url: str,
        data: Dict,
        timeout:int=3) -> Dict:
        """
        Partially updates data from the API through a PATCH request.

        Parameters:
            url (str): The API endpoint.

            data (dict): The data to update.

            timeout (int): The number of seconds to wait
                for the API request to complete.

        Returns:
            (dict): A representation of the
                updated records.
        """
        try:
            r = requests.patch(url=url, data=data, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"HTTP error: {e}. {r.json()}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error: {e}")
        except requests.exceptions.Timeout as e:
            raise Exception(f"Timeout error: {e}")
        except requests.exceptions.JSONDecodeError as e:
            raise Exception(f"Response is invalid JSON. {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(e)


    def post_api_data(
        self,
        url: str,
        data: Dict,
        timeout:int=3) -> List[Dict]:
        """
        Loads data into the API through a POST request.

        Parameters:
            url (str): The API endpoint.

            data (dict): The data to insert or upsert.

            timeout (int): The number of seconds to wait
                for the API request to complete.

        Returns:
            (list of dict): A representation of the
                newly-created or upserted records.
        """
        try:
            r = requests.post(url=url, json=data, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"HTTP error: {e}. {r.json()}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error: {e}")
        except requests.exceptions.Timeout as e:
            raise Exception(f"Timeout error: {e}")
        except requests.exceptions.JSONDecodeError as e:
            raise Exception(f"Response is invalid JSON. {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(e)


    def merge_date_ranges(self, date_ranges: List[datetime]):
        """
        """
        intervals = []
        for date_start, date_end in date_ranges:
            if not intervals:
                intervals.append((date_start, date_end))
                continue

            new_intervals = []
            for interval_start, interval_end in intervals:
                if (date_start <= interval_end) and (date_end >= interval_start):
                    date_start = min(date_start, interval_start)
                    date_end = max(date_end, interval_end)
                else:
                    new_intervals.append((interval_start, interval_end))

            new_intervals.append((date_start, date_end))
            intervals = new_intervals

        return intervals


    def batch(
        self,
        entities: List[object],
        batch_size: int):
        """
        """
        total_num_objects = len(entities)
        num_whole_batches = total_num_objects // batch_size
        num_partial_batches = 1 if total_num_objects % batch_size > 0 else 0
        num_batches = num_whole_batches + num_partial_batches

        batches = []
        for i in range(num_batches):
            idx = current_num_objects = batch_size * i

            if batch_size + current_num_objects < total_num_objects:
                batches.append(entities[idx:idx + batch_size])
            else:
                batches.append(entities[idx:])

        return batches


    def parse_decimal_columns(
        self,
        df: pd.DataFrame,
        cols: List[str],
        decimals:int=6):
        """Local function to round a DataFrame column"""
        for col in cols:
            df[col] = df[col].astype('float64')
            df[col] = df[col].round(decimals=decimals)


    def parse_utc_datetime_col(
        self,
        df: pd.DataFrame,
        cols: List[str]):
        """Local function to parse a DateFrame column as a UTC datetime."""
        for col in cols:
            df[col] = pd.to_datetime(df[col], utc=True)

   
    def get_sensor_neighbors(
        self,
        buoy_events_df: pd.DataFrame,
        entities_df: pd.DataFrame,
        entity_cols: List[str]):
        """
        """
        # Get geographically closest stations for each buoy measurement event
        try:
            logger.info("Calculating nearest neighbors "
                "for buoy measurement events.")
            nearest = distance.get_nearest(
                query_points=buoy_events_df,
                candidate_points=entities_df,
                metric='haversine')
        
            renamed_closest_entitites = []
            for j, jth_closest_entity in enumerate(nearest):
                jth_closest_entity.columns = [
                    str(col) + "_nearest_entity_{}".format(j+1)
                    for col in jth_closest_entity
                ]
                renamed_closest_entitites.append(jth_closest_entity)
            df = pd.concat([buoy_events_df] + renamed_closest_entitites, axis='columns')

        except Exception as e:
            raise Exception(f"Failed to calculate nearest neighbors. {e}")

        # Reshape resulting dataset
        try:
            logger.info("Reshaping resulting data.")

            # Rename id column referencing buoy measurement event as foreign key
            df = df.rename(columns={"id": "mobile_event"})

            # Define columns for neighboring entities
            buoy_cols = ['datetime', 'mobile_event']
            entity_1_cols = []
            entity_2_cols = []
            mapped_cols = []
            for c in entity_cols:
                entity_1_cols.append(f'{c}_nearest_entity_1')
                entity_2_cols.append(f'{c}_nearest_entity_2')
                mapped_cols.append(f'entity_{c}')

            cols_entity_1 = buoy_cols + entity_1_cols
            cols_entity_2 = buoy_cols + entity_2_cols

            # Subset result rows to first neighbor data
            # and standardize column names
            first_neighbors_df = df[cols_entity_1]
            first_col_mapping = dict(zip(entity_1_cols, mapped_cols))
            first_neighbors_df = first_neighbors_df.rename(columns=first_col_mapping)

            # Subset result rows to second neighbor data
            # and standardize column names
            second_neighbors_df = df[cols_entity_2]
            second_col_mapping = dict(zip(entity_2_cols, mapped_cols))
            second_neighbors_df = second_neighbors_df.rename(columns=second_col_mapping)

            # Concat neighbors into single DataFrame and clean columns
            neighbors_df = pd.concat([first_neighbors_df, second_neighbors_df])
            neighbors_df['datetime'] = pd.to_datetime(neighbors_df['datetime'], utc=True)
            logger.info(f"{len(neighbors_df)} row(s) in reshaped DataFrame.")

        except Exception as e:
            raise Exception(f"Failed to reshape neighbors results. {e}")

        return neighbors_df

