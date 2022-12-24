"""
buoys.py
"""

import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from load.common import LoadingClient
from retrieval.bog import BOG
from typing import Dict, List
from utilities import distance
from utilities import files
from utilities.logger import logger


class BuoyLoadingClient(LoadingClient):
    """
    A client for fetching, merging, and reshaping
    BOG buoy data for upserts into the mobile
    sensor and measurement database tables.
    """

    def __init__(self, source_id: str) -> None:
        """
        The class constructor.

        Parameters:
            source_id (str): The unique identifier for
                the BOG data source. This is used as a foreign
                key for creating new mobile sensors.

        Returns:
            None
        """
        super().__init__()
        self.bog = BOG()
        self.source_id = source_id


    def get_buoys(self) -> List[Dict]:
        """
        Retrieves all BOG buoy mobile sensors
        stored in the database.

        Parameters:
            None

        Returns:
            (list of Dict): The sensors.
        """
        try:
            url = f"{self.base_api_url}/mobilesensors/?source={self.source_id}"
            return self.get_api_data(url)
        except Exception as e:
            raise Exception(f"Failed to fetch buoys. {e}")


    def get_buoy_measurements(
        self,
        start: datetime=None,
        end: datetime=None,
        buoy_ids: List[str]=None) -> pd.DataFrame:
        """
        Queries the BOG API for buoy measurement data
        for the given sensors and specified timeframe.
        Then cleans and reformats the returned payload
        as a DataFrame.

        Parameters:
            start (datetime.datetime): The inclusive start
                date for which to retrieve measurements.
                Defaults to the previous date if not
                provided.

            end (datetime.datetime): The inclusive end
                date for which to retrieve measurements.
                Defaults to the current date if not
                provided.

            buoy_ids (list of str): The unique identifiers
                of the buoys to query. Defaults to `None`,
                in which case all buoys are retrieved.

        Returns:
            (pd.DataFrame): The cleaned measurements.
        """
        # Determine start and end datetimes
        start = datetime.utcnow() - timedelta(days=1) if not start else start
        end = datetime.utcnow() if not end else end

        # Query BOG API for historical measurements and store as DataFrame
        try:
            start = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            df = self.bog.get_historical_measurements(
                start_time=start,
                sensor_ids=buoy_ids
            )
            logger.info(f"{len(df)} record(s) found.")
        except Exception as e:
            raise Exception(f"Failed to retrieve historical BOG measurements. {e}")

        # Clean DataFrame
        try:
            logger.info("Cleaning DataFrame holding record(s).")
            df.datetime = df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
            df.loc[:, ['product']] = df.apply(lambda row: self.map_row_values(row, self.product_map), axis=1)
            df.loc[:, ['type']] = df.apply(lambda row: self.map_row_values(row, self.type_map), axis=1)
            df = df.dropna()
            df = df.rename(columns = {'sensor_id':'mobile_sensor'})
            df.value = df.value.round(3)
            df = df.replace([''], [None])
            logger.info(f"{len(df)} record(s) remaining after cleaning.")
        except Exception as e:
            raise Exception(f"Error cleaning BOG measurements. {e}")

        return df


    def load_buoys(self) -> List[Dict]:
        """
        Fetches buoy ids from the BOG API, cleans and reformats
        them, and then inserts the resulting payload into the
        into the mobile sensors DB table.

        Parameters:
            None

        Returns:
            (list of dict): A representation of the created
                sensors.
        """
        # Retrieve buoy ids
        try:
            logger.info("Querying the BOG API for all available buoy sensor ids.")
            buoy_ids = self.bog.get_sensor_ids()
            logger.info(f"{len(buoy_ids)} id(s) found.")
        except Exception as e:
            raise Exception(f"Failed to retrieve buoy ids. {e}")
    
        # Prepare request body for insert, getting rid
        # of any duplicate ids retrieved from the API
        logger.info("Preparing buoy ids for insert.")
        buoy_dict = [
            {'id': id, 'source': self.source_id}
            for id in set(buoy_ids)
        ]
        logger.info(f"{len(buoy_ids) - len(buoy_dict)} duplicate id(s) dropped.")

        # Insert mobile sensors data
        try:
            logger.info("Inserting buoys into database table.")
            url = f"{self.base_api_url}/mobilesensors/"
            created_buoys = self.post_api_data(url, buoy_dict)
            logger.info(f"{len(created_buoys)} buoys successfully inserted "
                "(or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Insert failed. {e}")

        return created_buoys


    def load_buoy_fishery_assignments(self) -> List[Dict]:
        """
        Loads buoy fishery assignments read from
        a CSV file into the database.

        Parameters:
            None

        Returns:
            (list of dict): The fishery assignments.
        """
        # Load buoy fishery assignments from file
        try:
            logger.info("Reading buoy fishery assignments from file.")
            assignments = files.load_df("database/buoy_fishery_assignments.csv", delimiter=',')
            assignments = assignments.replace([np.nan], [None])
            logger.info(f"{len(assignments)} fishery assignment(s) found.")
        except Exception as e:
            raise Exception(f"Failed to retrieve buoy fishery assignments. {e}")

        # Load assignments into database
        try:
            logger.info("Loading fishery assignments into database.")
            assignment_json = assignments.to_dict(orient='records')
            url = f"{self.base_api_url}/fisheryassignments/"
            self.post_api_data(url, assignment_json, timeout=100)
            logger.info(f"Assignments successfully upserted.")
        except Exception as e:
            raise Exception(f"Failed to load buoy fishery assignments "
                f"database. {e}")


    def load_buoy_measurement_events(
        self,
        buoy_measurements_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses cleaned rows of buoy measurements from the API
        to create mobile sensor measurement events and then
        uploads the events into the corresponding DB table.

        Parameters:
            buoy_measurements_df (pd.DataFrame): The
                measurements retrieved from the BOG API.

        Returns:
            (pd.DataFrame): The created measurement events.
        """
        # Parse measurement events from DataFrame
        try:
            logger.info("Parsing measurement events from cleaned data.")
            df = buoy_measurements_df
            df.loc[:, ['datetime']] = df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
            events_cols = ['datetime', 'latitude', 'longitude', 'mobile_sensor']
            events_df = df.loc[:, events_cols]
            events_df = events_df.drop_duplicates(events_cols)
            logger.info(f"{len(events_df)} unique measurement event(s) found.")
        except Exception as e:
            raise Exception(f"Failed to parse measurement events. {e}")

        # Load measurement events into DB
        try:
            logger.info("Inserting BOG measurement events into DB.")
            events_json = events_df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilemeasurementevents/"
            created_events = self.post_api_data(url, events_json, timeout=100)
            logger.info(f"{len(created_events)} event(s) succesfully created "
                "or retrieved if they already existed.")
        except Exception as e:
            raise Exception(f"Failed to insert BOG measurement events. {e}")

        # Read created events into DataFrame and clean
        try:
            created_events_df = pd.DataFrame(created_events)
            created_events_df = created_events_df.astype({
                'latitude': 'float64',
                'longitude': 'float64',
                'mobile_sensor': 'object'
            })
            created_events_df['datetime'] = pd.to_datetime(created_events_df['datetime'], utc=True)
        except Exception as e:
            raise Exception(f"Error parsing and cleaning created events as DataFrame. {e}")

        return created_events_df


    def load_buoy_measurement_products(
        self,
        buoy_measurements_df: pd.DataFrame,
        buoy_measurement_events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses measurement product information from the
        BOG records, merges that information with the
        newly-created buoy mobile measurement events,
        and then inserts the resulting dataset into
        the BOG measurement table.

        Parameters:
            buoy_measurements_df (pd.DataFrame): The
                cleaned measurements retrieved the BOG
                API.

            buoy_measurement_events_df (pd.DataFrame):
                The newly-created BOG buoy mobile
                    measurement events.

        Returns:
            (pd.DataFrame): The created BOG buoy mobile
                measurement products.
        """
        # Use abbreviated names
        df = buoy_measurements_df
        created_events_df = buoy_measurement_events_df

        # Join created measurement events with measurements to get foreign keys
        try:
            logger.info("Joining measurement event ids to measurements.")
            join_cols = ['datetime', 'latitude', 'longitude', 'mobile_sensor']    
            df.loc[:, ['datetime']] = pd.to_datetime(df['datetime'], infer_datetime_format=True, utc=True)
            created_events_df['mobile_sensor'] = created_events_df['mobile_sensor'].astype('object')
            measurements_df = df.merge(created_events_df, on=join_cols, how="left")
            measurements_df = measurements_df.loc[:, ['id', 'product', 'value', 'type']]
            measurements_df = measurements_df.reset_index(drop=True)
            measurements_df = measurements_df.rename(columns={"id": "mobile_measurement_event"})
        except Exception as e:
            raise Exception(f"Joining and cleaning process failed. {e}")

        # Load measurements into DB
        try:
            logger.info(f"Inserting {len(measurements_df)} BOG measurement(s) into DB.")
            measurements_json = measurements_df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilemeasurements/"
            created_measurements = self.post_api_data(url, measurements_json, timeout=100)
            logger.info(f"{len(created_measurements)} measurements "
                "successfully created (or retrieved if they already exist).")
        except Exception as e:
            raise Exception(f"Failed to insert BOG measurements. {e}")

        return pd.DataFrame(created_measurements)


    def load_buoy_neighbors(
        self,
        buoy_measurement_events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Computes each buoy measurement event's two closest
        neighbors according to both geographic location and
        time. Then cleans and reshapes the resulting
        DataFrame and inserts the records into the mobile
        measurement event neighbors database table.

        Parameters:
            buoy_measurement_events_df (pd.DataFrame): The
                buoy measurement events created by the DB.

        Returns:
            (pd.DataFrame): The newly-created buoy neighbors.
        """
        # Abbreviate DataFrame names
        df = buoy_measurement_events_df

        # Identify nearest two measurement event neighbors
        try:
            logger.info("Finding nearest two buoys for each measurement "
                "event based on haversine distance.")
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df = df.rename(columns={"mobile_sensor":"sensor_id"})
            raw_nearest_buoys_df = distance.find_nearest_buoys(df)
            logger.info(f"{len(raw_nearest_buoys_df)} record(s) returned.")
        except Exception as e:
            raise Exception(f"Failed to calculate distance. {e}")

        # Drop empty rows and return if no neighbors found.
        # NOTE: This could result if the selected timeframe
        # for querying BOG data contains only one buoy (e.g.,
        # 133) or if no buoys are close in time or physical
        # proximity.
        logger.info("Dropping empty rows (i.e., absence of neighbors).")
        raw_nearest_buoys_df = raw_nearest_buoys_df.dropna()
        logger.info(f"{len(raw_nearest_buoys_df)} row(s) remaining.")
        if not len(raw_nearest_buoys_df):
            logger.info("No buoy neighbors found. Skipping database insert step and continuing process.")
            return []

        # Reshape resulting data for database inesrt
        try:
            logger.info("Reshaping resulting data for DB insert.")

            # Parse datetime fields
            for col in ['datetime', 'datetime_nearest_buoy_1', 'datetime_nearest_buoy_2']:
                raw_nearest_buoys_df[col] = pd.to_datetime(raw_nearest_buoys_df[col], utc=True)

            # Rename id column referencing mobile event foreign key
            raw_nearest_buoys_df = raw_nearest_buoys_df.rename(columns={"id": "mobile_event"})

            # Subset result rows to first neighbor data
            first_neighbors_df = raw_nearest_buoys_df[[
                'mobile_event',
                'id_nearest_buoy_1',
                'distance_nearest_buoy_1',
            ]]

            # Standardize first neighbor column names
            first_neighbors_df = first_neighbors_df.rename(columns={
                'id_nearest_buoy_1': 'neighboring_mobile_event',
                'distance_nearest_buoy_1': 'distance',
            })

            # Subset result rows to second neighbor data 
            second_neighbors_df = raw_nearest_buoys_df[[
                'mobile_event',
                'id_nearest_buoy_2',
                'distance_nearest_buoy_2',
            ]]

            # Standardize second neighbor column names
            second_neighbors_df = second_neighbors_df.rename(columns={
                'id_nearest_buoy_2': 'neighboring_mobile_event',
                'distance_nearest_buoy_2': 'distance',
            })

            # Concat neighbors into single DataFrame
            neighbors_df = pd.concat([first_neighbors_df, second_neighbors_df])
            logger.info(f"{len(neighbors_df)} row(s) in reshaped DataFrame.")
        except Exception as e:
            raise Exception(f"Failed to reshape neighbors results. {e}")

        # Insert buoy neighbors into database
        try:
            logger.info("Inserting buoy neighbors into database table.")
            neighbors_json = neighbors_df.to_dict(orient="records")
            url = f"{self.base_api_url}/mobilemeasurementeventneighbors/"
            created_neighbors = self.post_api_data(url, neighbors_json)
            logger.info(f"{len(created_neighbors)} neighbors "
                "succesfully inserted into the database (or "
                "retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert buoy neighbors. {e}")

        return pd.DataFrame(created_neighbors)

