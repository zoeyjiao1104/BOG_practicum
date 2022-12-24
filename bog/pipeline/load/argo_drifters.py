"""
argo_drifters.py
"""

import pandas as pd
from datetime import datetime, timedelta
from load.common import LoadingClient
from retrieval.argo import ArgoDrifter
from typing import Dict, List
from utilities.logger import logger


class ArgoLoadingClient(LoadingClient):
    """
    A client for fetching, merging, and reshaping
    Argo drifter data for upserts into the mobile
    sensor, measurement event, and measurement
    database tables.
    """
   
    def __init__(self, source_id: str) -> None:
        """
        The class constructor.

        Parameters:
            source_id (str): The unique identifier for
                the Argo data source. This is used as a
                foreign key for creating new mobile sensors.

        Returns:
            None
        """
        super().__init__()
        self.argo = ArgoDrifter()
        self.source_id = source_id


    def get_argo_drifter_measurements(
        self,
        argo_drifters_df: pd.DataFrame,
        min_date: datetime,
        max_date: datetime,
        batch_size: int=20) -> pd.DataFrame:
        """
        """    
        # Get measurements for Argo drifters
        logger.info("Retrieving measurements for Argo drifters.")
        dfs = []
        argo_id_batches = self.batch(
            entities=argo_drifters_df['id'].tolist(),
            batch_size=batch_size
        )
        num_batches = len(argo_id_batches)

        for i in range(num_batches):
            try:
                # Call API (override by providing multiple sensor ids)
                logger.info("Retrieving Argo measurement batch "
                    f"{i + 1} of {num_batches}.")
                ids = argo_id_batches[i]
                df = self.argo.get_single_sensor_historical_measurements(
                    sensor_id=ids,
                    start_time=min_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_time=max_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                )

                # Handle exceptions
                if df is None:
                    continue
                    
                # Handle empty data
                if not len(df):
                    logger.info("No records found. Continuing to next batch.")
                    continue

                logger.info(f"{len(df)} record(s) found. Cleaning data.")

                # Reshape data
                df.reset_index(drop=True, inplace=True)
                df.dropna(inplace=True)
                df.drop_duplicates(inplace=True)
                df = df.groupby(['datetime', 'latitude', 'longitude']).min('sea_pressure')
                df.reset_index(inplace=True)
                df.rename(columns={'sensor_id':'id', 'salinity': 'sl', 'sea_temperature': 'wt', 'sea_pressure': 'wp' }, inplace=True)
                df = df.melt(
                    id_vars=['id','datetime', 'latitude','longitude'], 
                    value_vars=['sl', 'wt', 'wp'],
                    var_name='product',
                    value_name='value'
                )
                df['type'] = 'r'
                df['source'] = self.source_id

                # Reformat data
                df.value = df.value.round(5)
                df.longitude = df.longitude.round(6)
                df.latitude = df.latitude.round(6)
                df.datetime = df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")

                dfs.append(df)
            except Exception as e:
                raise Exception(f"Failed to retrieve and parse Argo data from API. {e}")

        final_df = pd.concat(dfs, ignore_index= True)
        final_df.to_csv("argo_measurements.csv")
        return final_df

 
    def load_argo_drifters(self) -> List[Dict]:
        """
        Fetches Argo drifter metadata, extracts the
        drifter ids, and then upserts those ids into
        the mobile sensors database table.

        Parameters:
            None

        Returns:
            (pd.DataFrame): A representation of the
                newly-created sensors.
        """
        # Get Argo drifter ids
        try:
            logger.info("Retrieving Argo drifter metadata.")
            argo_ids = self.argo.get_sensor_ids()
            logger.info(f"{len(argo_ids)} id(s) found")
        except Exception as e:
            raise Exception(f"Failed to retrieve Argo drifter metadata. {e}")

        # Read ids into DataFrame
        logger.info("Reading metadata into DataFrame.")
        source_ids = [self.source_id] * len(argo_ids)
        cols = ['id', 'source']
        df = pd.DataFrame(list(zip(argo_ids, source_ids)), columns=cols)

        # Upsert drifters to database table
        try:
            logger.info("Upserting Argo drifter metadata into DB table.")
            argo_dict = df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilesensors/"
            created_drifters = self.post_api_data(url, argo_dict)
            logger.info(f"{len(created_drifters)} Argo drifters "
                "successfully inserted (or retrieved if they "
                "already existed).")
        except Exception as e:
            raise Exception(f"Insert failed. {e}")

        return pd.DataFrame(created_drifters)


    def load_argo_drifter_measurement_events(
        self,
        argo_neighbors_df: pd.DataFrame):
        """
        """
        # Parse measurement events from neighbors DataFrame
        logger.info("Parsing Argo measurement events from DataFrame.")
        argo_event_cols = [
            'entity_id',
            'entity_datetime',
            'entity_latitude',
            'entity_longitude',
        ]
        argo_events_df = argo_neighbors_df.loc[:, argo_event_cols]
        argo_events_df = argo_events_df.rename(columns={
            'entity_id': 'mobile_sensor',
            'entity_datetime': 'datetime',
            'entity_latitude': 'latitude',
            'entity_longitude': 'longitude'
        })
        argo_events_df = argo_events_df.drop_duplicates()
        argo_events_json = argo_events_df.to_dict(orient='records')

        # Load measurement events into DB
        try:
            logger.info("Inserting Argo measurement events into DB.")
            url = f"{self.base_api_url}/mobilemeasurementevents/"
            created_events = self.post_api_data(url, argo_events_json, timeout=100)
            logger.info(f"{len(created_events)} event(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert Argo measurement events. {e}")

        return pd.DataFrame(created_events)


    def load_argo_drifter_measurement_products(
        self,
        argo_events_df: pd.DataFrame):
        """
        """

        # Merge created Argo events with retrieved
        # Argo measurements to associate foreign keys
        try:
            logger.info("Merging Argo events with Argo measurements.")

            # Load created events into DataFrame and prepare columns for merge
            argo_events_df['mobile_sensor'] = argo_events_df['mobile_sensor'].astype('int64')
            argo_events_df['datetime'] = pd.to_datetime(argo_events_df['datetime'], utc=True)
            argo_measurements_df['datetime'] = pd.to_datetime(argo_measurements_df['datetime'], utc=True)

            # Merge
            argo_measurements_df = argo_measurements_df.rename(columns={"id":"mobile_sensor"})
            argo_measurements_df.to_csv("measurements_argo.csv")
            argo_events_df.to_csv("created_argo_events.csv")
            argo_measurements_df = argo_measurements_df.merge(
                right=argo_events_df,
                how="left",
                on=['mobile_sensor', 'datetime'],
            )

            # Reshape and subset data
            argo_measurements_df = argo_measurements_df.rename(columns={'id':'mobile_measurement_event'})
            measurement_cols = ['mobile_measurement_event', 'product', 'value', 'type']
            argo_m_to_send_df = argo_measurements_df.loc[:, measurement_cols]
            argo_m_to_send_df = argo_m_to_send_df.dropna()
        except Exception as e:
            raise Exception(f"Failed to merge Argo events and products. {e}")

        # Insert measurement products into database
        try:
            logger.info("Inserting Argo measurement products into DB.")
            argo_m_to_send_df.to_csv("argo_products.csv")
            argo_products_json = argo_m_to_send_df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilemeasurements/"
            created_products = self.post_api_data(url, argo_products_json, timeout=100)
            logger.info(f"{len(created_products)} product(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert Argo measurement products. {e}")

        return pd.DataFrame(created_products)


    def load_argo_drifter_neighbors(
        self,
        argo_neighbors_df: pd.DataFrame,
        argo_events_df: pd.DataFrame):
        """
        """
        # Create station neighbors
        try:
            logger.info("Creating final DataFrame for Argo neighbors.")
            argo_neighbors_df['entity_datetime'] = pd.to_datetime(argo_neighbors_df['entity_datetime'], utc=True)
            argo_n_to_send_df = argo_neighbors_df.merge(
                right=argo_events_df,
                how="left",
                left_on=['entity_id', 'entity_datetime'],
                right_on=['mobile_sensor', 'datetime'],
            )
            argo_n_to_send_df = argo_n_to_send_df[[
                'mobile_event',
                'id',
                'distance'
            ]]
            argo_n_to_send_df = argo_n_to_send_df.rename(
                columns={
                    'id':'neighboring_mobile_event'
                }
            )
        except Exception as e:
            raise Exception(f"Failed to create final DataFrame. {e}")

        # Insert buoy-station neighbors into database
        try:
            logger.info("Inserting Argo neighbors into database table.")
            neighbors_json = argo_n_to_send_df.to_dict(orient="records")
            url = f"{self.base_api_url}/mobilemeasurementeventneighbors/"
            created_neighbors = self.post_api_data(url, neighbors_json)
            logger.info(f"{len(created_neighbors)} neighbors "
                "succesfully inserted into the database (or "
                "retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert Argo neighbors. {e}")

        return pd.DataFrame(created_neighbors)


    def link_argo_drifters_with_buoys(
        self,
        argo_drifters_df: pd.DataFrame,
        buoy_events_df: pd.DataFrame):
        """
        """
        # Retrieve base set of Argo drifter measurements
        buoy_events_df['datetime'] = pd.to_datetime(buoy_events_df['datetime'], utc=True)
        min_date = min(buoy_events_df['datetime']) - timedelta(minutes=180)
        max_date = max(buoy_events_df['datetime']) + timedelta(minutes=180)
        argo_measurements_df = self.get_argo_drifter_measurements(
            argo_drifters_df,
            min_date,
            max_date
        )

        # Compute Argo neighbors
        argo_drifter_cols = [
            'id',
            'source',
            'distance',
            'datetime',
            'latitude',
            'longitude'
        ]
        argo_neighbors_df = self.get_sensor_neighbors(
            buoy_events_df=buoy_events_df,
            entities_df=argo_measurements_df,
            entity_cols=argo_drifter_cols
        )

        # Load Argo measurement events
        argo_events_df = self.load_argo_drifter_measurement_events(argo_neighbors_df)

        # Load Argo measurement products
        self.load_argo_drifter_measurement_products(argo_events_df)

        # Load Argo neighbors
        self.load_argo_drifter_neighbors(argo_neighbors_df, argo_events_df)
