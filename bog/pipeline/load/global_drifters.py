"""
global_drifters.py
"""

import pandas as pd
from datetime import datetime, timedelta
from load.common import LoadingClient
from retrieval.drifter import GlobalDrifter
from typing import Dict, List
from utilities.logger import logger


class GlobalDrifterLoadingClient(LoadingClient):
    """
    A client for fetching, merging, and reshaping
    global drifter data for upserts into the mobile
    sensor, measurement event, and measurement
    database tables.
    """

    def __init__(self, source_id: str) -> None:
        """
        The class constructor.

        Parameters:
            source_id (str): The unique identifier for
                the global drifter data source. This is
                used as a foreign key for creating new
                mobile sensors.

        Returns:
            None
        """
        super().__init__()
        self.drifter_api = GlobalDrifter()
        self.source_id = source_id
    

    def get_drifter_measurements(
        self,
        global_drifters: List[Dict],
        min_date: datetime,
        max_date: datetime,
        batch_size: int=20):
        """
        """    
        # Get measurements for global drifters
        logger.info("Retrieving measurements for global drifters.")
        dfs = []
        global_id_batches = self.batch(
            entities=[int(g['id']) for g in global_drifters[:40]],
            batch_size=batch_size
        )
        num_batches = len(global_id_batches)

        for i in range(num_batches):
            try:
                # Call API (override by providing multiple sensor ids)
                logger.info("Retrieving global measurement batch "
                    f"{i + 1} of {num_batches}.")
                ids = global_id_batches[i]
                df = GlobalDrifter().get_single_sensor_historical_measurements(
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
                df = df[['platform_code','time (UTC)', 'latitude (degrees_north)', 'longitude (degrees_east)', 'sst (Deg C)']]
                df.reset_index(drop=True,inplace=True)
                df.dropna(inplace=True)
                df.drop_duplicates(inplace=True)
                df.rename(columns = {'platform_code':'id', 'time (UTC)':'datetime', 'latitude (degrees_north)':'latitude', 'longitude (degrees_east)': 'longitude', 'sst (Deg C)': 'sst'}, inplace=True)
                df = df.melt(
                    id_vars=['id','datetime', 'latitude','longitude'], 
                    value_vars=['sst'],
                    var_name='product',
                    value_name='value'
                )
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df['type'] = 'r'
                df['source'] = self.source_id

                # Reformat data
                df.value = df.value.round(5)
                df.longitude = df.longitude.round(6)
                df.latitude = df.latitude.round(6)
                df.datetime = df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")

                dfs.append(df)
            except Exception as e:
                raise Exception(f"Failed to retrieve and parse global drifter data from API. {e}")

        final_df = pd.concat(dfs, ignore_index= True)
        return final_df


    def load_drifters(self):
        """
        """
        # Get global drifter ids
        try:
            logger.info("Retrieving Global Drifter metadata.")
            global_drifter_ids = GlobalDrifter().get_sensor_ids()
            logger.info(f"{len(global_drifter_ids)} record(s) found")
        except Exception as e:
            raise Exception(f"Failed to retrieve Argo drifter metadata. {e}")

        # Read ids into DataFrame
        logger.info("Reading metadata into DataFrame.")
        source_ids = [self.source_id] * len(global_drifter_ids)
        cols = ['id', 'source']
        df = pd.DataFrame(list(zip(global_drifter_ids, source_ids)), columns=cols)

        # Upsert drifters to database table
        try:
            logger.info("Upserting Global Drifter metadata into DB table.")
            global_drifter_dict = df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilesensors/"
            created_drifters = self.post_api_data(url, global_drifter_dict)
            logger.info(f"{len(created_drifters)} Global Drifters "
                "successfully inserted (or retrieved if they "
                "already existed).")
        except Exception as e:
            raise Exception(f"Insert failed. {e}")

        return created_drifters


    def load_drifter_measurement_events(
        self,
        drifter_neighbors_df: pd.DataFrame):
        """
        """
        # Parse measurement events from neighbors DataFrame
        logger.info("Parsing global drifter measurement events from DataFrame.")
        drifter_event_cols = [
            'entity_id',
            'entity_datetime',
            'entity_latitude',
            'entity_longitude',
        ]
        drifter_events_df = drifter_neighbors_df.loc[:, drifter_event_cols]
        drifter_events_df = drifter_events_df.rename(columns={
            'entity_id': 'mobile_sensor',
            'entity_datetime': 'datetime',
            'entity_latitude': 'latitude',
            'entity_longitude': 'longitude'
        })
        drifter_events_df = drifter_events_df.drop_duplicates()
        drifter_events_json = drifter_events_df.to_dict(orient='records')

        # Load measurement events into DB
        try:
            logger.info("Inserting global drifter measurement events into DB.")
            url = f"{self.base_api_url}/mobilemeasurementevents/"
            created_events = self.post_api_data(url, drifter_events_json, timeout=100)
            logger.info(f"{len(created_events)} event(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert global drifter measurement events. {e}")

        return pd.DataFrame(created_events)


    def load_drifter_measurement_products(
        self,
        created_events_df: pd.DataFrame):
        """
        """
        # Merge created events with measurements to retrieve foreign keys
        try:
            logger.info("Merging events with measurements.")

            # Load created events into DataFrame and prepare columns for merge
            created_events_df['mobile_sensor'] = created_events_df['mobile_sensor'].astype('object')
            created_events_df['datetime'] = pd.to_datetime(created_events_df['datetime'], utc=True)
            drifter_measurements_df['datetime'] = pd.to_datetime(drifter_measurements_df['datetime'], utc=True)

            # Merge
            drifter_measurements_df = drifter_measurements_df.rename(columns={"id":"mobile_sensor"})
            drifter_measurements_df = drifter_measurements_df.merge(
                right=created_events_df,
                how="left",
                on=['mobile_sensor', 'datetime'],
            )

            # Reshape and subset data
            drifter_measurements_df = drifter_measurements_df.rename(columns={'id':'mobile_measurement_event'})
            measurement_cols = ['mobile_measurement_event', 'product', 'value', 'type']
            drifter_m_to_send_df = drifter_measurements_df.loc[:, measurement_cols]
            drifter_m_to_send_df = drifter_m_to_send_df.dropna()
        except Exception as e:
            raise Exception(f"Failed to merge Argo events and products. {e}")

        # Insert measurement products into database
        try:
            logger.info("Inserting Argo measurement products into DB.")
            drifter_products_json = drifter_m_to_send_df.to_dict(orient='records')
            url = f"{self.base_api_url}/mobilemeasurements/"
            created_products = self.post_api_data(url, drifter_products_json, timeout=100)
            logger.info(f"{len(created_products)} product(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert Argo measurement products. {e}")

        return pd.DataFrame(created_products)


    def load_drifter_neighbors(
        self,
        drifter_neighbors_df: pd.DataFrame,
        drifter_events_df: pd.DataFrame):
        """
        """
        # Create station neighbors
        try:
            logger.info("Creating final DataFrame for global drifter neighbors.")
            drifter_neighbors_df['entity_datetime'] = pd.to_datetime(drifter_neighbors_df['entity_datetime'], utc=True)
            drifter_n_to_send_df = drifter_neighbors_df.merge(
                right=drifter_events_df,
                how="left",
                left_on=['entity_id', 'entity_datetime'],
                right_on=['mobile_sensor', 'datetime'],
            )
            drifter_neighbors_df.to_csv("sigh2.csv")
            drifter_n_to_send_df = drifter_n_to_send_df[[
                'mobile_event',
                'id',
                'distance'
            ]]
            drifter_n_to_send_df = drifter_n_to_send_df.rename(
                columns={
                    'id':'neighboring_mobile_event'
                }
            )
        except Exception as e:
            raise Exception(f"Failed to create final DataFrame. {e}")

        # Insert drifter neighbors into database
        try:
            logger.info("Inserting drifter neighbors into database table.")
            neighbors_json = drifter_n_to_send_df.to_dict(orient="records")
            url = f"{self.base_api_url}/mobilemeasurementeventneighbors/"
            created_neighbors = self.post_api_data(url, neighbors_json)
            logger.info(f"{len(created_neighbors)} neighbors "
                "successfully inserted into the database (or "
                "retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert drifter neighbors. {e}")
    
        return pd.DataFrame(created_neighbors)
        

    def link_global_drifters_with_buoys(
        self,
        global_drifters: List[Dict],
        buoy_events_df: pd.DataFrame):
        """
        """
        # Retrieve base set of global drifter measurements
        buoy_events_df['datetime'] = pd.to_datetime(buoy_events_df['datetime'], utc=True)
        min_date = min(buoy_events_df['datetime']) - timedelta(minutes=180)
        max_date = max(buoy_events_df['datetime']) + timedelta(minutes=180)
        drifter_measurements_df = self.get_drifter_measurements(
            global_drifters,
            self.source_id,
            min_date,
            max_date
        )

        # Compute global drifter neighbors
        drifter_cols = [
            'id',
            'source',
            'distance',
            'datetime',
            'latitude',
            'longitude'
        ]
        drifter_neighbors_df = self.get_sensor_neighbors(
            buoy_events_df=buoy_events_df,
            entities_df=drifter_measurements_df,
            entity_cols=drifter_cols
        )

        # Load global drifter measurement events
        drifter_events_df = self.load_drifter_measurement_events(drifter_neighbors_df)

        # Load global drifter measurement products
        self.load_drifter_measurement_products(drifter_events_df)

        # Load global drifter neighbors
        self.load_drifter_neighbors(drifter_neighbors_df, drifter_events_df)
