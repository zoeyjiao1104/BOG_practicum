"""
stations.py
"""

import pandas as pd
from datetime import timedelta, timezone
from load.common import LoadingClient
from retrieval.dfo import DFO
from retrieval.noaa import NOAA
from utilities.logger import logger


class StationLoadingClient(LoadingClient):
    """
    A client for fetching, merging, and reshaping
    DFO and NOAA station data for upserts into the
    stationary sensor and measurement database tables.
    """

    def __init__(
        self,
        dfo_source_id: str,
        noaa_source_id: str) -> None:
        """
        The class constructor.

        Parameters:
            dfo_source_id (str): The unique identifier for
                the DFO data source. This is used as a foreign
                key for creating new mobile sensors.

            noaa_source_id (str): The unique identifier for
                the NOAA data source. This is used as a foreign
                key for creating new mobile sensors.

        Returns:
            None
        """
        super().__init__()
        self.dfo = DFO()
        self.dfo_source_id = dfo_source_id
        self.noaa = NOAA()
        self.noaa_source_id = noaa_source_id


    def get_station_measurements(
        self,
        stations_df: pd.DataFrame,
        buoy_events_df: pd.DataFrame,
        buffer_in_minutes: int=180) -> pd.DataFrame:
        """
        Queries the DFO and/or NOAA APIs for measurements of
        the stations that are geographically and temporally
        closest to the given buoy measurement events. Then
        cleans and formats the data, returning it as a
        DataFrame.

        Parameters:
            stations_df (pd.DataFrame): The created NOAA
                and DFO stations.

            buoy_events_df (pd.DataFrame): The
                BOG buoy measurement events.

            dfo_source_id (str): The unique identifier
                for the DFO data source in the DB.

            buffer_in_minutes (int): The inclusive
                number of minutes before and after the
                buoy measurement event for which to
                query for station data. Defaults to 180.

        Returns:
            (pd.DataFrame): The station measurements.
        """
        # Generate station neighbors for buoy events
        try:
            logger.info("Generating nearest stations for "
                "buoy measurement events.")
            neighbors_df = self.get_sensor_neighbors(
                buoy_events_df=buoy_events_df,
                entities_df=stations_df,
                entity_cols=[
                    'id',
                    'source',
                    'distance'
                ]
            )
            logger.info(f"{len(neighbors_df)} neighbor(s) calculated.")
        except Exception as e:
            raise Exception(f"Error generating station neighbors. {e}")

        # Get measurements for station neighbors
        logger.info("Retreving measurement records from NOAA and DFO APIs.")
        measurement_dfs = []
        station_neighbors = neighbors_df.to_dict(orient='records')
        

        for s in station_neighbors:
            try:
                # Create variables for query
                buoy_datetime = s['datetime'].to_pydatetime()
                buoy_mobile_event = s['mobile_event']
                start_time = buoy_datetime - timedelta(minutes=buffer_in_minutes)
                end_time = buoy_datetime + timedelta(minutes=buffer_in_minutes)
                station_id = s['entity_id']
                station_source_id = int(s['entity_source'])
                station_distance = float(s['entity_distance'])
                api = self.dfo if station_source_id == self.dfo_source_id else self.noaa
                api_type = 'DFO' if station_source_id == self.dfo_source_id else 'NOAA'

                # Query API for data
                m_df = api.get_single_sensor_historical_measurements(
                    sensor_id=station_id,
                    start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                logger.info(f"{len(m_df)} measurement(s) found for {api_type} station {station_id} "
                    f"from {start_time:%m-%d-%Y} to {end_time:%m-%d-%Y}.")
                
                # Handle case in which no measurements are available
                if not len(m_df):
                    logger.info("Continuing to next station")
                    continue

                # Subset to neighboring station measurements closest
                # in time to the buoy measurement event
                to_epoch_ms = lambda dt: dt.replace(tzinfo=timezone.utc).timestamp() * 1000
                buoy_epoch_ms = buoy_datetime.timestamp() * 1000
                m_df['datetime'] = pd.to_datetime(m_df['datetime'], utc=True)
                m_df['datetime_distance'] = abs(m_df['datetime'].apply(to_epoch_ms) - buoy_epoch_ms)
                closest_df = m_df.nsmallest(n=1, columns=['datetime_distance'], keep='all')

                # Clean and map resulting records
                closest_df.loc[:, ['product']] = closest_df.apply(lambda row: self.map_row_values(row, self.product_map), axis=1)
                closest_df.loc[:, ['type']] = closest_df.apply(lambda row: self.map_row_values(row, self.type_map), axis=1)
                closest_df = closest_df.replace([''], [None])
                closest_df['mobile_event'] = buoy_mobile_event
                closest_df['distance'] = station_distance

                # Append to collection
                measurement_dfs.append(closest_df)

            except Exception as e:
                raise Exception(f"Station measurement retrieval failed. {e}")

        logger.info("Concatenating and cleaning results.")
        measurement_df = pd.concat(measurement_dfs, ignore_index= True) 
        measurement_df = measurement_df.rename(columns={'sensor_id':'station'})
        measurement_df['datetime'] = pd.to_datetime(measurement_df['datetime'], infer_datetime_format=True, utc=True)

        return measurement_df


    def load_stations(self) -> pd.DataFrame:
        """
        Fetches station metadata from the NOAA and DFO APIs,
        cleans and reformats the retrieved records, and then
        inserts the records into the stationary sensors DB table.

        Parameters:
           None

        Returns:
            (pd.DataFrame): A representation of the created
                sensors.
        """
        # Define desired output columns
        col_map = {'station_id':'id', 'station_name': "name"}
        columns = [
            'id',
            'name',
            'state',
            'established',
            'timezone',
            'latitude',
            'longitude',
            'source'
        ]

        # Get DFO station metadata as DataFrame
        try:
            logger.info("Retrieving DFO station metadata.")
            dfo = DFO().get_station_metadata().reset_index()
            logger.info(f"{len(dfo)} record(s) found.")
        except Exception as e:
            raise(f"Failed to retrieve DFO station metadata. {e}")

        # Format DFO columns
        logger.info("Formatting DFO columns.")
        dfo['source'] = self.dfo_source_id
        df_dfo = dfo.rename(columns=col_map)[columns]
        df_dfo['established'] = df_dfo.established.str[:4]+'-01-01'

        # Get NOAA station metadata as DataFrame
        try:
            logger.info("Retrieving NOAA station metadata.")
            noaa = NOAA().get_station_metadata().reset_index()
            logger.info(f"{len(noaa)} record(s) found.")
        except Exception as e:
            raise Exception(f"Failed to get NOAA station metadata.")

        # Format NOAA columns
        logger.info("Formatting NOAA columns.")
        noaa['source'] = self.noaa_source_id
        df_noaa = noaa.rename(columns=col_map)[columns]
        df_noaa['established'] = df_noaa.established.str[:10]

        # Concatenate station DataFrames and clean values
        logger.info("Concatenating station DataFrames and cleaning values.")
        df = pd.concat([df_dfo, df_noaa], ignore_index = True)
        df.loc[df.state =='United States of America', 'state'] = 'US'
        df['depth'] = None
        df = df.round({'latitude': 5, 'longitude': 5})
        df = df.where(pd.notnull(df), None)

        # Upsert stations to DB table
        try:
            logger.info("Upserting stations into DB table.")
            station_json = df.to_dict(orient='records')
            url = f"{self.base_api_url}/stations/"
            upserted_stations = self.post_api_data(url, station_json, timeout=100)
            logger.info(f"{len(upserted_stations)} stations upserted successfully.")
        except Exception as e:
            raise Exception(f"Failed to upsert stations into database. {e}")

        return pd.DataFrame(upserted_stations)


    def load_station_measurement_events(
        self,
        station_measurements_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses cleaned rows of station measurements from the
        DFO and/or NOAA APIs to create stationary sensor
        measurement events. Then uploads the events into the
        corresponding DB table.

        Parameters:
            station_measurements_df (pd.DataFrame): The
                measurements retrieved from the DFO and/or
                NOAA APIs.

        Returns:
            (pd.DataFrame): The created measurement events.
        """
        # Prepare JSON for stationary event DB insert
        station_events_df = station_measurements_df.loc[:, ['datetime', 'station']]
        station_events_df.datetime = station_events_df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
        station_events_df = station_events_df.drop_duplicates()
        station_events_json = station_events_df.to_dict(orient='records')

        # Load measurement events into DB
        try:
            logger.info("Inserting station measurement events into DB.")
            url = f"{self.base_api_url}/stationarymeasurementevents/"
            created_events = self.post_api_data(url, station_events_json, timeout=100)
            logger.info(f"{len(created_events)} event(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert station measurement events. {e}")

        created_events_df = pd.DataFrame(created_events)
        created_events_df['datetime'] = pd.to_datetime(created_events_df['datetime'], utc=True)

        return created_events_df


    def load_station_measurement_products(
        self,
        station_measurements_df: pd.DataFrame,
        station_events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses measurement product information from the
        aggregated DFO and/or NOAA station records, merges
        that information with the newly-created stationary
        measurement events, and then inserts the resulting
        dataset into the stationary measurements table.

        Parameters:
            station_measurements_df (pd.DataFrame): The
                cleaned measurements retrieved from the
                DFO and/or NOAA API(s).

            station_events_df (pd.DataFrame): The 
                newly-created DFO and/or NOAA stationary
                measurement events.

        Returns:
            (pd.DataFrame): The created station measurement
                measurement products.
        """
        # Generate measurement products
        try:
            # Merge created events with measurements to retrieve foreign keys
            logger.info("Merging created station measurement "
                "events with measurements.")
            station_measurements_df = station_measurements_df.merge(
                right=station_events_df,
                how="left",
                on=['station', 'datetime']
            )

            # Reshape and subset data
            col_mapping = {'id':'stationary_measurement_event'}
            station_measurements_df = station_measurements_df.rename(columns=col_mapping)
            measurement_cols = [
                'stationary_measurement_event',
                'product',
                'value', 
                'type'
            ]
            station_measurements_df = station_measurements_df.loc[:, measurement_cols]

        except Exception as e:
            raise Exception(f"Failed to merge station events and products. {e}")

        # Insert measurement products into database
        try:
            logger.info("Inserting station measurement products into DB.")
            station_products_json = station_measurements_df.to_dict(orient='records')
            url = f"{self.base_api_url}/stationarymeasurements/"
            created_products = self.post_api_data(url, station_products_json, timeout=100)
            logger.info(f"{len(created_products)} product(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert station measurement events. {e}")

        return pd.DataFrame(created_products)


    def load_station_neighbors(
        self,
        station_events_df: pd.DataFrame,
        station_measurements_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses the measurements of the nearest neighbor
        stations to load new stationary measurement
        event neighbors into the corresponding database
        table.

        Parameters:
            station_events_df (pd.DataFrame): The 
                newly-created DFO and/or NOAA stationary
                measurement events.

            station_measurements_df (pd.DataFrame): The
                cleaned measurements retrieved the BOG
                API.

        Returns:
            (pd.DataFrame): The created station measurement
                event neighbors.
        """
        try:
            logger.info("Creating DataFrame for stationary "
                "measurement event neighbors.")

            merged_df = station_measurements_df.merge(
                right=station_events_df,
                how="left",
                on=['datetime', 'station']
            )
            station_neighbors_df = merged_df[[
                'id',
                'mobile_event',
                'distance'
            ]]
            station_neighbors_df = station_neighbors_df.rename(
                columns={'id':'neighboring_stationary_event'}
            )
            station_neighbors_df = station_neighbors_df.drop_duplicates()
        except Exception as e:
            raise Exception(f"Failed to create final DataFrame. {e}")

        # Insert buoy-station neighbors into database
        try:
            logger.info("Inserting buoy station neighbors into database table.")
            neighbors_json = station_neighbors_df.to_dict(orient="records")
            url = f"{self.base_api_url}/stationarymeasurementeventneighbors/"
            created_neighbors = self.post_api_data(url, neighbors_json)
            logger.info(f"{len(created_neighbors)} neighbors "
                "succesfully inserted into the database (or "
                "retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert buoy station neighbors. {e}")
        
        return pd.DataFrame(created_neighbors)


    def link_stations_with_buoys(
        self,
        stations_df: pd.DataFrame,
        buoy_events_df: pd.DataFrame):
        """
        """
        # Fetch station measurements
        measurements_df = self.get_station_measurements(
            stations_df,
            buoy_events_df,
            buffer_in_minutes=180)

        # Populate DB with station measurement events
        events_df = self.load_station_measurement_events(measurements_df)

        # Populate DB with station measurement products
        self.load_station_measurement_products(measurements_df, events_df)

        # Populate DB with station neighbors
        self.load_station_neighbors(events_df, measurements_df)
   