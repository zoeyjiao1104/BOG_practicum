"""
oscar.py
"""

import pandas as pd
from datetime import datetime, timedelta, timezone
from load.common import LoadingClient
from retrieval.nasa_oscar import NasaOscar
from typing import List
from utilities.logger import logger


class OSCARLoadingClient(LoadingClient):
    """
    A client for fetching, merging, and reshaping
    NASA OSCAR current data for upserts into the
    omnipresent measurement database tables.
    """

    def __init__(self, source_id: str) -> None:
        """
        The class constructor.

        Parameters:
            source_id (str): The unique identifier for
                the OSCAR data source. This is used as
                a foreign key for creating new
                ominipresent measurement events.

        Returns:
            None
        """
        super().__init__()
        self.source_id = source_id


    def get_oscar_measurement_dates(
        self,
        oscar_dates_df: pd.DataFrame,
        buoy_measurement_events_df: pd.DataFrame,
        request_buffer_in_days: int=7) -> List[datetime]:
        """
        Fetches the list of NASA OSCAR current datasets
        available for download on the government's
        website and determines which datasets to request
        based on the timestamps of the given buoy
        measurement events.

        Parameters:
            oscar_dates_df (pd.DataFrame): The current
                OSCAR dataset dates.

            buoy_measurement_events_df (pd.DataFrame): The
                BOG buoy measurement events. Expected to
                have a UTC datetime column called `datetime`.

            buffer_in_days (int): The inclusive
                number of days before and after the
                buoy measurement event for which to
                query for OSCAR data. Defaults to 7.

        Returns:
            (list of datetime): The datetimes to query
                for OSCAR data.
        """
        # Compute timeframe for subsetting OSCAR datasets
        # based on buoy measurement event datetimes
        logger.info("Computing time range for which to request data.")
        buffer_in_days = timedelta(days=request_buffer_in_days)
        min_date = min(buoy_measurement_events_df['datetime']) - buffer_in_days
        max_date = max(buoy_measurement_events_df['datetime']) + buffer_in_days
        trunc_min_date = datetime.combine(min_date.date(), datetime.min.time(), tzinfo=timezone.utc)
        trunc_max_date = datetime.combine(max_date.date(), datetime.min.time(), tzinfo=timezone.utc)

        # Filter OSCAR datasets by min and max buoy
        # measurement event times, with an added buffer
        logger.info("Filtering OSCAR datasets by time range.")
        oscar_dates_df['oscar_datetime'] = pd.to_datetime(oscar_dates_df['oscar_datetime'], utc=True)
        oscar_dates_df = oscar_dates_df[oscar_dates_df['oscar_datetime'] >= trunc_min_date]
        oscar_dates_df = oscar_dates_df[oscar_dates_df['oscar_datetime'] <= trunc_max_date]
        logger.info(f"{len(oscar_dates_df)} dataset(s) remaining after filtering.")

        # Halt process if no dates are available
        if not len(oscar_dates_df):
            return []

        # Join buoy and OSCAR datetimes to facilitate comparisons
        logger.info("Performing cross join of buoy event "
            "datetimes and OSCAR datetimes and then "
            "calculating their differences.")
        buoy_dates_df = buoy_measurement_events_df[['datetime']]
        dates_df = buoy_dates_df.merge(oscar_dates_df, how='cross')
        dates_df['difference'] = (dates_df['datetime'] - dates_df['oscar_datetime']).dt.days
        dates_df.to_csv("joined_dates.csv")

        # Find closest OSCAR datetime for each buoy measurement datetime
        logger.info("Group by buoy event datetime to find closest "
            "OSCAR reading for each.")
        grp_dates_df = (dates_df
            .groupby('datetime')
            .agg({'difference': ['min']})
            .reset_index())
        grp_dates_df.columns = grp_dates_df.columns.get_level_values(0)
        final_dates_df = grp_dates_df.merge(
            right=dates_df[['datetime', 'oscar_datetime']],
            how='left',
            on=['datetime'])

        # Parse and return unique request dates
        request_dates = final_dates_df['oscar_datetime'].unique().tolist()
        return request_dates


    def get_oscar_measurements(
        self,
        oscar_source_id: str,
        request_dates: List[datetime]) -> pd.DataFrame:
        """
        Requests NASA OSCAR datasets for the specified dates.
        Then cleans and reshapes the resulting measurement
        records. NOTE: These records are the nearest
        temporally to buoys, but not necessarily geographically.
        Geographic filtering is completed in another step.

        Parameters:
            oscar_source_id (str): The unique identifier
                for the NASA OSCAR data source in the DB.
                
            request_dates (list of datetime): The
                dates for which to request OSCAR data.

        Returns:
            (pd.DataFrame): The OSCAR measurements.
        """
        # Query unique OSCAR datasets
        logger.info("Requesting OSCAR data.")
        df_lst = []
        num_request_dates = len(request_dates)

        for i in range(num_request_dates):
            logger.info(f"Requesting range {i + 1} of {num_request_dates}.")
            try:
                time_nasa = NasaOscar(
                    requested_date=request_dates[i],
                    num_closest_days=1
                )
                timedata = time_nasa.data
                df_lst.append(timedata)
            except RuntimeError as e:
                logger.error(e)
                continue

        oscar_df = pd.concat(df_lst)
        logger.info(f"{len(oscar_df)} record(s) retrieved.")

        # Reshape DataFrame
        product_cols = {
            'current_direction': 'cd',
            'current_speed': 'cs',
            'meriodonal_current': 'mc',
            'zonal_current': 'zc'
        }
        oscar_df = oscar_df.rename(columns=product_cols)
        oscar_df = oscar_df.melt(
            id_vars=['datetime', 'latitude', 'longitude'], 
            value_vars=list(product_cols.values()),
            var_name='product',
            value_name='value'
        )

        # Format remaining columns
        oscar_df['datetime'] = pd.to_datetime(oscar_df['datetime'], utc=True)
        oscar_df['source'] = oscar_source_id
        oscar_df['type'] = 'r'
        oscar_df['quality'] = 'na'

        return oscar_df


    def get_oscar_neighbors(
        self,
        buoy_measurement_events_df,
        sensor_measurements_df):
        """
        """
        # Subset OSCAR dataframe
        deg_buffer = 2
        min_lat = buoy_measurement_events_df['latitude'].min() - deg_buffer
        max_lat = buoy_measurement_events_df['latitude'].max() + deg_buffer
        min_lon = buoy_measurement_events_df['longitude'].min() - deg_buffer
        max_lon = buoy_measurement_events_df['longitude'].max() + deg_buffer
        df = sensor_measurements_df
        oscar_subset_df = df[
            (df['latitude'] > min_lat) &
            (df['latitude'] < max_lat) &
            (df['longitude'] > min_lon) &
            (df['longitude'] < max_lon)
        ]

        # Compute OSCAR neighbors
        try:
            oscar_cols = [
                'source',
                'distance',
                'datetime',
                'latitude',
                'longitude'
            ]
            return self.get_sensor_neighbors(
                buoy_events_df=buoy_measurement_events_df,
                entities_df=oscar_subset_df,
                entity_cols=oscar_cols
            )
        except Exception as e:
            raise Exception(f"Failed to generate OSCAR neighbors. {e}")


    def load_oscar_measurement_events(
        self,
        oscar_neighbors_df: pd.DataFrame,
        sensor_measurements_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses cleaned rows of measurements from
        the NASA OSCAR API to create omnipresent
        measurement events. Then uploads the events
        into the corresponding DB table.

        Parameters:
            buoy_measurement_events_df (pd.DataFrame):
                The BOG buoy measurement events.

            sensor_measurements_df (pd.DataFrame): The
                measurements retrieved from the NASA
                OSCAR API.

        Returns:
            (pd.DataFrame): The created measurement events.
        """
        # Standardize OSCAR latitude, and datetime columns
        logger.info("Standardizing OSCAR latitude, longitude, and datetime columns.")
        self.parse_decimal_columns(oscar_neighbors_df, ['entity_latitude', 'entity_longitude', 'entity_distance'])
        self.parse_decimal_columns(sensor_measurements_df, ['latitude', 'longitude', 'value'])
        oscar_neighbors_df['entity_datetime'] = oscar_neighbors_df['entity_datetime'].astype('object')
        sensor_measurements_df['datetime'] = sensor_measurements_df['datetime'].astype('object')

        # Parse OSCAR measurement events from computed neighbors
        logger.info("Parsing OSCAR measurement events "
            "from computed neighbors.")
        mevent_col_mapping = {
            'entity_source': 'source',
            'entity_datetime': 'datetime',
            'entity_latitude': 'latitude',
            'entity_longitude': 'longitude'
        }
        mevent_df = oscar_neighbors_df[list(mevent_col_mapping.keys())]
        mevent_df = mevent_df.rename(columns=mevent_col_mapping)

        # Clean and reshape DataFrame
        logger.info("Cleaning and reshaping resulting DataFrame.")
        mevent_df = mevent_df.drop_duplicates()
        mevent_df = mevent_df.sort_values(by=['datetime'])
        mevent_df.datetime = pd.to_datetime(mevent_df.datetime)
        mevent_df.datetime = mevent_df.datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
        mevent_json = mevent_df.to_dict(orient='records')

        # Load measurement events into DB
        try:
            logger.info("Inserting OSCAR measurement events into DB.")
            url = f"{self.base_api_url}/omnipresentmeasurementevents/"
            created_events = self.post_api_data(url, mevent_json, timeout=100)
            logger.info(f"{len(created_events)} event(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert OSCAR measurement events. {e}")

        return pd.DataFrame(created_events)


    def merge_oscar_measurements(
        self,
        sensor_neighbors_df: pd.DataFrame,
        sensor_measurements_df: pd.DataFrame,
        sensor_measurement_events_df: pd.DataFrame):
        """
        Merges the OSCAR neighbor and measurement datasets
        with the newly-created omnipresent measurement events.
        The resulting dataset can be used to retrieve OSCAR
        neighbors and measurement products.

        Parameters:
            sensor_neighbors_df (pd.DataFrame): The
                created OSCAR measurement neighbors.

            sensor_measurements_df (pd.DataFrame): The
                cleaned measurements retrieved from OSCAR.

            sensor_measurement_events_df (pd.DataFrame):
                The newly-created OSCAR measurement events.

        Returns:
            (pd.DataFrame): The created omnipresent
                measurement products.
        """
        # Join OSCAR measurement events and measurements to get foreign key
        try:
            logger.info("Merging neighbors with measurements.")

            # Subset and rename OSCAR neighbor DataFrame columns
            temp_oscar_neighbors_df = sensor_neighbors_df[[
                'mobile_event',
                'entity_distance',
                'entity_datetime',
                'entity_latitude',
                'entity_longitude'
            ]]
            temp_oscar_neighbors_df = temp_oscar_neighbors_df.rename(
                columns={
                    'entity_distance': 'distance',
                    'entity_datetime': 'datetime',
                    'entity_latitude': 'latitude',
                    'entity_longitude': 'longitude'
                }
            )

            # Filter OSCAR measurements through merge with OSCAR neighbors
            neighbor_measurements_df = sensor_measurements_df.merge(
                right=temp_oscar_neighbors_df,
                how='right',
                on=['datetime', 'latitude', 'longitude'],
            )
            neighbor_measurements_df.to_csv("neighbor_measurements.csv")

            # Convert created events response into DataFrame and clean
            sensor_measurement_events_df.to_csv("oscar_events.csv")
            self.parse_decimal_columns(sensor_measurement_events_df, ['latitude', 'longitude'])
            self.parse_utc_datetime_col(sensor_measurement_events_df, ['datetime'])
            self.parse_utc_datetime_col(neighbor_measurements_df, ['datetime'])

            # Merge resulting DataFrame with created events
            logger.info("Merging resulting DataFrame with created events.")
            final_measurements_df = neighbor_measurements_df.merge(
                right=sensor_measurement_events_df,
                how='left',
                on=['datetime', 'latitude', 'longitude'])

        except Exception as e:
            raise Exception(f'Failed to merge OSCAR neighbors and measurements. {e}')

        return final_measurements_df


    def load_oscar_measurement_products(
        self,
        aggregated_sensor_measurement_df: pd.DataFrame):
        """
        Parses measurement product information from the
        aggregated OSCAR measurement dataset and then
        inserts the records into the omnipresent
        measurements table.

        Parameters:
            aggregated_sensor_measurement_df (pd.DataFrame):
                A dataset holding merged omnipresent
                measurement event, buoy mobile measurement
                event, and measurement product information
                for OSCAR neighbors.

        Returns:
            (pd.DataFrame): The created omnipresent
                measurement products.
        """
        # Reshape and rename measurement columns
        measurement_products_df = aggregated_sensor_measurement_df[[
            'id',
            'product',
            'type',
            'value',
            'quality',
        ]]
        measurement_products_df = measurement_products_df.rename(columns={
            'id': 'omnipresent_measurement_event'
        })

        # Insert measurement products into database
        try:
            logger.info("Inserting OSCAR measurement products into DB.")
            measurements_json = measurement_products_df.to_dict(orient='records')
            url = f"{self.base_api_url}/omnipresentmeasurements/"
            created_products = self.post_api_data(url, measurements_json, timeout=100)
            logger.info(f"{len(created_products)} measurement product(s) successfully "
                "created (or retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert OSCAR measurement products. {e}")

        return pd.DataFrame(created_products)


    def load_sensor_neighbors(
        self,
        aggregated_sensor_measurement_df: pd.DataFrame):
        """
        Parses omnipresent measurement event neighbor
        information from the aggregated OSCAR measurement
        dataset and then inserts the records into the
        omnipresent measurements table.

        Parameters:
            aggregated_sensor_measurement_df (pd.DataFrame):
                A dataset holding merged omnipresent
                measurement event, buoy mobile measurement
                event, and measurement product information
                for OSCAR neighbors.

        Returns:
            (pd.DataFrame): The created omnipresent
                measurement event neighbors.
        """
        # Parse OSCAR measurement event neighbors
        final_neighbors_df = aggregated_sensor_measurement_df[[
            'id',
            'mobile_event',
            'distance'
        ]]

        # Reshape neighbor columns
        final_neighbors_df = final_neighbors_df.rename(columns={
            'id': 'neighboring_omnipresent_event'
        })

        # Drop duplicates
        final_neighbors_df = final_neighbors_df.drop_duplicates()

        # Insert OSCAR measurement neighbors into database
        try:
            logger.info("Inserting OSCAR neighbors into database table.")
            neighbors_json = final_neighbors_df.to_dict(orient="records")
            url = f"{self.base_api_url}/omnipresentmeasurementeventneighbors/"
            created_neighbors = self.post_api_data(url, neighbors_json)
            logger.info(f"{len(created_neighbors)} neighbors "
                "successfully inserted into the database (or "
                "retrieved if they already existed).")
        except Exception as e:
            raise Exception(f"Failed to insert OSCAR neighbors. {e}")

        return pd.DataFrame(created_neighbors)


    def link_oscar_records_with_buoys(
        self,
        available_oscar_dates_df: pd.DataFrame,
        buoy_events_df: pd.DataFrame):
        """
        """
        # From the available dates, determine which should
        # be used for fetching OSCAR data 
        request_dates = self.get_oscar_measurement_dates(
            available_oscar_dates_df,
            buoy_events_df)

        # Retrieve NASA OSCAR data using relevant timestamps
        measurements_df = self.get_oscar_measurements(
            self.source_id,
            request_dates)

        # Get OSCAR neighbors
        neighbors_df = self.get_oscar_neighbors(
            buoy_events_df,
            measurements_df)

        # Populate DB with OSCAR measurement events
        events_df = self.load_oscar_measurement_events(
            neighbors_df,
            measurements_df)

        # Join buoy events with OSCAR neighbors and measurements
        agg_df = self.merge_oscar_measurements(
            neighbors_df,
            measurements_df,
            events_df)

        # From resulting dataset, populate DB with
        # OSCAR measurement products and neighbors
        self.load_oscar_measurement_products(agg_df)
        self.load_sensor_neighbors(agg_df)
