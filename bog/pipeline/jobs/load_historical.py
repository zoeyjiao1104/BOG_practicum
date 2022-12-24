"""
load_historical.py
"""

import concurrent.futures
import os
import pandas as pd
from datetime import datetime, time, timedelta
from load.buoys import BuoyLoadingClient
from load.oscar import OSCARLoadingClient
from load.stations import StationLoadingClient
from load.sources import SourcesLoadingClient
from load.jobs import JobsLoadingClient
from retrieval.bog import BOG
from retrieval.dfo import DFO
from retrieval.nasa_oscar import NasaOscar
from retrieval.noaa import NOAA
from typing import List, Tuple
from utilities.logger import logger



class LoadingOrchestrator:
    """
    Orchestrates the retrieval and loading of
    various datasets into the database.
    """

    def __init__(self) -> None:
        """
        The class constructor.

        Parameters:
            None

        Returns:
            None
        """
        # Initialize max number of threads to use when processing measurements
        try:
            max_num_threads = os.environ['MAX_NUM_THREADS']
            self.max_num_threads = int(max_num_threads)
        except KeyError as e:
            raise Exception("Missing required "
                f"environment variable: '{e}'.")
        except ValueError:
            raise Exception("Expected an integer for environment "
                " variable 'MAX_NUM_THREADS', but the value "
                f"'{max_num_threads}' was received.")

        # Perform one-time population of data source metadata.
        # NOTE: Any repeated inserts are safely ignored.
        sources = SourcesLoadingClient().load_sources()

        # Extract newly-created primary keys from data sources to
        # use as foreign keys for subsequent data upserts.
        sources_map = {s['name']: s['id'] for s in sources}
        try:
            bog_source_id = sources_map[BOG.SOURCE_NAME]
            dfo_source_id = sources_map[DFO.SOURCE_NAME]
            noaa_source_id = sources_map[NOAA.SOURCE_NAME]
            oscar_source_id = sources_map[NasaOscar.SOURCE_NAME]
        except KeyError as e:
            raise Exception(f"Missing expected source: '{e}'.")

        # Instantiate loading clients
        self.jobs_client = JobsLoadingClient()
        self.buoy_client = BuoyLoadingClient(bog_source_id)
        self.station_client = StationLoadingClient(dfo_source_id, noaa_source_id)
        self.oscar_client = OSCARLoadingClient(oscar_source_id)

        # Initialize sensor DataFrame references
        self.buoy_sensors_df = None
        self.stations_df = None

        # Initialize available OSCAR dataset reference
        self.oscar_datasets_df = None

        # Initialize buoy fishery assignment reference
        self.buoy_fishery_assignments = None


    def refresh_oscar_datasets(self) -> None:
        """
        Scrapes the NASA OSCAR website homepage to
        retrieve the list of available datasets.
        Updates the `oscar_datasets_df` field
        with the new value.

        Parameters:
            None

        Returns:
            None
        """
        try:
            logger.info("Retrieving list of current OSCAR datasets.")
            dates = NasaOscar.get_current_dataset_dates()
            available_oscar_dates_df = pd.DataFrame(dates, columns=["oscar_datetime"])
            logger.info(f"{len(available_oscar_dates_df)} dataset(s) found.")
        except Exception as e:
            raise Exception(f"Orchestrator failed to get current "
                f"OSCAR dataset dates. {e}")

        self.oscar_datasets_df = available_oscar_dates_df
    

    def refresh_sensors(self):
        """
        Fetches the latest mobile and stationary sensors
        available via external APIs and inserts them into
        the database. (Pre-existing records are ignored
        rather than updated.) Then updates the `buoy_sensors_df`,
        `stations_df`, `argo_df`, and `drifters_df` fields
        to refer to the new sensor sets.
        
        Parameters:
            None

        Returns:
            None
        """
        try:
            logger.info("Fetching mobile and stationary sensors "
                "to upload into database.")

            # Populate DB with BOG buoy metadata
            buoys = self.buoy_client.load_buoys()
            self.buoy_sensors_df = pd.DataFrame(buoys)

            # Populate DB with DFO and NOAA station metadata
            self.stations_df = self.station_client.load_stations()

        except Exception as e:
           raise Exception(f"Orchestrator failed to load all sensors. {e}")


    def refresh_buoy_fishery_assignments(self):
        """
        Loads new buoy fishery assignments.
        """
        try:
            self.buoy_fishery_assignments = self.buoy_client.load_buoy_fishery_assignments()
        except Exception as e:
            raise Exception(f"Failed to load fishery assignments. {e}")


    def load_measurements(
        self,
        start: datetime=None,
        end: datetime=None,
        buoy_ids: List[str]=None):
        """
        Loads BOG buoy data and associated entities
        from a given start date to the present time
        in UTC.
        """
        try:
            # Retrieve start and end dates from config settings
            logger.info("Generating start and end dates for data queries.")
            if not start:
                start = datetime.combine(self.origin_date, time.min)
            if not end:
                end = datetime.utcnow()

            # Batch buoy measurements for given date range
            batches = self._generate_buoy_measurement_batches(start, end, buoy_ids=buoy_ids)

            # Process measurements for each date range using multi-threading
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_num_threads) as executor:
                results = {
                    executor.submit(self._process_buoys_for_date_range, batch):batch
                    for batch in batches
                }
                for future in concurrent.futures.as_completed(results):
                    future.result()

        except Exception as e:
            sensor_text = "all sensors" if not buoy_ids else ', '.join(buoy_ids) 
            error_msg = f"Orchestrator failed to load historic " \
                f"data for buoys {sensor_text}. {e}"
            raise Exception(error_msg)


    def _generate_buoy_measurement_batches(
        self,
        start: datetime,
        end: datetime,
        max_days_per_batch: int=1,
        buoy_ids: List[str]=None) -> List[Tuple[datetime, datetime, pd.DataFrame]]:
        """
        Batches buoy measurements occurring in the given
        date range into a list of smaller ranges of a
        maximum size.

        Parameters:
            start (datetime): The inclusive start date.

            end (datetime): The inclusive end date.

            max_days_per_batch (int): The maximum number
                of days to include in a single batch.
                Defaults to 1.

            buoy_ids (list of str): The buoys for which
                to collect data. Defaults to `None`, in
                which case all buoys are queried.

        Returns:
            (list of (datetime, datetime, pd.DataFrame)):
                A three-item tuple consisting of the 
                start and end datetimes represented by
                the batch and the measurements within
                the batch.
        """
        # Get buoy measurements for date range
        logger.info(f"Retrieving buoy measurements from {start:%m-%d-%Y} to {end:%m-%d-%Y}.")
        buoy_measurements_df = self.buoy_client.get_buoy_measurements(
            start=start,
            end=end,
            buoy_ids=buoy_ids
        )

        # Parse buoy datetime column to allow filtering
        buoy_measurements_df['datetime'] = pd.to_datetime(buoy_measurements_df['datetime'], utc=True)

        # Create batches
        measurement_batches = []
        days_lookback = (end - start).days
        num_batches = (days_lookback // max_days_per_batch) + \
            (1 if days_lookback % max_days_per_batch > 0 else 0)
        logger.info(f"Batching buoy measurements into {num_batches} "
            "processing groups based on date.")

        for i in range(num_batches):

            # Determine start and end datetime for batch
            batch_start = start + timedelta(days=max_days_per_batch*i)
            batch_end = start + timedelta(days=max_days_per_batch*(i+1))
            
            # Subset buoy measurements
            time_mask = ((buoy_measurements_df["datetime"] >= batch_start) & 
                    (buoy_measurements_df["datetime"] <= batch_end))
            filtered_df = buoy_measurements_df[time_mask]

            # Skip if no measurements found in date range
            if not len(filtered_df):
                continue

            # Otherwise, append date range and measurements to list
            measurement_batches.append((batch_start, batch_end, filtered_df))

        return measurement_batches


    def _process_buoys_for_date_range(
        self,
        batch: List[Tuple[datetime, datetime, pd.DataFrame]]) -> None:
        """
        Associates BOG buoy measurements occurring within a
        date range with other data entities (stations, drifters,
        etc.), loading all new datasets into the database.
        """
        # Unpack batch for dates and buoy measurement DataFrame
        start, end, batch_df = batch
        logger.info(f"Processing batch for dates "
            f"{start:%m-%d-%Y} to {end:%m-%d-%Y}."
            f"{len(batch_df)} measurements found.")        

        # Otherwise, populate DB with BOG buoy measurement events
        created_buoy_events_df = self.buoy_client.load_buoy_measurement_events(batch_df)

        # Populate DB with BOG buoy measurement products
        self.buoy_client.load_buoy_measurement_products(
            batch_df,
            created_buoy_events_df)

        # Populate BOG buoy measurement event neighbors
        self.buoy_client.load_buoy_neighbors(created_buoy_events_df)

        # Link neighboring DFO and NOAA stations with buoys
        self.station_client.link_stations_with_buoys(
            self.stations_df,
            created_buoy_events_df)

        # Link neighboring NASA OSCAR records with buoys
        self.oscar_client.link_oscar_records_with_buoys(
            self.oscar_datasets_df,
            created_buoy_events_df)



if __name__ == "__main__":
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    orchestrator = LoadingOrchestrator()
    orchestrator.refresh_buoy_fishery_assignments()
    