"""
sources.py
"""

from load.common import LoadingClient
from utilities import files
from utilities.logger import logger


class SourcesLoadingClient(LoadingClient):
    """
    A client for fetching and inserting data source
    metadata into the sources database table.
    """

    def load_sources(self):
        """
        Loads data sources from a file and then writes those
        sources to a database table if they don't already exist.
        """
        # Load data sources from file
        try:
            logger.info("Loading data source metadata from file.")
            sources = files.load_json(file_name="database/sources.json")
            logger.info(f"{len(sources)} source(s) found.")
        except Exception as e:
            raise Exception(f"Failed to load sources from file. {e}")

        # Load sources into database
        try:
            logger.info("Posting sources into database table.")
            sources_url = f"{self.base_api_url}/sources/"
            upserted_sources = self.post_api_data(sources_url, sources)
            logger.info(f"{len(upserted_sources)} sources upserted successfully.")
        except Exception as e:
            raise Exception(f"Failed to insert sources into database. {e}")

        return upserted_sources