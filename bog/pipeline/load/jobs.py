"""
jobs.py
"""

from datetime import datetime
from load.common import LoadingClient
from typing import Dict, List
from utilities.logger import logger


class JobsLoadingClient(LoadingClient):
    """
    A client for fetching and inserting jobs
    into the corresponding database table.
    """

    def create_job(
        self,
        job_name: str,
        query_date_start: datetime,
        query_date_end: datetime) -> Dict:
        """
        Creates a new job in the database.

        Parameters:
            job_name (str): The standardized name
                of the job.

            query_date_start (datetime): The inclusive
                lower bound for which data was collected.

            query_date_end (datetime): The inclusive
                upper bound for which data was collected.

        Returns:
            (dict): A representation of the
                newly-created job.
        """
        try:
            # Compose job
            format = "%Y-%m-%d %H:%M:%S"
            query_start = query_date_start.strftime(format)
            query_end = query_date_end.strftime(format)
            job = {
                "name": job_name,
                "status": "running",
                "query_date_start_utc": query_start,
                "query_date_end_utc": query_end
            }

            # Send request
            logger.info("Posting new job to database.")
            job_url = f"{self.base_api_url}/jobs/"
            created_job = self.post_api_data(job_url, job)
            logger.info("Job created successfully.")
        except Exception as e:
            raise Exception(f"Failed to insert new job into database. {e}")

        return created_job


    def get_latest_job_executions(self) -> List[Dict]:
        """
        Retrieves the latest execution times of
        each successful type of job.

        Parameters:
            None

        Returns:
            (list of dict): The list of job executions.
        """
        try:
            job_url = f"{self.base_api_url}/jobs/latest/"
            return self.get_api_data(job_url)
        except Exception as e:
            raise Exception(f"Failed to retrieve job execution times. {e}")


    def mark_job_as_completed(
        self,
        job_id: str,
        retry_count: int) -> None:
        """
        Marks a job as a completed in the database.

        Parameters:
            job_id (str): The unique identifier for the job.

            retry_count (int): The current number of
                times the job has been re-attempted.

        Returns:
            None
        """
        try:            
            # Compose job
            logger.info(f"Marking job {job_id} as completed in the database.")
            format = "%Y-%m-%d %H:%M:%S"
            completed_at_utc = datetime.utcnow().strftime(format)
            job = {
                "id": job_id,
                "status": "completed",
                "completed_at_utc": completed_at_utc,
                "retry_count": retry_count
            }

            # Send request
            job_url = f"{self.base_api_url}/jobs/{job_id}/"
            self.patch_api_data(job_url, job)
            logger.info("Job updated successfully.")
        except Exception as e:
            raise Exception(f"Error marking job {job_id} as completed. {e}")


    def mark_job_as_failure(
        self,
        job_id: str,
        error: str,
        retry_count: int) -> None:
        """
        Marks a job as a failure in the database.

        Parameters:
            job_id (str): The unique identifier for the job.

            error (str): The error message.

            retry_count (int): The current number of
                times the job has been re-attempted.

        Returns:
            None
        """
        try:            
            # Compose job
            logger.info(f"Marking job {job_id} as a failure in the database.")
            format = "%Y-%m-%d %H:%M:%S"
            last_error_at_utc = datetime.utcnow().strftime(format)
            job = {
                "id": job_id,
                "status": "error",
                "last_error_at_utc": last_error_at_utc,
                "error_message": error,
                "retry_count": retry_count
            }

            # Send request
            job_url = f"{self.base_api_url}/jobs/{job_id}/"
            self.patch_api_data(job_url, job)
            logger.info("Job updated successfully.")
        except Exception as e:
            raise Exception(f"Error marking job {job_id} as failure. {e}")
