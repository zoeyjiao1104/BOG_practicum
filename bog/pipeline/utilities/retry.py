"""
retry.py
"""

import functools
from datetime import datetime
from load.jobs import JobsLoadingClient
from typing import Any, Dict
from utilities.logger import logger


def data_job(
    _func=None,
    *,
    job_args: Dict,
    retry_count:int=3,
    exc_type:BaseException=Exception) -> Any:
    """
    Implements a decorator for data processing jobs
    that may be retried up to a maximum number of times.

    References:
    - https://stackoverflow.com/a/21788594
    - https://realpython.com/primer-on-python-decorators/#decorators-with-arguments
    
    Parameters:
        job_args (Dict): Arguments to create and update
            a job in the database.

        retry_count (int): The maximum number of times a
            function should be retried upon failure.
            Defaults to 3.

        exc_type (type): The type of error to
            be raised upon failure. Defaults to
            `Exception`.

    Returns:
        (any): The output of the wrapped function.
    """
    # Define decorator function
    def decorator(func):
        """
        The standard decorator.
        """
        # Define function for retry logic
        @functools.wraps(func)
        def execute(*args, **kwargs):
            """
            Attempts to execute a function up
            to `count` number of times, inclusive.
            """
            # Create new job in DB
            client = JobsLoadingClient()
            created_job = client.create_job(**job_args)

            # Attempt function
            for i in range(retry_count + 1):
                try:
                    # Execute function
                    result = func(*args, **kwargs)
                    
                    # Mark job as completed if function succeeds
                    client.mark_job_as_completed(
                        job_id=created_job['id'],
                        retry_count=created_job['retry_count']
                    )

                    return result

                except exc_type as e:
                    # Mark job as failure
                    client.mark_job_as_failure(
                        job_id=created_job['id'],
                        error=str(e),
                        retry_count=created_job['retry_count']
                    )

                    # Handle retries
                    if i < retry_count:
                        created_job['retry_count'] = created_job['retry_count'] + 1
                        logger.error(f"Function failed with error '{e}'. "
                            "Retrying function.")
                        continue

                    # If no retries left, raise Exception
                    logger.error(f"Function failed with error '{e}'. "
                        f"Maximum number of retries ({retry_count}) "
                        "reached.")
                    raise

        # Return function reference
        return execute

    # Return decorator reference based on arguments received
    if _func is None:
        return decorator
    else:
        return decorator(_func)
