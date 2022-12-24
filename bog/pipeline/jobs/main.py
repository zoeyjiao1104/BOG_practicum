"""
main.py
"""

import os
from datetime import datetime, time, timezone
from jobs.load_historical import LoadingOrchestrator
from load.jobs import JobsLoadingClient
from jobs.load_models import ModelOrchestrator
from schedule import every, repeat, run_pending
from utilities.logger import logger
from utilities.retry import data_job


#region_______________________SETUP_______________________

# Retrieve default "origin date" for historical data load
try:
    format = "%Y-%m-%d"
    origin_date = os.environ['ORIGIN_DATE']
    origin_date = datetime.strptime(origin_date, format)
except KeyError as e:
    raise Exception(f"Missing required environment variable: '{e}'.")
except ValueError:
    raise Exception("Environment variable 'origin_date' "
        f"must have format {format}, but the value '{origin_date}' " 
        "was received.")

# Retrieve default schedule for processing new measurements
try:
    recent_load_sch_in_min = os.environ['LOAD_SCHEDULE_IN_MIN']
    recent_load_sch_in_min = int(recent_load_sch_in_min)
except KeyError as e:
    raise Exception(f"Missing required environment variable: '{e}'.")
except ValueError:
    raise Exception("Expected an integer for environment "
        " variable 'recent_load_sch_in_min', but the value "
        f"'{recent_load_sch_in_min}' was received.")

# Instantiate clients
orchestrator = LoadingOrchestrator()
jobs_client = JobsLoadingClient()
model_orchestrator = ModelOrchestrator()

#endregion_______________________SETUP_____________________


@repeat(every(1).day)
def load_oscar_datasets():
    """
    Creates a new `load-oscar-datasets` job in the database
    and then proceeds to update an in-memory reference to the
    list of available NASA OSCAR datasets, updating the status
    of the job upon reaching a terminal state (i.e.,
    'completed' or 'error'). Automatically retries the job
    upon failure up to the number of times specified by
    configuration settings.

    Parameters:
        None

    Returns:
        None
    """
    # Compose job arguments
    oscar_origin_date =  datetime(year=1992, month=10, day=5, tzinfo=timezone.utc)
    job_args = {
        'job_name': 'refresh-oscar-datasets',
        'query_date_start': oscar_origin_date,
        'query_date_end': datetime.utcnow()
    }

    # Compose local function to execute job
    @data_job(job_args=job_args, retry_count=3)
    def execute_job():
        """Executes `refresh-oscar` job."""
        orchestrator.refresh_oscar_datasets()

    # Execute job
    execute_job()


@repeat(every(1).minutes)
def load_measurements() -> None:
    """
    Creates a new `load-measurements` job in the database
    and then proceeds to upsert new sensor, measurement
    event, measurement, and fishery assignment records,
    updating the status of the job upon reaching a terminal
    state (i.e., 'completed' or 'error'). Automatically
    retries the job upon failure up to the number of times
    specified by configuration settings.

    Parameters:
        None

    Returns:
        None
    """
    # Define job name
    job_name = 'load-measurements'

    # Fetch max datetime from last successful job
    executions = jobs_client.get_latest_job_executions()
    last_query_end_utc = executions.get(job_name)
    
    # Set max datetime as start of new job if it exists;
    # otherwise, default to the pre-configured 'ORIGIN' date.
    if last_query_end_utc:
        query_start_utc = datetime.strptime(last_query_end_utc, "%Y-%m-%dT%H:%M:%SZ")
        query_start_utc = query_start_utc.replace(tzinfo=timezone.utc)
    else:
        query_start_utc = datetime.combine(origin_date, time.min, tzinfo=timezone.utc)
    
    # Define end of job
    query_end_utc = datetime.now(timezone.utc)

    # Define job arguments
    job_args = {
        'job_name': job_name,
        'query_date_start': query_start_utc,
        'query_date_end': query_end_utc
    }

    # Define job as local function
    @data_job(job_args=job_args, retry_count=1)
    def execute_job() -> None:
        """Executes `load-measurements` job."""
        orchestrator.refresh_sensors()
        orchestrator.refresh_buoy_fishery_assignments()
        orchestrator.load_measurements(
            start=query_start_utc,
            end=query_end_utc,
            buoy_ids=orchestrator.buoy_sensors_df['id'].tolist()
        )

    execute_job()

    # check if models have been successfully trained
    if executions.get('train-anomaly-detection', False):
        # run models
        anomaly_job_args = {
            'job_name': 'run-anomaly-detection',
            'query_date_start': query_start_utc,
            'query_date_end': query_end_utc
        }
        @data_job(job_args=anomaly_job_args, retry_count=1)
        def execute_anomaly_scores_job() -> None:
            """Calculates anomaly scores for mobile measurement events"""
            model_orchestrator.generate_anomaly_scores()
        
        execute_anomaly_scores_job()

    if executions.get('train-prediction-models', False):
        # run models
        prediction_job_args = {
            'job_name': 'run-prediction-models',
            'query_date_start': query_start_utc,
            'query_date_end': query_end_utc
        }
        @data_job(job_args=prediction_job_args, retry_count=1)
        def execute_prediction_job() -> None:
            """Calculates predictions for mobile measurement events."""
            model_orchestrator.generate_predictions()
        
        execute_prediction_job()


def load_models() -> None:
    """
    Creates new 'train-anomaly-detection' and 'train-prediction-models' jobs
    
    Runs only at startup. 
    """
    # Define job name
    job_name = 'train-anomaly-detection'

    # Fetch max datetime from last successful job
    executions = jobs_client.get_latest_job_executions()
    last_query_end_utc = executions.get(job_name)
    
    # Set max datetime as start of new job if it exists;
    # otherwise, default to the pre-configured 'ORIGIN' date.
    if last_query_end_utc:
        query_start_utc = datetime.strptime(last_query_end_utc, "%Y-%m-%dT%H:%M:%SZ")
        query_start_utc = query_start_utc.replace(tzinfo=timezone.utc)
    else:
        query_start_utc = datetime.combine(origin_date, time.min, tzinfo=timezone.utc)
    
    # Define end of job
    query_end_utc = datetime.now(timezone.utc)

    # Define job as local function
    @data_job(job_args={
        'job_name': 'train-anomaly-detection',
        'query_date_start': query_start_utc,
        'query_date_end': query_end_utc
    }, retry_count=1)
    def execute_train_anomaly_job() -> None:
        """Executes `train-anomaly-detection` job."""
        model_orchestrator.train_anomaly_detection()

    @data_job(job_args={'job_name': 'train-prediction-models',
        'query_date_start': query_start_utc,
        'query_date_end': query_end_utc
    }, retry_count=1)
    def execute_train_prediction_job() -> None:
        """Executes 'train-prediction-models' job."""
        model_orchestrator.train_prediction_models()

    logger.info("Training anomaly detection model.")
    execute_train_anomaly_job()

    logger.info("Training location prediction model.")
    execute_train_prediction_job()
    
    # generate the historical anomaly scores after training
    model_orchestrator.generate_anomaly_scores()


if __name__ == "__main__":
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    try:
        # Perform bulk loads of data on start-up
        load_oscar_datasets()
        load_measurements()
        load_models()

        # Perform subsequent loads on recurring schedule
        while True:
            run_pending()

    except Exception as e:
        logger.error(f"Execution failed. {e}")
