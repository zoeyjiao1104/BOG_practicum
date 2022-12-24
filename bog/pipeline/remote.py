import pandas as pd
from analysis.forecast import train_random_forest
from google.cloud import storage
from utilities.logger import logger

# Define constants
GC_PROJECT_NAME = "southern-field-305921"
GC_BUCKET_NAME = "bog-temp"
GC_BLOB_BASE_NAME = "_rf.joblib"

# Read training data from file
try:
    logger.info("Reading training data from CSV file")
    training_dataset = pd.read_csv("predictions_training_set.csv")
    logger.info(f"{len(training_dataset)} record(s) found.")
except Exception as e:
    err_msg = "Remote processing failed. Failed to " \
        f"read training data from file. {e}"
    logger.error(err_msg)
    raise Exception(err_msg)

# Train on each predictive variable and persist trained
# model to Google Cloud Storage bucket
try:
    for outcome in ["latitude", "longitude"]:
        logger.info(f"Training prediction model for '{outcome}'.")
        train_random_forest(training_dataset, outcome)
        logger.info(f"Training for '{outcome}' prediction model complete.")

        logger.info("Persisting model to Google Cloud storage.")
        blob_name = f"{outcome}{GC_BLOB_BASE_NAME}"
        storage_client = storage.Client(project=GC_PROJECT_NAME)
        bucket = storage_client.get_bucket(GC_BUCKET_NAME)
        blob = bucket.blob(blob_name)
        with open(f"./analysis/models/{blob_name}", "rb") as f:
            blob.upload_from_file(f)
        logger.info(f"File '{GC_BUCKET_NAME}/{blob_name}' written successfully.")
except Exception as e:
    err_msg = f"Training failed: {e}."
    logger.error(err_msg)
    raise Exception(err_msg)
