"""
load_models.py
"""

import pandas as pd
import os
import requests
from analysis.anomaly import (
    train_isolation_forest,
    perform_anomaly_detection
)
from analysis.forecast import (
    train_random_forest,
    perform_predictions
)
from io import StringIO
from utilities.logger import logger

base_url = os.environ['BASE_API_URL']


class ModelOrchestrator:

    def load_data(self):
        """Retrieve data from API
        
        TODO: Filter by time to limit data retrieved in prediction calls
        """
        logger.info("Requesting training data from API.")
        training_data_endpoint = f"{base_url}/trainingdata/?format=csv"
        response = requests.get(training_data_endpoint)
        if not response.ok:
            raise Exception("Failed to fetch training data from API. "
                f"{response.text}")

        dataset = pd.read_csv(StringIO(response.text))
        logger.info(f"Retrieved {len(dataset)} row(s) of training data.")

        logger.info("Subsetting dataset to include those without prior predictions.")
        dataset = dataset[dataset["is_prediction"] == False].reset_index()
        logger.info(f"{len(dataset)} row(s) remaining after subsetting.")

        logger.info("Extracting columns of interest.")
        dataset = dataset[[col for col in dataset.columns if "-qf" not in col]]
        logger.info("Training set data loaded.")

        return dataset

    def train_anomaly_detection(self):
        train_isolation_forest(self.load_data())

    def train_prediction_models(self):
        for outcome in ["latitude", "longitude"]:
            logger.info(f"Training prediction model for '{outcome}'.")
            train_random_forest(self.load_data(), outcome)
            logger.info(f"Training for '{outcome}' model complete.")

    def post_predictions(self, row):
        """Post predictions to api"""
        row = row.to_dict()
        record = {
            "mobile_sensor": row["mobile_sensor"],
            "is_prediction": True,
            "datetime": row["datetime"].isoformat(timespec='seconds')#.strftime("%Y-%m-%d %H:%M:%S%z")
        }
        for col in row:
            if col.startswith("predicted_"):
                record[col[10:]] = format(row[col], '.6f')
        return record

    def generate_predictions(self):
        """Calculate and post the predicted locations of buoys"""
        dataset = self.load_data()
        prediction_dfs = []
        for outcome in ["latitude", "longitude"]:
            predictions = perform_predictions(dataset, outcome)
            prediction_dfs.append(predictions)
        full_table = pd.concat(prediction_dfs, axis="columns")
        results = full_table.apply(self.post_predictions, axis='columns')
        mobile_measurement_event_endpoint = f"{base_url}/mobilemeasurementevents/"
        resp = requests.post(mobile_measurement_event_endpoint, json=results.to_list())
        print(resp.reason, resp.text)

    def patch_record(self, row):
        """Retrieve the id and anomaly score of buoy events and patch the relevant record"""
        id = row["mobile_measurement_event"]
        mobile_measurement_event_endpoint = f"{base_url}/mobilemeasurementevents/{id}/"
        data = {"anomaly_score": row["anomaly_score"]}
        requests.patch(mobile_measurement_event_endpoint, data=data)

    def generate_anomaly_scores(self):
        """Calculate and update the anoamly scores of buoy measurement events"""
        dataset = self.load_data()
        results = perform_anomaly_detection(dataset)
        dataset["anomaly_score"] = results
        dataset.apply(self.patch_record, axis='columns')



if __name__ == "__main__":
    orchestrator = ModelOrchestrator()
    orchestrator.train_anomaly_detection()
    orchestrator.train_prediction_models()
    orchestrator.generate_anomaly_scores()
    orchestrator.generate_predictions()