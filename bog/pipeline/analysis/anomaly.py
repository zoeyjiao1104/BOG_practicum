"""
anomaly.py
"""

import re
import pandas as pd
import numpy as np
import os
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import OneHotEncoder
from pathlib import Path
from joblib import load, dump
from utilities.logger import logger


HERE = Path(__file__).resolve().parent

def get_times_from_start(df: pd.DataFrame) -> pd.DataFrame:
    """ Creates timediff column representing distance from sensor's first reading """
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df['seconds_since_start'] = (df['datetime'] - df["datetime"].min()).dt.total_seconds()
    return df

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Format data for an isolation forest model"""
    print("testing")
    regex = r"(id|datetime|anomaly_score|station|event|(-q(f?)))(_previous_[0-9])?$"
    not_in_use = [col for col in df.columns if re.search(regex, col)]
    categorical_values = ["fishery", "fishing_technology", "wcd"]
    categorical_pattern = "(" + "|".join(categorical_values) + ").*"
    categorical_columns = [col for col in df.columns if re.search(categorical_pattern, col)]
    numeric_columns = [col for col in df.columns if col not in not_in_use + categorical_columns]

    df = get_times_from_start(df)
    buoys = df.drop(not_in_use, axis=1)

    # fill NaN for numeric columns with most median value within each group (buoy, fishery)
    for col in numeric_columns:
        try:
            buoys[col] = buoys[col].fillna(buoys.groupby("mobile_sensor")[col].transform(lambda x: x.median()))
        except:
            buoys[col] = buoys[col].fillna(buoys.groupby("fishery")[col].transform(lambda x: x.median()))

        if buoys[col].hasnans:
            buoys[col] = buoys[col].fillna(buoys[col].median())

    buoys = buoys[categorical_columns + numeric_columns]

    ohe = OneHotEncoder(categories='auto')
    array_hot_encoded = ohe.fit_transform(buoys[categorical_columns]).toarray()
    feature_labels = ohe.get_feature_names_out()
    data_hot_encoded = pd.DataFrame(array_hot_encoded, columns=feature_labels)
    data_other_cols = buoys.drop(columns=categorical_columns)
    buoys_if = pd.concat([data_hot_encoded, data_other_cols], axis=1)
    logger.info("Preprocessed data for anomaly detection.")
    return buoys_if


def train_isolation_forest(df: pd.DataFrame) -> IsolationForest:
    """Creates isolation forest trained on dataframe records
    
    Args:
        df: DataFrame with data from BOG buoy
    Returns: Isolation Forest model"""
    buoys_if = preprocess_data(df)

    iforest = IsolationForest()

    iforest= iforest.fit(buoys_if)

    os.makedirs(f'{HERE}/models', exist_ok=True)   
    destination = HERE / "models" / f"anomaly_if.joblib"
    dump(iforest, destination)
    logger.info(f"Model saved to {destination}")
    return iforest


def perform_anomaly_detection(df: pd.DataFrame):
    """Given a df of buoy data and a fitted model pre-saved, return anomaly scores"""
    destination = HERE / "models" / f"anomaly_if.joblib"
    iforest = load(destination)
    buoys_data = preprocess_data(df)
    for feature in iforest.feature_names_in_:
        if feature not in buoys_data.columns:
            # should only occur for fisheries that were in fitting data, but not in
            # smaller set. NOTE: can also happen for missing measurements
            buoys_data[feature] = 0
    new_columns = [col for col in buoys_data.columns if col not in iforest.feature_names_in_]
    if new_columns != []:
        logger.info(f"Dropping {new_columns} as they were not present in trianing set")
        buoys_data.drop(columns=new_columns, inplace=True)
    results = iforest.score_samples(buoys_data)
    results = np.abs(results)
    return results
