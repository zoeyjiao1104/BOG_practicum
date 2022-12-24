import pandas as pd
import numpy as np
import os
import math
import matplotlib
import matplotlib.pyplot as plt
from datetime import timedelta
from pathlib import Path
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_squared_error
from joblib import dump, load
from utilities.logger import logger


HERE = Path(__file__).resolve().parent

def sort_df(df: pd.DataFrame):
    """ Cleans given dataset, sorts by time and sensor id.  """
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S%z", utc=True)
    df = df.sort_values(['mobile_sensor', 'datetime'])
    return df



def get_times_from_start(df: pd.DataFrame) -> pd.DataFrame:
    """ Creates timediff column representing distance from sensor's first reading """
    first_sensor_datetime = df.groupby('mobile_sensor')['datetime'].transform('first')
    df["first_sensor_datetime"] = pd.to_datetime(first_sensor_datetime, utc=True)
    df['seconds_since_start'] = (df['datetime'] - df["first_sensor_datetime"]).dt.total_seconds()
    return df

def get_seconds_from_previous(df: pd.DataFrame) -> pd.DataFrame:
    """Creates timediff column representing distance from sensor's previous reading"""
    delta = df["datetime"] - df["datetime"].shift(1)
    df["seconds_since_previous"] = delta.dt.total_seconds()
    return df

# This can be changed and tested with more robust imputation methods. An
# iterative imputer, interpolation, or bfill are potential ideas. Furthermore
# dropping columns with a certian percentage of nans could improve performance.
def handle_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Restrict data to numbers, impute na with median"""
    df = df.select_dtypes(include=['number'])
    df = df.dropna(axis=1, how='all')
    df = df.fillna(df.median())
    return df

def shift_whole_dataset(df: pd.DataFrame, outcome: str, steps=1) -> pd.DataFrame:
    """ Creates features of previous readings

    Input: 
        df - dataframe with measurements
        mobile_sensor - which sensor id you want to shift measurements for
    Output: shifted sensor id dataframe
    """
    columns_to_keep = ["seconds_since_previous", "seconds_since_start", "mobile_sensor", outcome]
    current_columns = df[columns_to_keep]
    shifted_datasets = [current_columns]
    for i in range(steps):
        shifted_df = df.groupby("mobile_sensor").shift(i+1)
        shifted_df = shifted_df.add_suffix("_previous_{}".format(i+1))
        shifted_datasets.append(shifted_df)
    full_dataset = pd.concat(shifted_datasets, axis=1).reset_index(drop=True)
    full_dataset = full_dataset.dropna()
    return full_dataset

def filter_features(df: pd.DataFrame, outcome: str) -> pd.DataFrame:
    """Filters dataframe to only include relevant features"""
    metadata_columns = ["mobile_sensor", "seconds_since_start", "seconds_since_previous", outcome]
    if outcome == "battery":
        relevant_products = []
    # directional products
    else:
        # NOTE: predict deltas instead of raw values
        # NOTE: convert polar to cartesean coordinates
        relevant_products = [
            "ws", "wd", "bs", "bd", "cs", "cd", "wcd", "wgs", "pd", "a"
        ]
        if outcome == "latitude":
            relevant_products += ["zc", "plat", "lat"]
        elif outcome == "longitude":
            relevant_products += ["mc", "plon", "lon"]
    allowed_products = "|".join(relevant_products)
    metadata = "|".join(metadata_columns)
    df = df.filter(regex="^((((stationary|omnipresent|buoys)_neighbor_[0-9]_)?(" + allowed_products + ")-.{1,5}(_previous_[0-9])?)|(" + metadata + "))")
    return df
    

def prepare_dataset(df: pd.DataFrame, outcome: str) -> pd.DataFrame:
    """ Prepares an input dataset with full feature columns for modeling """
    df = sort_df(df)
    df = get_seconds_from_previous(df)
    df = get_times_from_start(df)
    df = filter_features(df, outcome)
    df = handle_missing_data(df)
    df = shift_whole_dataset(df, outcome)
    return df
    

def linear_pred(train_x, test_x, outcome, regressors):
    """
    Given a list of regressors, makes X dataset matrix with
    those regressors and uses linear regression to 
    predict the test coordinates after splitting X 
    with function above. 
    Input:  train_x, train_y, test_x: from sample_split function
            regressors: List of column names to regress on
    Returns Array of battery temperature that was predicted
    """  
    model = LinearRegression()
    model.fit(train_x[regressors], train_x[outcome])
    bat_hat = model.predict(test_x[regressors]).reshape(-1, 1)

    return bat_hat

def rf_predict(train_x, test_x, outcome, regressors, params):
    """Predict with random forest model
    
    Input:  train_x, train_y, test_x: from sample_split function
            regressors: List of column names to regress on
            params: parameters we input into random forest model
    Returns Array of battery temperature that was predicted
    """
    d, leaf, split, n = params 
    
    rf = RandomForestRegressor(n_estimators= n, max_depth = d, 
                                min_samples_split = split, min_samples_leaf = leaf,
                               random_state= 0)
    rf.fit(train_x[regressors].values, train_x[outcome].values)
    prediction = rf.predict(test_x[regressors].to_numpy().reshape(1, -1))

    return prediction


def split_by_time(data: pd.DataFrame, n_test):
    """Split dataset purely by time, without regard for sensor ids
    
    This is done to help generalize the model for new sensors and to
    ease the splitting process. SInce each predicition is dependent only
    on the values in its row (previous n measurements where n is probably
    low), as long as we ensure no measurements past the time t when
    the n_test'th measurement was taken, there is no leakage.
    """
    data = data.sort_values("seconds_since_start")
    return data.iloc[:-n_test, :], data.iloc[-n_test:, :]


def walk_forward_validation(data: pd.DataFrame, n_test: int, outcome: str, regressors: list, params: tuple):
	logger.info("Beginning walk-forward validation")
	predictions = list()
	# split dataset
	train, test = split_by_time(data, n_test)
	# seed history with training dataset
	history = [row for index, row in train.iterrows()]
	# step over each time-step in the test set
	for i in range(len(test)):
		logger.info(f"Time step {i} of {len(test) - 1}.")
		# split test row into input and output columns
		testX, testy = test.iloc[i, :][regressors], test.iloc[i, :][outcome]
		# fit model on history and make a prediction
		yhat = rf_predict(pd.DataFrame(history), testX, outcome, regressors, params)
		# store forecast in list of predictions
		predictions.append(yhat)
		# add actual observation to history for the next loop
		history.append(test.iloc[i, :])
	# estimate prediction error
	error = mean_squared_error(test[outcome], predictions)
	return error, test[outcome], predictions

def rf_hyperparameter_tuning(df, outcome, parameters, regressors):
    """Function for tuning hyperparameters of random forest model.

    Input:
        df: the dataframe with measurements
        Parameters: a dictionary with keys "max_depth", "min_samples_leaf",
                    "min_samples_split", "n_estimators" 
                    Values are list of hyperparameters of each category to try
        pred: the prediction function we use ("linear" or "rf")
        num_future_days: same as sample_split num_future_days
        regressors: List of column names to regress on for battery temperature
        include: same as sample_split include
    Output: 
        best_params: the best combination of parameters yielding lowest loss
        best_error: the error from that best combination
    """
    best_error = None
    max_depth = parameters["max_depth"]
    min_samples_leaf = parameters['min_samples_leaf']
    min_samples_split = parameters['min_samples_split']
    n_estimators = parameters['n_estimators']

    best_params = None

    percent_train = 0.8
    num_test = int((1 - percent_train) * len(df))

    for d in max_depth:
        for leaf in min_samples_leaf:
            for split in min_samples_split:
                for n in n_estimators:
                    error, _, _ = walk_forward_validation(df, num_test, outcome, 
                                        regressors, (d, leaf, split, n))
                    if best_error == None or error < best_error:
                        best_error = error
                        best_params = (d, leaf, split, n)
    return best_params, best_error
#### have to implement above in place of gridsearchcv to take into account the time

def get_random_forest(df: pd.DataFrame, outcome, percent_train) -> RandomForestRegressor:
    """Trains and returns optimal random forest model
 
    Inputs:
        df: the dataframe with measurements
        outcome: name of dependent variable
        percent_train: same as sample_split train_size
    Outputs:
        random_forest
    """        
    num_test = int((1 - percent_train) * len(df))
    df_features = df
    df_features['random'] = np.random.random(len(df_features))
    regressors = list(df_features.columns.drop(outcome))
    train, test = split_by_time(df, num_test)
    rf_model = RandomForestRegressor(n_estimators= 300, random_state= 0)
    rf_model.fit(train[regressors], train[outcome])

    # feature selection
    feature_importances = permutation_importance(rf_model, train[regressors], train[outcome], n_repeats = 5, max_samples=0.5)
    important_features = np.argpartition(feature_importances["importances_mean"], -1)
    feature_indicies = important_features[np.argsort(feature_importances["importances_mean"][important_features])]
    
    perm_regs = []
    for arg in feature_indicies[::-1]:
        if list(regressors)[arg] == "random":
            break
        else:
            perm_regs.append(list(regressors)[arg])
    top_features = list(set(perm_regs).union({"mobile_sensor", "seconds_since_previous"}))

    # hyper-parameter tuning
    param_grid = {
        'max_depth': [70, 90],
        'min_samples_leaf': [4, 5],
        'min_samples_split': [8],#, 12],
        'n_estimators': [300]#, 500]
    }
    # best_hyperparameters, best_error = rf_hyperparameter_tuning(df, outcome, param_grid, top_features)
    # d, leaf, split, n = best_hyperparameters

    opt_rf = RandomForestRegressor(
        n_estimators=300,
        max_depth=70, 
        min_samples_split=8,
        min_samples_leaf=4,
        random_state= 0)
    
    opt_rf.fit(df[top_features], df[outcome])
    return opt_rf

def train_random_forest(df: pd.DataFrame, outcome: str, percent_train=0.8):
    """Train and save random forest on df predicting outcome"""
    logger.info("Preparing dataset.")
    sorted_df = prepare_dataset(df, outcome)
    logger.info("Dataset preparation complete.")

    logger.info("Getting random forest.")
    rf = get_random_forest(sorted_df, outcome, percent_train)
	
    logger.info("Writing model to file.")
    os.makedirs(f'{HERE}/models', exist_ok=True)   
    destination = HERE / "models" / f"{outcome}_rf.joblib"
    dump(rf, destination)
    print(f"Model saved to {destination}")

def perform_predictions(df: pd.DataFrame, outcome: str):
    """Predict the outcome at the next timestep for each mobile_sensor in df."""
    # prepare data for predictions
    default_seconds_from_present = 7200
    latest_measurements = df.groupby("mobile_sensor")["datetime"].max()
    prediction_times = pd.to_datetime(latest_measurements, utc=True) + timedelta(seconds=default_seconds_from_present)
    prediction_times = pd.DataFrame(prediction_times).reset_index()
    df = pd.concat([prediction_times, df])
    cleaned_df = prepare_dataset(df, outcome)
    prediction_data = cleaned_df.groupby("mobile_sensor").tail(1)
    destination = HERE / "models" / f"{outcome}_rf.joblib"
    rf = load(destination)
    # remove features that model did not train using
    untrained_features = [col for col in prediction_data.columns if col not in rf.feature_names_in_]
    if untrained_features != []:
        print(f"Dropping {untrained_features} as the model was not trained with them.")
        prediction_data.drop(columns=untrained_features, inplace=True)
    # make predictions
    predictions = rf.predict(prediction_data)
    prediction_data[f"predicted_{outcome}"] = predictions
    prediction_data = prediction_data.join(prediction_times.set_index("mobile_sensor"), on="mobile_sensor")
    return prediction_data[["mobile_sensor", "datetime", f"predicted_{outcome}"]]

if __name__ == "__main__":
    url = "http://localhost:8000/api/v1/trainingdata/?format=csv"
    df = pd.read_csv(url)
    perform_predictions(df, "latitude")
    # train_random_forest(df, "latitude", 0.8)
