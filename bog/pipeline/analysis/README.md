# analysis

Files for forecasting, predicting, and describing buoy motion and battery temperature.

## Note on Prediction Scripts

This analysis portion has three prediction scripts: one for location, one for depth, and one for battery temperature. Each one is structured very similarly. 

As the initial input, we are given a dataset from Blue Ocean Gear buoys and associated station and ocean data. To use this data to be able to forecast the buoy's movements and battery measurements, we first clean the dataframe and create shifting functions that would align each buoy's location, depth, or battery temperature data with its measurements from the previous timestamp. This allows our random forest and linear regression models to be able to train on relevant past data instead of predicting future data with present data that we would not have. These dataframe cleaning and shifting functions are the first three functions of each prediction script. 

We also include a dataframe splitting function called sample_split to split the data into relevant training and test data. We use this in conjunction with a time series cross validation error to evaluate our models. The lower the time series cross validation error, the better our model. Our loss estimates for the location of the buoy were based on the Euclidean distance between the predicted and actual locations, and for the depth and battery temperature predictions we used a Mean Squared Error loss. We also add a random forest hyperparameter tuning function that uses this time series cross validation method to find the best combination of hyperparameters to input into the random forest regression. 

In each script we have a prediction function using random forest regression and linear regression. The rf_predict and linear_pred functions are able to get predictions for a test input given relevant training, regressor, and parameter data. We found that the random forest prediction yielded better results in general, so we made a get_rf_pred function that would be able to 1. find the best hyperparameters and regressors, and 2. save the random forest model using that best combination to a pickle file that can be retrieved later to predict actual buoy measurements. We also added a mapping function that would be able to plot test points to see how accurate the predictions end up visually. 

There are a couple important things to note in these scripts. 

1. While we do have a preliminary feature importance selector using permutation importances, we recommend fine tuning that even further. Some suggestions would be to adjust some of the columns of the dataframe to prevent overfitting or add new ones that help capture the interaction betweenn columns.

2. Our method of retrieving the dataset is a pretty preliminary method. It just gathers the data from a google drive document. In the future, we would recommend fixing that so that it can mount itself to a more official database source so that it can gather new data and retrieve directly from that source. 

3. Finally, to improve the runtime and lessen the computational load we limited the hyperparameters tested to a small set, but if possible we would recommend expanding that set even further to find an even more refined combination of parameters. 