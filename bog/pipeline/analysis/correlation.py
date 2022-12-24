"""
filename: correlation.py
author: Wenshi Wang 
description: calculate the correlation of speed/direction for buoys and nearest buoys
"""
import os
from utilities import files
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib.ticker as ticker
from astropy.stats import circcorrcoef
from astropy import units as u

fname = 'buoys/buoys_oscar.tsv'
buoys_neighbors_oscar = files.load_df(fname)

def get_degree(lat,lon,pre_lat,pre_lon):
    """
    Calculate buoy direction from north
        
    Inputs
        lat: (float) the current latitude of a buoy
        lon: (float) the current longitude of a buoy
        pre_lat: (float) the previous latitude of a buoy
        pre_lon: (float) the previous longitude of a buoy
        
    Outputs
        degree: (float) the moving direction of a buoy in three-decimal format
            
    sample input: get_degree(37.4708,-121.94,37.4712,-121.93)
    """    
    radians = np.arctan2(lat - pre_lat, lon - pre_lon) 
    degrees = 180 * radians / np.pi
    degree = (450 - degrees) % 360
    return round(degree,3)

def get_directions(df):
    """
    Calculate the moving directions for buoy and nearest buoys, merge into the data frame
        
    Inputs
        df: (data frame) the original buoy data frame without direction columns
        
    Outputs
        df: (data frame) the new buoy data frame with direction columns
            
    sample input: get_directions(buoys_neighbors)
    """    
    for suffix in ['','_nearest_buoy_1','_nearest_buoy_2']:
        df['direction'+suffix] = df.apply(lambda x: get_degree(x['latitude'+suffix],
                                                               x['longitude'+suffix],
                                                               x['previous_latitude'+suffix],
                                                               x['previous_longitude'+suffix]),
                                          axis=1)
    return df
    
def viz_correlation(df,variable,item,nearest=1):
    """
    Visualize the correlation of speed/direction for buoys and nearest buoys
        
    Inputs
        df: (data frame) the buoy data frame
        variable: (str) the variable to make the correlation for, could be speed or direction
        item: (str) the item to compare with the buoy, could be another buoy or an oscar
        nearest: (int) the first nearest buoy or the second nearest buoy
        
    Outputs
        fig: (.png) the scatter plot with buoy data in x axis, nearest buoy data in y axis
            
    sample input: viz_correlation(buoys_neighbors,'direction','buoy',1)
    """
    df = get_directions(df)

    if (variable != 'speed') and (variable != 'direction'):
        raise ValueError('Please choose speed or direction as the variable')
    if (nearest != 1) and (nearest != 2):
        raise ValueError('Please choose the first or the second nearest buoy')
    
    if item == 'buoy':
        y = variable+'_nearest_'+item+'_'+str(nearest)
    elif item == 'oscar':
        y = 'current_'+ variable+'_nearest_'+item+'_'+str(nearest)    
        
    scatter = sns.scatterplot(data=df,
                              x=variable,
                              y=y,
                              size='distance_nearest_'+item+'_'+str(nearest),
                              hue='distance_nearest_'+item+'_'+str(nearest),
                              alpha=0.4)
    scatter.set_title('Scatter plot for {} of {}'.format(variable,item))
    scatter.set_xlabel(variable+' of buoy')
    scatter.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
    scatter.get_figure().savefig(os.getcwd()+"\\data\\buoys\\scatter_test_{}_{}.png".format(variable,item))

def get_correlation(df,variable,item,distance_max,nearest=1):
    """
    Calculate the correlation of speed/direction for buoys and nearest buoys given a distance limit
        
    Inputs
        df: (data frame) the buoy data frame
        variable: (str) the variable to make the correlation for, could be speed or direction
        item: (str) the item to compare with the buoy, could be another buoy or an oscar
        distance_max: (float) the maximum distance allowed between buoys and nearest buoys 
        nearest: (int) the first nearest buoy or the second nearest buoy
        
    Outputs
        corr: (float) the correlation value for speed/the circular correlation value for direction
            
    sample input: get_correlation(buoys_neighbors,'direction','oscar',5,1)
    """
    df = get_directions(df)
    df = df[df['distance_nearest_'+item+'_'+str(nearest)]<distance_max]
    df_sliced = df.filter(regex=variable)
    if item =='buoy':
        df_sliced = df_sliced[[variable,variable+'_nearest_'+item+'_'+str(nearest)]]
    elif item == 'oscar':
        df_sliced = df_sliced[[variable,'current_'+variable+'_nearest_'+item+'_'+str(nearest)]]
        
    df_sliced = df_sliced.dropna()
    if variable == 'speed': 
        corr_df = df_sliced.corr()
        corr = corr_df.iloc[0:1,1].values[0]
    elif variable =='direction':
        alpha = (df_sliced. iloc[:, 0].values)*u.deg
        beta = (df_sliced. iloc[:, 1].values)*u.deg
        corr = circcorrcoef(alpha, beta).value 
    return corr
    
def plot_distance_corr(df,variable,distance_max,nearest=1,bucket=10):
    """
    Plot the correlation values of speed/direction for buoys and nearest buoys as distance changes,
    given a distance limit
        
    Inputs
        df: (data frame) the buoy data frame
        variable: (str) the variable to make the correlation for, could be speed or direction
        distance_max: (float) the maximum distance allowed between buoys and nearest buoys 
        nearest: (int) the first nearest buoy or the second nearest buoy
        bucket: (int) the bin size of distance
        
    Outputs
        distance_plot: (.png) the bar chart with distance value as x-axis and correlation value as y-axis
            
    sample input: plot_distance_corr(buoys_neighbors,'direction',5,1,10)
    """    
    distances = [x/bucket for x in range(distance_max*bucket)]
    corr = [0]*len(distances)
    distances_dict = dict(zip(distances,corr))
    
    for distance in distances_dict.keys():
        distances_dict[distance] = get_correlation(df,variable,distance,nearest)

    distance_df = pd.DataFrame(distances_dict.items())
    distance_plot = sns.barplot(x=0, y=1, data=distance_df)
    distance_plot.xaxis.set_major_locator(ticker.AutoLocator())
    distance_plot.xaxis.set_minor_locator(ticker.AutoMinorLocator())
    distance_plot.set(xlabel='Distance', ylabel='Correlation')

    distance_plot.get_figure().savefig(os.getcwd()+"\\data\\buoys\\distance_corr.png")

def get_degree_diff(degree_1,degree_2):
    """
    This function calculates the difference between two degrees. 
    If the absolute value of the difference is less than 180, we keep the value.
    If the absolute value of the difference is more than 180, we use the difference between 360 and the value.
    """
    diff = abs(degree_1-degree_2)
    if diff>180:
        diff = 360-diff
    return diff

def viz_degree_diff(df,nearest=1):
    """
    This function visualize the distribution of degree difference 
    between buoy and nearest buoy
    """
    df = get_directions(df)
    df['direction_diff_nearest_'+str(nearest)] = df.apply(lambda x: get_degree_diff(x['direction'],x['direction_nearest_buoy_'+str(nearest)]),
                                                          axis=1)    
    hist = sns.histplot(df,
                        x='direction_diff_nearest_'+str(nearest),
                        kde=True,
                        bins=50)
    hist.set_title('Direction difference between buoy and nearest {} buoy'.format(nearest))
    hist.get_figure().savefig(os.getcwd()+"\\data\\buoys\\degree_diff_dist.png")
