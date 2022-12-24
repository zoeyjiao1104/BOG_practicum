"""
filename: buoy_motion.py
author: Wenshi Wang 
description: predict range of motion
"""
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import os
import datetime
from utilities import files
import bog
import smallest_enclosing_circle as sec

boundaries = gpd.read_file(os.getcwd()+"\\data\\shapefiles\\usa_map\\us_boundaries.shp")
bog = bog.BOG_API()

def format_time(datetime_string):
    """ Makes datetime object from 'yyyy-mm-ddThh:mm:ssZ' or 'yyyy-mm-dd' """
    if len(datetime_string) == 20:
        dt = datetime.datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%SZ")
    elif len(datetime_string) == 10:
        dt = datetime.datetime.strptime(datetime_string, "%Y-%m-%d")
    else:
        error_message = ("datetime should be of format "
                         "yyyy-mm-ddThh:mm:ssZ' or 'yyyy-mm-dd'. "
                         "received {}".format(datetime_string))
        raise ValueError(error_message)
    return dt

class SmallestCircle():
    def __init__(self,buoy_id,start_time,end_time):
        """
        SAMPLE params: (155,'2021-04-09','2021-09-09')
        Description of initialized values
        Initializes: 
            station_id : (str) the id of a station   
            start_time: (str) the time to start collecting from. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"            
        """
        self.buoy_id = buoy_id
        self.start_time = start_time
        self.end_time = end_time
        self.buoy_df_east = self.viz_buoy()[0]
        self.buoy_df_west = self.viz_buoy()[1]

    def get_buoy_data(self):
        """
        Create the geo data frame for a given buoy
        
        Inputs
            buoy_id: (int) the id of a single buoy
            start_time: (str) the time to start collecting from. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"        
    
        Outputs
            buoy_gdf (geo data frame): the data frame with geometry column within a given time range

        sample input: get_buoy_data(155,'2021-04-09','2021-09-09')
        """
        start_time = format_time(self.start_time)
        end_time = format_time(self.end_time)
    
        buoy_df = bog.create_buoy_df(self.buoy_id)
        buoy_df['time'] = buoy_df['time'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ"))
        buoy_df = buoy_df[(buoy_df['time'] > start_time) & (buoy_df['time'] <= end_time)]
        buoy_df['time'] = buoy_df['time'].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        buoy_df['time'] = buoy_df['time'].astype(pd.StringDtype()).replace('000000','000',regex=True)

        buoy_gdf = gpd.GeoDataFrame(buoy_df, 
                                    geometry=gpd.points_from_xy(buoy_df.buoy_lon, buoy_df.buoy_lat))
        buoy_gdf = buoy_gdf.set_crs('EPSG:4269') # the EPSG of boudaries is 4269
        return buoy_gdf

    def viz_buoy(self):
        """
        Create the visualization for a given buoy and split the data frame into two (east coast and west coast, if exists)
        
        Inputs
            buoy_id: (int) the id of a single buoy
            start_time: (str) the time to start collecting from. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"        
        
        Outputs
            fig (.png): the fig zoom into a small area to see if there is any potential anomaly for a buoy
            buoy_df_east,buoy_df_west (tuple of data frames): two dataframes with buoy information 
            when it was located in east coast or west coast
            
        sample input: viz_buoy(155,'2021-04-09','2021-09-09')
        """
        buoy_df = self.get_buoy_data()
    
        buoy_df_east = buoy_df[buoy_df['buoy_lon']>-100] # east coast 
        buoy_df_west = buoy_df[buoy_df['buoy_lon']<-100] # west coast
    
        buoy_df_east.name = 'east'
        buoy_df_west.name = 'west'
    
        for buoy_df in [buoy_df_east,buoy_df_west]:
            if buoy_df.empty:
                print('This buoy has not visited the {} coast yet.'.format(buoy_df.name))
            else:
                fig, ax = plt.subplots(figsize=(15,15))
                boundaries.plot(ax=ax, color='white', edgecolor='black')
    
                times = np.unique(buoy_df['time'])
                colors = np.linspace(0, 1, len(times))
                colordict = dict(zip(times, colors))  
                buoy_df['Color'] = buoy_df['time'].apply(lambda x: colordict[x])
    
                ax = buoy_df.plot(ax=ax, c=buoy_df.Color)
                ax.axis('off')
                ax.set_xlim([buoy_df['buoy_lon'].min(), buoy_df['buoy_lon'].max()])
                ax.set_ylim([buoy_df['buoy_lat'].min(), buoy_df['buoy_lat'].max()])
                ax.set_title('Buoy {} motion visualization'.format(self.buoy_id))
    
                fig.savefig(os.getcwd()+"\\data\\buoys\\{}_{}.png".format(self.buoy_id,buoy_df.name))
        return buoy_df_east,buoy_df_west

    def make_smallest_circle(self,buoy_df_east,buoy_df_west):
        """
        Create the range of motion for a given buoy
            
        Inputs
            buoy_id: (int) the id of a single buoy
            start_time: (str) the time to start collecting from. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            buoy_df_east (data frame): dataframe with buoy information when it was located in east coast
            buoy_df_west (data frame): dataframe with buoy information when it was located in west coast
        
        Outputs
            circles: (list) the list of tuples with mean longitude, mean latitude and radius 
            when the buoy was located in east coast or west coast
            
        sample input: make_smallest_circle(155,'2021-04-09','2021-09-09')
        """    
        circles = []
        for buoy_df in [buoy_df_east,buoy_df_west]:
            if buoy_df.empty:
                print('This buoy has not visited the {} coast yet. No smallest circle there.'.format(buoy_df.name))
            else:
                buoy_points = buoy_df[['buoy_lon','buoy_lat']]
                buoy_points = buoy_points.to_records(index=False)
                buoy_points = list(buoy_points)
        
                circle = sec.make_circle(buoy_points)
                circles.append(circle)
        return circles

    def identify_anomaly(self):
        """
        Identify potential anomalous points for a given buoy and append into one data frame
            
        Inputs
            buoy_id: (int) the id of a single buoy
            start_time: (str) the time to start collecting from. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
            end_time: (str) the time to end collection. Format: "yyyy-mm-ddThh:mm:ssZ" or "yyyy-mm-dd"
        
        Outputs
            anomaly_df: (data frame) the data frame with all potential anomalous points for a given buoy, including east coast and west coast
            
        sample input: identify_anomaly(155,'2021-04-09','2021-09-09')
        """            
        circle_east,circle_west = self.make_smallest_circle(self.buoy_df_east,self.buoy_df_west)
        r_base_east = circle_east[2]
        r_base_west = circle_west[2]
        anomaly_df = pd.DataFrame()
        
        for index, row in self.buoy_df_east.iterrows():
            east_drop = self.buoy_df_east.drop(index=index)
            circle = self.make_smallest_circle(east_drop,self.buoy_df_west)[0]
            r_i = circle[2]
            if r_base_east/r_i>2:
                anomaly_df = anomaly_df.append(row)
            
        for index, row in self.buoy_df_west.iterrows():
            west_drop = self.buoy_df_west.drop(index=index)
            circle = self.make_smallest_circle(self.buoy_df_east,west_drop)[1]
            r_i = circle[2]
            if r_base_west/r_i>2:
                anomaly_df = anomaly_df.append(row)
        
        return anomaly_df