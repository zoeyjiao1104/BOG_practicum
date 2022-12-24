"""
models.py
"""

import pytz
from django.db import models


class MeasurementEvent(models.Model):
    """
    An abstract representation of an event where measurements
    were taken. For example, if a buoy or station transmits
    measurements at time `t`, all of these measurements will
    refer to its measurement event. This is done to eliminate
    redundancy (as every measurement from the same time will
    have the same location, depth, station/buoy, etc.).
    """
    datetime = models.DateTimeField("Datetime of measurement event")
    anomaly_score = models.FloatField(null=True)

    class Meta:
        abstract = True
        

class Measurement(models.Model):
    """
    Measurements in 'long' format. That is, rather than having a
    column for each measured parameter/product, there is a product
    and value column and each row represents a product-value pair.
    This eliminates the large amount of nulls that would come from
    expecting every measurement type each time one appears.
    """

    class Meta:
        abstract = True

    class Products(models.TextChoices):
        """ Measured parameter """
        WATER_LEVEL = 'wl', 'Water Level'
        AIR_TEMPERATURE = 'at', 'Air Temperature'
        WATER_TEMPERATURE = 'wt', 'Water Temperature'
        BATTERY_TEMPERATURE = 'bt', 'Battery Temperature'
        WIND_SPEED = 'ws', 'Wind Speed'
        WIND_DIRECTION = 'wd', 'Wind Direction'
        CURRENT_SPEED = 'cs', 'Current Speed'
        CURRENT_DIRECTION = 'cd', 'Current Direction'
        BUOY_SPEED = 'bs', 'Buoy Speed'
        BUOY_DIRECTION = 'bd', 'Buoy Direction'
        WIND_CARDINAL_DIRECTION = 'wcd', 'Wind Cardinal Direction'
        WIND_GUST_SPEED = 'wgs', 'Wind Gust Speed'
        WIND = 'w', 'Wind'
        TIDE = 't', 'Tide'
        CURRENT_BIN = 'cb', 'Current Bin'
        CURRENT_FLOOD_DIRECTION = 'cfd', 'Current Flood Direction'
        CURRENT_EBB_DIRECTION = 'ced', 'Current Ebb Direction'
        CURRENT_DEPTH = 'cdpth', 'Current Depth'
        CURRENT_VELOCITY_MAJOR = 'cvm', 'Current Velocity Major'
        DEPTH = 'depth', 'Depth'
        PREVIOUS_LONGITUDE = 'plon', 'Previous Longitude'
        PREVIOUS_LATITUDE = 'plat', 'Previous Latitude'
        ACCELERATION = 'a', 'Acceleration'
        UC_TEMPERATURE = 'uct', 'UC Temperature'
        POSITION_DELTA = 'pd', 'Position Delta'
        MOMSN = 'momsn', 'momsn'
        ICE_PERCENT = 'ice', 'Ice percent'
        ERR_CELSIUS  = 'err', 'Err (Celsius)'
        ANOM_CELSIUS  = 'anom', 'Anom (Celsius)' 
        SST_CELSIUS  = 'sst', 'Sst (Celsius)' 
        WATER_PRESSURE = 'wp', 'Water Pressure'
        SALINITY = 'sl', 'Salinity'
        ZONAL_CURRENT = 'zc', 'Zonal Current'
        MERIDONAL_CURRENT = 'mc', 'Meridonal Current'
        LATITUDE = "lat", "Latitude"
        LONGITUDE = "lon", "Longitude"

    class Types(models.TextChoices):
        """ 
        Type of measurement. Quartiles and mean are used for
        aggregated readings, predictions and interpolation are for
        measurements that were not directly taken, and raw is for
        a normal measurement. 
        """
        RAW = 'r', 'Raw'
        Q1 = 'q1', 'First Quartile'
        Q2 = 'q2', 'Second Quartile'
        Q3 = 'q3', 'Third Quartile'
        MEAN = 'm', 'Mean'
        PREDICTION = 'pr', 'Prediction'
        PREDICTION_H_L = 'prhl', 'Prediction Highs Lows'
        INTERPOLATION = 'in', 'Interpolation'
        QUARTOD_FLAGS = 'qf', 'Quartod Flags'
        ST_DEVIATION = 'sd', 'Standard Deviation'
        QUALITY = 'q', 'Quality'
        PREDICTION_MEAN = 'prm', 'Prediction Mean'
        PREDICTION_BORES = 'prb', 'Prediction Bores'
        
    class QualityControlCodes(models.TextChoices):
        """ QC/QA flags, if available, according to source. """
        GOOD = 'g', "Good"
        NA = 'na', "Not evaluated or unknown"
        SUSPECT = 's', 'Suspect data'
        BAD = 'b', 'Bad Data'

    product = models.CharField(
        max_length=5,
        choices=Products.choices,
        null = True
    )
    value = models.CharField(
        max_length=10,
        null=True
    )
    type = models.CharField(
        max_length=5,
        choices=Types.choices,
        null=True
    )
    quality = models.CharField(
        max_length=2,
        choices=QualityControlCodes.choices,
        null=True
    )
    confidence = models.FloatField(
        "Confidence in measurement, 1 for observations, [0,1] for predictions",
        null=True,
    )

    class Meta:
        abstract = True
