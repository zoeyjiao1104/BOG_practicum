"""
models.py
"""

from django.db import models
from ..measurements_base.models import (
    Measurement,
    MeasurementEvent
)
from ..measurements_mobile.models import (
    MobileMeasurementEvent
)
from ..sources.models import Source


class Station(models.Model):
    """
    A stationary sensor, such as a NOAA or DFO weather station.
    """
    class Meta:
        ordering = ['id']

    id = models.CharField(max_length=24, primary_key=True)
    name = models.CharField(max_length=200)
    state = models.CharField(max_length=10, null=True)
    established = models.DateField(null=True)
    timezone = models.CharField(max_length=15,null=True)
    source = models.ForeignKey(Source, on_delete=models.CASCADE)
    latitude = models.DecimalField(
        "Latitude of measurement event",
        max_digits=9,
        decimal_places=6
    )
    longitude = models.DecimalField(
        "Longitude of measurement event",
        max_digits=9,
        decimal_places=6
    )
    depth = models.DecimalField(max_digits=5, decimal_places=2,null=True)


class StationaryMeasurementEvent(MeasurementEvent):
    """
    Measurement events from stations. Each station
    will always have the same location, so these only
    need to refer back to the station object.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'id',
                    'datetime',
                    'station'
                ],
                name='unique_stationary_measurement_event'
            )
        ]

    station = models.ForeignKey(
        Station,
        on_delete=models.CASCADE
    )


class StationaryMeasurementEventNeighbor(models.Model):
    """
    An association between a mobile measurement event and
    a stationary measurement event deemed to be nearest
    neighbors due to their time and physical proximity.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'mobile_event',
                    'neighboring_stationary_event',
                ],
                name='unique_stationary_measurement_event_neighbor'
            )
        ]

    mobile_event = models.ForeignKey(
        MobileMeasurementEvent,
        on_delete=models.CASCADE
    )
    neighboring_stationary_event = models.ForeignKey(
        StationaryMeasurementEvent,
        on_delete=models.CASCADE
    )
    distance = models.DecimalField(
        "Distance from neighbor in radians.",
        max_digits=20,
        decimal_places=10
    )
    

class StationaryMeasurement(Measurement):
    """
    A measurement recorded during a `StationaryMeasurementEvent`.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'product',
                    'value',
                    'type',
                    'quality',
                    'stationary_measurement_event'
                ],
                name='unique_stationary_measurement'
            )
        ]

    stationary_measurement_event = models.ForeignKey(
        StationaryMeasurementEvent,
        on_delete=models.CASCADE
    )
