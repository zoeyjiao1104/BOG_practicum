"""
models.py
"""

from django.db import models
from ..measurements_base.models import MeasurementEvent, Measurement
from ..measurements_mobile.models import MobileMeasurementEvent
from ..sources.models import Source


class OmnipresentMeasurementEvent(MeasurementEvent):
    """
    Measurement events taken from something like a satellite, where the
    measurement is taken far from the actual sensor and spans the
    entire globe (or a lot of it). Each measurement event will have
    its own latitude and longitude, and refer to the source (NASA's
    OSCAR satellite system for example).
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'latitude',
                    'longitude',
                    'datetime',
                    'source'
                ],
                name='unique_omnipresent_measurement_event'
            )
        ]

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
    source = models.ForeignKey(
        Source,
        on_delete=models.CASCADE
    )
    

class OmnipresentMeasurementEventNeighbor(models.Model):
    """
    An association between a mobile measurement event and
    an omnipresent measurement event deemed to be nearest
    neighbors due to their time and physical proximity.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'mobile_event',
                    'neighboring_omnipresent_event',
                ],
                name='unique_omnipresent_measurement_event_neighbor'
            )
        ]

    mobile_event = models.ForeignKey(
        MobileMeasurementEvent,
        on_delete=models.CASCADE
    )
    neighboring_omnipresent_event = models.ForeignKey(
        OmnipresentMeasurementEvent,
        on_delete=models.CASCADE
    )
    distance = models.DecimalField(
        "Distance from neighbor in radians.",
        max_digits=20,
        decimal_places=10
    )
  

class OmnipresentMeasurement(Measurement):
    """
    A measurement recorded during an `OmnipresentMeasurementEvent`.    
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'product',
                    'type',
                    'value',
                    'quality',
                    'omnipresent_measurement_event'
                ],
                name='unique_omnipresent_measurement'
            )
        ]

    omnipresent_measurement_event = models.ForeignKey(
        OmnipresentMeasurementEvent,
        on_delete=models.CASCADE
    )

