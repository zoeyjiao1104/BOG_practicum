"""
models.py
"""

from datetime import datetime
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import F, Q
from ..measurements_base.models import MeasurementEvent, Measurement
from ..sources.models import Source


class MobileSensor(models.Model):
    """A sensor like a buoy that moves."""

    id = models.CharField(
        max_length=24,
        primary_key=True
    )
    source = models.ForeignKey(
        Source,
        on_delete=models.CASCADE
    )

    def get_most_recent_measurement_event(self):
        """Retrieves the sensor's most recent measurement event."""
        try:
            latest_measurement_event = self.mobile_measurement_event.latest("datetime")
            latest_measurements = latest_measurement_event.mobilemeasurement_set
        except ObjectDoesNotExist:
            latest_measurements = self.mobile_measurement_event.none()
        return latest_measurements


    def get_fishery(self, datetime):
        """Retrieve the fishery assignment for the given datetime."""
        assignments = self.buoyfisheryassignment_set.filter(start_date__lte=datetime)
        assignments = assignments.filter(Q(end_date__gte=datetime) | Q(end_date__isnull=True))
        return assignments.first()

    
    def get_fishery_object(self):
        if isinstance(self.datetime, str):
            dt = datetime.fromisoformat(self.datetime)
        else:
            dt = self.datetime
        return self.mobile_sensor.get_fishery(dt)

    def get_fishery_name(self):
        try:
            return self.get_fishery_object().fishery
        except AttributeError:
            return None

    def get_fishing_technology(self):
        try:
            return self.get_fishery_object().fishing_technology
        except AttributeError:
            return None

    @property
    def fishery_assignments(self):
        """
        Returns the mobile measurement event's past
        and current fishery assignments.
        """
        # Initialize query conditions
        matching_sensor = Q(buoy=self.mobile_sensor)
        valid_start_date = Q(start_date__lte=self.datetime)
        valid_end_date = Q(end_date__gte=self.datetime)
        active_assignment = Q(end_date__isnull=True)

        # Fetch matching assignment if exists
        assignments = (MobileSensorFisheryAssignment
            .objects
            .filter(
                matching_sensor &
                valid_start_date &
                (active_assignment | valid_end_date)
            )
            .all())

        # Return current assignment id if found
        return assignments.values() if assignments else None


class MobileSensorFisheryAssignment(models.Model):
    """
    Represents the assignment of a Blue
    Ocean Gear buoy to a fishery for a
    specified time period.
    """
    class Meta:
        constraints = [
            # Should add unique constraint of buoy and date range
            models.CheckConstraint(
                check=Q(end_date__gte=F('start_date')),
                name='valid_fishery_assignment_date'
            )
        ]
        ordering = ['id']

    class Fisheries(models.TextChoices):
        """The fishery where the buoy was deployed."""
        DEMO = 'demo'
        DUNGENESS_CRAB = 'dungeness crab'
        LOBSTER = 'lobster'
        MONKFISH = 'monkfish'
        MUSSEL_FARM = 'mussel farm'
        NA = 'n/a'
        SCALLOP = 'scallop'
        SNOW_CRAB = 'snow crab'
        WHELK = 'whelk'

    
    class FishingTechnology(models.TextChoices):
        """The fishing gear or technology used with the buoy."""
        AQUACULTURE = 'aquaculture'
        GILLNET = 'gillnet'
        NA = 'n/a'
        ROPELESS_GEAR = 'ropeless gear'

    class TransmissionType(models.TextChoices):
        """The technology used to transmit readings."""
        NA = 'n/a'
        RADIO = 'radio'
        SATELLITE = 'satellite'


    id = models.BigIntegerField(primary_key=True)
    buoy = models.ForeignKey(
        MobileSensor,
        on_delete=models.CASCADE
    )
    fishery = models.CharField(
        max_length=50,
        choices=Fisheries.choices,
        null=True
    )
    fishing_technology = models.CharField(
        max_length=50,
        choices=FishingTechnology.choices,
        null=True
    )
    transmission_type = models.CharField(
        max_length=255,
        choices=TransmissionType.choices,
        default=TransmissionType.NA
    )
    start_date = models.DateField()
    end_date = models.DateField(null=True)


class MobileMeasurementEvent(MeasurementEvent):
    """
    A measurement event from a sensor that moves,
    like a buoy. Therefore, the event has an
    associated latitude, longitude, and mobile
    sensor reference.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'latitude',
                    'longitude',
                    'datetime',
                    'mobile_sensor'
                ],
                name='unique_mobile_measurement_event'
            )
        ]
        ordering = ['datetime']

    # 6 decimals allows ~10 cm level precision
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
    mobile_sensor = models.ForeignKey(
        MobileSensor,
        on_delete=models.CASCADE,
        related_name="mobile_measurement_event",
    )
    is_prediction = models.BooleanField(default=False)


class MobileMeasurementEventWideManager(models.Manager):
    """
    """
    def get_queryset(self):
        queryset = super().get_queryset()


class MobileMeasurementEventNeighbor(models.Model):
    """
    Represents an association between two mobile
    measurement events deemed to be nearest neighbors
    due to their time and physical proximity.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'mobile_event',
                    'neighboring_mobile_event',
                ],
                name='unique_mobile_measurement_event_neighbor'
            )
        ]

    mobile_event = models.ForeignKey(
        MobileMeasurementEvent,
        on_delete=models.CASCADE,
        related_name="mobile_event"
    )
    neighboring_mobile_event = models.ForeignKey(
        MobileMeasurementEvent,
        on_delete=models.CASCADE,
        related_name="neighboring_mobile_event"
    )
    distance = models.DecimalField(
        "Distance from neighbor in radians.",
        max_digits=20,
        decimal_places=10
    )


class MobileMeasurement(Measurement):
    """
    Inherits from the abstract `Measurements` table
    and refers to a `MobileMeasurementEvents` record
    as a foreign key.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    'product',
                    'value',
                    'type',
                    'quality',
                    'mobile_measurement_event'
                ],
                name='unique_mobile_measurement'
            )
        ]

    mobile_measurement_event = models.ForeignKey(
        MobileMeasurementEvent,
        on_delete=models.CASCADE
    )
