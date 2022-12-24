"""
factories.py
"""

import factory
import pytz
import random
from ..measurements_mobile.factories import (
    MobileMeasurementEventFactory
)
from .models import (
    Station,
    StationaryMeasurement,
    StationaryMeasurementEvent,
    StationaryMeasurementEventNeighbor
)
from ..sources.factories import SourceFactory


class StationFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `Station` instances.
    """

    class Meta:
        model = Station

    id = factory.Sequence(lambda n: f'Station {n}')
    name = factory.Faker('sentence', nb_words=3, variable_nb_words=True)
    state = factory.Faker("lexify", text="??")
    established = factory.Faker("date")
    timezone = factory.Faker("lexify", text="US/????")
    source = factory.SubFactory(SourceFactory)
    latitude = factory.Faker("latitude")
    longitude = factory.Faker("longitude")
    depth = factory.Faker("numerify", text="#")
    

class StationaryMeasurementEventFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `StationaryMeasurementEvent` instances.
    """
    
    class Meta:
        model = StationaryMeasurementEvent

    id = factory.Sequence(lambda n: n)
    datetime = factory.Faker("date_time", tzinfo=pytz.UTC)
    anomaly_score = factory.Faker("numerify", text="0.##")
    station = factory.SubFactory(StationFactory)


class StationaryMeasurementEventNeighborFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `MobileMeasurementEventNeighbor` instances.
    """
    class Meta:
        model = StationaryMeasurementEventNeighbor

    id = factory.Sequence(lambda n: n)
    mobile_event = factory.SubFactory(MobileMeasurementEventFactory)
    neighboring_stationary_event = factory.SubFactory(StationaryMeasurementEvent)
    distance = factory.Faker("numerify", text="%!!!!.######")


class StationaryMeasurementFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `StationaryMeasurement` instances.
    """
    
    class Meta:
        model = StationaryMeasurement

    id = factory.Sequence(lambda n: n)
    product = factory.Faker("random_element", elements=["wl", "at", "wt", "bt", "ws", "bs"])
    value = factory.Faker("random_number", digits=3)
    type = factory.Faker("random_element", elements=["r", "m", "pr"])
    quality = factory.Faker("random_element", elements=["g", "na", "s", "b"])
    confidence = factory.Faker("numerify", text="0.##")
    stationary_measurement_event = factory.SubFactory(StationaryMeasurementEventFactory)


class StationWithEventsFactory(StationFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `Station` instances that contain
    one or more `StationaryMeasurementEvent`
    instances.
    """
    
    @factory.post_generation
    def events(obj, create, extracted, **kwargs):
        """
        Creates a batch of `StationaryMeasurementEvent`
        instances to associate with a newly-created
        `Station`. A random number of events between 1 
        and 10, inclusive, is generated by default 
        unless specified.

        References:
        - https://factoryboy.readthedocs.io/en/stable/reference.html#factory.PostGeneration
        """
        if not create:
            return

        try:
            num_events = int(extracted) if extracted else random.randint(1, 10)
        except ValueError:
            raise ValueError("Invalid value received for 'extracted': "
                f"'{extracted}'. An integer was expected.")

        return StationaryEventWithMeasurementsFactory.create_batch(
            size=num_events,
            station=obj)


class StationaryEventWithMeasurementsFactory(StationaryMeasurementEventFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `StationaryMeasurementEvent instances
    associated with random `StationaryMeasurement`
    instances.
    """

    @factory.post_generation
    def measurements(obj, create, extracted, **kwargs):
        """
        Creates a batch of `StationaryMeasurement`
        instances to associate with a newly-created
        `StationaryMeasurementEvent`. A random number
        of events between 5 and 10, inclusive, is
        generated by default unless specified.

        References:
        - https://factoryboy.readthedocs.io/en/stable/reference.html#factory.PostGeneration
        """
        if not create:
            return

        try:
            num_measurements = int(extracted) if extracted \
                else random.randint(5, 10)
        except ValueError:
            raise ValueError("Invalid value received for 'extracted': "
                f"'{extracted}'. An integer was expected.")

        return StationaryMeasurementFactory.create_batch(
            size=num_measurements,
            stationary_measurement_event=obj)
        
