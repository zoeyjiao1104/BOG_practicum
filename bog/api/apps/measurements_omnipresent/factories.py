"""
factories.py
"""

import factory
import pytz
import random
from ..measurements_mobile.factories import (
    MobileSensorFactory
)
from .models import (
    OmnipresentMeasurement,
    OmnipresentMeasurementEvent,
    OmnipresentMeasurementEventNeighbor
)
from ..sources.factories import SourceFactory


class OmnipresentMeasurementEventFactory(
    factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing fake 
    `OmnipresentMeasurementEvent` instances.
    """

    class Meta:
        model = OmnipresentMeasurementEvent

    id = factory.Sequence(lambda n: n)
    datetime = factory.Faker("date_time", tzinfo=pytz.UTC)
    anomaly_score = factory.Faker("numerify", text="0.##")
    source = factory.SubFactory(SourceFactory)
    latitude = factory.Faker("latitude")
    longitude = factory.Faker("longitude")


class OmnipresentMeasurementEventNeighborFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `MobileMeasurementEventNeighbor` instances.
    """
    class Meta:
        model = OmnipresentMeasurementEventNeighbor

    id = factory.Sequence(lambda n: n)
    mobile_event = factory.SubFactory(MobileSensorFactory)
    neighboring_omnipresent_event = factory.SubFactory(OmnipresentMeasurementEventFactory)
    distance = factory.Faker("numerify", text="%!!!!.######")


class OmnipresentMeasurementFactory(
    factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing fake 
    `OmnipresentMeasurement` instances.
    """

    class Meta:
        model = OmnipresentMeasurement

    id = factory.Sequence(lambda n: n)
    product = factory.Faker("random_element", elements=["wl", "at", "wt", "bt", "ws", "bs"])
    value = factory.Faker("random_number")
    type = factory.Faker("random_element", elements=["r", "m", "pr"])
    quality = factory.Faker("random_element", elements=["g", "na", "s", "b"])
    confidence = factory.Faker("numerify", text="0.##")
    omnipresent_measurement_event = factory.SubFactory(OmnipresentMeasurementEventFactory)


class OmnipresentEventWithMeasurementFactory(
    OmnipresentMeasurementEventFactory):
    """
    A `DjangoModelFactory` for producing 
    fake `OmnipresentMeasurementEvent`
    instances associated with random 
    `OmnipresentMeasurement` instances.
    """

    @factory.post_generation
    def measurements(obj, create, extracted, **kwargs):
        """
        Creates a batch of `OmnipresentMeasurement`
        instances to associate with a newly-created
        `OmnipresentMeasurementEvent`. A random number
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

        return OmnipresentMeasurementFactory.create_batch(
            size=num_measurements,
            omnipresent_measurement_event=obj)

