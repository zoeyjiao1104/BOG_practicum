"""
tests.py
"""

from ..common.testmixins import (
    BulkGetOrCreateTestMixin
)
from ..sources.factories import SourceFactory
from django.urls import reverse
from .factories import (
    OmnipresentMeasurementEventFactory,
    OmnipresentMeasurementEventNeighborFactory,
    OmnipresentMeasurementFactory
)
from ..measurements_mobile.factories import (
    MobileSensorFactory,
    MobileMeasurementEventFactory
)
from .models import (
    OmnipresentMeasurement,
    OmnipresentMeasurementEvent,
    OmnipresentMeasurementEventNeighbor
)
from rest_framework.test import APITestCase
from .serializers import (
    OmnipresentMeasurementEventSerializer,
    OmnipresentMeasurementEventNeighborSerializer,
    OmnipresentMeasurementSerializer
)
from typing import Dict, List

   
class OmnipresentMeasurementEventTestCase(
    BulkGetOrCreateTestMixin, 
    APITestCase):
    """
    Tests for omnipresent measurement events.
    """
    model = OmnipresentMeasurementEvent
    url = reverse('omnipresentmeasurementevents-list')

    def _compose_data(self, num_objects: int) -> List[Dict]:
        """
        Arranges data for testing.

        Parameters:
            num_objects (int): The number of
                objects to build (not create
                in the database).

        Returns:
            (list of dict): The objects.
        """
        source = SourceFactory.create()
        events = OmnipresentMeasurementEventFactory.build_batch(
            size=num_objects,
            source=source)
        serializer = OmnipresentMeasurementEventSerializer(
            instance=events,
            many=True)
        return serializer.data


class OmnipresentMeasurementEventNeighborTestCase(
    BulkGetOrCreateTestMixin, 
    APITestCase):
    """
    Tests for omnipresent measurement event neighbors.
    """
    model = OmnipresentMeasurementEventNeighbor
    url = reverse('omnipresentmeasurementeventneighbors-list')
  
    def _compose_data(self, num_objects: int) -> List[Dict]:
        """
        Arranges data for testing.

        Parameters:
            num_objects (int): The number of
                objects to build (not create
                in the database).

        Returns:
            (list of dict): The objects.
        """
        # Create measurement events
        omnipresent_source, mobile_source = SourceFactory.create_batch(size=2)
        mobile_sensor = MobileSensorFactory.create(source=mobile_source)
        mobile_event = MobileMeasurementEventFactory.create(
            mobile_sensor=mobile_sensor
        )
        omnipresent_events = OmnipresentMeasurementEventFactory.create_batch(
            size=num_objects,
            source=omnipresent_source
        )

        # Create neighbors
        data = []
        for event in omnipresent_events:
            neighbor = OmnipresentMeasurementEventNeighborFactory.build(
                mobile_event=mobile_event,
                neighboring_omnipresent_event=event
            )
            data.append(neighbor)

        # Serialize results
        serializer = OmnipresentMeasurementEventNeighborSerializer(
            instance=data,
            many=True)
        
        return serializer.data


class OmnipresentMeasurementTestCase(
    BulkGetOrCreateTestMixin,
    APITestCase):
    """
    Tests for omnipresent measurements.
    """

    model = OmnipresentMeasurement
    url = reverse('omnipresentmeasurements-list')
  
    def _compose_data(self, num_objects: int) -> List[Dict]:
        """
        Arranges data for testing.

        Parameters:
            num_objects (int): The number of
                objects to build (not create
                in the database).

        Returns:
            (list of dict): The objects.
        """
        # Create omnipresent measurement events
        source = SourceFactory.create()
        events =  OmnipresentMeasurementEventFactory.create_batch(
            size=num_objects,
            source=source)
    
        # Create omnipresent event measurements
        measurements = []
        for event in events:
            measurement = OmnipresentMeasurementFactory.build(
                omnipresent_measurement_event=event
            )
            measurements.append(measurement)

        # Serialize results
        serializer = OmnipresentMeasurementSerializer(
            instance=measurements,
            many=True)
        
        return serializer.data
