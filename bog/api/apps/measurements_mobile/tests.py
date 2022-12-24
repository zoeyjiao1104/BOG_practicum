"""
tests.py
"""

from ..common.testmixins import (
    BulkCreateTestMixin,
    BulkGetOrCreateTestMixin
)
from ..sources.factories import SourceFactory
from django.urls import reverse
from .factories import (
    MobileSensorFactory,
    MobileMeasurementFactory,
    MobileMeasurementEventFactory,
    MobileMeasurementEventNeighborFactory
)
from .models import (
    MobileSensor,
    MobileMeasurementEvent,
    MobileMeasurementEventNeighbor,
    MobileMeasurement
)
from rest_framework.test import APITestCase
from .serializers import (
    MobileMeasurementSerializer,
    MobileMeasurementEventSerializer,
    MobileMeasurementEventNeighborSerializer,
    MobileSensorSerializer
)
from typing import Dict, List


class MobileSensorTestCase(
    BulkCreateTestMixin,
    APITestCase):
    """
    Tests for mobile sensors.
    """
    model = MobileSensor
    url = reverse('mobilesensors-list')

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
        sensors = MobileSensorFactory.build_batch(
            size=num_objects,
            source=source)
        serializer = MobileSensorSerializer(
            instance=sensors,
            many=True)
        return serializer.data

   
class MobileMeasurementEventTestCase(
    BulkGetOrCreateTestMixin,
    APITestCase):
    """
    Tests for mobile measurement events.
    """
    model = MobileMeasurementEvent
    url = reverse('mobilemeasurementevents-list')

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
        sensor = MobileSensorFactory.create(source=source)
        data =  MobileMeasurementEventFactory.build_batch(
            size=num_objects,
            mobile_sensor=sensor)
        serializer = MobileMeasurementEventSerializer(
            instance=data,
            many=True)
        return serializer.data


class MobileMeasurementEventNeighborTestCase(
    BulkGetOrCreateTestMixin,
    APITestCase):
    """
    Tests for mobile measurement event neighbors.
    """
    model = MobileMeasurementEventNeighbor
    url = reverse('mobilemeasurementeventneighbors-list')
  
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
        source = SourceFactory.create()
        sensor = MobileSensorFactory.create(source=source)
        ref_event = MobileMeasurementEventFactory.create()
        neighbor_events = MobileMeasurementEventFactory.create_batch(
            size=num_objects,
            mobile_sensor=sensor
        )

        # Create event neighbors
        data = []
        for event in neighbor_events:
            neighbor = MobileMeasurementEventNeighborFactory.build(
                mobile_event=ref_event,
                neighboring_mobile_event=event
            )
            data.append(neighbor)

        # Serialize results
        serializer = MobileMeasurementEventNeighborSerializer(
            instance=data,
            many=True)
        
        return serializer.data


class MobileMeasurementTestCase(BulkGetOrCreateTestMixin, APITestCase):
    """
    Tests for mobile measurement event neighbors.
    """
    model = MobileMeasurement
    url = reverse('mobilemeasurements-list')
  
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
        source = SourceFactory.create()
        sensor = MobileSensorFactory.create(source=source)
        events = MobileMeasurementEventFactory.create_batch(
            size=num_objects,
            mobile_sensor=sensor)

        # Create measurements
        measurements = []
        for event in events:
            measurement = MobileMeasurementFactory.build(
                mobile_measurement_event=event
            )
            measurements.append(measurement)

        # Serialize results
        serializer = MobileMeasurementSerializer(
            instance=measurements,
            many=True)
        
        return serializer.data

