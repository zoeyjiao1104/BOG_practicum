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
    StationFactory,
    StationaryMeasurementFactory,
    StationaryMeasurementEventFactory,
    StationaryMeasurementEventNeighborFactory,
)
from ..measurements_mobile.factories import (
    MobileSensorFactory,
    MobileMeasurementEventFactory
)
from .models import (
    Station,
    StationaryMeasurement,
    StationaryMeasurementEvent,
    StationaryMeasurementEventNeighbor
)
from rest_framework.test import APITestCase
from .serializers import (
    StationSerializer,
    StationaryMeasurementSerializer,
    StationaryMeasurementEventSerializer,
    StationaryMeasurementEventNeighborSerializer
)
from typing import Dict, List


class StationTestCase(
    BulkGetOrCreateTestMixin, 
    APITestCase):
    """
    Tests for stations.
    """
    model = Station
    url = reverse('stations-list')

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
        stations = StationFactory.build_batch(
            size=num_objects,
            source=source)
        serializer = StationSerializer(
            instance=stations,
            many=True)
        return serializer.data

   
class StationaryMeasurementEventTestCase(
    BulkGetOrCreateTestMixin, 
    APITestCase):
    """
    Tests for stationary measurement events.
    """
    model = StationaryMeasurementEvent
    url = reverse('stationarymeasurementevents-list')

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
        station = StationFactory.create(source=source)
        data =  StationaryMeasurementEventFactory.build_batch(
            size=num_objects,
            station=station)
        serializer = StationaryMeasurementEventSerializer(
            instance=data,
            many=True)
        return serializer.data


class StationaryMeasurementEventNeighborTestCase(
    BulkGetOrCreateTestMixin, 
    APITestCase):
    """
    Tests for stationary measurement event neighbors.
    """
    model = StationaryMeasurementEventNeighbor
    url = reverse('stationarymeasurementeventneighbors-list')
  
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
        station_source, mobile_source = SourceFactory.create_batch(size=2)
        station = StationFactory.create(source=station_source)
        mobile_sensor = MobileSensorFactory.create(source=mobile_source)
        stationary_events = StationaryMeasurementEventFactory.create_batch(
            size=num_objects,
            station=station
        )
        mobile_event = MobileMeasurementEventFactory.create(
            mobile_sensor=mobile_sensor
        )

        # Create neighbors
        data = []
        for event in stationary_events:
            neighbor = StationaryMeasurementEventNeighborFactory.build(
                mobile_event=mobile_event,
                neighboring_stationary_event=event
            )
            data.append(neighbor)

        # Serialize results
        serializer = StationaryMeasurementEventNeighborSerializer(
            instance=data,
            many=True)
        
        return serializer.data


class StationaryMeasurementTestCase(
    BulkGetOrCreateTestMixin,
    APITestCase):
    """
    Tests for stationary measurements.
    """

    model = StationaryMeasurement
    url = reverse('stationarymeasurements-list')
  
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
        # Create stationary measurement events
        source = SourceFactory.create()
        station = StationFactory.create(source=source)
        events =  StationaryMeasurementEventFactory.create_batch(
            size=num_objects,
            station=station)
    
        # Create stationary event measurements
        measurements = []
        for event in events:
            measurement = StationaryMeasurementFactory.build(
                stationary_measurement_event=event
            )
            measurements.append(measurement)

        # Serialize results
        serializer = StationaryMeasurementSerializer(
            instance=measurements,
            many=True)
        
        return serializer.data
