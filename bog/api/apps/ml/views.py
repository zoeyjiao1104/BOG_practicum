"""
views.py
"""

from ..measurements_mobile.models import (
    MobileMeasurementEvent,
    MobileMeasurement
)
from ..measurements_mobile.serializers import (
    MobileMeasurementEventWideSerializer,
    MobileMeasurementWideSerializer
)
from ..measurements_omnipresent.models import (
    OmnipresentMeasurementEvent,
    OmnipresentMeasurement
)
from ..measurements_omnipresent.serializers import (
    OmnipresentMeasurementEventWideSerializer,
    OmnipresentMeasurementWideSerializer
)
from ..measurements_stationary.models import (
    StationaryMeasurementEvent,
    StationaryMeasurement
)
from ..measurements_stationary.serializers import (
    StationMeasurementEventWideSerializer,
    StationMeasurementWideSerializer
)
from rest_framework_csv.renderers import CSVRenderer
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.settings import api_settings
from .serializers import TrainingDataSerializer


class MeasurementEventViewSet(viewsets.ViewSet):
    """Endpoint that combines results from all measurement event subclasses."""
    
    def list(self, request):
        """
        Get results from station, mobile, and omnipresent measurement events.
        
        References:
        - https://stackoverflow.com/questions/32454394/how-to-combine-two-similar-views-into-a-single-response
        """
        station_queryset = StationaryMeasurementEvent.objects.all()
        station_serializer = StationMeasurementEventWideSerializer(station_queryset, many=True)

        mobile_queryset = MobileMeasurementEvent.objects.all()
        mobile_serializer = MobileMeasurementEventWideSerializer(mobile_queryset, many=True)

        omnipresent_queryset = OmnipresentMeasurementEvent.objects.all()
        omnipresent_serializer = OmnipresentMeasurementEventWideSerializer(omnipresent_queryset, many=True)

        data = (
            station_serializer.data +
            mobile_serializer.data +
            omnipresent_serializer.data
        )
        return Response(data, status=status.HTTP_200_OK)


class MeasurementViewSet(viewsets.ViewSet):
    """
    Endpoint that combines results from all measurement subclasses
    """
    renderer_classes = tuple(api_settings.DEFAULT_RENDERER_CLASSES) + (CSVRenderer,)

    def list(self, request):
        """
        Get results from station, mobile, and omnipresent measurements.
        
        References:
        - https://stackoverflow.com/questions/32454394/how-to-combine-two-similar-views-into-a-single-response
        """
        station_queryset = StationaryMeasurement.objects.all()
        station_serializer = StationMeasurementWideSerializer(station_queryset, many=True)

        mobile_queryset = MobileMeasurement.objects.all()
        mobile_serializer = MobileMeasurementWideSerializer(mobile_queryset, many=True)

        omnipresent_queryset = OmnipresentMeasurement.objects.all()
        omnipresent_serializer = OmnipresentMeasurementWideSerializer(omnipresent_queryset, many=True)

        data = (
            station_serializer.data +
            mobile_serializer.data +
            omnipresent_serializer.data
        )
        return Response(data)

    def get_paginated_response(self, data):
        """
        Customizes default pagination to send the
        entire response body payload at once.
        """
        return Response(data)


class TrainingDataViewSet(viewsets.ViewSet):
    """Endpoint that combines results from all measurement subclasses."""
    
    renderer_classes = tuple(api_settings.DEFAULT_RENDERER_CLASSES) + (CSVRenderer,)

    def list(self, request):
        """Creates a dataset for model training."""
        mobile_queryset = MobileMeasurementEvent.objects.all()
        mobile_serializer = TrainingDataSerializer(mobile_queryset, many=True)
        return Response(mobile_serializer.data, status=status.HTTP_200_OK)

    def get_paginated_response(self, data):
        """
        Customizes default pagination to send the
        entire response body payload at once.
        """
        return Response(data, status=status.HTTP_200_OK)