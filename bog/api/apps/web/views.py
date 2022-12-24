"""
views.py
"""

from datetime import datetime
from ..measurements_mobile.models import (
    MobileSensor,
    MobileMeasurementEvent,
    MobileSensorFisheryAssignment
)
from ..measurements_mobile.serializers import (
    MobileMeasurementEventSerializer
)
from ..measurements_stationary.models import (
    Station
)
from ..measurements_stationary.serializers import (
    StationSerializer
)
from rest_framework.decorators import action
from rest_framework import viewsets
from rest_framework.response import Response
from .serializers import MobileSensorReportSerializer
from ..sources.models import Source


class MobileSensorReportViewSet(viewsets.ModelViewSet):
    """A ViewSet for viewing a report for a mobile sensor."""
    queryset = MobileSensor.objects.all()
    serializer_class = MobileSensorReportSerializer
    pagination_class = None


class LandingPageViewSet(viewsets.ModelViewSet):
    """A ViewSet for populating a landing page."""
    serializer_class = MobileMeasurementEventSerializer
    pagination_class = None

    @action(detail=False, methods=['get'], url_path="filters", url_name="filters")
    def get_filter_options(self, request):
        """Returns options for the landing page filter menu."""
        bog_source = (Source
            .objects
            .filter(name__icontains='Blue Ocean Gear')
            .first())
        buoy_ids = (MobileSensor
            .objects
            .filter(source__pk=bog_source.id)
            .order_by('id')
            .values_list('id', flat=True))
        fisheries = (MobileSensorFisheryAssignment
            .objects
            .all()
            .order_by('fishery')
            .distinct('fishery')
            .values_list('fishery', flat=True))

        return Response({
            "buoyIds": buoy_ids,
            "fisheries": fisheries
        }, status=200)
    

    @action(detail=False, methods=['post'], url_path='buoysearch', url_name='buoysearch')
    def search_buoy_measurement_events(self, request):
        """Generates a search query for buoy measurement events."""
        # Parse parameters from POST request body
        min_time = request.data.get('min_time', None)
        max_time = request.data.get('max_time', None)
        min_lat = request.data.get('min_lat', None)
        max_lat = request.data.get('max_lat', None)
        min_lon = request.data.get('min_lon', None)
        max_lon = request.data.get('max_lon', None)
        buoy_ids = request.data.get('buoy_ids', None)
        fisheries = request.data.get('fisheries', None)

        # Initialize buoy measurement events
        events = self.get_queryset()

        # Filter events by latitude and longitude
        if min_lat:
            events = events.filter(latitude__gte=min_lat).distinct()
        if max_lat:
            events = events.filter(latitude__lte=max_lat).distinct()
        if min_lon:
            events = events.filter(longitude__gte=min_lon).distinct()
        if max_lon:
            events = events.filter(longitude__lte=max_lon).distinct()

        # Filter events by date
        def parse_date(date_str: str):
            """
            Converts a date string received in one of two
            expected formats into a `datetime` instance.
            """
            try:
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return datetime.strptime(date_str, "%Y-%m-%d")

        if min_time:
            min_time_date = parse_date(min_time)
            events = events.filter(datetime__gte=min_time_date).distinct()

        if max_time:
            max_time_date = parse_date(max_time)
            events = events.filter(datetime__lte=max_time_date).distinct()

        # Filter events by buoy id(s)
        if buoy_ids and isinstance(buoy_ids, list):
            events = events.filter(mobile_sensor__in=buoy_ids)

        # Filter events by fishery
        if fisheries:
            events = events.filter(mobile_sensor__buoyfisheryassignment__fishery__in=fisheries)

        serializer_instance = self.serializer_class(events, many=True)
        return Response(serializer_instance.data, status=200)


    @action(detail=False, methods=['get'], url_path='stations', url_name='stations')
    def get_stations(self, request):
        """Returns all stations."""
        queryset = Station.objects.all()
        serializer = StationSerializer(queryset, many=True)
        return Response(serializer.data, status=200)


    def get_queryset(self):
        """
        """
        # Initialize QuerySet to BOG buoys only
        bog_source = (Source
            .objects
            .filter(name__icontains='Blue Ocean Gear')
            .first())
        buoy_ids = (MobileSensor
            .objects
            .filter(source__pk=bog_source.id)
            .values_list('id', flat=True))
        events = (MobileMeasurementEvent
            .objects
            .filter(mobile_sensor__in=buoy_ids))

        # Parse request body
        query_dict = self.request.GET.dict()

        # Filter records by time
        for time_filter in ["min_time", "max_time"]:  
            if time_filter in query_dict:
                try:
                    time = datetime.strptime(query_dict[time_filter], "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    time = datetime.strptime(query_dict[time_filter], "%Y-%m-%d")
                if time_filter == "max_time":
                    events = events.filter(datetime__lte=time).distinct()

        # Filter records by latitude and longitude
        if "min_lat" in query_dict:
            events = events.filter(latitude__gte=query_dict["min_lat"]).distinct()
        if "max_lat" in query_dict:
            events = events.filter(latitude__lte=query_dict["max_lat"]).distinct()
        if "min_long" in query_dict:
            events = events.filter(longitude__gte=query_dict["min_long"]).distinct()
        if "max_long" in query_dict:
            events = events.filter(longitude__lte=query_dict["max_long"]).distinct()

        return events
