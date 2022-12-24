"""
serializers.py
"""

from ..measurements_mobile.models import (
    MobileMeasurementEvent,
    MobileSensor
)
from ..measurements_mobile.serializers import (
    HistoricalSeriesListSerializer,
    MobileMeasurementSerializer
)
from rest_framework import serializers


class MobileSensorHistoricalSeriesSerializer(serializers.ModelSerializer):
    """
    A custom serializer for displaying mobile sensor measurements over time.
    """
    mobilemeasurement_set = MobileMeasurementSerializer(many=True)
    fishery = serializers.CharField(source="get_fishery_name", required=False)
    fishing_technology = serializers.CharField(source="get_fishing_technology", required=False)

    class Meta:
        model = MobileMeasurementEvent
        list_serializer_class = HistoricalSeriesListSerializer
        fields = '__all__'


class MobileSensorReportSerializer(serializers.ModelSerializer):
    """
    A custom serializer for generating reports for mobile sensors.
    """
    historicalSeries = MobileSensorHistoricalSeriesSerializer(many=True, source="mobile_measurement_event")
    class Meta:
        model = MobileSensor
        fields = '__all__'
