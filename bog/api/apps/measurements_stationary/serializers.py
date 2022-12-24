"""
serializers.py
"""

from .models import (
    Station,
    StationaryMeasurementEvent,
    StationaryMeasurementEventNeighbor,
    StationaryMeasurement
)
from rest_framework import serializers
from ..sources.models import Source
from typing import List

 
class StationSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `Station` model.
    Overrides the default `create` method to either
    create or update a station record based on whether
    a matching record already exists in the database.
    """
    source = serializers.PrimaryKeyRelatedField(queryset=Source.objects.all())

    class Meta:
        model = Station
        fields = [
            'id',
            'name',
            'state',
            'established',
            'timezone',
            'source',
            'latitude',
            'longitude',
            'depth'
        ]

    def unique_fields(self) -> List[str]:
        """The model fields serving as unique keys."""
        return ['id']
    
    def creation_fields(self) -> List[str]:
        """The model fields to use for upserts."""
        return [
            'name',
            'state',
            'established',
            'timezone',
            'source',
            'latitude',
            'longitude',
            'depth'
        ]


class StationaryMeasurementEventSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `StationaryMeasurementEvent` model.
    """
    station = serializers.PrimaryKeyRelatedField(queryset=Station.objects.all())

    class Meta:
        model = StationaryMeasurementEvent
        fields  = [
            'id',
            'datetime',
            'station'
        ]


class StationMeasurementEventWideSerializer(serializers.ModelSerializer):
    """
    A wide serializer. Standardizes the columns of
    `StationaryMeasurementEvent` to be sensor `id`,
    `source`, `lat`, `long`, and `datetime`.
    """
    sensor_id = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()

    class Meta:
        model = StationaryMeasurementEvent
        exclude = ['station'] 

    def get_latitude(self, obj):
        return obj.station.latitude 

    def get_longitude(self, obj):
        return obj.station.longitude

    def get_sensor_id(self, obj):
        return obj.station.id

    def get_source(self, obj):
        return obj.station.source.name


class StationaryMeasurementEventNeighborSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `StationaryMeasurementEventNeighbor` model.
    Returns all model fields.
    """
    queryset = StationaryMeasurementEvent.objects.all()
    mobile_event = serializers.PrimaryKeyRelatedField(queryset=queryset)
    neighboring_stationary_event = serializers.PrimaryKeyRelatedField(queryset=queryset)

    class Meta:
        model = StationaryMeasurementEventNeighbor
        fields  = [
            'id',
            'mobile_event',
            'neighboring_stationary_event',
            'distance'
        ]


class StationaryMeasurementSerializer(serializers.ModelSerializer):
    """
    A custom serializer for one or more `StationaryMeasurement` objects.
    Returns all model fields.
    """
    stationary_measurement_event = serializers.PrimaryKeyRelatedField(queryset=StationaryMeasurementEvent.objects.all())

    class Meta:
        model = StationaryMeasurement
        fields = '__all__'


class StationMeasurementWideSerializer(serializers.ModelSerializer):
    """
    A wide serializer for one or more `StationaryMeasurement` objects.
    """
    measurement_event_id = serializers.SerializerMethodField()

    class Meta:
        model = StationaryMeasurement
        fields = '__all__'

    def get_measurement_event_id(self, obj):
        return 'se-' + str(obj.stationary_measurement_event.id)


class StationMeasurementEventWithMeasurementsSerializer(serializers.ModelSerializer):
    """
    Serializes one or more `StationaryMeasurementEvent`s
    with their corresponding measurements.
    """
    stationarymeasurement_set = StationaryMeasurementSerializer(many=True)

    class Meta:
        model = StationaryMeasurementEvent
        fields = '__all__'

