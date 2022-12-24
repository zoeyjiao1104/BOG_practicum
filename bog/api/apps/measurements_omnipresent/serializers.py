"""
serializers.py
"""

from .models import (
    OmnipresentMeasurement,
    OmnipresentMeasurementEvent,
    OmnipresentMeasurementEventNeighbor
)
from ..sources.models import Source
from rest_framework import serializers


class OmnipresentMeasurementEventSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurementEvent` model.
    Returns all model fields.
    """
    source = serializers.PrimaryKeyRelatedField(queryset=Source.objects.all())

    class Meta:
        model = OmnipresentMeasurementEvent
        fields = '__all__'


class OmnipresentMeasurementEventWideSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurementEvent` model.
    Returns all model fields.
    """
    sensor_id = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()

    class Meta:
        model = OmnipresentMeasurementEvent
        fields = '__all__'

    def get_sensor_id(self, obj):
        return obj.source.id

    def get_source(self, obj):
        return obj.source.name


class OmnipresentMeasurementEventNeighborSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurementEventNeighbors`
    model. Returns all model fields.
    """
    queryset = OmnipresentMeasurementEvent.objects.all()
    mobile_event = serializers.PrimaryKeyRelatedField(queryset=queryset)
    neighboring_omnipresent_event = serializers.PrimaryKeyRelatedField(queryset=queryset)

    class Meta:
        model = OmnipresentMeasurementEventNeighbor
        fields  = [
            'id',
            'mobile_event',
            'neighboring_omnipresent_event',
            'distance'
        ]


class OmnipresentMeasurementSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurement` model.
    Returns all model fields.
    """
    omnipresent_measurement_event = serializers.PrimaryKeyRelatedField(queryset=OmnipresentMeasurementEvent.objects.all())

    class Meta:
        model = OmnipresentMeasurement
        fields = '__all__'


class OmnipresentMeasurementWideSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurement` model.
    Returns all model fields.
    """
    measurement_event_id = serializers.SerializerMethodField()

    class Meta:
        model = OmnipresentMeasurement
        fields = '__all__'

    def get_measurement_event_id(self, obj):
        return 'oe-' + str(obj.omnipresent_measurement_event.id)


class OmnipresentEventWithMeasurementSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `OmnipresentMeasurementEvent`
    model. Returns all model fields in addition to the event's
    collected measurements.
    """
    omnipresentmeasurement_set = OmnipresentMeasurementSerializer(many=True)

    class Meta:
        model = OmnipresentMeasurementEvent
        fields = '__all__'

