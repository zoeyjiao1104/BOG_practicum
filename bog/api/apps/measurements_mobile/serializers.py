"""
serializers.py
"""

from .models import (
    MobileSensor,
    MobileMeasurementEvent,
    MobileMeasurementEventNeighbor,
    MobileMeasurement,
    MobileSensorFisheryAssignment
)
from .models import Source
from rest_framework import serializers


class MobileSensorSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `MobileSensor` model.
    """
    source = serializers.PrimaryKeyRelatedField(queryset=Source.objects.all())

    class Meta:
        model = MobileSensor
        fields = ['id', 'source']


class MobileSensorFisheryAssignmentSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `MobileSensorFisheryAssignment` model.
    """
    class Meta:
        model = MobileSensorFisheryAssignment
        fields = [
            'id',
            'buoy',
            'transmission_type',
            'fishery',
            'fishing_technology',
            'start_date',
            'end_date'
        ]


class MobileMeasurementSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `MobileMeasurement` model.
    """
    
    mobile_measurement_event = serializers.PrimaryKeyRelatedField(queryset=MobileMeasurementEvent.objects.all())

    class Meta:
        model = MobileMeasurement
        fields = [
            'id',
            'product',
            'value',
            'type',
            'quality',
            'mobile_measurement_event'
        ]


class MobileMeasurementEventSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `MobileMeasurementEvent` model.
    """
    class Meta:
        model = MobileMeasurementEvent
        fields  = [
            'id',
            'datetime',
            'latitude',
            'longitude',
            'mobile_sensor',
            'anomaly_score',
            'is_prediction',
            'event_measurements'
        ]

    mobile_sensor = serializers.PrimaryKeyRelatedField(queryset=MobileSensor.objects.all())
    event_measurements = MobileMeasurementSerializer(
        many=True,
        read_only=True,
        source='mobilemeasurement_set'
    )


class MobileMeasurementEventNeighborSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `MobileMeasurementEventNeighbor` model.
    """
    queryset = MobileMeasurementEvent.objects.all()
    mobile_event = serializers.PrimaryKeyRelatedField(queryset=queryset)
    neighboring_mobile_event = serializers.PrimaryKeyRelatedField(queryset=queryset)

    class Meta:
        model = MobileMeasurementEventNeighbor
        fields  = [
            'id',
            'mobile_event',
            'neighboring_mobile_event',
            'distance'
        ]


class MobileMeasurementEventWideSerializer(serializers.ModelSerializer):
    """
    """
    sensor_id = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()

    class Meta:
        model = MobileMeasurementEvent
        exclude = ['mobile_sensor']

    def get_sensor_id(self, obj):
        return obj.mobile_sensor.id

    def get_source(self, obj):
        return obj.mobile_sensor.source.name


class MobileMeasurementWideSerializer(serializers.ModelSerializer):
    """
    """
    measurement_event_id = serializers.SerializerMethodField()

    class Meta:
        model = MobileMeasurement
        fields = '__all__'

    def get_measurement_event_id(self, obj):
        return 'me-' + str(obj.mobile_measurement_event.id)


class HistoricalSeriesListSerializer(serializers.ListSerializer):

    def to_representation(self, data):
        # additional_data = data.
        nested_data = super().to_representation(data)
        returned_data = []
        for measurement_event in nested_data:
            pivoted_measurement_event = {}
            for measurements in measurement_event["mobilemeasurement_set"]:
                product_name = measurements["product"] + "-" + measurements["type"]
                pivoted_measurement_event[product_name] = measurements["value"]
            pivoted_measurement_event["mobile_measurement_event"] = measurement_event["id"]
            pivoted_measurement_event['is_prediction'] = measurement_event['is_prediction']
            for key in measurement_event:
                if key not in ["mobilemeasurement_set", "id"]:
                    pivoted_measurement_event[key] = measurement_event[key]
            returned_data.append(pivoted_measurement_event)
        return returned_data


class HistoricalSeriesSerializer(serializers.ModelSerializer):
    mobilemeasurement_set = MobileMeasurementSerializer(many=True)

    class Meta:
        model = MobileMeasurementEvent
        list_serializer_class = HistoricalSeriesListSerializer
        fields = '__all__'

