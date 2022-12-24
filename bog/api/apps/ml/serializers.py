"""
serializers.py
"""

from collections import OrderedDict
from ..measurements_mobile.models import (
    MobileMeasurementEvent,
    MobileMeasurementEventNeighbor
)
from ..measurements_mobile.serializers import (
    HistoricalSeriesSerializer,
    MobileMeasurementSerializer
)
from ..measurements_omnipresent.models import (
    OmnipresentMeasurementEventNeighbor
)
from ..measurements_omnipresent.serializers import (
    OmnipresentEventWithMeasurementSerializer
)
from ..measurements_stationary.models import (
    StationaryMeasurementEventNeighbor
)
from ..measurements_stationary.serializers import (
    StationMeasurementEventWithMeasurementsSerializer
)
from rest_framework import serializers


class NeighborListSerializer(serializers.ListSerializer):
    """List neighbor events in distance order with one level of nesting removed"""

    def to_representation(self, data):
        instances = super().to_representation(data)
        instances = sorted(instances, key=lambda dict: dict["distance"])
        for instance in instances:
            neighboring_event_key = [key for key in instance if key.startswith("neighboring")][0]
            measurement_event_representation = instance.pop(neighboring_event_key)
            for key in measurement_event_representation:
                instance[key] = measurement_event_representation[key]
        new_instances = []
        for i, instance in enumerate(instances):
            new_instances.append(
                OrderedDict((f"neighbor_{i+1}_{k}", v) for k, v in instance.items())
            )
        return new_instances


class MobileNeighborTrainingSerializer(serializers.ModelSerializer):
    """Flattens all details in 'neighboring_buoys' key"""
    neighboring_mobile_event = HistoricalSeriesSerializer()
    class Meta:
        model = MobileMeasurementEventNeighbor
        fields = '__all__'
        list_serializer_class = NeighborListSerializer


class StationNeighborTrainingSerializer(serializers.ModelSerializer):
    """Flattens all details in 'neighboring_stations' key"""
    neighboring_stationary_event = StationMeasurementEventWithMeasurementsSerializer()
    class Meta:
        model = StationaryMeasurementEventNeighbor
        fields = '__all__'
        list_serializer_class = NeighborListSerializer


class OmnipresentNeighborTrainingSerializer(serializers.ModelSerializer):
    neighboring_omnipresent_event = OmnipresentEventWithMeasurementSerializer()

    class Meta:
        model = OmnipresentMeasurementEventNeighbor
        fields = '__all__'
        list_serializer_class = NeighborListSerializer


class TrainingDataSerializer(serializers.ModelSerializer):
    """Removes one level of nesting from neighboring buoys list"""

    def to_representation(self, instance):
        instance_dict = super().to_representation(instance)

        # flatten "neighboring_<type>" keys. They originally map to lists of 
        # neighbor measurement events (where each key is prepended with neighbor_i)
        # each neighboring event will now have keys in first level as 
        # <neighbor_type>_neighbor_<number>_<original key name>
        for neighbor_type in ["stationary", "buoys", "omnipresent"]:
            neighbors = instance_dict.pop(f"neighboring_{neighbor_type}")
            for neighbor in neighbors:
                for key in neighbor:
                    instance_dict[f"{neighbor_type}_{key}"] = neighbor[key]
        
        # flatten measurementevents dicts
        measurement_keys = [key for key in instance_dict if "measurement_set" in key]
        for key in measurement_keys:
            if "neighbor" in key:
                prepend = "_".join(key.split("_")[:3]) + "_"
            else:
                prepend = ""
            measurements = instance_dict.pop(key, [])
            for measurement in measurements:
                product = f"{prepend}{measurement['product']}-{measurement['type']}"
                instance_dict[product] = measurement['value']

        instance_dict["mobile_measurement_event"] = instance_dict["id"]

        return instance_dict

    neighboring_buoys = MobileNeighborTrainingSerializer(many=True, source="mobile_event")
    neighboring_stationary = StationNeighborTrainingSerializer(many=True, source="stationarymeasurementeventneighbor_set")
    neighboring_omnipresent = OmnipresentNeighborTrainingSerializer(many=True, source="omnipresentmeasurementeventneighbor_set")
    mobilemeasurement_set = MobileMeasurementSerializer(many=True)
    fishery = serializers.CharField(source="mobile_sensor.get_fishery_name")
    fishing_technology = serializers.CharField(source="mobile_sensor.get_fishing_technology")
    
    class Meta:
        model = MobileMeasurementEvent
        fields = '__all__'

