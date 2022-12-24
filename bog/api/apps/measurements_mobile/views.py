"""
views.py
"""

from ..measurements_base.views import (
    BaseMeasurementEventViewSet,
    BaseMeasurementEventNeighborViewSet,
    BaseMeasurementViewSet,
    NonCreateActionsViewSet
)
from ..common.viewmixins import BulkCreateMixin
from django.db import transaction
from django_filters.rest_framework import (
    DjangoFilterBackend
)
from .models import (
    MobileSensor,
    MobileMeasurementEvent,
    MobileMeasurementEventNeighbor,
    MobileMeasurement,
    MobileSensorFisheryAssignment
)
from .models import Source
from rest_framework import viewsets
from rest_framework.response import Response
from .serializers import (
    MobileMeasurementEventNeighborSerializer,
    MobileMeasurementEventSerializer,
    MobileMeasurementSerializer,
    MobileSensorFisheryAssignmentSerializer,
    MobileSensorSerializer
)

class MobileSensorViewSet(
    BulkCreateMixin,
    NonCreateActionsViewSet):
    """A viewset for mobile sensors."""
    foreign_model_lookup = {'source': Source}
    queryset = MobileSensor.objects.all()
    serializer_class = MobileSensorSerializer
    filter_backends = DjangoFilterBackend


class MobileMeasurementEventViewSet(BaseMeasurementEventViewSet):
    """A viewset for mobile measurement events."""
    foreign_model_lookup = {'mobile_sensor': MobileSensor}
    queryset = MobileMeasurementEvent.objects.all()
    serializer_class = MobileMeasurementEventSerializer


class MobileMeasurementEventNeighborViewSet(
    BaseMeasurementEventNeighborViewSet):
    """A viewset for mobile measurement event neighbors."""
    foreign_model_lookup = {
        'mobile_event': MobileMeasurementEvent,
        'neighboring_mobile_event': MobileMeasurementEvent
    }
    queryset = MobileMeasurementEventNeighbor.objects.all()
    serializer_class = MobileMeasurementEventNeighborSerializer
    

class MobileMeasurementViewSet(BaseMeasurementViewSet):
    """A viewset for mobile measurements.""" 
    foreign_model_lookup = {'mobile_measurement_event': MobileMeasurementEvent}
    queryset = MobileMeasurement.objects.all()
    serializer_class = MobileMeasurementSerializer


class MobileSensorFisheryAssignmentViewSet(viewsets.ModelViewSet):
    """
    Endpoint for retrieving and upserting fishery assignments.
    """
    queryset = MobileSensorFisheryAssignment.objects.all()
    serializer_class = MobileSensorFisheryAssignmentSerializer
    pagination_class = None

    def create(self, request):
        try:
            with transaction.atomic():
                db_objs = []
                buoy_cache = {}
                request_objs = request.data if isinstance(request.data, list) else [request.data]

                for obj in request_objs:
                    # Retrieve reference to mobile sensor (buoy) id
                    buoy_id = obj.pop('buoy')
                    if buoy_id in buoy_cache:
                        buoy = buoy_cache[buoy_id]
                    else:
                        buoy = MobileSensor.objects.get(pk=buoy_id)
                        buoy_cache[buoy_id] = buoy
                    obj['buoy'] = buoy

                    # Perform upsert operation
                    assignment, _ = (MobileSensorFisheryAssignment
                        .objects
                        .update_or_create(
                            id=obj['id'],
                            defaults=obj))
                    db_objs.append(assignment)

        except Exception as e:
            return Response(str(e), status=500)

        serializer = self.serializer_class(db_objs, many=True)
        return Response(serializer.data, status=201)

