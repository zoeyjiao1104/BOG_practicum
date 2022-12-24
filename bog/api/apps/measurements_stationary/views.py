"""
views.py
"""

from ..measurements_base.views import (
    BaseMeasurementEventViewSet, 
    BaseMeasurementEventNeighborViewSet,
    BaseMeasurementViewSet,
    NonCreateActionsViewSet
)
from ..common.viewmixins import BulkGetOrCreateMixin
from ..measurements_mobile.models import MobileMeasurementEvent
from ..sources.models import Source
from .models import (
    Station,
    StationaryMeasurementEvent,
    StationaryMeasurementEventNeighbor,
    StationaryMeasurement
)
from .serializers import (
    StationSerializer,
    StationaryMeasurementEventSerializer,
    StationaryMeasurementEventNeighborSerializer,
    StationaryMeasurementSerializer
)


class StationViewSet(
    BulkGetOrCreateMixin,
    NonCreateActionsViewSet):
    """A viewset for stations like NOAA or DFO."""
    create_defaults = []
    foreign_model_lookup = {'source': Source}
    queryset = Station.objects.all()
    serializer_class = StationSerializer
    pagination_class = None


class StationaryMeasurementEventViewSet(BaseMeasurementEventViewSet):
    """A viewset for stationary measurement events."""
    foreign_model_lookup = {'station': Station}
    queryset = StationaryMeasurementEvent.objects.all()
    serializer_class = StationaryMeasurementEventSerializer
    
    
class StationaryMeasurementEventNeighborViewSet(
    BaseMeasurementEventNeighborViewSet):
    """A viewset for stationary measurement event neighbors."""
    foreign_model_lookup = {
        'mobile_event': MobileMeasurementEvent,
        'neighboring_stationary_event': StationaryMeasurementEvent
    }
    queryset = StationaryMeasurementEventNeighbor.objects.all()
    serializer_class = StationaryMeasurementEventNeighborSerializer
   

class StationaryMeasurementViewSet(BaseMeasurementViewSet):
    """A viewset for stationary measurements."""
    foreign_model_lookup = {
        'stationary_measurement_event': StationaryMeasurementEvent
    }
    queryset = StationaryMeasurement.objects.all()
    serializer_class = StationaryMeasurementSerializer
