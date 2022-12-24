"""
omnipresent.py
"""

from ..measurements_base.views import (
    BaseMeasurementEventNeighborViewSet,
    BaseMeasurementEventViewSet,
    BaseMeasurementViewSet,
)
from ..sources.models import Source
from ..measurements_mobile.models import MobileMeasurementEvent
from .models import (
    OmnipresentMeasurement,
    OmnipresentMeasurementEvent,
    OmnipresentMeasurementEventNeighbor
)
from .serializers import (
    OmnipresentMeasurementSerializer,
    OmnipresentMeasurementEventSerializer,
    OmnipresentMeasurementEventNeighborSerializer
)


class OmnipresentMeasurementEventViewSet(
    BaseMeasurementEventViewSet):
    """A viewset for omnipresent measurement events."""
    foreign_model_lookup = {'source': Source}
    queryset = OmnipresentMeasurementEvent.objects.all()
    serializer_class = OmnipresentMeasurementEventSerializer


class OmnipresentMeasurementEventNeighborsViewSet(
    BaseMeasurementEventNeighborViewSet):
    """A viewset for omnipresent measurement event neighbors."""
    foreign_model_lookup = {
        'mobile_event': MobileMeasurementEvent,
        'neighboring_omnipresent_event': OmnipresentMeasurementEvent
    }
    queryset = OmnipresentMeasurementEventNeighbor.objects.all()
    serializer_class = OmnipresentMeasurementEventNeighborSerializer
   

class OmnipresentMeasurementViewSet(BaseMeasurementViewSet):
    """A viewset for omnipresent measurements."""
    foreign_model_lookup = {
        'omnipresent_measurement_event': OmnipresentMeasurementEvent
    }
    queryset = OmnipresentMeasurement.objects.all()
    serializer_class = OmnipresentMeasurementSerializer
