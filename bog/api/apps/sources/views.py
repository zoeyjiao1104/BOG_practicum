"""
views.py
"""

from ..measurements_base.views import NonCreateActionsViewSet
from ..common.viewmixins import BulkCreateMixin, BulkGetOrCreateMixin
from .models import Source
from .serializers import SourceSerializer


class SourceViewSet(BulkGetOrCreateMixin, NonCreateActionsViewSet):
    """A viewset for measurement data sources."""
    foreign_model_lookup = {}
    queryset = Source.objects.all()
    serializer_class = SourceSerializer
    pagination_class = None
    create_defaults = ['name', 'website']