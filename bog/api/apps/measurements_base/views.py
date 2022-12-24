"""
views.py
"""

from ..common.viewmixins import (
    BulkCreateMixin,
    BulkGetOrCreateMixin
)
from rest_framework.mixins import (
    DestroyModelMixin,
    ListModelMixin,
    RetrieveModelMixin,
    UpdateModelMixin
)
from rest_framework.viewsets import GenericViewSet


class NonCreateActionsViewSet(
    ListModelMixin,
    RetrieveModelMixin,
    UpdateModelMixin,
    DestroyModelMixin,
    GenericViewSet):
    """
    A base viewset comprised of default
    non-create actions: `list`, `retrieve`, 
    `update` (including partial updates),
    and `destroy`.
    """
    pass


class BaseMeasurementEventViewSet(
    BulkGetOrCreateMixin,
    NonCreateActionsViewSet):
    """
    A viewset for measurement events.
    Implements a custom bulk get-or-create
    action while the remaining actions (i.e.,
    list, retrieve, update, and destroy) are
    included out-of-the-box.
    """
    create_defaults = []


class BaseMeasurementEventNeighborViewSet(
    BulkGetOrCreateMixin,
    NonCreateActionsViewSet):
    """
    A viewset for measurement events.
    Implements a custom bulk get-or-create
    action while the remaining actions (i.e.,
    list, retrieve, update, and destroy) are
    included out-of-the-box.
    """
    create_defaults = ['distance']


class BaseMeasurementViewSet(
    BulkGetOrCreateMixin,
    NonCreateActionsViewSet):
    """
    A viewset for measurements.
    Implements a custom bulk get-or-create
    action while the remaining actions (i.e.,
    list, retrieve, update, and destroy) are
    included out-of-the-box.
    """
    create_defaults = []


class SensorWithBulkCreateViewset(
    BulkCreateMixin,
    NonCreateActionsViewSet):
    """
    A viewset for measurement sensors.
    Implements a custom bulk create
    action while the remaining actions (i.e.,
    list, retrieve, update, and destroy) are
    included out-of-the-box.
    """
    create_defaults = []


class SensorWithBulkGetOrCreateViewset(
    BulkGetOrCreateMixin,
    NonCreateActionsViewSet):
    """
    A viewset for measurement sensors.
    Implements a custom bulk get-or-create
    action while the remaining actions (i.e.,
    list, retrieve, update, and destroy) are
    included out-of-the-box.
    """
    create_defaults = []

