"""
tests.py
"""

from ..common.testmixins import BulkUpsertTestMixin
from django.urls import reverse
from .factories import SourceFactory
from .models import Source
from rest_framework.test import APITestCase
from .serializers import SourceSerializer


class SourceTestCase(
    BulkUpsertTestMixin,
    APITestCase):
    """Tests for mobile sensors."""
    model = Source
    serializer = SourceSerializer
    factory = SourceFactory
    url = reverse('sources-list')
