"""
serializers.py
"""

from .models import Source
from rest_framework import serializers


class SourceSerializer(serializers.ModelSerializer):
    """A custom serializer for the `Sources` model."""
    class Meta:
        model = Source
        fields = ['id', 'name', 'website']

