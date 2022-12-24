"""
serializers.py
"""

from .models import Job
from rest_framework import serializers


class JobSerializer(serializers.ModelSerializer):
    """
    A custom serializer for the `Job` model.
    """

    class Meta:
        model = Job
        fields = [
            'id',
            'name',
            'status',
            'query_date_start_utc',
            'query_date_end_utc',
            'started_at_utc',
            'completed_at_utc',
            'last_error_at_utc',
            'error_message',
            'retry_count'
        ]