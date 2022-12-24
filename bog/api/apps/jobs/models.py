"""
models.py
"""

import uuid
from django.db import models


class Job(models.Model):
    """
    Represents a data processing job.
    """

    class Name(models.TextChoices):
        """The jobs available for execution."""
        DATA_RETENTION = 'data-retention'
        LOAD_MEASUREMENTS = 'load-measurements'
        LOAD_OSCAR_DATASETS = 'refresh-oscar-datasets'
        TRAIN_ANOMALY_DETECTION = 'train-anomaly-detection'
        TRAIN_PREDICTIONS = 'train-prediction-models'
        RUN_ANOMALY_DETECTION = 'run-anomaly-detection'
        RUN_PREDICTION_MODELS = 'run-prediction-models'

    class Status(models.TextChoices):
        """Available statuses for jobs."""
        COMPLETED = 'completed'
        ERROR = 'error'
        RUNNING = 'running'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, choices=Name.choices)
    status = models.CharField(max_length=255, choices=Status.choices)
    query_date_start_utc = models.DateTimeField()
    query_date_end_utc = models.DateTimeField()
    started_at_utc = models.DateTimeField(auto_now_add=True)
    completed_at_utc = models.DateTimeField(null=True)
    last_error_at_utc = models.DateTimeField(null=True)
    error_message = models.TextField(null=True)
    retry_count = models.SmallIntegerField(default=0)
