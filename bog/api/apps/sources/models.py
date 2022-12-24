"""
models.py
"""

from django.db import models


class Source(models.Model):
    """
    Sources of oceanographic data (NOAA, NASA, etc.)
    and associated metadata.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['name'],
                name='unique_source'
            )
        ]

    name = models.CharField(max_length=200)
    website = models.URLField(max_length=2000)