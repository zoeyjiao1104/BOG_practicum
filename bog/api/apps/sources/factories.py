"""
factories.py
"""

import factory
from .models import Source


class SourceFactory(factory.django.DjangoModelFactory):
    """
    A `DjangoModelFactory` for producing fake `Source` instances.
    """

    class Meta:
        model = Source

    name = factory.Sequence(lambda n: f'Source {n}')
    website = factory.Faker('domain_name')