"""
viewmixins.py
"""

from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist
)
from django.db import IntegrityError, transaction
from rest_framework import status
from rest_framework.response import Response
from typing import Dict


class ForeignRefReplacementMixin():
    """
    A mixin for handling replacement of foreign key
    references. Implementing classes must provide the
    field `foreign_model_lookup`.

    References:
    - https://www.django-rest-framework.org/api-guide/generic-views/#creating-custom-mixins
    """
    
    def update_foreign_refs(
        self,
        record: Dict,
        cache_lookup: Dict):
        """
        For a given record, identifies all foreign key
        fields specified by `foreign_model_lookup`
        and then substitutes their values with the 
        corresponding foreign model instances.
        """
        for fkey in self.foreign_model_lookup.keys():
            # Parse foreign key from request record
            try:
                id = record.pop(fkey)
            except KeyError:
                raise Exception(f"Key '{fkey}' not found in request data record.")

            # Attempt to retrieve cached model instance
            cache = cache_lookup.get(fkey, {})
            instance = cache.get(id, None)

            # If not found, fetch instance from DB
            if instance is None:
                model = self.foreign_model_lookup[fkey]
                try:
                    instance = model.objects.get(pk=id)
                except ObjectDoesNotExist:
                    raise Exception(f"The object corresponding to request field "
                        f"'{fkey}: {id}' was not found in the database.")

            # Update record and cache
            record[fkey] = instance
            cache[id] = instance
            cache_lookup[fkey] = cache

        return record, cache_lookup
          

class BulkGetOrCreateMixin(ForeignRefReplacementMixin):
    """
    A mixin for performing bulk get-or-create
    operations. Implementing classes must provide the
    fields `create_defaults`, `foreign_model_lookup`,
    and `serializer_class`.

    References:
    - https://docs.djangoproject.com/en/4.1/ref/models/querysets/#get-or-create
    - https://www.django-rest-framework.org/api-guide/generic-views/#creating-custom-mixins
    """

    def create(self, request):
        """
        Creates one or more objects in the database using an
        all-or-nothing transaction, retrieving the objects
        instead if they already exist (i.e., "Get or Create").
        """
        with transaction.atomic():
            cache_lookup = {}
            objs = []
            obj_created = False
            err_msg_prefix = 'Bulk get or create operation failed.'

            for record in request.data:

                # Update record by swapping given foreign keys with model instances
                try:
                    record, cache_lookup = self.update_foreign_refs(record, cache_lookup)
                except Exception as e:
                    return Response(
                        data=f'{err_msg_prefix} {e}',
                        status=status.HTTP_400_BAD_REQUEST)

                # Get or create new entity in DB
                try:
                    defaults = {d:record[d] for d in self.create_defaults}
                    obj, created = (self
                        .queryset
                        .model
                        .objects
                        .get_or_create(**record, defaults=defaults))
                except (IntegrityError, MultipleObjectsReturned) as e:
                    return Response(
                        data=f"{err_msg_prefix} {e}",
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
                # Store entities
                objs.append(obj)
                if created:
                    obj_created = True

            # Serialize entities
            try:
                serializer = self.serializer_class(objs, many=True)
            except Exception as e:
                return Response(
                    data=f"Failed to serialize objects for response payload. {e}",
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            status_code = status.HTTP_201_CREATED if obj_created else status.HTTP_200_OK
            
            return Response(
                data=serializer.data,
                status=status_code)
            

class BulkCreateMixin(ForeignRefReplacementMixin):
    """
    A mixin for performing bulk create operations.
    Implementing classes must provide the
    fields `foreign_model_lookup` and `serializer_class`.

    References:
    - https://docs.djangoproject.com/en/4.1/ref/models/querysets/#bulk-create
    - https://www.django-rest-framework.org/api-guide/generic-views/#creating-custom-mixins
    """

    def create(self, request):
        """
        Bulk inserts one or more objects in the database
        in an all-or-nothing transaction, ignoring records
        where integrity errors (i.e., due to dupes) occurred.
        """
        with transaction.atomic():
            cache_lookup = {}
            records = []
            err_msg_prefix = 'Bulk create operation failed.'

            # Update records by swapping their foreign keys
            # with corresponding model instances
            for record in request.data:
                try:
                    record, cache_lookup = self.update_foreign_refs(record, cache_lookup)
                    records.append(self.queryset.model(**record))
                except Exception as e:
                    return Response(
                        data=f'{err_msg_prefix} {e}',
                        status=status.HTTP_400_BAD_REQUEST)

            # Create new entities in DB
            try:
                objs = (self
                    .queryset
                    .model
                    .objects
                    .bulk_create(
                        objs=records,
                        ignore_conflicts=True,
                        batch_size=1000))
            except IntegrityError as e:
                return Response(
                    data=f"{err_msg_prefix} {e}",
                    status=status.HTTP_400_BAD_REQUEST)

            # Serialize entities
            try:
                serializer = self.serializer_class(objs, many=True)
            except Exception as e:
                return Response(
                    data=f"Failed to serialize objects for response payload. {e}",
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
            return Response(
                data=serializer.data,
                status=status.HTTP_201_CREATED)
        

class BulkUpsertMixin(ForeignRefReplacementMixin):
    """
    A mixin for performing bulk upsert operations.
    Implementing classes must provide the
    fields `foreign_model_lookup`, `serializer_class`,
    `upsert_defaults`, and `upsert_lookup_keys`.

    References:
    - https://docs.djangoproject.com/en/4.1/ref/models/querysets/#update-or-create
    - https://www.django-rest-framework.org/api-guide/generic-views/#creating-custom-mixins
    """

    def create(self, request):
        """
        Upserts one or more objects in the database in an
        all-or-nothing transaction (i.e., "Update or Create").
        """
        with transaction.atomic():
            cache_lookup = {}
            objs = []
            obj_created = False
            err_msg_prefix = 'Bulk update or create operation failed.'
            
            for record in request.data:

                # Update record by swapping given foreign keys with model instances
                try:
                    record, cache_lookup = self.update_foreign_refs(record, cache_lookup)
                except Exception as e:
                    return Response(
                        data=f'{err_msg_prefix} {e}',
                        status=status.HTTP_400_BAD_REQUEST)

                # Update or create new entity in DB
                try:
                    defaults = {d:record[d] for d in self.upsert_defaults}
                    obj, created = (self
                        .queryset
                        .model
                        .objects
                        .update_or_create(self.upsert_lookup_keys, defaults=defaults))
                except (IntegrityError, MultipleObjectsReturned) as e:
                    return Response(
                        data=f"{err_msg_prefix} {e}",
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
                # Store entities
                objs.append(obj)
                if created:
                    obj_created = True

            # Serialize entities
            try:
                serializer = self.serializer_class(objs, many=True)
            except Exception as e:
                return Response(
                    data=f"Failed to serialize objects for response payload. {e}",
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            status_code = status.HTTP_201_CREATED if obj_created else status.HTTP_200_OK
            
            return Response(
                data=serializer.data,
                status=status_code)
            