"""
views.py
"""

from django.db.models import Max
from django.http import JsonResponse
from .models import Job
from rest_framework import viewsets
from rest_framework.decorators import action
from .serializers import JobSerializer


class JobViewSet(viewsets.ModelViewSet):
    """
    A ViewSet for data processing jobs. Default
    operations include listing, retrieving,
    creating, updating, partially updating,
    and deleting objects.
    """
    queryset = Job.objects.all()
    serializer_class = JobSerializer

    @action(
        detail=False,
        methods=['get'],
        url_path='latest',
        url_name='latest'
    )
    def get_latest(self, request):
        """
        Retrieves the latest data query timestamp
        for each type of job that successfully
        completed, if such jobs exist.
        """
        latest_successful_jobs = (Job
            .objects
            .filter(status=Job.Status.COMPLETED)
            .values('name')
            .annotate(latest_execution=Max('query_date_end_utc')))

        payload = {
            j['name']: j['latest_execution']
            for j in latest_successful_jobs
        }

        return JsonResponse(
            data=payload,
            status=200,
            safe=False)
