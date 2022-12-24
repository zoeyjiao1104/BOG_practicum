"""
urls.py
"""

from django.conf import settings
from django.urls import path, re_path, include, reverse_lazy
from django.conf.urls.static import static
from django.contrib import admin
from django.views.generic.base import RedirectView
from rest_framework.routers import DefaultRouter
from apps.jobs.views import JobViewSet
from apps.measurements_mobile.views import (
    MobileMeasurementEventNeighborViewSet,
    MobileMeasurementEventViewSet,
    MobileMeasurementViewSet,
    MobileSensorViewSet,
    MobileSensorFisheryAssignmentViewSet
)
from apps.measurements_omnipresent.views import (
    OmnipresentMeasurementEventNeighborsViewSet,
    OmnipresentMeasurementEventViewSet,
    OmnipresentMeasurementViewSet
)
from apps.measurements_stationary.views import (
    StationaryMeasurementEventNeighborViewSet,
    StationaryMeasurementEventViewSet,
    StationaryMeasurementViewSet,
    StationViewSet
)
from apps.ml.views import (
    MeasurementEventViewSet,
    MeasurementViewSet,
    TrainingDataViewSet,
)
from apps.sources.views import SourceViewSet
from apps.web.views import (
    MobileSensorReportViewSet,
    LandingPageViewSet
)

#######################
# ROUTER SETUP
#######################

router = DefaultRouter()

# PIPELINE JOBS
router.register(r'jobs', JobViewSet)

# DATASET SOURCES
router.register(r'sources', SourceViewSet)

# MOBILE MEASUREMENTS
router.register(
    prefix=r'mobilesensors',
    viewset=MobileSensorViewSet,
    basename='mobilesensors')
router.register(
    prefix=r'mobilemeasurementevents',
    viewset=MobileMeasurementEventViewSet,
    basename='mobilemeasurementevents')
router.register(
    prefix=r'mobilemeasurementeventneighbors',
    viewset=MobileMeasurementEventNeighborViewSet,
    basename='mobilemeasurementeventneighbors')
router.register(
    prefix=r'mobilemeasurements',
    viewset=MobileMeasurementViewSet,
    basename='mobilemeasurements')
router.register(
    prefix=r'fisheryassignments',
    viewset=MobileSensorFisheryAssignmentViewSet,
    basename='fisheryassignments')

# STATIONARY MEASUREMENTS
router.register(
    prefix=r'stations',
    viewset=StationViewSet,
    basename='stations')
router.register(
    prefix=r'stationarymeasurementevents',
    viewset=StationaryMeasurementEventViewSet,
    basename='stationarymeasurementevents')
router.register(
    prefix=r'stationarymeasurementeventneighbors',
    viewset=StationaryMeasurementEventNeighborViewSet,
    basename='stationarymeasurementeventneighbors')
router.register(
    prefix=r'stationarymeasurements',
    viewset=StationaryMeasurementViewSet,
    basename='stationarymeasurements')

# OMNIPRESENT MEASUREMENTS
router.register(
    prefix=r'omnipresentmeasurementevents',
    viewset=OmnipresentMeasurementEventViewSet,
    basename='omnipresentmeasurementevents')
router.register(
    prefix=r'omnipresentmeasurementeventneighbors',
    viewset=OmnipresentMeasurementEventNeighborsViewSet,
    basename='omnipresentmeasurementeventneighbors')
router.register(
    prefix=r'omnipresentmeasurements',
    viewset=OmnipresentMeasurementViewSet,
    basename='omnipresentmeasurements')

# MACHINE LEARNING
router.register(
    prefix=r'measurementevents',
    viewset=MeasurementEventViewSet,
    basename='measurementevents')
router.register(
    prefix=r'measurements',
    viewset=MeasurementViewSet,
    basename='measurements')
router.register(
    prefix=r'trainingdata',
    viewset=TrainingDataViewSet,
    basename='trainingdata')

# WEB
router.register(
    prefix=r'landingpage',
    viewset=LandingPageViewSet,
    basename='landingpage')
router.register(
    prefix=r'buoyreports',
    viewset=MobileSensorReportViewSet,
    basename='buoyreports')

#######################
# URL PATTERN SETUP
#######################

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/v1/', include(router.urls)),
    # path('api-token-auth/', views.obtain_auth_token),
    # path('api-auth/', include('rest_framework.urls', namespace='rest_framework')),

    # the 'api-root' from django rest-frameworks default router
    # http://www.django-rest-framework.org/api-guide/routers/#defaultrouter
    re_path(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'), permanent=False)),

] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
