import os
from .base import BaseConfig
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Local(BaseConfig):
    DEBUG = True
    ALLOWED_HOSTS = ["*"]

    # Testing
    INSTALLED_APPS = BaseConfig.INSTALLED_APPS
    INSTALLED_APPS += ('django_nose',)
    TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'
    NOSE_ARGS = [
        BASE_DIR,
        '-s',
        '--nocapture',
        '--nologcapture',
    ]

    # Mail
    # EMAIL_HOST = 'localhost'
    # EMAIL_PORT = 1025
    # EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

    # Cross-origin requests
    # https://github.com/adamchainz/django-cors-headers

    CORS_ALLOWED_ORIGINS = [
        "http://localhost:3000",
    ]
