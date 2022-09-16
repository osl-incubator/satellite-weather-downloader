__version__ = '0.1.0'

from .celeryapp import app as celery_app

__all__ = ("celery_app",)
