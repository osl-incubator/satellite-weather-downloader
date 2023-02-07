# This will make sure the app is always imported when
# Django starts so that shared_task will use this app.
from .beat import app as beat_downloader

__all__ = ('beat_downloader',)
