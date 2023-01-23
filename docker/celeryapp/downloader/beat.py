# Create app celery to start _downloader
from celery import Celery
from celery.signals import worker_ready
from celeryapp import delay_controller as delay

app = Celery('beat_downloader')

app.config_from_object('celeryapp.downloader.config')

# Beat tasks schedules
app.conf.beat_schedule = {
    'download-brasil-copernicus-netcdf': {
        'task': 'extract_br_netcdf_monthly',
        'schedule': delay.get_task_schedule('extract_br_netcdf_monthly'),
    },
    'scan-for-new-dates-to-fetch': {
        'task': 'scan_for_missing_dates',
        'schedule': delay.get_task_schedule('scan_for_missing_dates'),
    },
}

# Send signal to run at worker startup
@worker_ready.connect
def at_start(sender, **kwargs):
    """Run tasks at startup"""
    with sender.app.connection() as conn:
        sender.app.send_task(
            'initialize_satellite_download_db', connection=conn
        )
        sender.app.send_task(
            'create_copernicus_data_tables', connection=conn
        )
