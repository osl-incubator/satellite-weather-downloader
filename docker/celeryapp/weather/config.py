import os

from dotenv import load_dotenv

load_dotenv()

## Broker settings.
broker_url = os.getenv('CELERY_BROKER_URL')

# List of modules to import when the Celery worker starts.
accept_content = ['application/json']
result_serializer = 'json'
worker_concurrency = 6
worker_max_tasks_per_child = 10
timezone = 'America/Sao_Paulo'
task_always_eager = False
task_create_missing_queues = True
