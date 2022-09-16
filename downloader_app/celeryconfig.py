# Parameters are used in the celery app
# Optional configuration, see the application user guide.

import os

from dotenv import load_dotenv

load_dotenv()

## Broker settings.
broker_url = 'amqp://guest:guest@rabbitmq:5672'

# List of modules to import when the Celery worker starts.
imports = ('downloader_app.tasks',)


# broker_url = os.getenv('CELERY_BROKER_URL')
# include = ('downloader_app.tasks',)
# accept_content = ['application/json']
# result_serializer = 'json'
# worker_max_tasks_per_child = 10
# timezone = 'America/Sao_Paulo'
# task_always_eager = False

# print(broker_url)
