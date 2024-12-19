import os
from typing import Any
from rq.job import Job
from src import mailer
from src.utils import increment_total_time_transcribed
from src.services.webhook_service import WebhookService
from src.events.rabbitmq.event_dispatcher import EventDispatcher

allowed_webhooks_file = os.environ.get('ALLOWED_WEBHOOKS_FILE', 'allowed_webhooks.json')
webhook_store = WebhookService(allowed_webhooks_file)

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

dispatcher = EventDispatcher()  # Initialize EventDispatcher

class JobCallbackException(Exception):
    pass

def success(job: Job, connection: Any, result: Any, *args, **kwargs):
    email = job.meta.get("email")
    webhook_id = job.meta.get("webhook_id")
    filename = job.meta.get("uploaded_filename")
    uuid = job.meta.get("uuid") or ""
    libraryId = job.meta.get("library_id") or ""
    if filename is None:
        raise JobCallbackException('Missing filename in job meta')

    url = (os.environ.get("BASE_URL") or '') + "/v1/download/" + job.id

    duration = result['segments'][-1]['end']
    increment_total_time_transcribed(duration, conn=connection)

    if email:
        try:
            mailer.send_success_email(email, filename=filename, url=url)
        except Exception as e:
            print(f"Unable to send email in successful job: {e}")
            if ENVIRONMENT != 'dev':
                # If not dev environment, we raise the exception to ensure visibility
                raise JobCallbackException("Unable to send email in successful job")

    if webhook_id:
        try:
            webhook_store.post_to_webhook(webhook_id, job.id, filename, url, success=True)
        except Exception as e:
            print(f"Unable to post to webhook in successful job: {e}")
            # Do not raise an error to avoid callback failure over a single webhook issue.

    # Publish success event via EventDispatcher
    message = {
        "status": "success",
        "job_id": job.id,
        "filename": filename,
        "url": url,
        "duration": duration,
    }
    if uuid and libraryId:
        message["video"] = {
            "uuid": uuid,
            "libraryId": libraryId
        }
    try:
        dispatcher.dispatch_event("job_success", message)
    except Exception as e:
        # Log and continue without raising
        print(f"Unable to dispatch success event: {e}")

def failure(job: Job, connection: Any, type: Any, value: Any, traceback: Any):
    email = job.meta.get("email")
    webhook_id = job.meta.get("webhook_id")
    filename = job.meta.get("uploaded_filename")
    uuid = job.meta.get("uuid") or ""
    libraryId = job.meta.get("library_id") or ""
    if filename is None:
        raise JobCallbackException('Missing filename in job meta')

    if email:
        try:
            mailer.send_failure_email(email)
        except Exception as e:
            print(f"Unable to send email in failed job: {e}")
            if ENVIRONMENT != 'dev':
                raise JobCallbackException("Unable to send email in failed job")

    if webhook_id:
        try:
            webhook_store.post_to_webhook(webhook_id, job.id, filename, None, success=False)
        except Exception as e:
            print(f"Unable to post to webhook in failed job: {e}")
            # Again, not raising to avoid a callback failure.

    # Publish failure event via EventDispatcher
    message = {
        "status": "failure",
        "job_id": job.id,
        "filename": filename or "Unknown",
        "error_type": str(type),
        "error_value": str(value)
    }
    if uuid and libraryId:
        message["video"] = {
            "uuid": uuid,
            "libraryId": libraryId
        }
    try:
        dispatcher.dispatch_event("job_failure", message)
    except Exception as e:
        # Log and continue without raising
        print(f"Unable to dispatch failure event: {e}")
