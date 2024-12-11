import json
import os
import requests
import tempfile
import urllib.parse
import mimetypes
import logging

from src.events.rabbitmq.subscriber import Subscriber
from src.events.rabbitmq.event_dispatcher import EventDispatcher
from src.main import rq_queue, DEFAULT_MODEL, DEFAULT_TASK, webhook_store, callbacks

SERVICE_URL = os.environ.get("SERVICE_URL", "localhost:4000")
DEFAULT_UPLOADED_FILENAME = "untitled-transcription"

# Initialize the event dispatcher
event_dispatcher = EventDispatcher()

def get_extension_from_content_type(content_type: str) -> str:
    """
    Get the file extension based on the Content-Type header.
    """
    return mimetypes.guess_extension(content_type) or ""

def process_video_event(ch, method, properties, body):
    """
    Callback function to process the 'video_uploaded' event from RabbitMQ.
    """
    temp_dir = './upload-shared-tmp'
    os.makedirs(temp_dir, exist_ok=True)  # Ensure the directory exists

    try:
        # Parse the incoming message
        event = json.loads(body)
        if "video" not in event or "extraFields" not in event["video"] or "libraryId" not in event["video"] or "uuid" not in event["video"]:
            print("Invalid event payload.")
            return

        video = event["video"]
        library_id = video["libraryId"]
        uuid = video["uuid"]
        uploaded_filename = video.get("title", DEFAULT_UPLOADED_FILENAME)
        temp_video_path = video["extraFields"].get("tempVideoPath")

        # Download the video file
        download_url = f"http://{SERVICE_URL}/download/video/{library_id}/{uuid}"
        print(f"Downloading video from: {download_url}")

        response = requests.get(download_url, stream=True)
        if response.status_code != 200:
            print(f"Failed to download video. Status code: {response.status_code}")
            return

        # Determine the file extension
        content_type = response.headers.get("Content-Type", "")
        file_extension = get_extension_from_content_type(content_type)

        if not file_extension:
            file_extension = os.path.splitext(temp_video_path)[-1] if temp_video_path else ".bin"

        # Save the video file to a temporary location
        with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False, suffix=file_extension) as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            temp_filename = temp_file.name

        print(f"Video saved to temporary file: {temp_filename}")

        # Set default parameters
        requested_model = DEFAULT_MODEL
        task = DEFAULT_TASK
        language = None
        email = None
        webhook_id = None

        # Simulate webhook or email callback if provided in the payload
        quoted_email = video.get("email_callback")
        quoted_webhook_id = video.get("webhook_id")

        if quoted_email:
            email = urllib.parse.unquote(quoted_email)
        else:
            email = None

        if quoted_webhook_id:
            webhook_id = urllib.parse.unquote(quoted_webhook_id)
            if not webhook_store.is_valid_webhook(webhook_id):
                print("Invalid webhook ID.")
                return

        # Enqueue the transcription job
        job = rq_queue.enqueue(
            'transcriber.transcribe',
            args=(temp_filename, requested_model, task, language, email, webhook_id),
            result_ttl=3600 * 24 * 7,
            job_timeout=3600 * 4,
            meta={
                'email': email,
                'webhook_id': webhook_id,
                'uploaded_filename': uploaded_filename
            },
            on_success=callbacks.success,
            on_failure=callbacks.failure
        )

        print(f"Enqueued transcription job with ID: {job.get_id()}")

        # Dispatch the 'job_started' event
        event_dispatcher.dispatch_event(
            event_type="job_started",
            payload={
                "job_id": job.get_id(),
                "uploaded_filename": uploaded_filename,
                "library_id": library_id,
                "uuid": uuid,
                "status": "started"
            }
        )
        print(f"Dispatched 'job_started' event for job ID: {job.get_id()}")

    except Exception as e:
        logging.exception(f"Error processing video event: {e}")

    finally:
        print("Event processing complete.")

def start_subscriber():
    """
    Initialize the RabbitMQ subscriber and start consuming messages.
    """
    print("Starting RabbitMQ subscriber...")
    queue_name = os.environ.get("RABBITMQ_CONSUME_QUEUE", None)
    if queue_name is None:
        raise ValueError("RABBITMQ_CONSUME_QUEUE must be set")
    subscriber = Subscriber(queue_name, process_video_event)
    try:
        subscriber.consume()
    except KeyboardInterrupt:
        print("Subscriber interrupted by user.")
    finally:
        print("Shutting down subscriber...")
        subscriber.close()
        event_dispatcher.close()  # Clean up dispatcher connection

if __name__ == "__main__":
    start_subscriber()
