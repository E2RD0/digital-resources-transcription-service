import json
import os
import requests
import tempfile
import urllib.parse
import mimetypes
import logging
import whisper

from src.events.rabbitmq.subscriber import Subscriber
from src.events.rabbitmq.event_dispatcher import EventDispatcher
from src.main import rq_queue, DEFAULT_MODEL, DETECT_MODEL, DEFAULT_TASK, webhook_store, callbacks

SERVICE_URL = os.environ.get("SERVICE_URL", "localhost:4000")
DEFAULT_UPLOADED_FILENAME = "untitled-transcription"

# Initialize the event dispatcher
event_dispatcher = EventDispatcher()

def get_extension_from_content_type(content_type: str) -> str:
    """
    Get the file extension based on the Content-Type header.
    """
    return mimetypes.guess_extension(content_type) or ""

def process_resource_events(payload: dict, properties):
    event_type = properties.type if hasattr(properties, "type") else None
    if event_type not in (
        "teaching-action.digital-resources-service.resource_uploaded",
        "teaching-action.digital-resources-service.video_transcription_requested",
    ):
        print(f"Ignoring message with event type: {event_type}")
        return

    uuid = None
    library_id = None

    try:
        event = payload.get("data", {})
        video = event.get("resource", {})
        uuid = video.get("uuid")
        library_external_id = video.get("library", {}).get("externalId")

        if(not video.get("type", False) == "video"):
            print("Resource is not a video. Ignoring.")
            return

        if not uuid or not library_external_id:
            print("Invalid payload: missing resource.uuid or library.externalId")
            return

        languagesToTranslate = event.get("requestedLangs", "")
        languagesToTranslate = languagesToTranslate.split(',') if languagesToTranslate else []
        languagesToTranslate = [lang for lang in languagesToTranslate if lang]

        uploaded_filename = video.get("title", DEFAULT_UPLOADED_FILENAME)
        temp_video_path = video.get("tempResourceFilePath")

        if not temp_video_path:
            print("No video file path provided.")
            return

        # Download video
        temp_dir = './upload-shared-tmp'
        os.makedirs(temp_dir, exist_ok=True)
        download_url = f"{SERVICE_URL}/resource/download/{library_external_id}/{uuid}"
        print(f"Downloading video from: {download_url}")

        response = requests.get(download_url, stream=True)
        if response.status_code != 200:
            print(f"Failed to download video. Status code: {response.status_code}")
            send_error_event({"uuid": uuid, "libraryId": library_external_id}, response.status_code, "Failed to download video from service")
            return

        # Save temp file
        content_type = response.headers.get("Content-Type", "")
        file_extension = get_extension_from_content_type(content_type) or os.path.splitext(temp_video_path)[-1]
        with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False, suffix=file_extension) as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            temp_filename = temp_file.name

        print(f"Video saved to temporary file: {temp_filename}")

        # Detect language using Whisper
        model = whisper.load_model(DETECT_MODEL)
        audio = whisper.pad_or_trim(whisper.load_audio(temp_filename))
        mel = whisper.log_mel_spectrogram(audio).to(model.device)
        _, probs = model.detect_language(mel)
        detected_lang = max(probs, key=probs.get)

        detectedLanguage = {
            "detectedLanguage": whisper.tokenizer.LANGUAGES[detected_lang],
            "languageCode": detected_lang
        }

        send_detected_language_event(video, detectedLanguage)

        requested_model = DEFAULT_MODEL
        task = DEFAULT_TASK
        language = detected_lang or "transcribe"
        email = urllib.parse.unquote(video.get("email_callback", "")) or None
        webhook_id = urllib.parse.unquote(video.get("webhook_id", "")) or None
        
        if not languagesToTranslate:
            languagesToTranslate = ['es', 'en', 'fr', 'pt']
        if language in languagesToTranslate:
            languagesToTranslate.remove(language)

        if webhook_id and not webhook_store.is_valid_webhook(webhook_id):
            print("Invalid webhook ID.")
            return

        # Main job
        job = rq_queue.enqueue(
            'transcriber.transcribe',
            args=(temp_filename, requested_model, task, language, email, webhook_id),
            result_ttl=3600 * 24 * 7,
            job_timeout=3600 * 4,
            meta={
                'email': email,
                'webhook_id': webhook_id,
                'uploaded_filename': uploaded_filename,
                'library_id': library_external_id,
                'uuid': uuid,
                'languages_to_translate': languagesToTranslate
            },
            on_success=callbacks.success,
            on_failure=callbacks.failure
        )

        send_job_started_event(job, {
            "uploaded_filename": uploaded_filename,
            "library_id": library_external_id,
            "uuid": uuid,
            "language": language
        })

    except Exception as e:
        send_error_event({"uuid": uuid, "libraryId": library_external_id}, type(e).__name__, str(e))
        logging.exception(f"Error processing video event: {e}")
    finally:
        print("Event processing complete.")

def start_subscriber():
    """
    Initialize the RabbitMQ subscriber and start consuming messages.
    """
    print("Starting RabbitMQ subscriber...")
    subscriber = Subscriber(process_resource_events)
    try:
        subscriber.consume()
    except KeyboardInterrupt:
        print("Subscriber interrupted by user.")
    finally:
        print("Shutting down subscriber...")
        subscriber.close()
        event_dispatcher.close()  # Clean up dispatcher connection
        
def send_error_event(video, errType, errValue):
    # Publish failure event via EventDispatcher
    message = {
        "status": "failure",
        "job_id": None,
        "error_type": str(errType),
        "error_value": str(errValue),
    }
    if(video["uuid"] and video["libraryId"]):
        message["video"] = {
            "uuid": video["uuid"],
            "library_external_id": video["libraryId"]
        } 
    event_dispatcher.dispatch_event("transcription_failed", message)

def send_job_started_event(job, video):
    print(f"Enqueued transcription job with ID: {job.get_id()}")
    message = {
        "job_id": job.get_id(),
        "filename": video["uploaded_filename"],
        "status": "started",
        "language": video.get("language", "transcribe"),
    }
    if video.get("uuid") and video.get("library_id"):
        message["resource"] = {
            "uuid": video["uuid"],
            "library_external_id": video["library_id"]
        }
    event_dispatcher.dispatch_event(
        event_type="transcription_started",
        payload=message
    )
    print(f"Dispatched 'transcription_started' event for job ID: {job.get_id()}")
   
def send_detected_language_event(video, videoLang):
    message = {
        "status": "language_detected",
        "language": videoLang["detectedLanguage"],
        "languageCode": videoLang["languageCode"]
    }
    if(video["uuid"] and video["library"]['externalId']):
        message["resource"] = {
            "uuid": video["uuid"],
            "library_external_id": video["library"]['externalId']
        } 
    event_dispatcher.dispatch_event("language_detected", message)


if __name__ == "__main__":
    start_subscriber()
