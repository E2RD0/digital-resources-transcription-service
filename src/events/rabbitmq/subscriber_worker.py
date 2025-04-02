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

def process_video_event(ch, method, properties, body):
    """
    Callback function to process the 'video_uploaded' event from RabbitMQ.
    """
    temp_dir = './upload-shared-tmp'
    os.makedirs(temp_dir, exist_ok=True)  # Ensure the directory exists
        # Parse the incoming message
    event = json.loads(body)
    if "video" not in event or "library" not in event["video"] or "uuid" not in event["video"]:
        print("Invalid event payload")
        return

    video = event["video"]
    uuid = video["uuid"] if "uuid" in video else ""
    library_id = video["library"]['externalId'] if "library" in video and "externalId" in video["library"] else ""
    
    try:
        languagesToTranslate = event["requestedLangs"].split(',') if "requestedLangs" in event else []
        if languagesToTranslate and len(languagesToTranslate) == 1 and languagesToTranslate[0] == "":
            languagesToTranslate = []
        uploaded_filename = video.get("title", DEFAULT_UPLOADED_FILENAME)
        temp_video_path = video["tempVideoPath"]
        
        if not temp_video_path:
            print("No video file path provided.")
            return

        # Download the video file
        download_url = f"{SERVICE_URL}/video/download/{library_id}/{uuid}"
        print(f"Downloading video from: {download_url}")

        response = requests.get(download_url, stream=True)
        if response.status_code != 200:
            print(f"Failed to download video. Status code: {response.status_code}")
            send_error_event(
                {
                    "uuid" : uuid,
                    "libraryId": library_id,
                },
                response.status_code,
                "Failed to download video from service"
            )
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
        
        # Detect the language of the video
        model = whisper.load_model(DETECT_MODEL)

        # load audio and pad/trim it to fit 30 seconds
        audio = whisper.load_audio(temp_filename)
        audio = whisper.pad_or_trim(audio)

        # make log-Mel spectrogram and move to the same device as the model
        mel = whisper.log_mel_spectrogram(audio).to(model.device)

        # detect the spoken language
        _, probs = model.detect_language(mel)
        detected_lang = max(probs, key=probs.get)
        detectedLanguage = {
            "detectedLanguage": whisper.tokenizer.LANGUAGES[detected_lang],
            "languageCode": detected_lang
        }

        send_detected_language_event(video, detectedLanguage)
        # Set default parameters
        requested_model = DEFAULT_MODEL
        task = DEFAULT_TASK
        language = detected_lang or None
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
                'uploaded_filename': uploaded_filename,
                'library_id': library_id,
                'uuid': uuid
            },
            on_success=callbacks.success,
            on_failure=callbacks.failure
        )
        
        send_job_started_event(job, {
            "uploaded_filename": uploaded_filename,
            "library_id": library_id,
            "uuid": uuid,
            "language": language or "transcribe"
        })
        
        langs = languagesToTranslate if (languagesToTranslate and len(languagesToTranslate)) else ['es', 'en', 'fr', 'pt']
        if(language and (language in langs)):
            langs.remove(language)
        for lang in langs:
            job = rq_queue.enqueue(
                'transcriber.transcribe',
                args=(temp_filename, requested_model, 'transcribe', lang, email, webhook_id),
                result_ttl=3600 * 24 * 7,
                job_timeout=3600 * 4,
                meta={
                    'email': email,
                    'webhook_id': webhook_id,
                    'uploaded_filename': uploaded_filename,
                    'library_id': library_id,
                    'uuid': uuid
                },
                on_success=callbacks.success,
                on_failure=callbacks.failure
            )
            send_job_started_event(job, {
                "uploaded_filename": uploaded_filename,
                "library_id": library_id,
                "uuid": uuid,
                "language": lang
            })

    except Exception as e:
        send_error_event(
            {
                "uuid": uuid,
                "libraryId": library_id
            },
            type(e).__name__,
            str(e)
        )
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
        
def send_error_event(video, errType, errValue):
    # Publish failure event via EventDispatcher
    message = {
        "status": "job_failed",
        "job_id": None,
        "error_type": str(errType),
        "error_value": str(errValue)
    }
    if(video["uuid"] and video["libraryId"]):
        message["video"] = {
            "uuid": video["uuid"],
            "libraryId": video["libraryId"]
        } 
    event_dispatcher.dispatch_event("job_failed", message)

def send_job_started_event(job, video):
    print(f"Enqueued transcription job with ID: {job.get_id()}")
    # Dispatch the 'job_started' event
    event_dispatcher.dispatch_event(
        event_type="job_processing",
        payload={
            "job_id": job.get_id(),
            "uploadedFilename": video["uploaded_filename"],
            "libraryId": video["library_id"],
            "uuid": video["uuid"],
            "status": "started",
            "language": video.get("language", "transcribe"),
        }
    )
    print(f"Dispatched 'job_started' event for job ID: {job.get_id()}")
   
def send_detected_language_event(video, videoLang):
    message = {
        "status": "language_detected",
        "language": videoLang["detectedLanguage"],
        "languageCode": videoLang["languageCode"]
    }
    if(video["uuid"] and video["library"]['externalId']):
        message["video"] = {
            "uuid": video["uuid"],
            "libraryId": video["library"]['externalId']
        } 
    event_dispatcher.dispatch_event("language_detected", message)


if __name__ == "__main__":
    start_subscriber()
