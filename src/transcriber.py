from typing import Any
from src.utils import is_model_supported 
import whisper
from sentry_sdk import set_user

def transcribe(filename: str, requestedModel: str, task: str, language: str, email: str, webhook_id: str) -> dict[str, Any]:
    # Mail is not used here, but it makes it easier for the worker to log mail
    print("Executing transcribing of " + filename + " for " + " using " + requestedModel + " model ")
    set_user({"email": email})
    if (not is_model_supported(requestedModel)):
        return {
            "error": "Model not supported"
        }, 405
    model = whisper.load_model(requestedModel)
    return model.transcribe(filename, language=language, task=task)
