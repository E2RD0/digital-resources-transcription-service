import whisper

print("Downloading models...")

# Get available models available_models
available_models = whisper.available_models()

print("Available models: " + str(available_models))

SUPPORTED_MODELS = ['tiny', 'base', 'small', 'medium'];

# For each model, download the model using load_model to cache it
for model in available_models:
    print("Downloading model: " + model)
    if model not in SUPPORTED_MODELS:
        print("Model not supported: " + model)
        continue
    whisper.load_model(model)

print("Done!")