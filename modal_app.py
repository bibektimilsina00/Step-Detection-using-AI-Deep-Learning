import modal

app = modal.App("step-detection-app")

# Create a volume for persistent model storage (optional - for production)
model_volume = modal.Volume.from_name("step-detection-models", create_if_missing=True)

image = (
    modal.Image.debian_slim()
    .pip_install_from_requirements("requirements.txt")
    .add_local_dir(".", remote_path="/root")
)


@app.function(
    image=image,
    # Optional: Mount persistent volume for models (uncomment if needed)
    # volumes={"/root/persistent_models": model_volume}
    scaledown_window=600,
)
@modal.asgi_app()
def fastapi_app():
    from src.step_detection.api.api import app as fastapi_app

    return fastapi_app
