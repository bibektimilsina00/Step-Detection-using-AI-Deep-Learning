import modal

app = modal.App("step-detection-app")

image = modal.Image.debian_slim().pip_install_from_requirements("requirements.txt")

@app.function(image=image, min_containers=1)
@modal.asgi_app()
def fastapi_app():
    from src.step_detection.api.api import app as fastapi_app
    return fastapi_app
