"""
FastAPI Step Detection API
REST API for real-time step detection using TensorFlow models.
"""

import json
import os
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ..core.detector import SimpleStepCounter, StepDetector, load_model_info


class SensorReading(BaseModel):
    """Sensor reading data model."""

    accel_x: float
    accel_y: float
    accel_z: float
    gyro_x: float
    gyro_y: float
    gyro_z: float


class StepDetectionResponse(BaseModel):
    """Step detection response model."""

    step_start: bool
    step_end: bool
    start_probability: float
    end_probability: float
    step_count: int
    timestamp: str


class StepCountResponse(BaseModel):
    """Step count response model."""

    step_count: int
    last_detection: Optional[Dict] = None


# Initialize FastAPI app
app = FastAPI(
    title="Step Detection API",
    description="Real-time step detection using TensorFlow CNN",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for detectors
detector: Optional[StepDetector] = None
counter: Optional[SimpleStepCounter] = None
model_info: Dict = {}


@app.on_event("startup")
async def startup_event():
    """Initialize detectors on startup."""
    global detector, counter, model_info

    model_path = "models/step_detection_model.keras"
    metadata_path = "models/model_metadata.json"

    if os.path.exists(model_path):
        try:
            detector = StepDetector(model_path)
            counter = SimpleStepCounter(model_path)
            model_info = load_model_info(metadata_path)
            print(f"✅ Models loaded successfully from {model_path}")
        except Exception as e:
            print(f"❌ Error loading models: {e}")
    else:
        print(f"⚠️ Model not found: {model_path}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Step Detection API",
        "version": "1.0.0",
        "status": "active" if detector is not None else "model_not_loaded",
        "endpoints": {
            "detect_step": "POST /detect_step - Detect steps from sensor data",
            "step_count": "GET /step_count - Get current step count",
            "reset_count": "POST /reset_count - Reset step count",
            "session_summary": "GET /session_summary - Get session summary",
            "model_info": "GET /model_info - Get model information",
            "websocket": "WS /ws/realtime - Real-time step detection via WebSocket",
        },
    }


@app.post("/detect_step", response_model=StepDetectionResponse)
async def detect_step(reading: SensorReading):
    """Detect steps from sensor reading."""
    if detector is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        result = detector.process_reading(
            reading.accel_x,
            reading.accel_y,
            reading.accel_z,
            reading.gyro_x,
            reading.gyro_y,
            reading.gyro_z,
        )

        return StepDetectionResponse(
            step_start=result["step_start"],
            step_end=result["step_end"],
            start_probability=result["predictions"]["start_prob"],
            end_probability=result["predictions"]["end_prob"],
            step_count=result["step_count"],
            timestamp=result["timestamp"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detection error: {str(e)}")


@app.get("/step_count", response_model=StepCountResponse)
async def get_step_count():
    """Get current step count."""
    if counter is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return StepCountResponse(
        step_count=counter.get_count(), last_detection=counter.last_detection
    )


@app.post("/reset_count")
async def reset_step_count():
    """Reset step count."""
    if counter is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    counter.reset()
    if detector is not None:
        detector.reset()

    return {"message": "Step count reset", "step_count": 0}


@app.get("/session_summary")
async def get_session_summary():
    """Get session summary."""
    if detector is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return detector.get_session_summary()


@app.post("/save_session")
async def save_session(filename: str = "session.json"):
    """Save current session data."""
    if detector is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        detector.save_session(filename)
        return {"message": f"Session saved to {filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Save error: {str(e)}")


@app.get("/model_info")
async def get_model_info():
    """Get model information."""
    if not model_info:
        return {"message": "Model info not available"}

    return {
        "model_info": model_info,
        "api_status": "active" if detector is not None else "model_not_loaded",
        "thresholds": (
            {
                "start_threshold": detector.start_threshold if detector else None,
                "end_threshold": detector.end_threshold if detector else None,
            }
            if detector
            else None
        ),
    }


@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time step detection."""
    await websocket.accept()

    if detector is None:
        await websocket.send_text(
            json.dumps({"error": "Model not loaded", "status": "error"})
        )
        await websocket.close(code=1003)
        return

    try:
        while True:
            # Receive sensor data from client
            data = await websocket.receive_text()

            try:
                sensor_data = json.loads(data)

                # Validate required fields
                required_fields = [
                    "accel_x",
                    "accel_y",
                    "accel_z",
                    "gyro_x",
                    "gyro_y",
                    "gyro_z",
                ]
                if not all(field in sensor_data for field in required_fields):
                    await websocket.send_text(
                        json.dumps(
                            {
                                "error": "Missing required sensor data fields",
                                "required": required_fields,
                                "status": "error",
                            }
                        )
                    )
                    continue

                # Process the reading
                result = detector.process_reading(
                    sensor_data["accel_x"],
                    sensor_data["accel_y"],
                    sensor_data["accel_z"],
                    sensor_data["gyro_x"],
                    sensor_data["gyro_y"],
                    sensor_data["gyro_z"],
                )

                # Log step detection details
                start_prob = result["predictions"]["start_prob"]
                end_prob = result["predictions"]["end_prob"]
                step_start = result["step_start"]
                step_end = result["step_end"]
                step_count = result["step_count"]

                # Clean logging with emojis
                if step_start or step_end or "completed_step" in result:
                    print(f"🦶 Step detected! Count: {step_count}")
                else:
                    print(f"⚪ No step | Count: {step_count}")

                # Send response back to client
                response = {
                    "step_start": bool(result["step_start"]),
                    "step_end": bool(result["step_end"]),
                    "start_probability": float(result["predictions"]["start_prob"]),
                    "end_probability": float(result["predictions"]["end_prob"]),
                    "step_count": int(result["step_count"]),
                    "timestamp": str(result["timestamp"]),
                    "status": "success",
                }

                await websocket.send_text(json.dumps(response))

            except json.JSONDecodeError:
                await websocket.send_text(
                    json.dumps({"error": "Invalid JSON format", "status": "error"})
                )
            except Exception as e:
                await websocket.send_text(
                    json.dumps(
                        {"error": f"Processing error: {str(e)}", "status": "error"}
                    )
                )

    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    except Exception as e:
        print(f"WebSocket error: {e}")
        await websocket.close(code=1011)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "model_loaded": detector is not None,
        "api_version": "1.0.0",
    }


if __name__ == "__main__":
    import uvicorn

    print("Starting Step Detection API...")
    print("Documentation available at: http://localhost:8000/docs")

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
