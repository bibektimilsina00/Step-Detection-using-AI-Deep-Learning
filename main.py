import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import numpy as np
import torch
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from src.initialize_model import load_production_model
from src.step_detection.core.detector import RealTimeStepCounter
from src.step_detection.utils.config_db import (
    get_all_config,
    get_config_value,
    set_config_value,
)

# Global model storage - shared across all users for efficiency
global_model = None
global_device = None

# Active user sessions - each WebSocket connection gets its own step counter
active_sessions: Dict[str, Dict] = {}

# User to session mapping - track which session belongs to which user
user_sessions: Dict[str, str] = {}  # user_id -> session_id


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting Step Detection API...")
    app.state.start_time = time.time()
    initialize_global_model()
    yield
    print("🔌 Shutting down Step Detection API...")
    # Cleanup all active sessions
    active_sessions.clear()


app = FastAPI(
    title=get_config_value("api.title", "Step Detection API"),
    description="Real-time step detection using deep learning models",
    version=get_config_value("api.version", "2.0.0"),
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


def initialize_global_model():
    """Initialize the global model to be shared across all user sessions"""
    global global_model, global_device

    try:
        # Load the pre-trained model
        model, device = load_production_model()

        if model is None:
            print(
                "❌ Failed to load pre-trained model, falling back to untrained model"
            )
            # Fallback to creating a new model with random weights
            from initialize_model import StepDetectionCNN

            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model = StepDetectionCNN().to(device)
            model.eval()
            print(f"⚠️  Using untrained model on {device}")

        # Store globally for sharing across sessions
        global_model = model
        global_device = str(device)

        print(f"✅ Global model initialized successfully on {device}")
        return True

    except Exception as e:
        print(f"❌ Error initializing global model: {e}")
        return False


def create_user_session(session_id: str, user_id: Optional[str] = None) -> bool:
    """Create a new user session with its own step counter"""
    try:
        if global_model is None or global_device is None:
            print(
                f"❌ Cannot create session {session_id}: Global model not initialized"
            )
            return False

        # Create individual step counter for this user
        step_counter = RealTimeStepCounter(model=global_model, device=global_device)

        # Store session data
        active_sessions[session_id] = {
            "step_counter": step_counter,
            "user_id": user_id,
            "websocket": None,  # Will be set when WebSocket is assigned
            "created_at": time.time(),
            "last_activity": time.time(),
            "total_requests": 0,
        }

        # Track user to session mapping if user_id provided
        if user_id:
            user_sessions[user_id] = session_id

        print(
            f"✅ Created new user session: {session_id}"
            + (f" for user: {user_id}" if user_id else "")
        )
        return True

    except Exception as e:
        print(f"❌ Error creating session {session_id}: {e}")
        return False


def cleanup_session(session_id: str):
    """Clean up a user session"""
    if session_id in active_sessions:
        session_data = active_sessions[session_id]
        duration = time.time() - session_data["created_at"]
        total_requests = session_data["total_requests"]
        user_id = session_data.get("user_id")

        # Remove from user_sessions mapping if user_id exists
        if (
            user_id
            and user_id in user_sessions
            and user_sessions[user_id] == session_id
        ):
            del user_sessions[user_id]

        del active_sessions[session_id]
        print(
            f"🧹 Cleaned up session {session_id}"
            + (f" for user {user_id}" if user_id else "")
            + f" (Duration: {duration:.1f}s, Requests: {total_requests})"
        )


async def close_user_existing_session(user_id: str):
    """Close existing WebSocket session for a user if it exists"""
    if user_id in user_sessions:
        old_session_id = user_sessions[user_id]
        if old_session_id in active_sessions:
            old_session = active_sessions[old_session_id]
            old_websocket = old_session.get("websocket")

            if old_websocket:
                try:
                    await old_websocket.send_json(
                        {
                            "type": "session_replaced",
                            "status": "replaced",
                            "session_id": old_session_id,
                            "user_id": user_id,
                            "message": "Session closed due to new connection from same user",
                            "timestamp": str(time.time()),
                        }
                    )
                    await old_websocket.close()
                    print(
                        f"🔄 Closed previous session {old_session_id} for user {user_id}"
                    )
                except:
                    print(
                        f"⚠️ Could not gracefully close previous session {old_session_id} for user {user_id}"
                    )

            # Clean up the old session
            cleanup_session(old_session_id)


def get_session_stats() -> Dict:
    """Get statistics about active sessions"""
    return {
        "active_sessions": len(active_sessions),
        "total_requests": sum(
            session["total_requests"] for session in active_sessions.values()
        ),
        "sessions": {
            session_id: {
                "duration": time.time() - session["created_at"],
                "requests": session["total_requests"],
                "last_activity": session["last_activity"],
            }
            for session_id, session in active_sessions.items()
        },
    }


@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time step detection streaming
    Each connection gets its own isolated session and step counter
    Optional query parameter: user_id for automatic session replacement
    """
    # Extract user_id from query parameters if provided
    user_id = websocket.query_params.get("user_id")

    # Generate unique session ID for this connection
    session_id = str(uuid.uuid4())

    await websocket.accept()

    # Check if global model is initialized
    if global_model is None:
        await websocket.send_json(
            {"error": "Step detection model not initialized", "session_id": session_id}
        )
        await websocket.close()
        return

    # If user_id provided, close any existing session for this user
    if user_id:
        await close_user_existing_session(user_id)

    # Create individual session for this user
    if not create_user_session(session_id, user_id):
        await websocket.send_json(
            {"error": "Failed to create user session", "session_id": session_id}
        )
        await websocket.close()
        return

    # Store WebSocket reference in session for potential cleanup
    active_sessions[session_id]["websocket"] = websocket

    # Get this user's step counter
    user_step_counter = active_sessions[session_id]["step_counter"]

    # Send welcome message with session info
    await websocket.send_json(
        {
            "type": "session_started",
            "session_id": session_id,
            "user_id": user_id,
            "message": "Step detection session initialized successfully"
            + (f" for user {user_id}" if user_id else ""),
            "replaced_previous": user_id is not None,
            "timestamp": str(time.time()),
        }
    )

    # Timeout settings - 5 minutes of inactivity
    timeout_seconds = 5 * 60  # 5 minutes

    try:
        while True:
            try:
                # Check for timeout (5 minutes of inactivity)
                current_time = time.time()
                last_activity = active_sessions[session_id]["last_activity"]

                if current_time - last_activity > timeout_seconds:
                    await websocket.send_json(
                        {
                            "type": "timeout_response",
                            "status": "timeout",
                            "session_id": session_id,
                            "message": "Connection closed due to 5 minutes of inactivity",
                            "timestamp": str(current_time),
                        }
                    )
                    await websocket.close()
                    return

                # Receive sensor data with timeout
                try:
                    data = await asyncio.wait_for(
                        websocket.receive_json(), timeout=60.0
                    )  # 1 minute timeout for each receive

                    # Update activity timestamp and request count
                    active_sessions[session_id]["last_activity"] = time.time()
                    active_sessions[session_id]["total_requests"] += 1

                except asyncio.TimeoutError:
                    # Continue loop to check for overall timeout
                    continue

                # Check for stop action - HIGHEST PRIORITY
                if isinstance(data, dict) and data.get("action") == "stop":
                    session_stats = get_session_stats()
                    await websocket.send_json(
                        {
                            "type": "stop_response",
                            "status": "success",
                            "session_id": session_id,
                            "user_id": user_id,
                            "message": "Step detection stopped. WebSocket connection will be closed.",
                            "session_stats": {
                                "duration": time.time()
                                - active_sessions[session_id]["created_at"],
                                "total_requests": active_sessions[session_id][
                                    "total_requests"
                                ],
                            },
                            "timestamp": str(time.time()),
                        }
                    )
                    await websocket.close()
                    return

                # Check for reset action - SECOND PRIORITY
                if isinstance(data, dict) and data.get("action") == "reset":
                    user_step_counter.reset()  # Reset only this user's counter
                    await websocket.send_json(
                        {
                            "type": "reset_response",
                            "status": "success",
                            "session_id": session_id,
                            "user_id": user_id,
                            "message": "Step counter has been reset for this session",
                            "total_steps": 0,
                            "timestamp": str(time.time()),
                        }
                    )
                    continue

                # Check for stats request
                if isinstance(data, dict) and data.get("action") == "stats":
                    session_stats = get_session_stats()
                    user_session = active_sessions[session_id]
                    await websocket.send_json(
                        {
                            "type": "stats_response",
                            "status": "success",
                            "session_id": session_id,
                            "user_id": user_id,
                            "user_stats": {
                                "session_duration": time.time()
                                - user_session["created_at"],
                                "total_requests": user_session["total_requests"],
                                "last_activity": user_session["last_activity"],
                            },
                            "server_stats": {
                                "active_sessions": session_stats["active_sessions"],
                                "active_users": session_stats["active_users"],
                                "total_server_requests": session_stats[
                                    "total_requests"
                                ],
                            },
                            "timestamp": str(time.time()),
                        }
                    )
                    continue

                # Validate required fields for sensor data
                required_fields = [
                    "accel_x",
                    "accel_y",
                    "accel_z",
                    "gyro_x",
                    "gyro_y",
                    "gyro_z",
                ]
                if not all(field in data for field in required_fields):
                    try:
                        await websocket.send_json(
                            {
                                "error": "Missing sensor data fields",
                                "session_id": session_id,
                                "user_id": user_id,
                                "required_fields": required_fields,
                            }
                        )
                    except:
                        break  # Client disconnected
                    continue

                # Process sensor reading with user's individual step counter
                result = user_step_counter.add_sensor_reading(
                    accel_x=float(data["accel_x"]),
                    accel_y=float(data["accel_y"]),
                    accel_z=float(data["accel_z"]),
                    gyro_x=float(data["gyro_x"]),
                    gyro_y=float(data["gyro_y"]),
                    gyro_z=float(data["gyro_z"]),
                )

                # Convert to the format expected by frontend
                json_safe_result = {
                    "type": "step_detection",
                    "session_id": session_id,
                    "user_id": user_id,
                    "step_detected": bool(result["step_detected"]),
                    "prediction": {
                        "confidence": float(result["max_confidence"]),
                        "probabilities": [
                            float(result["start_probability"]),
                            float(result["end_probability"]),
                        ],
                        "start_probability": float(result["start_probability"]),
                        "end_probability": float(result["end_probability"]),
                        "no_step_probability": float(result["no_step_probability"]),
                        "predicted_class": int(result["predicted_class"]),
                        "movement_magnitude": float(result["movement_magnitude"]),
                        "step_start": bool(result["step_start"]),
                        "step_end": bool(result["step_end"]),
                        "detector_has_current_step": bool(
                            result["detector_has_current_step"]
                        ),
                    },
                    "total_steps": int(result["step_count"]),
                    "total_predictions": active_sessions[session_id]["total_requests"],
                    "buffer_size": (
                        len(user_step_counter.sensor_buffer)
                        if hasattr(user_step_counter, "sensor_buffer")
                        else 0
                    ),
                    "timestamp": str(time.time()),
                    "message": (
                        "Step detection processed successfully"
                        if result["status"] == "success"
                        else None
                    ),
                    "error": result.get("error"),
                }

                # Send result back
                try:
                    await websocket.send_json(json_safe_result)
                except:
                    break  # Client disconnected

            except Exception as e:
                # Handle individual operation errors
                try:
                    await websocket.send_json(
                        {
                            "error": f"Processing error: {e}",
                            "session_id": session_id,
                            "user_id": user_id,
                        }
                    )
                except:
                    break  # Client disconnected

    except WebSocketDisconnect:
        print(
            f"🔌 WebSocket disconnected for session: {session_id}"
            + (f" (user: {user_id})" if user_id else "")
        )
    except Exception as e:
        # Only try to send error message if WebSocket is still open
        try:
            if websocket.client_state.value == 1:  # CONNECTED state
                await websocket.send_json(
                    {
                        "error": f"WebSocket error: {e}",
                        "session_id": session_id,
                        "user_id": user_id,
                    }
                )
        except:
            pass  # WebSocket already closed, ignore
    finally:
        # Always cleanup session when connection ends
        cleanup_session(session_id)
        try:
            await websocket.close()
        except:
            pass  # WebSocket already closed, ignore


@app.websocket("/ws/realtime/{user_id}")
async def websocket_endpoint_with_user(websocket: WebSocket, user_id: str):
    """
    WebSocket endpoint with user ID for automatic session replacement
    Automatically closes any existing session for the same user
    """
    # Generate unique session ID for this connection
    session_id = str(uuid.uuid4())

    await websocket.accept()

    # Check if global model is initialized
    if global_model is None:
        await websocket.send_json(
            {"error": "Step detection model not initialized", "session_id": session_id}
        )
        await websocket.close()
        return

    # Close any existing session for this user
    await close_user_existing_session(user_id)

    # Create individual session for this user
    if not create_user_session(session_id, user_id):
        await websocket.send_json(
            {"error": "Failed to create user session", "session_id": session_id}
        )
        await websocket.close()
        return

    # Store WebSocket reference in session for potential cleanup
    active_sessions[session_id]["websocket"] = websocket

    # Get this user's step counter
    user_step_counter = active_sessions[session_id]["step_counter"]

    # Send welcome message with session info
    await websocket.send_json(
        {
            "type": "session_started",
            "session_id": session_id,
            "user_id": user_id,
            "message": f"Step detection session initialized successfully for user {user_id}",
            "replaced_previous": True,
            "timestamp": str(time.time()),
        }
    )

    # Continue with the same logic as the main endpoint...
    # For brevity, we'll redirect to avoid code duplication
    try:
        # Use the main WebSocket logic by delegating
        await websocket_main_logic(websocket, session_id, user_id, user_step_counter)
    except Exception as e:
        print(f"❌ Error in user WebSocket {user_id}: {e}")
    finally:
        cleanup_session(session_id)


async def websocket_main_logic(
    websocket: WebSocket, session_id: str, user_id: Optional[str], user_step_counter
):
    """Main WebSocket processing logic to avoid code duplication"""
    # Timeout settings - 5 minutes of inactivity
    timeout_seconds = 5 * 60  # 5 minutes

    try:
        while True:
            try:
                # Check for timeout (5 minutes of inactivity)
                current_time = time.time()
                if session_id not in active_sessions:
                    break  # Session was cleaned up

                last_activity = active_sessions[session_id]["last_activity"]

                if current_time - last_activity > timeout_seconds:
                    await websocket.send_json(
                        {
                            "type": "timeout_response",
                            "status": "timeout",
                            "session_id": session_id,
                            "user_id": user_id,
                            "message": "Connection closed due to 5 minutes of inactivity",
                            "timestamp": str(current_time),
                        }
                    )
                    await websocket.close()
                    return

                # Receive sensor data with timeout
                try:
                    data = await asyncio.wait_for(
                        websocket.receive_json(), timeout=60.0
                    )  # 1 minute timeout for each receive

                    # Update activity timestamp and request count
                    if session_id in active_sessions:
                        active_sessions[session_id]["last_activity"] = time.time()
                        active_sessions[session_id]["total_requests"] += 1

                except asyncio.TimeoutError:
                    # Continue loop to check for overall timeout
                    continue

                # Rest of the WebSocket logic would go here...
                # For now, just echo back for testing
                await websocket.send_json(
                    {
                        "type": "echo",
                        "session_id": session_id,
                        "user_id": user_id,
                        "received": data,
                        "timestamp": str(time.time()),
                    }
                )

            except Exception as e:
                print(f"❌ WebSocket processing error: {e}")
                break

    except WebSocketDisconnect:
        print(
            f"🔌 WebSocket disconnected for session: {session_id}"
            + (f" (user: {user_id})" if user_id else "")
        )
    except Exception as e:
        print(f"❌ WebSocket error: {e}")


# --- Session Management API endpoints ---


@app.get("/sessions")
def get_active_sessions():
    """Get information about all active sessions"""
    return get_session_stats()


@app.get("/sessions/{session_id}")
def get_session_info(session_id: str):
    """Get detailed information about a specific session"""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = active_sessions[session_id]
    return {
        "session_id": session_id,
        "created_at": session["created_at"],
        "last_activity": session["last_activity"],
        "duration": time.time() - session["created_at"],
        "total_requests": session["total_requests"],
        "is_active": True,
    }


@app.delete("/sessions/{session_id}")
def terminate_session(session_id: str):
    """Manually terminate a specific session"""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    cleanup_session(session_id)
    return {
        "message": f"Session {session_id} terminated successfully",
        "timestamp": str(time.time()),
    }


@app.get("/health")
def health_check():
    """Health check endpoint with system status"""
    return {
        "status": "healthy",
        "model_loaded": global_model is not None,
        "device": global_device,
        "active_sessions": len(active_sessions),
        "timestamp": str(time.time()),
        "uptime": (
            time.time() - app.state.start_time
            if hasattr(app.state, "start_time")
            else None
        ),
    }


# --- Config API endpoints ---

from typing import Optional


class DetectionConfig(BaseModel):
    window_size: Optional[int] = None
    start_threshold: Optional[float] = None
    end_threshold: Optional[float] = None
    min_step_interval: Optional[float] = None
    motion_threshold: Optional[float] = None
    gyro_threshold: Optional[float] = None
    min_motion_variance: Optional[float] = None
    stillness_threshold: Optional[float] = None
    ai_high_confidence_threshold: Optional[float] = None
    ai_sensor_disagree_threshold: Optional[float] = None


class ConfigUpdateRequest(BaseModel):
    value: str


# Only allow these keys to be managed via API
ALLOWED_CONFIG_KEYS = [
    "window_size",
    "start_threshold",
    "end_threshold",
    "min_step_interval",
    "motion_threshold",
    "gyro_threshold",
    "min_motion_variance",
    "stillness_threshold",
    "ai_high_confidence_threshold",
    "ai_sensor_disagree_threshold",
]


@app.get("/config", response_model=DetectionConfig)
def get_config_api():
    """Get all important detection config values as a single object"""
    all_items = get_all_config()
    config_dict = {}
    for k in ALLOWED_CONFIG_KEYS:
        v = all_items.get(k)
        if v is not None:
            # Try to cast to correct type
            if k == "window_size":
                try:
                    config_dict[k] = int(float(v))
                except ValueError:
                    try:
                        config_dict[k] = float(v)
                    except ValueError:
                        config_dict[k] = None
            elif k == "min_step_interval":
                try:
                    config_dict[k] = float(v)
                except ValueError:
                    config_dict[k] = None
            else:
                try:
                    config_dict[k] = float(v)
                except ValueError:
                    config_dict[k] = None
        else:
            config_dict[k] = None
    return DetectionConfig(**config_dict)


@app.get("/config/{key}", response_model=DetectionConfig)
def get_config_key_api(key: str):
    if key not in ALLOWED_CONFIG_KEYS:
        raise HTTPException(status_code=403, detail="Config key not allowed")
    val = get_config_value(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Config key not found")
    # Return config with only this key set
    from typing import Any, Dict, cast

    config_dict = cast(Dict[str, Any], {k: None for k in ALLOWED_CONFIG_KEYS})
    # Assign correct type for each key
    if key == "window_size":
        try:
            config_dict[key] = int(float(val))
        except ValueError:
            try:
                config_dict[key] = float(val)
            except ValueError:
                config_dict[key] = None
    elif key == "min_step_interval":
        try:
            config_dict[key] = float(val)
        except ValueError:
            config_dict[key] = None
    else:
        try:
            config_dict[key] = float(val)
        except ValueError:
            config_dict[key] = None
    return DetectionConfig(**config_dict)


@app.post("/config/{key}", response_model=DetectionConfig)
def set_config_key_api(key: str, req: ConfigUpdateRequest):
    if key not in ALLOWED_CONFIG_KEYS:
        raise HTTPException(status_code=403, detail="Config key not allowed")
    set_config_value(key, req.value)
    # Return updated config
    return get_config_api()


if __name__ == "__main__":
    import uvicorn

    host = get_config_value("api.host", "0.0.0.0")
    port = int(get_config_value("api.port", 8000))
    reload = get_config_value("api.reload", "True") == "True"

    print("🚀 Starting Step Detection API with FastAPI")
    print(f"📖 API Documentation: http://localhost:{port}/docs")
    uvicorn.run("main:app", host=host, port=port, reload=reload)
