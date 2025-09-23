#!/usr/bin/env python3
"""
Simple WebSocket client to test the step detection API
"""
import asyncio
import json
import time

import websockets


async def test_websocket():
    """Test WebSocket connection"""
    uri = "ws://localhost:8000/ws/realtime"

    try:
        print("🔌 Connecting to WebSocket...")
        async with websockets.connect(uri) as websocket:
            print("✅ Connected successfully!")

            # Wait for welcome message
            try:
                welcome_msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"📨 Received: {welcome_msg}")

                # Send a test sensor data
                test_data = {
                    "accel_x": 0.1,
                    "accel_y": 0.2,
                    "accel_z": 9.8,
                    "gyro_x": 0.01,
                    "gyro_y": 0.02,
                    "gyro_z": 0.03,
                }

                print(f"📤 Sending test data: {test_data}")
                await websocket.send(json.dumps(test_data))

                # Wait for response
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"📨 Received response: {response}")

                # Keep connection alive for a bit
                print("⏱️  Keeping connection alive for 10 seconds...")
                await asyncio.sleep(10)

                print("✅ Test completed successfully!")

            except asyncio.TimeoutError:
                print("⏰ Timeout waiting for message")
            except Exception as e:
                print(f"❌ Error during communication: {e}")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"🔌 Connection closed: {e}")
    except Exception as e:
        print(f"❌ Connection error: {e}")


async def test_websocket_with_user():
    """Test WebSocket connection with user ID"""
    user_id = "test_user_123"
    uri = f"ws://localhost:8000/ws/realtime/{user_id}"

    try:
        print(f"🔌 Connecting to WebSocket with user ID: {user_id}...")
        async with websockets.connect(uri) as websocket:
            print("✅ Connected successfully!")

            # Wait for welcome message
            welcome_msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            print(f"📨 Received: {welcome_msg}")

            print("✅ User-based connection test completed!")

    except Exception as e:
        print(f"❌ User connection error: {e}")


if __name__ == "__main__":
    print("🧪 Testing WebSocket connections...\n")

    # Test regular connection
    print("1️⃣ Testing regular WebSocket connection:")
    asyncio.run(test_websocket())

    print("\n" + "=" * 50 + "\n")

    # Test user-based connection
    print("2️⃣ Testing user-based WebSocket connection:")
    asyncio.run(test_websocket_with_user())
