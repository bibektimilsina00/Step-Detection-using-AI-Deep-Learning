#!/usr/bin/env python3
"""
Multi-Device Step Detection Simulator

Simulates multiple devices sending sensor data to the step detection WebSocket endpoint.
Displays real-time step counts for each device with enhanced walking simulation.
"""

import argparse
import asyncio
import json
import math
import random
import ssl
import threading
import time
from collections import defaultdict

import websockets
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text


class DeviceSimulator:
    def __init__(self, device_id, uri):
        self.device_id = device_id
        self.uri = uri
        self.step_count = 0
        self.last_update = time.time()
        self.start_time = time.time()
        self.total_sent = 0
        self.total_received = 0
        self.errors = 0
        self.avg_response_time = 0
        self.last_sensor_data = {}
        self.last_response = {}
        self.connected = False
        self.session_id = None

        # Device-specific walking parameters for variety
        device_num = int(
            device_id.split("_")[1]
        )  # Extract number from device_01, device_02, etc.

        # Different walking speeds and patterns for each device
        self.walking_freq = 1.6 + (
            device_num * 0.1
        )  # 1.7 to 2.3 Hz (102-138 steps/min)
        self.step_amplitude = 4.0 + (device_num * 0.2)  # 4.2 to 5.8 amplitude
        self.gyro_multiplier = 1.8 + (device_num * 0.1)  # Different gyro sensitivity
        self.noise_level = 0.08 + (device_num * 0.02)  # Different noise levels
        self.phase_offset = device_num * 0.5  # Different phase offsets for variety

    def generate_walking_data(self):
        """Generate realistic walking sensor data that meets step detection thresholds"""
        elapsed = time.time() - self.start_time

        # Use device-specific walking parameters
        walking_freq = self.walking_freq
        step_amplitude = self.step_amplitude
        gyro_multiplier = self.gyro_multiplier
        noise_level = self.noise_level
        phase_offset = self.phase_offset
        device_num = int(self.device_id.split("_")[1])

        # Generate step-like motion patterns with device-specific variations
        # Primary motion in X and Y axes to create magnitude > 11.0
        # Use multiple harmonics for realistic walking pattern
        accel_x = (
            step_amplitude
            * math.sin(2 * math.pi * walking_freq * elapsed + phase_offset)
            + 1.2
            * math.sin(
                4 * math.pi * walking_freq * elapsed + phase_offset
            )  # Second harmonic
            + random.uniform(-0.8, 0.8)
        )

        accel_y = (
            step_amplitude
            * 0.8
            * math.cos(2 * math.pi * walking_freq * elapsed + phase_offset)
            + 0.9 * math.cos(4 * math.pi * walking_freq * elapsed + phase_offset)
            + random.uniform(-0.8, 0.8)
        )

        # Z-axis: gravity with walking perturbations
        accel_z = (
            9.8
            + 2.2
            * math.sin(
                2 * math.pi * walking_freq * elapsed + math.pi / 4 + phase_offset
            )
            + random.uniform(-0.5, 0.5)
        )

        # Gyroscope data - strong rotation during steps to exceed 1.5 threshold
        gyro_x = (
            gyro_multiplier
            * math.sin(2 * math.pi * walking_freq * elapsed + phase_offset)
            + 0.8 * math.sin(4 * math.pi * walking_freq * elapsed + phase_offset)
            + random.uniform(-0.4, 0.4)
        )

        gyro_y = (
            gyro_multiplier
            * 0.8
            * math.cos(2 * math.pi * walking_freq * elapsed + phase_offset)
            + 0.6 * math.cos(4 * math.pi * walking_freq * elapsed + phase_offset)
            + random.uniform(-0.4, 0.4)
        )

        gyro_z = gyro_multiplier * 0.6 * math.sin(
            2 * math.pi * walking_freq * elapsed * 1.3 + phase_offset
        ) + random.uniform(-0.3, 0.3)

        # Add realistic noise and micro-vibrations with device-specific noise level
        for axis in [accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z]:
            axis += random.uniform(-noise_level, noise_level)

        # Add occasional impact spikes (foot strikes) - vary by device
        if random.random() < (0.06 + device_num * 0.01):  # 6-16% chance based on device
            impact_strength = random.uniform(1.2, 2.8)
            accel_x += impact_strength * random.choice([-1, 1])
            accel_y += impact_strength * random.choice([-1, 1])
            gyro_z += random.uniform(0.3, 1.0) * random.choice([-1, 1])

        # Ensure we meet the minimum thresholds
        accel_magnitude = math.sqrt(accel_x**2 + accel_y**2 + accel_z**2)
        gyro_magnitude = math.sqrt(gyro_x**2 + gyro_y**2 + gyro_z**2)

        # If magnitude is too low, boost it
        if accel_magnitude < 11.5:
            boost_factor = 11.5 / accel_magnitude
            accel_x *= boost_factor
            accel_y *= boost_factor

        if gyro_magnitude < 1.6:
            boost_factor = 1.6 / gyro_magnitude
            gyro_x *= boost_factor
            gyro_y *= boost_factor

        data = {
            "accel_x": round(accel_x, 3),
            "accel_y": round(accel_y, 3),
            "accel_z": round(accel_z, 3),
            "gyro_x": round(gyro_x, 3),
            "gyro_y": round(gyro_y, 3),
            "gyro_z": round(gyro_z, 3),
        }

        self.last_sensor_data = data
        return data

    def is_active(self):
        """Check if device is actively communicating"""
        elapsed = time.time() - self.last_update
        return self.connected and elapsed < 5.0

    async def simulate(self):
        try:
            print(f"🔌 Device {self.device_id}: Connecting to {self.uri}...")

            # Create SSL context that doesn't verify certificates
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(self.uri, ssl=ssl_context) as websocket:
                self.connected = True
                print(f"✅ Device {self.device_id}: Connected successfully!")

                # Wait for welcome message
                welcome_msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                welcome_data = json.loads(welcome_msg)
                self.session_id = welcome_data.get("session_id")
                self.last_response = welcome_data
                print(f"📨 Device {self.device_id}: Session {self.session_id}")

                while True:
                    start_time = time.time()

                    # Generate walking sensor data
                    sensor_data = self.generate_walking_data()
                    self.total_sent += 1

                    # Send data
                    await websocket.send(json.dumps(sensor_data))

                    # Receive response
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    response_time = time.time() - start_time
                    response_data = json.loads(response)

                    self.total_received += 1
                    self.last_response = response_data

                    # Update response time average
                    self.avg_response_time = (
                        self.avg_response_time * (self.total_received - 1)
                        + response_time
                    ) / self.total_received

                    # Update step count
                    if "total_steps" in response_data:
                        old_count = self.step_count
                        self.step_count = response_data["total_steps"]
                        self.last_update = time.time()

                        # Debug: Print step detection info (only for first few steps)
                        if self.step_count > old_count and self.step_count <= 3:
                            print(
                                f"🎯 Device {self.device_id}: STEP DETECTED! Count: {self.step_count}"
                            )
                    elif "step_count" in response_data:  # Fallback for older format
                        old_count = self.step_count
                        self.step_count = response_data["step_count"]
                        self.last_update = time.time()

                        # Debug: Print step detection info (only for first few steps)
                        if self.step_count > old_count and self.step_count <= 3:
                            print(
                                f"🎯 Device {self.device_id}: STEP DETECTED! Count: {self.step_count} (legacy format)"
                            )

                        # Debug: Print step detection info
                        if self.step_count > old_count:
                            print(
                                f"🎯 Device {self.device_id}: STEP DETECTED! Count: {self.step_count}"
                            )
                            print(f"   Response: {response_data}")
                        elif (
                            self.total_sent % 50 == 0
                        ):  # Print every 50 messages for debugging
                            print(
                                f"📊 Device {self.device_id}: Sent {self.total_sent}, Steps: {self.step_count}"
                            )
                            print(
                                f"   Last response keys: {list(response_data.keys())}"
                            )
                            if "movement_magnitude" in response_data:
                                print(
                                    f"   Movement magnitude: {response_data['movement_magnitude']:.2f}"
                                )

                    # Small delay to simulate real-time data (50Hz sampling)
                    await asyncio.sleep(0.02)

        except websockets.exceptions.ConnectionClosed:
            self.connected = False
            print(f"🔌 Device {self.device_id}: Connection closed")
        except asyncio.TimeoutError:
            self.errors += 1
            print(f"⏰ Device {self.device_id}: Response timeout")
        except Exception as e:
            self.errors += 1
            print(f"❌ Device {self.device_id}: Error - {e}")
            self.connected = False

    async def reset_counter(self, websocket):
        """Send reset command to backend"""
        try:
            reset_data = {"action": "reset"}
            await websocket.send(json.dumps(reset_data))

            # Wait for reset response
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            response_data = json.loads(response)

            if response_data.get("type") == "reset_response":
                self.step_count = 0
                self.last_update = time.time()
                print(f"🔄 Device {self.device_id}: Step counter reset successfully")
                return True
            else:
                print(f"❌ Device {self.device_id}: Reset failed - {response_data}")
                return False

        except Exception as e:
            print(f"❌ Device {self.device_id}: Reset error - {e}")
            return False


def keyboard_input_handler(devices, console):
    """Handle keyboard input for device control"""
    while True:
        try:
            cmd = input().strip().lower()

            if cmd == "help" or cmd == "h":
                console.print("\n[bold cyan]Available Commands:[/bold cyan]")
                console.print("  [yellow]r all[/yellow]     - Reset all devices")
                console.print("  [yellow]r 1[/yellow]       - Reset device_01")
                console.print("  [yellow]r 2[/yellow]       - Reset device_02")
                console.print("  [yellow]status[/yellow]    - Show detailed status")
                console.print(
                    "  [yellow]data[/yellow]      - Show last sensor data & responses"
                )
                console.print("  [yellow]help[/yellow]      - Show this help")
                console.print("  [yellow]quit[/yellow]      - Stop simulation")
                console.print()

            elif cmd.startswith("r "):
                parts = cmd.split()
                if len(parts) == 2:
                    target = parts[1]
                    if target == "all":
                        console.print("[yellow]🔄 Resetting all devices...[/yellow]")
                        # This would need to be implemented with actual websocket connections
                        # For now, just reset local counters
                        for device in devices:
                            device.step_count = 0
                            device.last_update = time.time()
                        console.print("[green]✅ All devices reset locally[/green]")
                    else:
                        # Reset specific device
                        device_id = f"device_{target.zfill(2)}"
                        device = next(
                            (d for d in devices if d.device_id == device_id), None
                        )
                        if device:
                            device.step_count = 0
                            device.last_update = time.time()
                            console.print(
                                f"[green]✅ Device {device_id} reset locally[/green]"
                            )
                        else:
                            console.print(f"[red]❌ Device {device_id} not found[/red]")

            elif cmd == "status":
                table = create_status_table(devices, verbose=True)
                console.print("\n[bold]Current Status:[/bold]")
                console.print(table)
                console.print()

            elif cmd == "data":
                console.print("\n[bold]Last Sensor Data & Responses:[/bold]")
                for device in devices:
                    console.print(f"\n[yellow]Device {device.device_id}:[/yellow]")
                    if device.last_sensor_data:
                        console.print(f"  📊 Sensor: {device.last_sensor_data}")
                    if device.last_response:
                        console.print(f"  📨 Response: {device.last_response}")
                console.print()

            elif cmd == "reset_backend":
                console.print(
                    "[yellow]🔄 Attempting to reset all devices on backend...[/yellow]"
                )
                # This would require maintaining websocket connections
                console.print("[red]❌ Backend reset not implemented yet[/red]")

            elif cmd == "quit" or cmd == "q":
                console.print("[yellow]🛑 Stopping simulation...[/yellow]")
                break

        except KeyboardInterrupt:
            break
        except Exception as e:
            console.print(f"[red]❌ Command error: {e}[/red]")


def create_status_table(devices, verbose=False):
    """Create a rich table showing device status"""
    table = Table(title="📊 Multi-Device Step Detection Simulation")
    table.add_column("Device ID", style="cyan", no_wrap=True)
    table.add_column("Step Count", style="green", justify="right")
    table.add_column("Data Sent", style="yellow", justify="right")
    table.add_column("Errors", style="red", justify="right")
    table.add_column("Last Update", style="magenta", justify="right")
    table.add_column("Status", style="blue")

    for device in devices:
        elapsed = time.time() - device.last_update
        if device.is_active():
            status = "🟢 Active"
        elif device.connected and elapsed < 15:
            status = "🟡 Idle"
        elif device.connected:
            status = "🟠 Slow"
        else:
            status = "🔴 Disconnected"

        last_update_str = time.strftime("%H:%M:%S", time.localtime(device.last_update))

        table.add_row(
            device.device_id,
            str(device.step_count),
            str(device.total_sent),
            str(device.errors),
            last_update_str,
            status,
        )

    return table


async def display_status(devices, console, verbose=False, interval=0.5):
    """Display real-time status using rich live display"""
    with Live(console=console, refresh_per_second=2) as live:
        while True:
            table = create_status_table(devices, verbose)
            live.update(table)
            await asyncio.sleep(interval)


async def main():
    console = Console()

    parser = argparse.ArgumentParser(
        description="Simulate multi-device step detection with realistic walking data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Simulate 5 devices (local by default)
  %(prog)s --devices 10       # Simulate 10 devices
  %(prog)s --deployment local # Use localhost explicitly
  %(prog)s --deployment modal # Use Modal deployment
  %(prog)s --verbose          # Show detailed information
  %(prog)s --host 192.168.1.100 --port 8080  # Custom host/port
  %(prog)s --url wss://your-app.modal.run/ws/realtime  # Custom URL
        """,
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=5,
        help="Number of devices to simulate (default: 5)",
    )
    parser.add_argument(
        "--deployment",
        choices=["local", "modal"],
        help="Deployment environment (sets URL automatically)",
    )
    parser.add_argument(
        "--host", default="localhost", help="WebSocket host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="WebSocket port (default: 8000)"
    )
    parser.add_argument(
        "--url",
        help="Full WebSocket URL (overrides host/port and deployment). Use wss:// for secure connections",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Simulation duration in seconds (0 = run until interrupted)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed information including response times and session IDs",
    )

    args = parser.parse_args()

    # Print header
    console.print("\n🚀 [bold green]Multi-Device Step Detection Simulator[/bold green]")
    console.print(f"📱 Simulating [cyan]{args.devices}[/cyan] devices")

    # Determine WebSocket URL
    if args.url:
        base_uri = args.url.rstrip("/")
        display_url = base_uri
    elif args.deployment == "modal":
        base_uri = (
            "wss://nyu-vision--step-detection-app-fastapi-app.modal.run/ws/realtime"
        )
        display_url = base_uri
    elif args.deployment == "local":
        base_uri = f"ws://{args.host}:{args.port}/ws/realtime"
        display_url = base_uri
    else:
        # Default to local if no deployment specified
        base_uri = f"ws://{args.host}:{args.port}/ws/realtime"
        display_url = base_uri

    console.print(
        f"🌐 Connecting to [blue]{display_url}[/blue] (session-based multi-user)"
    )
    console.print("🎯 Generating realistic walking sensor data\n")

    devices = []
    tasks = []

    # Create device simulators
    for i in range(1, args.devices + 1):
        device_id = f"device_{i:02d}"  # Zero-padded for better sorting
        uri = f"{base_uri}?user_id={device_id}"
        device = DeviceSimulator(device_id, uri)
        devices.append(device)
        tasks.append(device.simulate())

    # Add status display task
    tasks.append(display_status(devices, console, args.verbose))

    # Start keyboard input handler in background thread
    console.print("💡 [bold]Interactive Commands Available:[/bold]")
    console.print("   Type [yellow]'help'[/yellow] for available commands")
    console.print("   Type [yellow]'data'[/yellow] to see sensor data & responses")
    console.print("   Press Ctrl+C to stop\n")

    # Start keyboard input handler in background thread
    keyboard_thread = threading.Thread(
        target=keyboard_input_handler, args=(devices, console), daemon=True
    )
    keyboard_thread.start()

    try:
        if args.duration > 0:
            # Run for specified duration
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=args.duration)
            console.print(f"\n⏱️  Simulation completed after {args.duration} seconds")
        else:
            # Run until interrupted
            await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        console.print("\n🛑 Simulation stopped by user")
    except asyncio.TimeoutError:
        console.print(f"\n⏱️  Simulation completed after {args.duration} seconds")
    except Exception as e:
        console.print(f"\n❌ Simulation error: {e}")

    # Final summary
    console.print("\n📈 [bold]Final Summary:[/bold]")
    final_table = create_status_table(devices, args.verbose)
    console.print(final_table)


if __name__ == "__main__":
    asyncio.run(main())
