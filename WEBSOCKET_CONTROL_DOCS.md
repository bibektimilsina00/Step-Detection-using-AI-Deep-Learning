# WebSocket Control Documentation - Multi-User Support

## WebSocket Endpoint

```
ws://localhost:8000/ws/realtime
```

## 🎯 Multi-User Features

- ✅ **Isolated Sessions**: Each connection gets a unique session ID
- ✅ **Independent Step Counting**: Users don't interfere with each other
- ✅ **Individual Timeouts**: Each user has their own 5-minute timeout
- ✅ **Session Statistics**: Track performance per user and server-wide

## Control Actions

### 1. Stop WebSocket (Complete Shutdown)

**Send this to immediately close the connection:**

```json
{
  "action": "stop"
}
```

**Response before connection closes:**

```json
{
  "type": "stop_response",
  "status": "success",
  "session_id": "abc123-def456-...",
  "message": "Step detection stopped. WebSocket connection will be closed.",
  "session_stats": {
    "duration": 123.45,
    "total_requests": 100
  },
  "timestamp": "1234567890.123"
}
```

_Connection closes immediately after this response._

### 2. Reset Step Counter (Keep Connection Open)

**Send this to reset steps but continue detection:**

```json
{
  "action": "reset"
}
```

**Response:**

```json
{
  "type": "reset_response",
  "status": "success",
  "message": "Step counter has been reset",
  "total_steps": 0,
  "timestamp": "1234567890.123"
}
```

_Connection remains open for continued detection._

### 3. Automatic Timeout

**Connection automatically closes after 5 minutes of inactivity.**

**Timeout response:**

```json
{
  "type": "timeout_response",
  "status": "timeout",
  "message": "Connection closed due to 5 minutes of inactivity",
  "timestamp": "1234567890.123"
}
```

## Usage Examples

### JavaScript

```javascript
// Stop the WebSocket
websocket.send(JSON.stringify({ action: "stop" }));

// Reset step counter
websocket.send(JSON.stringify({ action: "reset" }));
```

### Flutter/Dart

```dart
// Stop the WebSocket
channel.sink.add(jsonEncode({"action": "stop"}));

// Reset step counter
channel.sink.add(jsonEncode({"action": "reset"}));
```

## Important Notes

- **Stop** = Closes connection completely
- **Reset** = Resets counter but keeps connection alive
- **Timeout** = Auto-closes after 5 minutes of no activity
- Always handle the response message before connection closes
