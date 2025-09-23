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
  "session_id": "abc123-def456-...",
  "message": "Step counter has been reset for this session",
  "total_steps": 0,
  "timestamp": "1234567890.123"
}
```

### 3. Get Session Stats

**Send this to get session and server statistics:**

```json
{
  "action": "stats"
}
```

**Response:**

```json
{
  "type": "stats_response",
  "status": "success",
  "session_id": "abc123-def456-...",
  "user_stats": {
    "session_duration": 123.45,
    "total_requests": 100,
    "last_activity": 1234567890.123
  },
  "server_stats": {
    "active_sessions": 5,
    "total_server_requests": 1234
  },
  "timestamp": "1234567890.123"
}
```

### 4. Session Started (Automatic)

**Received automatically when connection is established:**

```json
{
  "type": "session_started",
  "session_id": "abc123-def456-...",
  "message": "Step detection session initialized successfully",
  "timestamp": "1234567890.123"
}
```

### 5. Step Detection Response (Enhanced)

**Enhanced with session information:**

```json
{
  "type": "step_detection",
  "session_id": "abc123-def456-...",
  "step_detected": true,
  "prediction": {
    /* ... prediction data ... */
  },
  "total_steps": 42,
  "total_predictions": 100,
  "buffer_size": 50,
  "timestamp": "1234567890.123"
}
```

## 🔄 Automatic Behaviors

### Timeout (5 minutes inactivity)

```json
{
  "type": "timeout_response",
  "status": "timeout",
  "session_id": "abc123-def456-...",
  "message": "Connection closed due to 5 minutes of inactivity",
  "timestamp": "1234567890.123"
}
```

## 📊 REST API Endpoints

### Get All Active Sessions

```http
GET /sessions
```

### Get Specific Session Info

```http
GET /sessions/{session_id}
```

### Terminate Session

```http
DELETE /sessions/{session_id}
```

### Health Check

```http
GET /health
```

## 💻 Usage Examples

### JavaScript

```javascript
const ws = new WebSocket("ws://localhost:8000/ws/realtime");

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  if (data.type === "session_started") {
    console.log("Session ID:", data.session_id);
  }

  if (data.type === "step_detection") {
    console.log(`Session ${data.session_id}: ${data.total_steps} steps`);
  }
};

// Stop the WebSocket
ws.send(JSON.stringify({ action: "stop" }));

// Get session stats
ws.send(JSON.stringify({ action: "stats" }));
```

### Flutter/Dart

```dart
final channel = WebSocketChannel.connect(
  Uri.parse('ws://localhost:8000/ws/realtime')
);

channel.stream.listen((message) {
  final data = jsonDecode(message);

  if (data['type'] == 'session_started') {
    print('Session ID: ${data['session_id']}');
  }

  if (data['type'] == 'step_detection') {
    print('Session ${data['session_id']}: ${data['total_steps']} steps');
  }
});

// Stop the WebSocket
channel.sink.add(jsonEncode({"action": "stop"}));

// Get session stats
channel.sink.add(jsonEncode({"action": "stats"}));
```

## 🎉 Multi-User Benefits

- 🔐 **Isolated**: Each user's data is completely separate
- 📊 **Scalable**: Supports multiple concurrent users
- 🎯 **Accurate**: No step count interference between users
- 📈 **Trackable**: Monitor individual and server-wide performance
- 🛡️ **Robust**: Individual timeouts and error handling per session

## Important Notes

- Each WebSocket connection gets a unique `session_id`
- **Stop** = Closes connection and cleans up session
- **Reset** = Resets step counter for that session only
- **Stats** = Get performance metrics for user and server
- **Timeout** = Auto-closes after 5 minutes per session
- All responses include the `session_id` for tracking
