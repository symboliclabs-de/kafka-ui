# Kafka UI

A simple web interface for viewing Kafka messages in real-time. Uses WebSocket connection to automatically display incoming messages from the selected topic.

## Getting Started

```bash
# Install dependencies
npm install

# Start the server
npm start
```

The application will be available at: `http://localhost:3000`

## Configuration (Optional)

By default, the application connects to Kafka on `localhost:9092` and runs on port `3000`.

To customize these settings, create a `.env` file:

```bash
# Kafka Server (default: localhost:9092)
KAFKA_HOST=your-kafka-server
KAFKA_PORT=9093

# Web Server Port (default: 3000)
PORT=8080
```
