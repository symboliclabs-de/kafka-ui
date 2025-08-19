require('dotenv').config();
const express = require('express');
const { Kafka } = require('kafkajs');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Statische Dateien servieren
app.use(express.static('public'));

// Kafka Configuration from environment variables
const KAFKA_HOST = process.env.KAFKA_HOST || 'localhost';
const KAFKA_PORT = process.env.KAFKA_PORT || '9092';

// Kafka Setup
const kafka = new Kafka({
  clientId: 'kafka-ui-client',
  brokers: [`${KAFKA_HOST}:${KAFKA_PORT}`]
});

console.log(`Kafka Configuration:`);
console.log(`- Host: ${KAFKA_HOST}`);
console.log(`- Port: ${KAFKA_PORT}`);

let consumer = null;
let messages = [];
let wsClients = new Set();
let currentTopic = null;

// WebSocket Server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('Client connected');
  wsClients.add(ws);
  
  // Sende aktuelle Messages an neuen Client
  ws.send(JSON.stringify({
    type: 'messages',
    data: messages
  }));

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      if (message.type === 'subscribe') {
        const { topic } = message;
        console.log(`Subscribing to topic: ${topic}`);
        
        try {
          // Stop existing consumer if running
          if (consumer) {
            await consumer.disconnect();
            console.log('Disconnected previous consumer');
          }
          
          // Clear previous messages
          messages = [];
          currentTopic = topic;
          
          // Create new consumer
          consumer = kafka.consumer({ groupId: `kafka-ui-group-${Date.now()}` });
          await consumer.connect();
          await consumer.subscribe({ topic, fromBeginning: true });
          
          // Start consuming
          await consumer.run({
            eachMessage: async ({ topic, message }) => {
              const hasTimestamp = message.timestamp !== null && message.timestamp !== undefined;
              console.log(`Received message from ${topic}${hasTimestamp ? `, timestamp: ${message.timestamp}` : ' (no timestamp)'}`);
              
              const kafkaMessage = {
                id: Date.now() + Math.random(),
                topic,
                timestamp: hasTimestamp ? parseInt(message.timestamp) : null,
                receivedAt: Date.now(), // Fallback timestamp when message was received
                key: message.key ? message.key.toString() : null,
                value: message.value ? message.value.toString() : null,
                headers: message.headers
              };

              messages.unshift(kafkaMessage);
              
              if (messages.length > 100) {
                messages = messages.slice(0, 100);
              }

              console.log(`Total messages now: ${messages.length}`);

              broadcast({
                type: 'newMessage',
                data: kafkaMessage
              });
            },
          });
          
          ws.send(JSON.stringify({
            type: 'status',
            message: `Subscribed to topic: ${topic}`
          }));
          
          // Send current (empty) messages
          broadcast({
            type: 'messages',
            data: messages
          });
          
        } catch (error) {
          console.error('Subscribe error:', error);
          ws.send(JSON.stringify({
            type: 'error',
            message: `Failed to subscribe to topic: ${error.message}`
          }));
        }
      }
      
      if (message.type === 'refresh') {
        // Clear messages and send empty array
        messages = [];
        broadcast({
          type: 'messages',
          data: messages
        });
      }
    } catch (error) {
      console.error('WebSocket message error:', error);
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    wsClients.delete(ws);
  });
});

function broadcast(data) {
  wsClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// Kafka connection test
async function testKafkaConnection() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    console.log('Connected to Kafka');
    await admin.disconnect();
  } catch (error) {
    console.error('Kafka connection error:', error);
  }
}

// API Endpoints
app.get('/api/topics', async (req, res) => {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    await admin.disconnect();
    res.json(topics);
  } catch (error) {
    console.error('Error fetching topics:', error);
    res.status(500).json({ error: error.message });
  }
});

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  testKafkaConnection();
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  if (consumer) {
    await consumer.disconnect();
  }
  process.exit(0);
});
