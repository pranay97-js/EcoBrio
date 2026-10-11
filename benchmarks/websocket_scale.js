import ws from 'k6/ws';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const broadcastLatency = new Trend('ecobrio_ws_broadcast_latency_ms');
export const messagesReceived = new Counter('ecobrio_ws_messages_received_total');

export const options = {
  scenarios: {
    ws_subscribers: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '5s', target: 50 },
        { duration: '10s', target: 100 },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    // PRD Target: Live Broadcast Latency < 15ms
    ecobrio_ws_broadcast_latency_ms: ['p(95)<15', 'p(99)<25'],
  },
};

const WS_URL = __ENV.WS_URL || 'ws://localhost:8080/ws/incidents';

export default function () {
  const clientId = `k6-client-${__VU}-${Date.now()}`;
  const url = `${WS_URL}?client_id=${clientId}`;

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      // Keep connection open to listen for broadcasts
    });

    socket.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.timestamp) {
          const sentTime = new Date(msg.timestamp).getTime();
          const latency = Date.now() - sentTime;
          if (latency >= 0 && latency < 5000) {
            broadcastLatency.add(latency);
          }
        }
        messagesReceived.add(1);
      } catch (e) {
        // Ignored
      }
    });

    socket.on('error', (err) => {
      console.error(`WebSocket client ${clientId} error:`, err);
    });

    socket.setTimeout(() => {
      socket.close();
    }, 15000);
  });

  check(res, {
    'websocket connected successfully': (r) => r && r.status === 101,
  });
}
