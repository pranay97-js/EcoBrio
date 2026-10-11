import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const walIngestLatency = new Trend('ecobrio_wal_ingest_latency_ms');
export const framesWritten = new Counter('ecobrio_wal_frames_written_total');

export const options = {
  scenarios: {
    telemetry_surge: {
      executor: 'constant-vus',
      vus: 100,
      duration: '15s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<20', 'p(99)<35'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const sensorId = `SMART-BIN-NODE-${(__VU % 20) + 1}`;
  const payload = JSON.stringify({
    sensor_id: sensorId,
    stream_type: 'PLASTICS',
    fill_level_percent: Math.floor(Math.random() * 100),
    battery_millivolts: 3700 - Math.floor(Math.random() * 200),
    temperature_celsius: 25.5 + Math.random() * 5,
    timestamp: new Date().toISOString(),
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/telemetry/ingest`, payload, params);
  const latency = Date.now() - start;

  walIngestLatency.add(latency);

  if (res.status === 201) {
    framesWritten.add(1);
  }

  check(res, {
    'status is 201 created': (r) => r.status === 201,
  });
}
