import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

// Custom Prometheus-aligned metrics
export const bookingLatency = new Trend('ecobrio_booking_latency_ms');
export const conflictRate = new Rate('ecobrio_booking_conflicts');
export const successCounter = new Counter('ecobrio_booking_success_total');

export const options = {
  scenarios: {
    peak_throughput: {
      executor: 'constant-vus',
      vus: 500,
      duration: '30s',
      gracefulStop: '5s',
    },
  },
  thresholds: {
    // PRD Target: p99 Latency < 40ms on reservation APIs
    http_req_duration: ['p(99)<40', 'p(95)<25'],
    // PRD Target: Peak Throughput target >= 3,000 RPS
    http_reqs: ['rate>=3000'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const HUB_ID = '11111111-0000-0000-0000-000000000001';
const SLOT_ID = '11111111-0000-0000-0000-000000000001';

export default function () {
  const citizenId = `citizen-vu-${__VU}-${__ITER}`;
  const idempotencyKey = `idemp-${__VU}-${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    slot_id: SLOT_ID,
    hub_id: HUB_ID,
    citizen_id: '11111111-1111-1111-1111-111111111111',
    stream_type: 'PLASTICS',
    quantity: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': idempotencyKey,
    },
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/bookings`, payload, params);
  const latency = Date.now() - start;

  bookingLatency.add(latency);

  const isSuccess = res.status === 201;
  const isConflict = res.status === 409;

  if (isSuccess) {
    successCounter.add(1);
    conflictRate.add(0);
  } else if (isConflict) {
    conflictRate.add(1);
  }

  check(res, {
    'status is 201 or 409': (r) => r.status === 201 || r.status === 409,
    'sub-40ms latency': () => latency < 40,
  });
}
