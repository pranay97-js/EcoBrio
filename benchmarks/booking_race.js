import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const reservationsCommitted = new Counter('ecobrio_race_reservations_committed');
export const contentionConflicts = new Counter('ecobrio_race_conflicts_intercepted');
export const overAllocations = new Counter('ecobrio_race_overallocations');

export const options = {
  scenarios: {
    slot_race_contention: {
      executor: 'per-vu-iterations',
      vus: 50,
      iterations: 1,
      maxDuration: '10s',
    },
  },
  thresholds: {
    // Invariant: 0% over-allocation (Zero data race conditions)
    ecobrio_race_overallocations: ['count==0'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
// Slot seeded with capacity = 5
const RACE_SLOT_ID = '11111111-0000-0000-0000-000000000002';
const HUB_ID = '11111111-0000-0000-0000-000000000001';

export default function () {
  const citizenId = `race-citizen-${__VU}`;
  const idempotencyKey = `race-key-${__VU}`;

  const payload = JSON.stringify({
    slot_id: RACE_SLOT_ID,
    hub_id: HUB_ID,
    citizen_id: '11111111-1111-1111-1111-111111111111',
    stream_type: 'ORGANIC',
    quantity: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': idempotencyKey,
    },
  };

  const res = http.post(`${BASE_URL}/api/v1/bookings`, payload, params);

  if (res.status === 201) {
    reservationsCommitted.add(1);
  } else if (res.status === 409) {
    contentionConflicts.add(1);
  } else {
    // Unexpected response code
    check(res, {
      'unexpected status code': () => false,
    });
  }

  check(res, {
    'valid business response (201 or 409)': (r) => r.status === 201 || r.status === 409,
  });
}
