# ECOBrio API Specifications

All requests use JSON request/response formats. State-mutating operations require `X-Request-ID` and `X-Idempotency-Key` headers.

---

## 1. System Health & Observability

### `GET /healthz`
Basic container process liveness check.
* **Status:** `200 OK`
```json
{
  "status": "UP",
  "service": "ecobrio"
}
```

### `GET /readyz`
Full dependency readiness check verifying PostgreSQL 16 and Redis 7 ping states.
* **Status:** `200 OK` (when all dependencies healthy) or `503 Service Unavailable`
```json
{
  "status": "UP",
  "timestamp": "2026-10-10T14:30:00Z",
  "components": {
    "postgres": "UP",
    "redis": "UP"
  }
}
```

### `GET /metrics`
Exposes standard Prometheus metrics in text format for scraper discovery.

---

## 2. Collection Hubs & Capacity Slots

### `GET /api/v1/hubs`
Query nearby collection hubs by latitude, longitude, and radius.
* **Query Parameters:**
  - `lat`: float64 (e.g., `51.5074`)
  - `lon`: float64 (e.g., `-0.1278`)
  - `radius`: float64 in kilometers (default: `10.0`)

### `GET /api/v1/hubs/{id}/slots`
List available drop-off capacity slots for a hub.

### `POST /api/v1/bookings`
Idempotent slot reservation attempt.
* **Headers:**
  - `X-Idempotency-Key: <UUID>`
* **Request Body:**
```json
{
  "slot_id": "s1111111-1111-1111-1111-111111111111",
  "citizen_id": "11111111-1111-1111-1111-111111111111",
  "stream_type": "EWASTE"
}
```
* **Responses:**
  - `201 Created`: Slot confirmed.
  - `409 Conflict`: Slot capacity exhausted (0% overselling guarantee).

---

## 3. Credit Clearinghouse & Ledger

### `POST /api/v1/submissions`
Register material surrender via QR code scan.

### `POST /api/v1/submissions/{id}/inspection`
Collector inspection pass/fail transition.

### `GET /api/v1/credits/me`
Retrieve authenticated citizen's points balance and immutable transaction ledger history.

---

## 4. Geospatial Incidents & AI Triage

### `POST /api/v1/triage`
Submit unstructured citizen narrative for KachraSeth AI extraction and asynchronous queue dispatch.
```json
{
  "citizen_text": "Discarded lithium batteries leaking at Central Station exit 3",
  "coordinates": [51.5074, -0.1278]
}
```

### `GET /ws/incidents`
Persistent WebSocket connection for real-time incident updates filtered by viewport bounding box.
