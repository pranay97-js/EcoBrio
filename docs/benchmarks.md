# ECOBrio Performance Benchmarks Protocol

ECOBrio enforces strict separation between **PRD Target Thresholds** and **Empirically Measured Results**.

---

## 1. PRD Targets vs Verification Mechanism

| Metric / Feature | Target Threshold | Verification Mechanism | Status |
| :--- | :--- | :--- | :--- |
| **Peak Booking Throughput** | $\ge 3,000$ Requests/Sec | `k6` load script (500 VUs against localhost) | Target Specification |
| **Reservation API Latency** | $\text{p99} < 40\text{ms}$ | Prometheus histogram `ecobrio_request_duration_seconds` | Target Specification |
| **Concurrency Safety** | $0\%$ Over-allocation | Concurrent `k6` race test against last 5 available slots | Verified Invariant |
| **Durability & Recovery** | $100\%$ byte integrity / Zero loss | Chaos test: `kill -9` during 5k sensor writes + restart recovery | Target Specification |
| **Live Broadcast Latency** | $< 15\text{ms}$ end-to-end | Benchmarked WebSocket client telemetry listener | Target Specification |
| **Code Concurrency Safety** | Zero detected data races | Enforced via `go test -race ./...` in CI pipeline | Active Gate |

---

## 2. Benchmark Execution Guidelines

1. **Hardware & Environment Specification:**
   Always record host CPU, available RAM, OS build, Docker version, and container limits when publishing benchmark results.
2. **Warm-up Period:**
   Run a 30-second steady-state warm-up before capturing latency percentiles (p50, p95, p99).
3. **Contention Isolation:**
   Run pure booking race tests (high contention on shared rows) separately from uniform distribution throughput tests (randomized slot distribution).
