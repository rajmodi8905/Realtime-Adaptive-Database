import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const successfulOperations = new Counter('successful_operations');
const failedOperations = new Counter('failed_operations');
const operationSuccessRate = new Rate('operation_success_rate');
const lockWaitMs = new Trend('lock_wait_ms');
const coordinationOverheadMs = new Trend('coordination_overhead_ms');

// 1. Configure the load test here
export const options = {
  // Simulate 10 concurrent users
  vus: 10,
  // Run the test for 30 seconds
  duration: '30s',
};

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');

// 2. Define what each user does
export default function () {
  // REPLACE THIS URL with your actual FastAPI endpoint for creating/updating a record
  const url = `${BASE_URL}/api/crud`;
  const unique = `${__VU}_${__ITER}_${Date.now()}`;

  // Create a unique test payload WITH all required fields
  const payload = JSON.stringify({
    "operation": "create",
    "records": [
      {
        "username": `bench_${unique}`,
        "event_id": `evt_bench_${unique}`,
        "timestamp": new Date().toISOString(),
        "post": {
          "post_id": `post_bench_${unique}`,
          "title": `Load test title ${unique}`,
          "tags": ["bench", "k6"]
        },
        "device": {
          "device_id": `dev_bench_${unique}`,
          "model": "bench-model",
          "firmware": "1.0.0",
          "type": "test"
        },
        "metrics": {
          "latency_ms": 12.5,
          "battery_pct": 88,
          "signal_quality": "good"
        }
      }
    ]
  });



  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  // 3. Send the request
  const res = http.post(url, payload, params);
  let responseBody = null;
  try {
    responseBody = res.json();
  } catch (_) {
    responseBody = null;
  }

  const isHttpSuccess = res.status === 200 || res.status === 201;
  const isAppCommitted = responseBody?.data?.status === 'committed';
  const isSuccess = isHttpSuccess && isAppCommitted;

  if (isSuccess) {
    successfulOperations.add(1);
  } else {
    failedOperations.add(1);
  }
  operationSuccessRate.add(isSuccess);

  // Track lock wait time from response timings
  const lockWait = responseBody?.data?.timings?.lock_wait_ms || 0;
  if (lockWait >= 0) {
    lockWaitMs.add(lockWait);
  }

  // Approximate transaction coordination overhead as non-engine orchestration time
  const timings = responseBody?.data?.timings || {};
  const coordinationOverhead =
    Number(timings.metadata_lookup_ms || 0) +
    Number(timings.query_plan_ms || 0) +
    Number(timings.merge_ms || 0) +
    Number(timings.lock_wait_ms || 0);
  if (coordinationOverhead >= 0) {
    coordinationOverheadMs.add(coordinationOverhead);
  }

  // Print the error if the HTTP request fails entirely
  if (!isHttpSuccess && __ITER === 0 && __VU === 1) {
    console.log(`SERVER REJECTED REQUEST. Status: ${res.status}. Body: ${res.body}`);
  }

  // Print app-level rollback details even when HTTP status is 200
  if (isHttpSuccess && !isAppCommitted && __ITER === 0 && __VU === 1) {
    console.log(`HTTP SUCCESS BUT APP ROLLED BACK: ${res.body}`);
  }

  // 4. Verify the request succeeded (status 200 or 201)
  check(res, {
    'is status 200 or 201 and committed': () => isSuccess,
  });

  // Short pause between requests (optional, remove for absolute max load)
  sleep(0.1);
}
