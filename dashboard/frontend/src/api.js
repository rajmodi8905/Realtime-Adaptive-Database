// ── Centralized API Client ──────────────────────────────────────────────────
// Consistent { success, data, error } shape for all endpoints.
// All network/parse errors are normalized into the same shape.

const API_BASE = '';

class ApiError extends Error {
  constructor(message, status = 0, data = null) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.data = data;
  }
}

async function request(method, url, body = undefined) {
  try {
    const opts = {
      method,
      headers: { 'Content-Type': 'application/json' },
      cache: 'no-store',
    };
    if (body !== undefined) opts.body = JSON.stringify(body);

    const res = await fetch(`${API_BASE}${url}`, opts);
    const json = await res.json();

    // Backend already returns { success, data, error } — pass through
    if (typeof json === 'object' && json !== null && 'success' in json) {
      return json;
    }
    // Unexpected shape — wrap it
    return { success: res.ok, data: json, error: res.ok ? null : 'Unexpected response' };
  } catch (err) {
    return { success: false, data: null, error: err.message || 'Network error' };
  }
}

function get(url) { return request('GET', url); }
function post(url, body = {}) { return request('POST', url, body); }

// ── Endpoint mappers ────────────────────────────────────────────────────────

export const api = {
  // Setup
  bootstrap: (count) => post('/api/bootstrap', { record_count: count }),
  bootstrapCustom: (schema, records) => post('/api/bootstrap/custom', { schema, records }),

  // Session
  session: () => get('/api/session'),

  // Entities
  entities: () => get('/api/entities'),
  entity: (name, limit = 50, offset = 0) =>
    get(`/api/entities/${encodeURIComponent(name)}?limit=${limit}&offset=${offset}`),

  // Schema
  schema: () => get('/api/schema'),
  schemaplan: () => get('/api/schema/plan'),

  // CRUD
  crud: (body) => post('/api/crud', body),

  // Query
  queryPreview: (q) => post('/api/query/preview', q),
  queryExecute: (q) => post('/api/query/execute', q),

  // Query History
  queryHistory: (page = 1, limit = 50) => get(`/api/query/history?page=${page}&limit=${limit}`),
  queryHistoryDelete: (id) => post('/api/query/history/delete', { id }),
  queryHistoryClear: () => post('/api/query/history/clear'),
  queryHistoryReplay: (id) => get(`/api/query/history/${id}`),

  // Metrics & Monitoring
  metrics: () => get('/api/metrics'),
  metricsStream: () => `${API_BASE}/api/metrics/stream`,

  // Benchmarks
  benchmarkRun: (config) => post('/api/benchmark/run', config),
  benchmarkResults: () => get('/api/benchmark/results'),

  // ACID
  acidRun: (prop) => post(`/api/acid/run/${prop}`),
  acidRunAll: () => post('/api/acid/run-all'),
  acidStreamUrl: (prop) => `${API_BASE}/api/acid/stream/${prop}`,
  acidStreamAllUrl: () => `${API_BASE}/api/acid/stream-all`,
};

// ── Helpers ─────────────────────────────────────────────────────────────────

/** Unwrap API response — returns data or throws */
export function unwrap(res) {
  if (res.success) return res.data;
  throw new ApiError(res.error || 'Unknown error');
}

/** Safe unwrap — returns [data, error] tuple */
export function safeUnwrap(res) {
  if (res.success) return [res.data, null];
  return [null, res.error || 'Unknown error'];
}

export { ApiError };
