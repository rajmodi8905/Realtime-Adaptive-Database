const API_BASE = '';

async function post(url, body = {}) {
  const res = await fetch(`${API_BASE}${url}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function get(url) {
  const res = await fetch(`${API_BASE}${url}`);
  return res.json();
}

export const api = {
  bootstrap: (count) => post('/api/bootstrap', { record_count: count }),
  session: () => get('/api/session'),
  entities: () => get('/api/entities'),
  entity: (name, limit = 50, offset = 0) =>
    get(`/api/entities/${encodeURIComponent(name)}?limit=${limit}&offset=${offset}`),
  schema: () => get('/api/schema'),
  schemaplan: () => get('/api/schema/plan'),
  crud: (body) => post('/api/crud', body),
  queryPreview: (q) => post('/api/query/preview', q),
  queryExecute: (q) => post('/api/query/execute', q),
  acidStreamUrl: (prop) => `${API_BASE}/api/acid/stream/${prop}`,
  acidStreamAllUrl: () => `${API_BASE}/api/acid/stream-all`,
};
