import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function PerformanceBenchmark() {
  const [results, setResults] = useState([])
  const [running, setRunning] = useState(false)
  const [config, setConfig] = useState({
    label: '',
    iterations: 10,
    warmup: 2,
  })
  const toast = useToast()

  const fetchResults = useCallback(async () => {
    try {
      const res = await api.benchmarkResults()
      if (res.success) setResults(res.data || [])
    } catch (err) {
      // silent
    }
  }, [])

  useEffect(() => { fetchResults() }, [fetchResults])

  async function runBenchmark() {
    setRunning(true)
    try {
      const payload = {
        label: config.label || `Benchmark @ ${new Date().toLocaleTimeString()}`,
        iterations: config.iterations,
        warmup: config.warmup,
        queries: [{ operation: 'read', filters: {} }],
      }
      const res = await api.benchmarkRun(payload)
      if (res.success) {
        toast('Benchmark complete', 'success')
        setResults(prev => [res.data, ...prev])
      } else {
        toast(res.error || 'Benchmark failed', 'error')
      }
    } catch (err) {
      toast('Benchmark failed', 'error')
    } finally {
      setRunning(false)
    }
  }

  const latest = results[0]

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Performance Benchmark</h1>
      <p className="view-subtitle">Run on-demand benchmarks to measure query throughput and latency under load</p>

      {/* Config */}
      <div className="card">
        <div className="card-title">Benchmark Configuration</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
          <div className="form-group">
            <label>Label</label>
            <input className="input" placeholder="Optional label" value={config.label} onChange={e => setConfig(p => ({ ...p, label: e.target.value }))} />
          </div>
          <div className="form-group">
            <label>Iterations</label>
            <input className="input" type="number" min={1} max={100} value={config.iterations} onChange={e => setConfig(p => ({ ...p, iterations: +e.target.value }))} />
          </div>
          <div className="form-group">
            <label>Warmup Rounds</label>
            <input className="input" type="number" min={0} max={20} value={config.warmup} onChange={e => setConfig(p => ({ ...p, warmup: +e.target.value }))} />
          </div>
        </div>
        <button className="btn btn-primary" onClick={runBenchmark} disabled={running} style={{ marginTop: 8 }}>
          {running ? '⏳ Running…' : '🚀 Run Benchmark'}
        </button>
      </div>

      {/* Latest result */}
      {latest && (
        <div className="card mt-16">
          <div className="card-title">{latest.label || 'Latest Result'}</div>
          <div className="telemetry-chips">
            <Chip label="Runs" value={latest.results?.total_runs ?? 0} />
            <Chip label="Errors" value={latest.results?.errors ?? 0} variant={latest.results?.errors > 0 ? 'error' : ''} />
            <Chip label="Avg" value={`${latest.results?.avg_ms ?? 0}ms`} />
            <Chip label="P50" value={`${latest.results?.p50_ms ?? 0}ms`} />
            <Chip label="P95" value={`${latest.results?.p95_ms ?? 0}ms`} />
            <Chip label="P99" value={`${latest.results?.p99_ms ?? 0}ms`} />
            <Chip label="QPS" value={latest.results?.throughput_qps ?? 0} />
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 12, marginTop: 12 }}>
            <StatBox label="Min" value={`${latest.results?.min_ms ?? 0}ms`} />
            <StatBox label="Max" value={`${latest.results?.max_ms ?? 0}ms`} />
            <StatBox label="Warmup" value={latest.config?.warmup ?? 0} />
            <StatBox label="Iterations" value={latest.config?.iterations ?? 0} />
          </div>
        </div>
      )}

      {/* Historical results */}
      {results.length > 1 && (
        <div className="card mt-16">
          <div className="card-title">Historical Results</div>
          <div className="datatable-scroll">
            <table className="datatable">
              <thead>
                <tr>
                  <th className="datatable-th">Label</th>
                  <th className="datatable-th">Time</th>
                  <th className="datatable-th">Runs</th>
                  <th className="datatable-th">Avg (ms)</th>
                  <th className="datatable-th">P95 (ms)</th>
                  <th className="datatable-th">QPS</th>
                  <th className="datatable-th">Errors</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr key={i} className="datatable-row">
                    <td className="datatable-td" style={{ fontWeight: 600 }}>{r.label || '—'}</td>
                    <td className="datatable-td">{r.timestamp_iso?.replace('T', ' ') || '—'}</td>
                    <td className="datatable-td">{r.results?.total_runs ?? 0}</td>
                    <td className="datatable-td">{r.results?.avg_ms ?? 0}ms</td>
                    <td className="datatable-td">{r.results?.p95_ms ?? 0}ms</td>
                    <td className="datatable-td">{r.results?.throughput_qps ?? 0}</td>
                    <td className="datatable-td" style={{ color: (r.results?.errors ?? 0) > 0 ? 'var(--danger)' : 'var(--success)' }}>
                      {r.results?.errors ?? 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {results.length === 0 && !running && (
        <div className="card mt-16">
          <div className="placeholder">
            <span className="placeholder-icon">🏎️</span>
            <p>No benchmark results yet. Click "Run Benchmark" to start.</p>
          </div>
        </div>
      )}
    </motion.div>
  )
}

function Chip({ label, value, variant = '' }) {
  return (
    <div className={`telemetry-chip ${variant}`}>
      <span className="chip-label">{label}</span>
      <span className="chip-value">{value}</span>
    </div>
  )
}

function StatBox({ label, value }) {
  return (
    <div style={{
      padding: '14px 16px',
      background: 'var(--bg-elevated)',
      border: '1px solid var(--border-subtle)',
      borderRadius: 'var(--radius-sm)',
      textAlign: 'center',
    }}>
      <div style={{
        fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: 6,
      }}>{label}</div>
      <div style={{
        fontSize: 18, fontWeight: 800, fontFamily: "'JetBrains Mono', monospace",
        color: 'var(--text-primary)',
      }}>{value}</div>
    </div>
  )
}
