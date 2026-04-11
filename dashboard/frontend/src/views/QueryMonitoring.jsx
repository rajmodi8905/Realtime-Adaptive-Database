import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function QueryMonitoring() {
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(false)
  const [autoRefresh, setAutoRefresh] = useState(false)
  const toast = useToast()

  const fetchMetrics = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.metrics()
      if (res.success) setMetrics(res.data)
    } catch (err) {
      toast('Failed to load metrics', 'error')
    } finally {
      setLoading(false)
    }
  }, [toast])

  useEffect(() => { fetchMetrics() }, [fetchMetrics])

  // Auto-refresh
  useEffect(() => {
    if (!autoRefresh) return
    const id = setInterval(fetchMetrics, 5000)
    return () => clearInterval(id)
  }, [autoRefresh, fetchMetrics])

  async function handleReset() {
    if (!confirm('Reset all metrics?')) return
    const res = await api.metricsReset()
    if (res.success) {
      toast('Metrics reset', 'success')
      fetchMetrics()
    }
  }

  const w = metrics?.window || {}
  const ops = metrics?.operations || {}

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query Monitoring</h1>
      <p className="view-subtitle">Real-time query performance metrics and operation analysis</p>

      {/* Controls */}
      <div className="history-actions">
        <button className="btn btn-ghost btn-sm" onClick={fetchMetrics} disabled={loading}>↻ Refresh</button>
        <button className={`btn btn-sm ${autoRefresh ? 'btn-primary' : 'btn-ghost'}`} onClick={() => setAutoRefresh(p => !p)}>
          {autoRefresh ? '⏸ Auto 5s' : '▶ Auto-Refresh'}
        </button>
        <button className="btn btn-danger btn-sm" onClick={handleReset}>🗑 Reset</button>
      </div>

      {/* Summary chips */}
      <div className="telemetry-chips">
        <Chip label="Total" value={metrics?.total_queries ?? 0} />
        <Chip label="Window" value={w.count ?? 0} />
        <Chip label="Success" value={w.successes ?? 0} variant="success" />
        <Chip label="Errors" value={w.errors ?? 0} variant={w.errors > 0 ? 'error' : ''} />
        <Chip label="Error %" value={`${w.error_rate ?? 0}%`} variant={w.error_rate > 5 ? 'error' : ''} />
        <Chip label="QPS" value={w.throughput_qps ?? 0} />
      </div>

      {/* Latency card */}
      <div className="card">
        <div className="card-title">Latency Distribution</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 12 }}>
          <StatBox label="Average" value={`${w.avg_latency_ms ?? 0}ms`} />
          <StatBox label="P50" value={`${w.p50_latency_ms ?? 0}ms`} />
          <StatBox label="P95" value={`${w.p95_latency_ms ?? 0}ms`} accent />
          <StatBox label="P99" value={`${w.p99_latency_ms ?? 0}ms`} accent />
          <StatBox label="Min" value={`${w.min_latency_ms ?? 0}ms`} />
          <StatBox label="Max" value={`${w.max_latency_ms ?? 0}ms`} />
          <StatBox label="Lock Wait" value={`${w.avg_lock_wait_ms ?? 0}ms`} />
        </div>
      </div>

      {/* Operations breakdown */}
      <div className="card mt-16">
        <div className="card-title">Operation Breakdown</div>
        {Object.keys(ops).length === 0 ? (
          <div className="placeholder">
            <span className="placeholder-icon">📊</span>
            <p>No operations recorded yet</p>
          </div>
        ) : (
          <div className="datatable-scroll">
            <table className="datatable">
              <thead>
                <tr>
                  <th className="datatable-th">Operation</th>
                  <th className="datatable-th">Count</th>
                  <th className="datatable-th">Avg (ms)</th>
                  <th className="datatable-th">Errors</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(ops).map(([op, data]) => (
                  <tr key={op} className="datatable-row">
                    <td className="datatable-td" style={{ fontWeight: 700, textTransform: 'uppercase', fontSize: 10 }}>{op}</td>
                    <td className="datatable-td">{data.count}</td>
                    <td className="datatable-td">{data.avg_ms}ms</td>
                    <td className="datatable-td">
                      <span style={{ color: data.errors > 0 ? 'var(--danger)' : 'var(--success)' }}>{data.errors}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Time series (text fallback — charts can be added later) */}
      {metrics?.time_series?.length > 0 && (
        <div className="card mt-16">
          <div className="card-title">Time Series (30s buckets)</div>
          <div className="datatable-scroll" style={{ maxHeight: 300 }}>
            <table className="datatable">
              <thead>
                <tr>
                  <th className="datatable-th">Time</th>
                  <th className="datatable-th">Queries</th>
                  <th className="datatable-th">Errors</th>
                  <th className="datatable-th">Avg (ms)</th>
                </tr>
              </thead>
              <tbody>
                {metrics.time_series.slice(-20).reverse().map((b, i) => (
                  <tr key={i} className="datatable-row">
                    <td className="datatable-td">{new Date(b.timestamp * 1000).toLocaleTimeString()}</td>
                    <td className="datatable-td">{b.queries}</td>
                    <td className="datatable-td" style={{ color: b.errors > 0 ? 'var(--danger)' : 'inherit' }}>{b.errors}</td>
                    <td className="datatable-td">{b.avg_ms}ms</td>
                  </tr>
                ))}
              </tbody>
            </table>
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

function StatBox({ label, value, accent = false }) {
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
        color: accent ? 'var(--accent-3)' : 'var(--text-primary)',
      }}>{value}</div>
    </div>
  )
}
