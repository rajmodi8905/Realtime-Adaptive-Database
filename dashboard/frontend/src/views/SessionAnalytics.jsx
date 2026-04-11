import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function SessionAnalytics() {
  const [session, setSession] = useState(null)
  const [metrics, setMetrics] = useState(null)
  const [history, setHistory] = useState(null)
  const [loading, setLoading] = useState(false)
  const toast = useToast()

  const fetchAll = useCallback(async () => {
    setLoading(true)
    try {
      const [sessRes, metRes, histRes] = await Promise.all([
        api.session(),
        api.metrics(),
        api.queryHistory(1, 10),
      ])
      if (sessRes.success) setSession(sessRes.data)
      if (metRes.success) setMetrics(metRes.data)
      if (histRes.success) setHistory(histRes.data)
    } catch (err) {
      toast('Failed to load session data', 'error')
    } finally {
      setLoading(false)
    }
  }, [toast])

  useEffect(() => { fetchAll() }, [fetchAll])

  const w = metrics?.window || {}
  const startTime = session?.start_time ? new Date(session.start_time * 1000).toLocaleString() : '—'

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Session Analytics</h1>
      <p className="view-subtitle">Comprehensive session overview with performance insights</p>

      <div className="history-actions">
        <button className="btn btn-ghost btn-sm" onClick={fetchAll} disabled={loading}>↻ Refresh</button>
      </div>

      {/* Session info */}
      <div className="card">
        <div className="card-title">Session Info</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16 }}>
          <InfoRow label="Session Start" value={startTime} />
          <InfoRow label="MySQL" value={session?.mysql_status || '—'} accent={session?.mysql_status === 'connected' ? 'success' : 'error'} />
          <InfoRow label="MongoDB" value={session?.mongodb_status || '—'} accent={session?.mongodb_status === 'connected' ? 'success' : 'error'} />
          <InfoRow label="Registered Entities" value={session?.total_entities ?? '—'} />
          <InfoRow label="Registration" value={session?.registration_name || '—'} />
        </div>
      </div>

      {/* Performance summary */}
      <div className="card mt-16">
        <div className="card-title">Performance Summary</div>
        <div className="telemetry-chips">
          <Chip label="Total Queries" value={metrics?.total_queries ?? 0} />
          <Chip label="Errors" value={metrics?.total_errors ?? 0} variant={metrics?.total_errors > 0 ? 'error' : ''} />
          <Chip label="Avg Latency" value={`${w.avg_latency_ms ?? 0}ms`} />
          <Chip label="P95" value={`${w.p95_latency_ms ?? 0}ms`} />
          <Chip label="QPS" value={w.throughput_qps ?? 0} />
          <Chip label="Error Rate" value={`${w.error_rate ?? 0}%`} variant={w.error_rate > 5 ? 'error' : ''} />
        </div>

        {/* Operations */}
        {metrics?.operations && Object.keys(metrics.operations).length > 0 && (
          <div style={{ marginTop: 16 }}>
            <div style={{
              fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
              letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: 8,
            }}>Operations Mix</div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {Object.entries(metrics.operations).map(([op, data]) => (
                <div key={op} style={{
                  padding: '8px 14px',
                  background: 'var(--bg-elevated)',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: 'var(--radius-sm)',
                  fontSize: 11,
                }}>
                  <span style={{ fontWeight: 700, textTransform: 'uppercase', fontSize: 9, color: 'var(--text-muted)' }}>{op}</span>
                  <span style={{ marginLeft: 8, color: 'var(--text-primary)', fontWeight: 800, fontFamily: "'JetBrains Mono', monospace" }}>{data.count}</span>
                  <span style={{ marginLeft: 6, fontSize: 10, color: 'var(--text-muted)' }}>({data.avg_ms}ms avg)</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Entity overview */}
      {session?.entity_names && session.entity_names.length > 0 && (
        <div className="card mt-16">
          <div className="card-title">Registered Entities</div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {session.entity_names.map(name => (
              <span key={name} style={{
                padding: '6px 14px',
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-full)',
                fontSize: 12,
                fontFamily: "'JetBrains Mono', monospace",
                fontWeight: 600,
                color: 'var(--accent-3)',
              }}>{name}</span>
            ))}
          </div>
        </div>
      )}

      {/* Recent activity */}
      {history?.items?.length > 0 && (
        <div className="card mt-16">
          <div className="card-title">Recent Activity (Last 10)</div>
          <div className="datatable-scroll">
            <table className="datatable">
              <thead>
                <tr>
                  <th className="datatable-th">Time</th>
                  <th className="datatable-th">Operation</th>
                  <th className="datatable-th">Status</th>
                  <th className="datatable-th">Duration</th>
                </tr>
              </thead>
              <tbody>
                {history.items.map(item => (
                  <tr key={item.id} className="datatable-row">
                    <td className="datatable-td">{item.timestamp_iso?.replace('T', ' ') || '—'}</td>
                    <td className="datatable-td" style={{ fontWeight: 700, textTransform: 'uppercase', fontSize: 10 }}>{item.operation}</td>
                    <td className="datatable-td">
                      <span className={`history-status ${item.status}`}>
                        {item.status === 'error' ? '✗' : item.status === 'preview' ? '👁' : '✓'} {item.status}
                      </span>
                    </td>
                    <td className="datatable-td">{item.duration_ms < 1 ? '<1ms' : `${Math.round(item.duration_ms)}ms`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Empty state */}
      {!session && !loading && (
        <div className="card mt-16">
          <div className="placeholder">
            <span className="placeholder-icon">📊</span>
            <p>No session data available. Bootstrap the system first.</p>
          </div>
        </div>
      )}
    </motion.div>
  )
}

function InfoRow({ label, value, accent }) {
  const colorMap = { success: 'var(--success)', error: 'var(--danger)' }
  return (
    <div style={{
      padding: '12px 16px',
      background: 'var(--bg-elevated)',
      border: '1px solid var(--border-subtle)',
      borderRadius: 'var(--radius-sm)',
    }}>
      <div style={{
        fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: 4,
      }}>{label}</div>
      <div style={{
        fontSize: 14, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace",
        color: accent ? colorMap[accent] : 'var(--text-primary)',
      }}>{value}</div>
    </div>
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
