import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function SessionAnalytics() {
  const [session, setSession] = useState(null)
  const [metrics, setMetrics] = useState(null)
  const [history, setHistory] = useState(null)
  const [activeUsers, setActiveUsers] = useState(null)
  const [loading, setLoading] = useState(false)
  const toast = useToast()

  const fetchAll = useCallback(async () => {
    setLoading(true)
    try {
      const [sessRes, metRes, histRes, usersRes] = await Promise.all([
        api.session(),
        api.metrics(),
        api.queryHistory(1, 10),
        api.authSessions(),
      ])
      if (sessRes.success) setSession(sessRes.data)
      if (metRes.success) setMetrics(metRes.data)
      if (histRes.success) setHistory(histRes.data)
      if (usersRes.success) setActiveUsers(usersRes.data)
    } catch (err) {
      toast('Failed to load session data', 'error')
    } finally {
      setLoading(false)
    }
  }, [toast])

  useEffect(() => { fetchAll() }, [fetchAll])

  // Auto-refresh active users every 5 seconds
  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const res = await api.authSessions()
        if (res.success) setActiveUsers(res.data)
      } catch { /* silent */ }
    }, 5000)
    return () => clearInterval(timer)
  }, [])

  const w = metrics?.window || {}

  // Derive entity names from sql_tables + mongo_collections
  const allEntities = [
    ...(session?.sql_tables || []),
    ...(session?.mongo_collections || []),
  ]

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Session Analytics</h1>
      <p className="view-subtitle">Comprehensive session overview with performance insights</p>

      <div className="history-actions">
        <button className="btn btn-ghost btn-sm" onClick={fetchAll} disabled={loading}>↻ Refresh</button>
      </div>

      {/* ── Active Users ── */}
      <div className="card active-users-card">
        <div className="card-title">
          <span>Active Users</span>
          <span className="active-users-badge">{activeUsers?.count ?? 0} online</span>
        </div>

        {activeUsers?.sessions?.length > 0 ? (
          <div className="active-users-grid">
            {activeUsers.sessions.map((u, i) => (
              <motion.div
                key={u.username + u.login_time}
                className="active-user-tile"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: i * 0.06 }}
              >
                <div className="active-user-avatar">
                  {u.username[0].toUpperCase()}
                  <span className="active-user-dot" />
                </div>
                <div className="active-user-details">
                  <span className="active-user-name">{u.username}</span>
                  <span className="active-user-meta">
                    {u.duration_display} · {u.ip}
                  </span>
                </div>
              </motion.div>
            ))}
          </div>
        ) : (
          <div className="placeholder" style={{ padding: '24px 0' }}>
            <span className="placeholder-icon">👤</span>
            <p>No active users right now.</p>
          </div>
        )}
      </div>

      {session ? (
        <>
          {/* Database Pipeline Status */}
          <div className="card">
            <div className="card-title">Database Pipeline Status</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16 }}>
              <InfoRow
                label="Schema"
                value={session.schema_name || '—'}
              />
              <InfoRow
                label="Version"
                value={session.version || '—'}
              />
              <InfoRow
                label="Root Entity"
                value={session.root_entity || '—'}
              />
              <InfoRow
                label="MySQL"
                value={session.mysql_connected ? 'Connected' : 'Disconnected'}
                accent={session.mysql_connected ? 'success' : 'error'}
              />
              <InfoRow
                label="MongoDB"
                value={session.mongo_connected ? 'Connected' : 'Disconnected'}
                accent={session.mongo_connected ? 'success' : 'error'}
              />
              <InfoRow
                label="Total Fields"
                value={session.field_count ?? '—'}
              />
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
        </>
      ) : (
        /* Empty state when not bootstrapped */
        !loading && (
          <div className="card mt-16">
            <div className="placeholder">
              <span className="placeholder-icon">📊</span>
              <p>No database pipeline data available. Bootstrap the system first to view query metrics and database status.</p>
            </div>
          </div>
        )
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
