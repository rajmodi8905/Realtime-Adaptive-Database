import { useState, useEffect, useCallback } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer } from 'recharts'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

const MODES = [
  { key: 'read',   icon: '📖', label: 'Read',       desc: 'Benchmark query/read performance' },
  { key: 'create', icon: '✏️', label: 'Create',     desc: 'Benchmark record insertion speed' },
  { key: 'update', icon: '🔄', label: 'Update',     desc: 'Benchmark field update latency' },
  { key: 'delete', icon: '🗑️', label: 'Delete',     desc: 'Benchmark record deletion speed' },
  { key: 'custom', icon: '🎛️', label: 'Custom Mix', desc: 'User-defined operation ratio' },
]

export default function PerformanceBenchmark() {
  const [results, setResults] = useState([])
  const [running, setRunning] = useState(false)
  const [selectedMode, setSelectedMode] = useState('read')
  const [config, setConfig] = useState({
    label: '',
    iterations: 10,
    warmup: 2,
  })
  const [customMix, setCustomMix] = useState({ read: 10, create: 5, update: 0, delete: 0 })
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

  const totalMix = Object.values(customMix).reduce((a, b) => a + b, 0)

  async function runBenchmark() {
    setRunning(true)
    try {
      const modeLabel = MODES.find(m => m.key === selectedMode)?.label || selectedMode
      const payload = {
        label: config.label || `${modeLabel} Benchmark @ ${new Date().toLocaleTimeString()}`,
        iterations: config.iterations,
        warmup: config.warmup,
        mode: selectedMode,
      }
      if (selectedMode === 'custom') {
        payload.mix = { ...customMix }
        payload.iterations = 1  // custom mix defines its own counts
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

  let breakdownData = []
  if (latest && latest.results?.avg_breakdown_ms) {
    breakdownData = [{
      name: 'Timings (ms)',
      'Metadata Lookup': latest.results.avg_breakdown_ms.metadata_lookup_ms || 0,
      'Query Plan': latest.results.avg_breakdown_ms.query_plan_ms || 0,
      'SQL Execution': latest.results.avg_breakdown_ms.sql_ms || 0,
      'Mongo Execution': latest.results.avg_breakdown_ms.mongo_ms || 0,
      'Merging': latest.results.avg_breakdown_ms.merge_ms || 0,
    }]
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Performance Benchmark</h1>
      <p className="view-subtitle">Run targeted benchmarks to measure CRUD throughput and latency under load</p>

      {/* Mode Selector */}
      <div className="card">
        <div className="card-title">Benchmark Mode</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 12, marginBottom: 16 }}>
          {MODES.map(m => (
            <button
              key={m.key}
              onClick={() => setSelectedMode(m.key)}
              style={{
                padding: '16px 12px',
                background: selectedMode === m.key
                  ? 'linear-gradient(135deg, rgba(59,130,246,0.15), rgba(139,92,246,0.15))'
                  : 'var(--bg-elevated)',
                border: selectedMode === m.key
                  ? '2px solid var(--accent-1)'
                  : '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                cursor: 'pointer',
                textAlign: 'center',
                transition: 'all 0.2s ease',
              }}
            >
              <div style={{ fontSize: 24, marginBottom: 6 }}>{m.icon}</div>
              <div style={{
                fontSize: 13, fontWeight: 700, color: selectedMode === m.key ? 'var(--accent-1)' : 'var(--text-primary)',
                marginBottom: 4,
              }}>{m.label}</div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.3 }}>{m.desc}</div>
            </button>
          ))}
        </div>

        {/* Custom Mix Panel */}
        {selectedMode === 'custom' && (
          <div style={{
            padding: 16,
            background: 'rgba(139,92,246,0.05)',
            border: '1px solid rgba(139,92,246,0.2)',
            borderRadius: 'var(--radius-sm)',
            marginBottom: 16,
          }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--accent-3)', marginBottom: 12, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Custom Operation Mix
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
              {['read', 'create', 'update', 'delete'].map(op => (
                <div key={op} className="form-group" style={{ margin: 0 }}>
                  <label style={{ fontSize: 11, textTransform: 'uppercase', fontWeight: 700 }}>
                    {op === 'read' ? '📖' : op === 'create' ? '✏️' : op === 'update' ? '🔄' : '🗑️'} {op}
                  </label>
                  <input
                    className="input"
                    type="number"
                    min={0}
                    max={200}
                    value={customMix[op]}
                    onChange={e => setCustomMix(p => ({ ...p, [op]: Math.max(0, +e.target.value || 0) }))}
                  />
                </div>
              ))}
            </div>
            <div style={{ marginTop: 10, fontSize: 12, color: 'var(--text-muted)' }}>
              Total operations: <b style={{ color: 'var(--text-primary)' }}>{totalMix}</b>
              {totalMix > 0 && (
                <span> — {Object.entries(customMix).filter(([, v]) => v > 0).map(([k, v]) => `${v} ${k}`).join(' + ')}</span>
              )}
            </div>
          </div>
        )}

        {/* Config */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
          <div className="form-group">
            <label>Label</label>
            <input className="input" placeholder="Optional label" value={config.label} onChange={e => setConfig(p => ({ ...p, label: e.target.value }))} />
          </div>
          {selectedMode !== 'custom' && (
            <div className="form-group">
              <label>Iterations</label>
              <input className="input" type="number" min={1} max={100} value={config.iterations} onChange={e => setConfig(p => ({ ...p, iterations: +e.target.value }))} />
            </div>
          )}
          <div className="form-group">
            <label>Warmup Rounds</label>
            <input className="input" type="number" min={0} max={20} value={config.warmup} onChange={e => setConfig(p => ({ ...p, warmup: +e.target.value }))} />
          </div>
        </div>
        <button
          className="btn btn-primary"
          onClick={runBenchmark}
          disabled={running || (selectedMode === 'custom' && totalMix === 0)}
          style={{ marginTop: 8 }}
        >
          {running ? '⏳ Running…' : `🚀 Run ${MODES.find(m => m.key === selectedMode)?.label || ''} Benchmark`}
        </button>
      </div>

      {/* Latest result */}
      {latest && (
        <div className="card mt-16">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <div className="card-title" style={{ margin: 0 }}>{latest.label || 'Latest Result'}</div>
            {latest.config?.mode && (
              <span style={{
                fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                padding: '4px 10px', borderRadius: 'var(--radius-full)',
                background: 'rgba(59,130,246,0.15)', color: 'var(--accent-1)',
                letterSpacing: '0.06em',
              }}>
                {latest.config.mode}
              </span>
            )}
          </div>

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

          {/* Op counts (for custom mix) */}
          {latest.results?.op_counts && Object.keys(latest.results.op_counts).length > 1 && (
            <div style={{ marginTop: 16, padding: 12, background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
              <h4 style={{ marginBottom: 10, fontSize: 12, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-muted)' }}>Operation Distribution</h4>
              <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                {Object.entries(latest.results.op_counts).map(([op, count]) => (
                  <div key={op} style={{
                    padding: '8px 14px',
                    background: op === 'read' ? 'rgba(59,130,246,0.1)' : op === 'create' ? 'rgba(16,185,129,0.1)' : op === 'update' ? 'rgba(245,158,11,0.1)' : 'rgba(239,68,68,0.1)',
                    border: `1px solid ${op === 'read' ? 'rgba(59,130,246,0.25)' : op === 'create' ? 'rgba(16,185,129,0.25)' : op === 'update' ? 'rgba(245,158,11,0.25)' : 'rgba(239,68,68,0.25)'}`,
                    borderRadius: 'var(--radius-full)',
                    fontSize: 12, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace",
                  }}>
                    <span style={{ textTransform: 'uppercase' }}>{op}</span>: {count}
                  </div>
                ))}
              </div>
            </div>
          )}

          {latest.results?.avg_breakdown_ms && (
            <div style={{ marginTop: 24, paddingTop: 12, borderTop: '1px solid var(--border-subtle)' }}>
              <h4 style={{ marginBottom: 16 }}>Execution Pipeline Breakdown (Average ms)</h4>
              <div style={{ width: '100%', height: 120 }}>
                <ResponsiveContainer>
                  <BarChart data={breakdownData} layout="vertical" margin={{ top: 0, right: 30, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
                    <XAxis type="number" stroke="#ccc" />
                    <YAxis dataKey="name" type="category" stroke="#ccc" hide />
                    <RechartsTooltip contentStyle={{ backgroundColor: '#111', border: '1px solid #333' }} />
                    <Legend />
                    <Bar dataKey="Metadata Lookup" stackId="a" fill="#06b6d4" />
                    <Bar dataKey="Query Plan" stackId="a" fill="#3b82f6" />
                    <Bar dataKey="SQL Execution" stackId="a" fill="#10b981" />
                    <Bar dataKey="Mongo Execution" stackId="a" fill="#f59e0b" />
                    <Bar dataKey="Merging" stackId="a" fill="#ef4444" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {latest.results?.planned_queries && (
            <div style={{ marginTop: 16, padding: 12, background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
              <h4 style={{ marginBottom: 12, fontSize: 14 }}>Planned Queries (First Iteration)</h4>
              {latest.results.planned_queries.sql?.length > 0 && (
                <div style={{ marginBottom: 12 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: '#10b981' }}>SQL ({latest.results.planned_queries.sql.length})</span>
                  <pre style={{ fontSize: 11, padding: 8, background: '#111', borderRadius: 4, overflowX: 'auto', marginTop: 4 }}>
                    {JSON.stringify(latest.results.planned_queries.sql, null, 2)}
                  </pre>
                </div>
              )}
              {latest.results.planned_queries.mongo?.length > 0 && (
                <div>
                  <span style={{ fontSize: 12, fontWeight: 700, color: '#f59e0b' }}>MongoDB ({latest.results.planned_queries.mongo.length})</span>
                  <pre style={{ fontSize: 11, padding: 8, background: '#111', borderRadius: 4, overflowX: 'auto', marginTop: 4 }}>
                    {JSON.stringify(latest.results.planned_queries.mongo, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          )}
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
                  <th className="datatable-th">Mode</th>
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
                    <td className="datatable-td">
                      <span style={{
                        fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                        padding: '2px 8px', borderRadius: 'var(--radius-full)',
                        background: 'rgba(59,130,246,0.1)', color: 'var(--accent-1)',
                      }}>
                        {r.config?.mode || 'std'}
                      </span>
                    </td>
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
            <p>No benchmark results yet. Select a mode above and click "Run Benchmark" to start.</p>
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
