import { useState, useEffect, useRef } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'
import DataTable from '../components/DataTable'

export default function QueryWorkspace({ replayPayload, onReplayConsumed }) {
  const [input, setInput] = useState(JSON.stringify({ operation: 'read', filters: { username: 'username_1' } }, null, 2))
  const [result, setResult] = useState(null)
  const [resultMeta, setResultMeta] = useState(null)
  const [viewMode, setViewMode] = useState('table') // 'table' | 'json'
  const [loading, setLoading] = useState(false)
  const toast = useToast()
  const inputRef = useRef(null)

  // Handle replay from QueryHistory
  useEffect(() => {
    if (replayPayload) {
      setInput(JSON.stringify(replayPayload, null, 2))
      onReplayConsumed?.()
      // Auto-focus the textarea
      setTimeout(() => inputRef.current?.focus(), 100)
    }
  }, [replayPayload, onReplayConsumed])

  async function preview() {
    setLoading(true)
    const t0 = performance.now()
    try {
      const q = JSON.parse(input)
      const res = await api.queryPreview(q)
      const duration = performance.now() - t0
      if (res.success) {
        setResult(res.data)
        setResultMeta({ type: 'preview', duration, timestamp: new Date().toISOString() })
      } else {
        setResult({ error: res.error })
        setResultMeta({ type: 'preview', duration, status: 'error' })
      }
    } catch (err) {
      setResult({ error: err.message })
      toast('Invalid JSON', 'error')
    } finally {
      setLoading(false)
    }
  }

  async function execute() {
    setLoading(true)
    const t0 = performance.now()
    try {
      const q = JSON.parse(input)
      const res = await api.queryExecute(q)
      const duration = performance.now() - t0
      if (res.success) {
        setResult(res.data)
        setResultMeta({
          type: 'execute',
          duration,
          status: res.data?.status || 'unknown',
          timestamp: new Date().toISOString(),
          rowCount: extractRowCount(res.data),
        })
        toast('Query executed', 'success')
      } else {
        setResult({ error: res.error })
        setResultMeta({ type: 'execute', duration, status: 'error' })
        toast(`Error: ${res.error}`, 'error')
      }
    } catch (err) {
      setResult({ error: err.message })
      toast('Invalid JSON', 'error')
    } finally {
      setLoading(false)
    }
  }

  function extractRowCount(data) {
    if (!data) return 0
    // Try various shapes the result might come in
    const candidates = [data.data, data.sql_result?.data, data.sql_result?.rows, data.rows]
    for (const c of candidates) {
      if (Array.isArray(c)) return c.length
    }
    return 0
  }

  function getTableData() {
    if (!result) return []
    // Navigate to the actual rows array
    const candidates = [result.data, result.sql_result?.data, result.sql_result?.rows, result.rows]
    for (const c of candidates) {
      if (Array.isArray(c) && c.length > 0 && typeof c[0] === 'object') return c
    }
    return []
  }

  const tableData = getTableData()
  const hasTableData = tableData.length > 0

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query Workspace</h1>
      <p className="view-subtitle">Preview and execute JSON-based queries against the hybrid storage</p>

      <div className="card">
        <div className="form-group">
          <label>Query JSON</label>
          <textarea
            ref={inputRef}
            className="input"
            value={input}
            onChange={e => setInput(e.target.value)}
            rows={8}
            placeholder='{"operation": "read", "filters": {"username": "username_1"}}'
          />
        </div>
        <div className="query-actions">
          <button className="btn btn-ghost" onClick={preview} disabled={loading}>
            {loading ? '⏳' : '👁'} Preview Plan
          </button>
          <button className="btn btn-primary" onClick={execute} disabled={loading}>
            {loading ? '⏳' : '▶'} Execute
          </button>
        </div>
      </div>

      {/* Result telemetry */}
      {resultMeta && (
        <div className="telemetry-chips mt-16">
          <div className={`telemetry-chip ${resultMeta.status === 'error' ? 'error' : 'success'}`}>
            <span className="chip-label">Status</span>
            <span className="chip-value">{resultMeta.status || resultMeta.type}</span>
          </div>
          <div className="telemetry-chip">
            <span className="chip-label">Duration</span>
            <span className="chip-value">{Math.round(resultMeta.duration)}ms</span>
          </div>
          {resultMeta.rowCount != null && (
            <div className="telemetry-chip">
              <span className="chip-label">Rows</span>
              <span className="chip-value">{resultMeta.rowCount}</span>
            </div>
          )}
          <div className="telemetry-chip">
            <span className="chip-label">Time</span>
            <span className="chip-value">{resultMeta.timestamp?.split('T')[1]?.slice(0, 8) || '—'}</span>
          </div>
        </div>
      )}

      {/* Result display */}
      {result && (
        <div className="card mt-16">
          {/* Mode toggle */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
            <span className="card-title" style={{ margin: 0 }}>Results</span>
            <div className="result-mode-toggle">
              <button className={`result-mode-btn ${viewMode === 'table' ? 'active' : ''}`} onClick={() => setViewMode('table')}>
                Table
              </button>
              <button className={`result-mode-btn ${viewMode === 'json' ? 'active' : ''}`} onClick={() => setViewMode('json')}>
                JSON
              </button>
            </div>
          </div>

          {viewMode === 'table' && hasTableData ? (
            <DataTable
              data={tableData}
              exportFilename="query_results"
              compact
            />
          ) : (
            <pre className="query-result-pane" style={{ marginTop: 0 }}>
              {JSON.stringify(result, null, 2)}
            </pre>
          )}
        </div>
      )}
    </motion.div>
  )
}
