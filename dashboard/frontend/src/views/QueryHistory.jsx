import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function QueryHistory({ onReplay }) {
  const [history, setHistory] = useState({ items: [], total: 0, page: 1, total_pages: 1 })
  const [loading, setLoading] = useState(false)
  const [expandedId, setExpandedId] = useState(null)
  const toast = useToast()

  const fetchHistory = useCallback(async (page = 1) => {
    setLoading(true)
    try {
      const res = await api.queryHistory(page, 50)
      if (res.success) setHistory(res.data)
    } catch (err) {
      toast('Failed to load history', 'error')
    } finally {
      setLoading(false)
    }
  }, [toast])

  useEffect(() => { fetchHistory() }, [fetchHistory])

  async function handleDelete(e, id) {
    e.stopPropagation()
    const res = await api.queryHistoryDelete(id)
    if (res.success) {
      toast('Entry deleted', 'success')
      fetchHistory(history.page)
    } else {
      toast(res.error || 'Delete failed', 'error')
    }
  }

  async function handleClear() {
    if (!confirm('Clear all query history?')) return
    const res = await api.queryHistoryClear()
    if (res.success) {
      toast(`Cleared ${res.data.cleared} entries`, 'success')
      fetchHistory()
    } else {
      toast(res.error || 'Clear failed', 'error')
    }
  }

  function handleReplay(item) {
    if (onReplay && item.payload) {
      onReplay(item.payload)
      toast('Query loaded into workspace — ready to execute', 'info')
    }
  }

  function toggleExpand(id) {
    setExpandedId(prev => prev === id ? null : id)
  }

  function formatDuration(ms) {
    if (ms < 1) return '<1ms'
    if (ms < 1000) return `${Math.round(ms)}ms`
    return `${(ms / 1000).toFixed(2)}s`
  }

  function formatTime(iso) {
    if (!iso) return '—'
    return iso.replace('T', ' ')
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query History</h1>
      <p className="view-subtitle">View, search, and replay past queries with full audit trail</p>

      {/* Stats bar */}
      <div className="telemetry-chips">
        <div className="telemetry-chip">
          <span className="chip-label">Total</span>
          <span className="chip-value">{history.total}</span>
        </div>
        <div className="telemetry-chip">
          <span className="chip-label">Page</span>
          <span className="chip-value">{history.page} / {history.total_pages}</span>
        </div>
      </div>

      {/* Actions */}
      <div className="history-actions">
        <button className="btn btn-ghost btn-sm" onClick={() => fetchHistory(history.page)} disabled={loading}>
          {loading ? '↻ Loading…' : '↻ Refresh'}
        </button>
        <button className="btn btn-danger btn-sm" onClick={handleClear} disabled={loading || history.total === 0}>
          🗑 Clear All
        </button>
      </div>

      {/* History list */}
      <div className="card">
        {history.items.length === 0 ? (
          <div className="placeholder">
            <span className="placeholder-icon">📜</span>
            <p>No query history yet. Execute queries from the Query Workspace to see them here.</p>
          </div>
        ) : (
          <div className="datatable-scroll">
            <table className="datatable">
              <thead>
                <tr>
                  <th className="datatable-th">Time</th>
                  <th className="datatable-th">Operation</th>
                  <th className="datatable-th">Status</th>
                  <th className="datatable-th">Duration</th>
                  <th className="datatable-th">Summary</th>
                  <th className="datatable-th">Actions</th>
                </tr>
              </thead>
              <tbody>
                {history.items.map(item => (
                  <HistoryRow
                    key={item.id}
                    item={item}
                    expanded={expandedId === item.id}
                    onToggle={() => toggleExpand(item.id)}
                    onReplay={() => handleReplay(item)}
                    onDelete={(e) => handleDelete(e, item.id)}
                    formatDuration={formatDuration}
                    formatTime={formatTime}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {history.total_pages > 1 && (
          <div className="datatable-pagination">
            <button className="btn btn-ghost btn-sm" disabled={history.page <= 1} onClick={() => fetchHistory(history.page - 1)}>‹ Prev</button>
            <span className="datatable-page-info">Page {history.page} of {history.total_pages}</span>
            <button className="btn btn-ghost btn-sm" disabled={history.page >= history.total_pages} onClick={() => fetchHistory(history.page + 1)}>Next ›</button>
          </div>
        )}
      </div>
    </motion.div>
  )
}

function HistoryRow({ item, expanded, onToggle, onReplay, onDelete, formatDuration, formatTime }) {
  const isError = item.status === 'error'
  const isPreview = item.status === 'preview'

  return (
    <>
      <tr className="datatable-row history-row" onClick={onToggle}>
        <td className="datatable-td">{formatTime(item.timestamp_iso)}</td>
        <td className="datatable-td">
          <span style={{ fontWeight: 700, textTransform: 'uppercase', fontSize: '10px', letterSpacing: '0.06em' }}>
            {item.operation}
          </span>
        </td>
        <td className="datatable-td">
          <span className={`history-status ${item.status}`}>
            {isError ? '✗' : isPreview ? '👁' : '✓'} {item.status}
          </span>
        </td>
        <td className="datatable-td">{formatDuration(item.duration_ms)}</td>
        <td className="datatable-td">
          {item.result_summary?.row_count != null && `${item.result_summary.row_count} rows`}
          {item.result_summary?.fields != null && `${item.result_summary.fields} fields`}
          {item.result_summary?.errors?.length > 0 && (
            <span style={{ color: 'var(--danger)', marginLeft: 4 }}>
              {item.result_summary.errors[0]?.slice(0, 40)}
            </span>
          )}
        </td>
        <td className="datatable-td datatable-td-actions">
          <button className="btn-icon" onClick={(e) => { e.stopPropagation(); onReplay() }} title="Replay in Query Workspace">▶</button>
          <button className="btn-icon" onClick={onDelete} title="Delete entry">🗑</button>
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={6} style={{ padding: 0 }}>
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.25 }}
              style={{ overflow: 'hidden' }}
            >
              <div style={{ padding: '16px 24px', background: 'var(--bg-void)', borderTop: '1px solid var(--border-subtle)' }}>
                <div style={{ marginBottom: 10 }}>
                  <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.08em' }}>
                    Payload
                  </span>
                  <pre className="query-result-pane" style={{ marginTop: 6, maxHeight: 180 }}>
                    {JSON.stringify(item.payload, null, 2)}
                  </pre>
                </div>
                {item.result_summary && Object.keys(item.result_summary).length > 0 && (
                  <div>
                    <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.08em' }}>
                      Result Summary
                    </span>
                    <pre className="query-result-pane" style={{ marginTop: 6, maxHeight: 120 }}>
                      {JSON.stringify(item.result_summary, null, 2)}
                    </pre>
                  </div>
                )}
                <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
                  <button className="btn btn-primary btn-sm" onClick={onReplay}>▶ Replay in Workspace</button>
                  <button className="btn btn-ghost btn-sm" onClick={() => navigator.clipboard?.writeText(JSON.stringify(item.payload, null, 2))}>📋 Copy Payload</button>
                </div>
              </div>
            </motion.div>
          </td>
        </tr>
      )}
    </>
  )
}
