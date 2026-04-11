import { useState, useMemo } from 'react'

/**
 * Reusable DataTable component with:
 * - Sortable columns
 * - Local text filter
 * - Column visibility toggle
 * - Pagination
 * - Nested value rendering
 * - Copy row / Export JSON/CSV support
 */
export default function DataTable({
  data = [],
  columns: columnsProp,
  pageSize: defaultPageSize = 25,
  emptyMessage = 'No data available',
  title,
  onRowClick,
  compact = false,
  exportFilename = 'export',
  showTools = true,
}) {
  const [page, setPage] = useState(0)
  const [sortKey, setSortKey] = useState(null)
  const [sortDir, setSortDir] = useState('asc')
  const [filterText, setFilterText] = useState('')
  const [hiddenCols, setHiddenCols] = useState(new Set())
  const [showColPicker, setShowColPicker] = useState(false)
  const [pageSize, setPageSize] = useState(defaultPageSize)

  // Auto-detect columns from data if not provided
  const columns = useMemo(() => {
    if (columnsProp) return columnsProp
    if (!data.length) return []
    const keys = new Set()
    data.forEach(row => {
      if (row && typeof row === 'object') Object.keys(row).forEach(k => keys.add(k))
    })
    return Array.from(keys).map(key => ({ key, label: key }))
  }, [data, columnsProp])

  const visibleCols = useMemo(() => columns.filter(c => !hiddenCols.has(c.key)), [columns, hiddenCols])

  // Filter
  const filtered = useMemo(() => {
    if (!filterText.trim()) return data
    const term = filterText.toLowerCase()
    return data.filter(row =>
      visibleCols.some(col => {
        const val = getNestedValue(row, col.key)
        return val != null && String(val).toLowerCase().includes(term)
      })
    )
  }, [data, filterText, visibleCols])

  // Sort
  const sorted = useMemo(() => {
    if (!sortKey) return filtered
    const dir = sortDir === 'asc' ? 1 : -1
    return [...filtered].sort((a, b) => {
      const va = getNestedValue(a, sortKey)
      const vb = getNestedValue(b, sortKey)
      if (va == null && vb == null) return 0
      if (va == null) return dir
      if (vb == null) return -dir
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir
      return String(va).localeCompare(String(vb)) * dir
    })
  }, [filtered, sortKey, sortDir])

  // Paginate
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize)

  function handleSort(key) {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  function toggleCol(key) {
    setHiddenCols(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  function copyRow(row) {
    navigator.clipboard?.writeText(JSON.stringify(row, null, 2))
  }

  function exportJSON() {
    const blob = new Blob([JSON.stringify(sorted, null, 2)], { type: 'application/json' })
    downloadBlob(blob, `${exportFilename}.json`)
  }

  function exportCSV() {
    const headers = visibleCols.map(c => c.label || c.key)
    const lines = [headers.join(',')]
    sorted.forEach(row => {
      const vals = visibleCols.map(c => {
        const v = getNestedValue(row, c.key)
        let s = ''
        if (v != null) {
          s = typeof v === 'object' ? JSON.stringify(v) : String(v)
        }
        return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s
      })
      lines.push(vals.join(','))
    })
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
    downloadBlob(blob, `${exportFilename}.csv`)
  }

  return (
    <div className={`datatable-container ${compact ? 'compact' : ''}`}>
      {/* Toolbar */}
      {showTools && (
        <div className="datatable-toolbar">
          <div className="datatable-toolbar-left">
            {title && <span className="datatable-title">{title}</span>}
            <span className="datatable-count">{sorted.length} row{sorted.length !== 1 ? 's' : ''}</span>
          </div>
          <div className="datatable-toolbar-right">
            <input
              type="text"
              className="datatable-filter input"
              placeholder="Filter…"
              value={filterText}
              onChange={e => { setFilterText(e.target.value); setPage(0) }}
            />
            <div className="datatable-col-picker-wrap">
              <button className="btn btn-ghost btn-sm" onClick={() => setShowColPicker(p => !p)} title="Toggle columns">
                ⚙ Columns
              </button>
              {showColPicker && (
                <div className="datatable-col-picker">
                  {columns.map(c => (
                    <label key={c.key} className="datatable-col-label">
                      <input
                        type="checkbox"
                        checked={!hiddenCols.has(c.key)}
                        onChange={() => toggleCol(c.key)}
                      />
                      {c.label || c.key}
                    </label>
                  ))}
                </div>
              )}
            </div>
            <button className="btn btn-ghost btn-sm" onClick={exportJSON} title="Export JSON">JSON</button>
            <button className="btn btn-ghost btn-sm" onClick={exportCSV} title="Export CSV">CSV</button>
          </div>
        </div>
      )}

      {/* Table */}
      <div className="datatable-scroll">
        <table className="datatable">
          <thead>
            <tr>
              {visibleCols.map(col => (
                <th key={col.key} onClick={() => handleSort(col.key)} className="datatable-th">
                  {col.label || col.key}
                  {sortKey === col.key && <span className="sort-indicator">{sortDir === 'asc' ? ' ▲' : ' ▼'}</span>}
                </th>
              ))}
              <th className="datatable-th datatable-th-actions">⋯</th>
            </tr>
          </thead>
          <tbody>
            {paged.length === 0 ? (
              <tr><td colSpan={visibleCols.length + 1} className="datatable-empty">{emptyMessage}</td></tr>
            ) : (
              paged.map((row, i) => (
                <tr
                  key={i}
                  className={`datatable-row ${onRowClick ? 'clickable' : ''}`}
                  onClick={() => onRowClick?.(row, i + page * pageSize)}
                >
                  {visibleCols.map(col => (
                    <td key={col.key} className="datatable-td">
                      <CellValue value={getNestedValue(row, col.key)} />
                    </td>
                  ))}
                  <td className="datatable-td datatable-td-actions">
                    <button className="btn-icon" onClick={e => { e.stopPropagation(); copyRow(row) }} title="Copy row">📋</button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="datatable-pagination">
          <button className="btn btn-ghost btn-sm" disabled={page === 0} onClick={() => setPage(0)}>«</button>
          <button className="btn btn-ghost btn-sm" disabled={page === 0} onClick={() => setPage(p => p - 1)}>‹</button>
          <span className="datatable-page-info">
            Page {page + 1} of {totalPages}
          </span>
          <button className="btn btn-ghost btn-sm" disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}>›</button>
          <button className="btn btn-ghost btn-sm" disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>»</button>
          <select className="datatable-page-size" value={pageSize} onChange={e => { setPageSize(+e.target.value); setPage(0) }}>
            {[10, 25, 50, 100].map(n => <option key={n} value={n}>{n} / page</option>)}
          </select>
        </div>
      )}
    </div>
  )
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function getNestedValue(obj, key) {
  if (obj == null) return null
  if (key in obj) return obj[key]
  return key.split('.').reduce((acc, part) => acc?.[part], obj)
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

/** Render cell values including nested objects/arrays */
function CellValue({ value }) {
  if (value === null || value === undefined) return <span className="cell-null">—</span>
  if (typeof value === 'boolean') return <span className={`cell-bool ${value ? 'true' : 'false'}`}>{value ? '✓' : '✗'}</span>
  if (typeof value === 'number') return <span className="cell-number">{value}</span>
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="cell-null">[]</span>
    return (
      <details className="cell-expandable" style={{ cursor: 'pointer' }}>
        <summary className="cell-array">[{value.length} items]</summary>
        <pre style={{ margin: '4px 0 0', padding: '6px', background: 'rgba(0,0,0,0.2)', borderRadius: '4px', fontSize: '9px', maxHeight: '150px', overflow: 'auto', color: 'var(--text-secondary)' }}>
          {JSON.stringify(value, null, 2)}
        </pre>
      </details>
    )
  }
  if (typeof value === 'object') {
    return (
      <details className="cell-expandable" style={{ cursor: 'pointer' }}>
        <summary className="cell-object">{`{…}`}</summary>
        <pre style={{ margin: '4px 0 0', padding: '6px', background: 'rgba(0,0,0,0.2)', borderRadius: '4px', fontSize: '9px', maxHeight: '150px', overflow: 'auto', color: 'var(--text-secondary)' }}>
          {JSON.stringify(value, null, 2)}
        </pre>
      </details>
    )
  }
  const s = String(value)
  if (s.length > 80) return <span title={s}>{s.slice(0, 77)}…</span>
  return <span>{s}</span>
}
