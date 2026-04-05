import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

function cellStr(val) {
  if (val === undefined || val === null) return '—'
  if (typeof val === 'object') return JSON.stringify(val)
  return String(val)
}

function FilterBuilder({ filters, setFilters }) {
  function update(i, field, val) {
    const next = [...filters]
    next[i] = { ...next[i], [field]: val }
    setFilters(next)
  }
  function remove(i) { setFilters(filters.filter((_, j) => j !== i)) }
  function add() { setFilters([...filters, { key: '', value: '' }]) }

  return (
    <>
      {filters.map((f, i) => (
        <div key={i} className="filter-row">
          <input className="input" value={f.key} onChange={e => update(i, 'key', e.target.value)} placeholder="Field name" />
          <input className="input" value={f.value} onChange={e => update(i, 'value', e.target.value)} placeholder="Value" />
          <button className="btn btn-ghost btn-remove" onClick={() => remove(i)}>✕</button>
        </div>
      ))}
      <button className="btn btn-ghost btn-sm" onClick={add}>+ Add Filter</button>
    </>
  )
}

function filtersToObj(arr) {
  const r = {}
  arr.forEach(f => { if (f.key?.trim() && f.value?.trim()) r[f.key.trim()] = f.value.trim() })
  return r
}

export default function CrudRead() {
  const [filters, setFilters] = useState([{ key: '', value: '' }])
  const [fields, setFields] = useState('')
  const [limit, setLimit] = useState(10)
  const [records, setRecords] = useState([])
  const [columns, setColumns] = useState([])
  const [hasResult, setHasResult] = useState(false)
  const toast = useToast()

  async function handleRead() {
    const f = filtersToObj(filters)
    const fieldsStr = fields.trim()
    const fieldsList = fieldsStr ? fieldsStr.split(',').map(x => x.trim()).filter(Boolean) : undefined
    const body = { operation: 'read', filters: f, limit }
    if (fieldsList) body.fields = fieldsList
    try {
      const res = await api.crud(body)
      setHasResult(true)
      if (res.success) {
        const recs = res.data.sql_result?.records || []
        setRecords(recs)
        setColumns(recs.length > 0 ? [...new Set(recs.flatMap(r => Object.keys(r)))] : [])
        toast(`Read: ${recs.length} records`, 'success')
      } else {
        setRecords([])
        setColumns([])
        toast(`Read error: ${res.error}`, 'error')
      }
    } catch (err) { toast(`Error: ${err.message}`, 'error') }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Read Records</h1>
      <p className="view-subtitle">Query records using filters and field selection</p>
      <div className="card">
        <div className="form-group">
          <label>Filters</label>
          <FilterBuilder filters={filters} setFilters={setFilters} />
        </div>
        <div className="form-group">
          <label>Fields (comma-separated, optional)</label>
          <input className="input" value={fields} onChange={e => setFields(e.target.value)} placeholder="username, event_id, timestamp" />
        </div>
        <div className="form-group">
          <label>Limit</label>
          <input type="number" className="input" value={limit} onChange={e => setLimit(parseInt(e.target.value) || 10)} min={1} max={1000} style={{ maxWidth: 140 }} />
        </div>
        <button className="btn btn-primary" onClick={handleRead}>Read Records</button>
        {hasResult && (
          <div className="mt-20">
            {records.length > 0 ? (
              <div className="table-wrap">
                <table className="data-table">
                  <thead><tr>{columns.map(c => <th key={c}>{c}</th>)}</tr></thead>
                  <tbody>
                    {records.map((row, i) => (
                      <tr key={i}>{columns.map(c => <td key={c} title={cellStr(row[c])}>{cellStr(row[c])}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="placeholder">No records found</div>
            )}
          </div>
        )}
      </div>
    </motion.div>
  )
}
