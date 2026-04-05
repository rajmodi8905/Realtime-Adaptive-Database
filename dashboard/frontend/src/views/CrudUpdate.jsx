import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

function FilterBuilder({ filters, setFilters, label }) {
  function update(i, field, val) { const n = [...filters]; n[i] = { ...n[i], [field]: val }; setFilters(n) }
  function remove(i) { setFilters(filters.filter((_, j) => j !== i)) }
  function add() { setFilters([...filters, { key: '', value: '' }]) }
  return (
    <div className="form-group">
      <label>{label}</label>
      {filters.map((f, i) => (
        <div key={i} className="filter-row">
          <input className="input" value={f.key} onChange={e => update(i, 'key', e.target.value)} placeholder="Field name" />
          <input className="input" value={f.value} onChange={e => update(i, 'value', e.target.value)} placeholder={label.includes('Update') ? 'New value' : 'Value'} />
          <button className="btn btn-ghost btn-remove" onClick={() => remove(i)}>✕</button>
        </div>
      ))}
      <button className="btn btn-ghost btn-sm" onClick={add}>+ Add {label.includes('Update') ? 'Field' : 'Filter'}</button>
    </div>
  )
}

function filtersToObj(arr) { const r = {}; arr.forEach(f => { if (f.key?.trim() && f.value?.trim()) r[f.key.trim()] = f.value.trim() }); return r }

export default function CrudUpdate() {
  const [filters, setFilters] = useState([{ key: '', value: '' }])
  const [updates, setUpdates] = useState([{ key: '', value: '' }])
  const [result, setResult] = useState(null)
  const toast = useToast()

  async function handleUpdate() {
    const f = filtersToObj(filters)
    const u = filtersToObj(updates)
    if (Object.keys(f).length === 0) return toast('Add at least one filter', 'error')
    if (Object.keys(u).length === 0) return toast('Add at least one update field', 'error')
    setResult(null)
    try {
      const res = await api.crud({ operation: 'update', filters: f, updates: u })
      if (res.success && res.data.status === 'committed') {
        setResult({ ok: true, message: `Status: ${res.data.status}`, json: JSON.stringify(res.data, null, 2) })
        toast('Records updated', 'success')
      } else {
        setResult({ ok: false, message: res.error || res.data?.errors?.join(', ') || 'Unknown', json: JSON.stringify(res.data || {}, null, 2) })
        toast('Update failed', 'error')
      }
    } catch (err) { setResult({ ok: false, message: err.message }); toast(`Error: ${err.message}`, 'error') }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Update Records</h1>
      <p className="view-subtitle">Modify existing records using filters and field updates</p>
      <div className="card">
        <FilterBuilder filters={filters} setFilters={setFilters} label="Filters (identify records)" />
        <FilterBuilder filters={updates} setFilters={setUpdates} label="Update Fields" />
        <button className="btn btn-primary" onClick={handleUpdate}>Update Records</button>
        {result && (
          <motion.div className={`result-box ${result.ok ? 'success' : 'error'}`}
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}>
            <div className="result-title">{result.ok ? '✅ Update Committed' : '❌ Update Failed'}</div>
            <div className="result-body">{result.message}</div>
            {result.json && <div className="result-json">{result.json}</div>}
          </motion.div>
        )}
      </div>
    </motion.div>
  )
}
