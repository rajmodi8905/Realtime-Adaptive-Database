import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'
import Modal from '../components/Modal'

function FilterBuilder({ filters, setFilters }) {
  function update(i, field, val) { const n = [...filters]; n[i] = { ...n[i], [field]: val }; setFilters(n) }
  function remove(i) { setFilters(filters.filter((_, j) => j !== i)) }
  function add() { setFilters([...filters, { key: '', value: '' }]) }
  return (
    <div className="form-group">
      <label>Filters (identify records to delete)</label>
      {filters.map((f, i) => (
        <div key={i} className="filter-row">
          <input className="input" value={f.key} onChange={e => update(i, 'key', e.target.value)} placeholder="Field name" />
          <input className="input" value={f.value} onChange={e => update(i, 'value', e.target.value)} placeholder="Value" />
          <button className="btn btn-ghost btn-remove" onClick={() => remove(i)}>✕</button>
        </div>
      ))}
      <button className="btn btn-ghost btn-sm" onClick={add}>+ Add Filter</button>
    </div>
  )
}

function filtersToObj(arr) { const r = {}; arr.forEach(f => { if (f.key?.trim() && f.value?.trim()) r[f.key.trim()] = f.value.trim() }); return r }

export default function CrudDelete() {
  const [filters, setFilters] = useState([{ key: '', value: '' }])
  const [result, setResult] = useState(null)
  const [showModal, setShowModal] = useState(false)
  const toast = useToast()

  function handleDeleteClick() {
    const f = filtersToObj(filters)
    if (Object.keys(f).length === 0) return toast('Add at least one filter', 'error')
    setShowModal(true)
  }

  async function confirmDelete() {
    setShowModal(false)
    setResult(null)
    const f = filtersToObj(filters)
    try {
      const res = await api.crud({ operation: 'delete', filters: f })
      if (res.success && res.data.status === 'committed') {
        setResult({ ok: true, message: `Status: ${res.data.status}`, json: JSON.stringify(res.data, null, 2) })
        toast('Records deleted', 'success')
      } else {
        setResult({ ok: false, message: res.error || res.data?.errors?.join(', ') || 'Unknown' })
        toast('Delete failed', 'error')
      }
    } catch (err) { setResult({ ok: false, message: err.message }); toast(`Error: ${err.message}`, 'error') }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Delete Records</h1>
      <p className="view-subtitle">Remove records using filter criteria — this action is irreversible</p>
      <div className="card">
        <FilterBuilder filters={filters} setFilters={setFilters} />
        <button className="btn btn-danger" onClick={handleDeleteClick}>🗑 Delete Records</button>
        {result && (
          <motion.div className={`result-box ${result.ok ? 'success' : 'error'}`}
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}>
            <div className="result-title">{result.ok ? '✅ Records Deleted' : '❌ Delete Failed'}</div>
            <div className="result-body">{result.message}</div>
            {result.json && <div className="result-json">{result.json}</div>}
          </motion.div>
        )}
      </div>
      <Modal
        show={showModal}
        title="⚠️ Confirm Delete"
        body={`Delete all records matching: ${JSON.stringify(filtersToObj(filters))}?`}
        onCancel={() => setShowModal(false)}
        onConfirm={confirmDelete}
      />
    </motion.div>
  )
}
