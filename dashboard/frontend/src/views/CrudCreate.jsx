import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

function buildTemplate(schema) {
  if (!schema || schema.type !== 'object') return {}
  const obj = {}
  for (const [key, prop] of Object.entries(schema.properties || {})) {
    const t = prop.type || 'string'
    if (t === 'object') obj[key] = buildTemplate(prop)
    else if (t === 'array') {
      const items = prop.items || { type: 'string' }
      obj[key] = items.type === 'object' ? [buildTemplate(items)] : ['']
    }
    else if (t === 'integer') obj[key] = 0
    else if (t === 'number') obj[key] = 0.0
    else if (t === 'boolean') obj[key] = false
    else obj[key] = ''
  }
  return obj
}

export default function CrudCreate() {
  const [input, setInput] = useState('')
  const [constraints, setConstraints] = useState('')
  const [result, setResult] = useState(null)
  const toast = useToast()

  async function generateTemplate() {
    try {
      const res = await api.schema()
      if (!res.success) return toast('Failed to load schema', 'error')
      const template = buildTemplate(res.data.json_schema)
      setInput(JSON.stringify([template], null, 2))
      const c = res.data.constraints || {}
      let info = ''
      if (c.not_null) info += `Required: ${c.not_null.join(', ')}. `
      if (c.unique_candidates) info += `Unique: ${c.unique_candidates.join(', ')}`
      setConstraints(info)
    } catch (err) { toast(`Error: ${err.message}`, 'error') }
  }

  async function handleCreate() {
    setResult(null)
    try {
      const records = JSON.parse(input)
      if (!Array.isArray(records)) return toast('Input must be a JSON array', 'error')
      for (let i = 0; i < records.length; i++) {
        if (typeof records[i] !== 'object' || Array.isArray(records[i])) return toast(`Record ${i} must be an object`, 'error')
        if (!records[i].username) return toast(`Record ${i}: 'username' is required`, 'error')
      }
      const res = await api.crud({ operation: 'create', records })
      if (res.success && res.data.status === 'committed') {
        setResult({ ok: true, message: `Status: ${res.data.status}`, json: JSON.stringify(res.data, null, 2) })
        toast('Records created successfully', 'success')
      } else {
        const err = res.success ? (res.data.errors?.join(', ') || JSON.stringify(res.data)) : res.error
        setResult({ ok: false, message: err || 'Unknown error', json: JSON.stringify(res.data || {}, null, 2) })
        toast('Create failed', 'error')
      }
    } catch (err) {
      setResult({ ok: false, message: err.message })
      toast(`Error: ${err.message}`, 'error')
    }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Create Records</h1>
      <p className="view-subtitle">Insert new records into the hybrid database via transactional CRUD</p>
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
          <button className="btn btn-ghost" onClick={generateTemplate}>Generate Template</button>
          {constraints && <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{constraints}</span>}
        </div>
        <textarea className="input" value={input} onChange={e => setInput(e.target.value)} rows={12}
          placeholder="Paste JSON array of records…" />
        <div className="mt-12">
          <button className="btn btn-primary" onClick={handleCreate}>Create Records</button>
        </div>
        {result && (
          <motion.div className={`result-box ${result.ok ? 'success' : 'error'}`}
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}>
            <div className="result-title">{result.ok ? '✅ Records Created' : '❌ Create Failed'}</div>
            <div className="result-body">{result.message}</div>
            {result.json && <div className="result-json">{result.json}</div>}
          </motion.div>
        )}
      </div>
    </motion.div>
  )
}
