import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function BootstrapView({ onSessionUpdate }) {
  const [mode, setMode] = useState('quick')
  const [count, setCount] = useState(100)
  const [schemaInput, setSchemaInput] = useState('')
  const [recordsInput, setRecordsInput] = useState('[]')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const toast = useToast()

  const workingSample = {
    schema: {
      schema_name: 'quick_custom',
      version: '1.0.0',
      root_entity: 'order_event',
      json_schema: {
        type: 'object',
        properties: {
          customer_id: { type: 'string' },
          order_id: { type: 'string' },
          timestamp: { type: 'string' },
          payment: {
            type: 'object',
            properties: {
              status: { type: 'string' },
            },
          },
        },
      },
      constraints: {
        unique_candidates: ['order_id'],
        linking_field: 'customer_id',
      },
    },
    records: [
      {
        customer_id: 'c1',
        order_id: 'o1',
        timestamp: '2026-01-01T00:00:00Z',
        payment: { status: 'captured' },
      },
    ],
  }

  useEffect(() => {
    let mounted = true
    api.schema()
      .then((res) => {
        if (!mounted || !res.success || !res.data) return
        const initialSchema = {
          schema_name: res.data.schema_name,
          version: res.data.version,
          root_entity: res.data.root_entity,
          json_schema: res.data.json_schema,
          constraints: res.data.constraints || {},
        }
        setSchemaInput(JSON.stringify(initialSchema, null, 2))
      })
      .catch(() => {})
    return () => {
      mounted = false
    }
  }, [])

  function parseJson(label, raw) {
    try {
      return JSON.parse(raw)
    } catch (err) {
      throw new Error(`Invalid ${label} JSON: ${err.message}`)
    }
  }

  async function handleBootstrap() {
    setLoading(true)
    setResult(null)
    try {
      const res = await api.bootstrap(count)
      if (res.success) {
        setResult({ ok: true, message: res.data.message })
        onSessionUpdate(res.data.session)
        toast('Database bootstrapped successfully!', 'success')
      } else {
        setResult({ ok: false, message: res.error })
        toast(`Bootstrap failed: ${res.error}`, 'error')
      }
    } catch (err) {
      setResult({ ok: false, message: err.message })
      toast(`Bootstrap error: ${err.message}`, 'error')
    }
    setLoading(false)
  }

  async function handleCustomBootstrap() {
    setLoading(true)
    setResult(null)
    try {
      const schema = parseJson('schema', schemaInput)
      const parsedRecords = parseJson('records', recordsInput)
      const records = Array.isArray(parsedRecords) ? parsedRecords : [parsedRecords]

      if (records.length === 0) {
        throw new Error('Records cannot be empty')
      }

      const res = await api.bootstrapCustom(schema, records)
      if (res.success) {
        setResult({ ok: true, message: res.data.message })
        onSessionUpdate(res.data.session)
        toast('Custom schema + data bootstrapped successfully!', 'success')
      } else {
        setResult({ ok: false, message: res.error })
        toast(`Custom bootstrap failed: ${res.error}`, 'error')
      }
    } catch (err) {
      setResult({ ok: false, message: err.message })
      toast(`Custom bootstrap error: ${err.message}`, 'error')
    }
    setLoading(false)
  }

  function loadWorkingSample() {
    setSchemaInput(JSON.stringify(workingSample.schema, null, 2))
    setRecordsInput(JSON.stringify(workingSample.records, null, 2))
    setMode('custom')
    toast('Loaded a working sample schema + data', 'info')
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Bootstrap Database</h1>
      <p className="view-subtitle">Use quick bootstrap for demo data, or provide your own schema and records</p>
      <div className="card">
        <div style={{ display: 'flex', gap: 10, marginBottom: 16 }}>
          <button
            className={`btn btn-sm ${mode === 'quick' ? 'btn-primary' : 'btn-ghost'}`}
            onClick={() => setMode('quick')}
            disabled={loading}
          >
            Quick Bootstrap
          </button>
          <button
            className={`btn btn-sm ${mode === 'custom' ? 'btn-primary' : 'btn-ghost'}`}
            onClick={() => setMode('custom')}
            disabled={loading}
          >
            Custom Input
          </button>
          <button className="btn btn-sm btn-ghost" onClick={loadWorkingSample} disabled={loading}>
            Load Working Sample
          </button>
        </div>

        {mode === 'quick' ? (
          <div className="bootstrap-row">
            <div className="bootstrap-field">
              <label>Number of Records</label>
              <input
                type="number"
                className="input"
                value={count}
                onChange={e => setCount(parseInt(e.target.value) || 100)}
                min="1"
                max="10000"
              />
            </div>
            <button className="btn btn-primary btn-lg" onClick={handleBootstrap} disabled={loading}>
              {loading ? <span className="spinner" /> : <span>⚡</span>}
              Bootstrap Database
            </button>
          </div>
        ) : (
          <>
            <div className="placeholder" style={{ marginBottom: 14 }}>
              Custom schemas should include a linking field in constraints (or a top-level unique id). If you do not use <strong>username</strong>, the backend will infer a replacement from your schema, such as <strong>customer_id</strong>.
            </div>
            <div className="form-group" style={{ marginBottom: 14 }}>
              <label>Schema JSON</label>
              <textarea
                className="input"
                value={schemaInput}
                onChange={(e) => setSchemaInput(e.target.value)}
                placeholder='{"schema_name":"custom","version":"1.0.0","root_entity":"root","json_schema":{...}}'
              />
            </div>
            <div className="form-group">
              <label>Records JSON (array or single object)</label>
              <textarea
                className="input"
                value={recordsInput}
                onChange={(e) => setRecordsInput(e.target.value)}
                placeholder='[{"id":"1","name":"sample"}]'
              />
            </div>
            <button className="btn btn-primary btn-lg" onClick={handleCustomBootstrap} disabled={loading}>
              {loading ? <span className="spinner" /> : <span>📥</span>}
              Build with Custom Schema + Data
            </button>
          </>
        )}

        {result && (
          <motion.div
            className={`bootstrap-status ${result.ok ? 'success' : 'error'}`}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <span>{result.ok ? '✅' : '❌'}</span>
            <span>{result.message}</span>
          </motion.div>
        )}
      </div>
    </motion.div>
  )
}
