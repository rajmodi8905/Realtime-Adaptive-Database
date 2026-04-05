import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function BootstrapView({ onSessionUpdate }) {
  const [count, setCount] = useState(100)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const toast = useToast()

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

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Bootstrap Database</h1>
      <p className="view-subtitle">Initialize the pipeline: register schema → generate records → ingest → build storage → insert data</p>
      <div className="card">
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
