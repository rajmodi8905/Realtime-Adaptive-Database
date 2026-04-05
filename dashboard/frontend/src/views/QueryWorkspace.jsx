import { useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

export default function QueryWorkspace() {
  const [input, setInput] = useState(JSON.stringify({ operation: 'read', filters: { username: 'username_1' } }, null, 2))
  const [result, setResult] = useState(null)
  const toast = useToast()

  async function preview() {
    try {
      const q = JSON.parse(input)
      const res = await api.queryPreview(q)
      setResult(JSON.stringify(res.success ? res.data : { error: res.error }, null, 2))
    } catch (err) { setResult(`Parse error: ${err.message}`); toast('Invalid JSON', 'error') }
  }

  async function execute() {
    try {
      const q = JSON.parse(input)
      const res = await api.queryExecute(q)
      setResult(JSON.stringify(res.success ? res.data : { error: res.error }, null, 2))
      if (res.success) toast('Query executed', 'success')
      else toast(`Error: ${res.error}`, 'error')
    } catch (err) { setResult(`Parse error: ${err.message}`); toast('Invalid JSON', 'error') }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query Workspace</h1>
      <p className="view-subtitle">Preview and execute JSON-based queries against the hybrid storage</p>
      <div className="card">
        <div className="form-group">
          <label>Query JSON</label>
          <textarea className="input" value={input} onChange={e => setInput(e.target.value)} rows={8}
            placeholder='{"operation": "read", "filters": {"username": "username_1"}}' />
        </div>
        <div className="query-actions">
          <button className="btn btn-ghost" onClick={preview}>Preview Plan</button>
          <button className="btn btn-primary" onClick={execute}>Execute</button>
        </div>
        {result && <pre className="query-result-pane">{result}</pre>}
      </div>
    </motion.div>
  )
}
