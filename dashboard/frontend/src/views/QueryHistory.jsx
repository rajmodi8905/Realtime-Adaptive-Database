import { motion } from 'framer-motion'

export default function QueryHistory({ onReplay }) {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query History</h1>
      <p className="view-subtitle">View, search, and replay past queries</p>
      <div className="card">
        <p style={{ opacity: 0.5 }}>Query history will be available after Phase 1 implementation.</p>
      </div>
    </motion.div>
  )
}
