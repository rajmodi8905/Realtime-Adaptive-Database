import { motion } from 'framer-motion'

export default function SessionAnalytics() {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Session Analytics</h1>
      <p className="view-subtitle">Operation volume, success rates, and latency insights</p>
      <div className="card">
        <p style={{ opacity: 0.5 }}>Analytics will be available after Phase 5 implementation.</p>
      </div>
    </motion.div>
  )
}
