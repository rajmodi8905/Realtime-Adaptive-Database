import { motion } from 'framer-motion'

export default function QueryMonitoring() {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Query Monitoring</h1>
      <p className="view-subtitle">Real-time metrics and performance tracking</p>
      <div className="card">
        <p style={{ opacity: 0.5 }}>Monitoring will be available after Phase 4 implementation.</p>
      </div>
    </motion.div>
  )
}
