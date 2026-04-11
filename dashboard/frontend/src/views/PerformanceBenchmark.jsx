import { motion } from 'framer-motion'

export default function PerformanceBenchmark() {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Performance Benchmark</h1>
      <p className="view-subtitle">Compare logical-path vs direct-path query performance</p>
      <div className="card">
        <p style={{ opacity: 0.5 }}>Benchmarks will be available after Phase 4 implementation.</p>
      </div>
    </motion.div>
  )
}
