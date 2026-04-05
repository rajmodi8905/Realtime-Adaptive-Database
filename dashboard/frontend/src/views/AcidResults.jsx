import { useState, useRef, useCallback, forwardRef, useImperativeHandle, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

const ACID_ORDER = ['reconstruction', 'atomicity', 'consistency', 'isolation', 'durability']
const ACID_LETTERS = { reconstruction: 'R', atomicity: 'A', consistency: 'C', isolation: 'I', durability: 'D' }
const ACID_NAMES = { reconstruction: 'Reconstruction', atomicity: 'Atomicity', consistency: 'Consistency', isolation: 'Isolation', durability: 'Durability' }
const ACID_DESCRIPTIONS = {
  reconstruction: 'Verify data split across MySQL + MongoDB reconstructs into complete unified JSON',
  atomicity: 'Verify failed transactions roll back entirely — no partial writes',
  consistency: 'Verify schema constraint violations are cleanly rejected',
  isolation: 'Verify concurrent transactions don\'t interfere via locks',
  durability: 'Verify data persists after connection disconnect/reconnect',
}
const ACID_EXPLANATIONS = {
  reconstruction: 'The reconstructor fetches data fragments from both MySQL (relational tables) and MongoDB (document collections), merges them by primary key, and validates that every record emerges as a complete, backend-agnostic JSON object.',
  atomicity: 'A test record is created across both SQL and MongoDB. Then a deliberate failure is injected mid-transaction. The test verifies that the TransactionCoordinator rolls back ALL changes, leaving the database in its original clean state.',
  consistency: 'A valid record is first inserted successfully. Then an intentionally invalid record is submitted. The test confirms the coordinator rejects the bad insert while preserving the valid data.',
  isolation: 'Multiple threads simultaneously attempt to read and write the same entity. The ConcurrencyManager uses per-entity EXCLUSIVE and SHARED locks (pessimistic locking) to serialize access. The test verifies no dirty reads, lost updates, or phantom writes.',
  durability: 'A record is written, then the MongoDB connection is deliberately severed and re-established. The test reads the data back after reconnection and confirms it survived the connection cycle.',
}
const ACID_PHASES = {
  reconstruction: ['Fetch Data', 'Merge Backends', 'Validate Structure', 'Check Integrity'],
  atomicity: ['Create Record', 'Simulate Failure', 'Check Rollback', 'Verify State'],
  consistency: ['Insert Valid', 'Insert Invalid', 'Verify Rejection', 'Confirm Clean'],
  isolation: ['Spawn Threads', 'Concurrent R/W', 'Check Locks', 'Verify No Conflict'],
  durability: ['Write Record', 'Disconnect DB', 'Reconnect', 'Verify Persistence'],
}

function PhaseStepper({ phases, phaseIndex }) {
  return (
    <div className="stepper">
      {phases.map((phase, pi) => (
        <div key={pi} style={{ display: 'contents' }}>
          <div className={`stepper-step ${phaseIndex > pi ? 'completed' : phaseIndex === pi ? 'active' : ''}`}>
            <motion.div
              className="stepper-dot"
              animate={phaseIndex === pi ? { scale: [1, 1.2, 1] } : {}}
              transition={{ repeat: phaseIndex === pi ? Infinity : 0, duration: 1.5 }}
            >
              {phaseIndex > pi ? '✓' : pi + 1}
            </motion.div>
            <div className="stepper-label">{phase}</div>
          </div>
          {pi < phases.length - 1 && (
            <div className={`stepper-line ${phaseIndex > pi ? 'completed' : ''}`} />
          )}
        </div>
      ))}
    </div>
  )
}

function LogTerminal({ logs, termRef }) {
  return (
    <div className="mt-16">
      <div className="log-terminal-header">
        <span className="terminal-dot" />
        Live Logs
      </div>
      <div className="log-terminal" ref={termRef}>
        {logs.map((log, i) => (
          <span key={i} className="log-line">
            <span className="log-time">{log.time} </span>
            <span className={`log-level-${log.level}`}>[{log.level}] </span>
            <span className={log.msg?.includes('Lock') ? 'log-lock' : 'log-msg'}>
              {log.msg?.includes('Lock acquired') ? '🔒 ' : ''}
              {log.msg?.includes('Lock released') ? '🔓 ' : ''}
              {log.msg}
            </span>
          </span>
        ))}
      </div>
    </div>
  )
}

const AcidResults = forwardRef(function AcidResults({ results, setResults, logs, setLogs, statuses, setStatuses, running, setRunning }, ref) {
  const [expanded, setExpanded] = useState({})
  const [phaseCounters, setPhaseCounters] = useState({})
  const [completed, setCompleted] = useState(0)
  const [currentTest, setCurrentTest] = useState(null)
  const logRefs = useRef({})
  const currentTestRef = useRef(null)
  const activeStreams = useRef(new Set())
  const toast = useToast()

  useEffect(() => {
    return () => {
      activeStreams.current.forEach(es => es.close())
      activeStreams.current.clear()
    }
  }, [])

  const advancePhase = useCallback((test, msg) => {
    const lm = msg.toLowerCase()
    setPhaseCounters(prev => {
      const current = prev[test] ?? -1
      let next = current
      if (lm.includes('lock acquired') && current < 2) next = 2
      else if ((lm.includes('upsert') || lm.includes('insert') || lm.includes('create')) && current < 1) next = 1
      else if ((lm.includes('released') || lm.includes('verify') || lm.includes('read')) && current < 3) next = 3
      else if (current < 0) next = 0
      return next !== current ? { ...prev, [test]: next } : prev
    })
  }, [])

  const scrollLog = useCallback((test) => {
    setTimeout(() => {
      const el = logRefs.current[test]
      if (el) el.scrollTop = el.scrollHeight
    }, 50)
  }, [])

  function getStatus(t) {
    if (results[t]) return results[t].passed ? 'passed' : 'failed'
    if (currentTest === t) return 'running'
    return statuses[t] || 'pending'
  }

  const hasResults = ACID_ORDER.some(t => results[t])
  const passCount = ACID_ORDER.filter(t => results[t]?.passed).length
  const resultCount = ACID_ORDER.filter(t => results[t]).length
  const allPassed = resultCount > 0 && passCount === resultCount

  function toggleCard(t) {
    setExpanded(prev => ({ ...prev, [t]: !prev[t] }))
  }

  function runSingle(property) {
    setExpanded(prev => ({ ...prev, [property]: true }))
    setLogs(prev => ({ ...prev, [property]: [] }))
    setResults(prev => { const n = { ...prev }; delete n[property]; return n })
    setPhaseCounters(prev => ({ ...prev, [property]: 0 }))
    setCurrentTest(property)
    currentTestRef.current = property
    setStatuses(prev => ({ ...prev, [property]: 'running' }))

    const es = new EventSource(api.acidStreamUrl(property))
    activeStreams.current.add(es)
    es.onmessage = (event) => {
      const data = JSON.parse(event.data)
      if (data.type === 'log') {
        setLogs(prev => ({ ...prev, [property]: [...(prev[property] || []), data] }))
        advancePhase(property, data.msg)
        scrollLog(property)
      } else if (data.type === 'result') {
        setPhaseCounters(prev => ({ ...prev, [property]: ACID_PHASES[property].length }))
        setResults(prev => ({ ...prev, [property]: data.result }))
        setStatuses(prev => ({ ...prev, [property]: data.result.passed ? 'passed' : 'failed' }))
        setCurrentTest(null)
        currentTestRef.current = null
        toast(`${ACID_LETTERS[property]}: ${data.result.passed ? 'PASSED ✅' : 'FAILED ❌'}`, data.result.passed ? 'success' : 'error')
        es.close()
        activeStreams.current.delete(es)
      } else if (data.type === 'error') {
        setCurrentTest(null)
        currentTestRef.current = null
        setStatuses(prev => ({ ...prev, [property]: 'failed' }))
        toast(`Error: ${data.message}`, 'error')
        es.close()
        activeStreams.current.delete(es)
      } else if (data.type === 'done') { 
        es.close() 
        activeStreams.current.delete(es)
      }
    }
    es.onerror = () => { 
      setCurrentTest(null)
      currentTestRef.current = null
      setStatuses(prev => ({ ...prev, [property]: 'failed' }))
      es.close() 
      activeStreams.current.delete(es)
    }
  }

  function runAll() {
    setRunning(true)
    setCompleted(0)
    ACID_ORDER.forEach(t => {
      setLogs(prev => ({ ...prev, [t]: [] }))
      setResults(prev => { const n = { ...prev }; delete n[t]; return n })
      setPhaseCounters(prev => ({ ...prev, [t]: -1 }))
      setExpanded(prev => ({ ...prev, [t]: false }))
      setStatuses(prev => ({ ...prev, [t]: 'pending' }))
    })

    const es = new EventSource(api.acidStreamAllUrl())
    activeStreams.current.add(es)
    es.onmessage = (event) => {
      const data = JSON.parse(event.data)
      if (data.type === 'test_start') {
        setCurrentTest(data.test)
        currentTestRef.current = data.test
        setPhaseCounters(prev => ({ ...prev, [data.test]: 0 }))
        setExpanded(prev => {
          const n = { ...prev }
          ACID_ORDER.forEach(t => { n[t] = t === data.test })
          return n
        })
        setStatuses(prev => ({ ...prev, [data.test]: 'running' }))
      } else if (data.type === 'log') {
        const t = data.test || currentTestRef.current
        if (t) {
          setLogs(prev => ({ ...prev, [t]: [...(prev[t] || []), data] }))
          advancePhase(t, data.msg)
          scrollLog(t)
        }
      } else if (data.type === 'result') {
        const t = data.test
        setPhaseCounters(prev => ({ ...prev, [t]: ACID_PHASES[t].length }))
        setResults(prev => ({ ...prev, [t]: data.result }))
        setStatuses(prev => ({ ...prev, [t]: data.result.passed ? 'passed' : 'failed' }))
        setCompleted(prev => prev + 1)
      } else if (data.type === 'done') {
        setRunning(false)
        setCurrentTest(null)
        currentTestRef.current = null
        es.close()
        activeStreams.current.delete(es)
        setTimeout(() => { toast('Test suite completed', 'info') }, 100)
      } else if (data.type === 'error') {
        setRunning(false)
        setCurrentTest(null)
        currentTestRef.current = null
        toast(`Error: ${data.message}`, 'error')
        es.close()
        activeStreams.current.delete(es)
      }
    }
    es.onerror = () => {
      setRunning(false)
      setCurrentTest(null)
      currentTestRef.current = null
      es.close()
      activeStreams.current.delete(es)
    }
  }

  // Expose methods via ref
  useImperativeHandle(ref, () => ({ runSingle, runAll }))

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">ACID + R Test Results</h1>
      <p className="view-subtitle">Validation experiments for Atomicity, Consistency, Isolation, Durability & Reconstruction</p>

      {running && (
        <motion.div className="acid-master-bar" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
          <span className="acid-progress-label">{currentTest ? `Running: ${ACID_NAMES[currentTest]}` : 'Preparing…'}</span>
          <div className="acid-progress-track">
            <div className="acid-progress-fill" style={{ width: `${(completed / ACID_ORDER.length) * 100}%` }} />
          </div>
          <span className="acid-progress-label">{completed}/{ACID_ORDER.length}</span>
        </motion.div>
      )}

      {hasResults && !running && (
        <motion.div className="acid-master-bar" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }}>
          <span style={{ fontSize: 18 }}>{allPassed ? '✅' : '⚠️'}</span>
          <span className="acid-progress-label" style={{ fontSize: 15, fontWeight: 700 }}>
            {passCount}/{resultCount} tests passed
          </span>
        </motion.div>
      )}

      {ACID_ORDER.map(t => {
        const status = getStatus(t)
        return (
          <motion.div
            key={t}
            className={`acid-card ${status}`}
            layout
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ type: 'spring', damping: 20, stiffness: 200 }}
          >
            <div className="acid-card-header" onClick={() => toggleCard(t)}>
              <div className="acid-card-left">
                <div className={`acid-badge ${status}`}>{ACID_LETTERS[t]}</div>
                <div>
                  <div className="acid-card-name">{ACID_NAMES[t]}</div>
                  <div className="acid-card-desc">{ACID_DESCRIPTIONS[t]}</div>
                </div>
              </div>
              <div className="acid-card-status">
                {results[t] && <span className="acid-duration">{results[t].duration_ms?.toFixed(1) || '—'}ms</span>}
                <span className={`status-tag ${status}`}>
                  {{ passed: 'PASSED', failed: 'FAILED', running: 'RUNNING…', pending: 'PENDING' }[status]}
                </span>
              </div>
            </div>

            <AnimatePresence>
              {expanded[t] && (
                <motion.div
                  className="acid-card-body"
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: 'auto', opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                >
                  <PhaseStepper phases={ACID_PHASES[t]} phaseIndex={phaseCounters[t] ?? -1} />
                  <div className="acid-description">{ACID_EXPLANATIONS[t]}</div>

                  {logs[t]?.length > 0 && (
                    <LogTerminal logs={logs[t]} termRef={el => { logRefs.current[t] = el }} />
                  )}

                  {results[t]?.details && (
                    <div className="mt-12">
                      <div className="card-title" style={{ marginBottom: 8 }}>Details</div>
                      <div className="acid-details">{JSON.stringify(results[t].details, null, 2)}</div>
                    </div>
                  )}
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        )
      })}

      {!hasResults && !running && (
        <div className="placeholder">
          <span className="placeholder-icon">🧪</span>
          Run tests using the sidebar buttons or "Run All Tests"
        </div>
      )}
    </motion.div>
  )
})

export default AcidResults
