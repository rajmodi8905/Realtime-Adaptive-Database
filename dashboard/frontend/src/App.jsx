import { useState, useEffect, useRef, lazy, Suspense } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import Particles, { initParticlesEngine } from '@tsparticles/react'
import { loadSlim } from '@tsparticles/slim'
import { ToastProvider } from './components/Toast'
import SessionBar from './components/SessionBar'
import Sidebar from './components/Sidebar'
import { api } from './api'

// ── Lazy-loaded views ────────────────────────────────────────────────────────
const BootstrapView = lazy(() => import('./views/BootstrapView'))
const EntityBrowser = lazy(() => import('./views/EntityBrowser'))
const QueryWorkspace = lazy(() => import('./views/QueryWorkspace'))
const CrudCreate = lazy(() => import('./views/CrudCreate'))
const CrudRead = lazy(() => import('./views/CrudRead'))
const CrudUpdate = lazy(() => import('./views/CrudUpdate'))
const CrudDelete = lazy(() => import('./views/CrudDelete'))
const AcidResults = lazy(() => import('./views/AcidResults'))
const QueryHistory = lazy(() => import('./views/QueryHistory'))
const QueryMonitoring = lazy(() => import('./views/QueryMonitoring'))
const PerformanceBenchmark = lazy(() => import('./views/PerformanceBenchmark'))
const SessionAnalytics = lazy(() => import('./views/SessionAnalytics'))

// ── View Registry ────────────────────────────────────────────────────────────
// Add a new view: just add an entry here + import above + sidebar section
export const VIEW_REGISTRY = {
  bootstrap:   { component: BootstrapView,        label: 'Bootstrap',              needsSession: false },
  entities:    { component: EntityBrowser,         label: 'Entity Browser',         needsSession: true },
  query:       { component: QueryWorkspace,        label: 'Query Workspace',        needsSession: true },
  create:      { component: CrudCreate,            label: 'Create',                 needsSession: true },
  read:        { component: CrudRead,              label: 'Read',                   needsSession: true },
  update:      { component: CrudUpdate,            label: 'Update',                 needsSession: true },
  delete:      { component: CrudDelete,            label: 'Delete',                 needsSession: true },
  acid:        { component: AcidResults,           label: 'ACID Tests',             needsSession: true },
  history:     { component: QueryHistory,          label: 'Query History',          needsSession: true },
  monitoring:  { component: QueryMonitoring,       label: 'Query Monitoring',       needsSession: true },
  benchmark:   { component: PerformanceBenchmark,  label: 'Performance Benchmark',  needsSession: true },
  analytics:   { component: SessionAnalytics,      label: 'Session Analytics',      needsSession: true },
}

// ── Particles config ─────────────────────────────────────────────────────────
const particlesOptions = {
  background: { color: { value: 'transparent' } },
  fpsLimit: 60,
  particles: {
    number: { value: 80, density: { enable: true, width: 1920, height: 1080 } },
    color: { value: ['#6366f1', '#8b5cf6', '#a78bfa', '#22d3ee', '#ec4899'] },
    opacity: { value: { min: 0.15, max: 0.5 } },
    size: { value: { min: 1, max: 4 } },
    move: {
      enable: true,
      speed: 0.6,
      direction: 'none',
      random: true,
      straight: false,
      outModes: { default: 'out' },
      attract: { enable: true, rotateX: 600, rotateY: 1200 },
    },
    links: {
      enable: true,
      distance: 140,
      color: '#6366f1',
      opacity: 0.12,
      width: 1,
      triangles: { enable: true, color: '#6366f1', opacity: 0.02 },
    },
    twinkle: {
      particles: { enable: true, frequency: 0.03, opacity: 0.8 },
    },
  },
  interactivity: {
    events: {
      onHover: { enable: true, mode: 'grab' },
      onClick: { enable: true, mode: 'push' },
    },
    modes: {
      grab: { distance: 180, links: { opacity: 0.35, color: '#a78bfa' } },
      push: { quantity: 4 },
    },
  },
  detectRetina: true,
}

const pageVariants = {
  initial: { opacity: 0, y: 24, scale: 0.97 },
  animate: { opacity: 1, y: 0, scale: 1, transition: { duration: 0.45, ease: [0.16, 1, 0.3, 1] } },
  exit: { opacity: 0, y: -16, scale: 0.98, transition: { duration: 0.25 } },
}

// ── Dashboard ────────────────────────────────────────────────────────────────

function Dashboard() {
  const [view, setView] = useState('bootstrap')
  const [session, setSession] = useState({})
  const [particlesReady, setParticlesReady] = useState(false)

  // ACID state — shared between Sidebar triggers and AcidResults view
  const [acidResults, setAcidResults] = useState({})
  const [acidLogs, setAcidLogs] = useState({})
  const [acidStatuses, setAcidStatuses] = useState({})
  const [acidRunning, setAcidRunning] = useState(false)
  const acidRef = useRef(null)

  // Query replay state — set from QueryHistory, consumed by QueryWorkspace
  const [replayPayload, setReplayPayload] = useState(null)

  useEffect(() => {
    initParticlesEngine(async (engine) => {
      await loadSlim(engine)
    }).then(() => setParticlesReady(true))
  }, [])

  useEffect(() => {
    api.session()
      .then(res => { if (res.success) setSession(res.data) })
      .catch(() => {})
  }, [])

  function handleSessionUpdate(s) { setSession(s) }

  function handleRunTest(property) {
    setView('acid')
    setTimeout(() => { acidRef.current?.runSingle(property) }, 150)
  }

  function handleRunAll() {
    setView('acid')
    setTimeout(() => { acidRef.current?.runAll() }, 150)
  }

  function handleReplay(payload) {
    setReplayPayload(payload)
    setView('query')
  }

  function renderView() {
    const entry = VIEW_REGISTRY[view]
    if (!entry) return <VIEW_REGISTRY.bootstrap.component onSessionUpdate={handleSessionUpdate} />

    const Component = entry.component

    // Pass view-specific props
    const props = {}
    if (view === 'bootstrap') props.onSessionUpdate = handleSessionUpdate
    if (view === 'acid') {
      Object.assign(props, {
        ref: acidRef,
        results: acidResults, setResults: setAcidResults,
        logs: acidLogs, setLogs: setAcidLogs,
        statuses: acidStatuses, setStatuses: setAcidStatuses,
        running: acidRunning, setRunning: setAcidRunning,
      })
    }
    if (view === 'query') {
      props.replayPayload = replayPayload
      props.onReplayConsumed = () => setReplayPayload(null)
    }
    if (view === 'history') {
      props.onReplay = handleReplay
    }

    return <Component {...props} />
  }

  return (
    <div className="app-layout">
      <div className="ambient-orb ambient-orb-1" />
      <div className="ambient-orb ambient-orb-2" />
      <div className="ambient-orb ambient-orb-3" />

      {particlesReady && <Particles id="tsparticles" options={particlesOptions} />}

      <SessionBar session={session} />

      <div className="main-layout">
        <Sidebar
          currentView={view}
          onNavigate={setView}
          acidStatuses={acidStatuses}
          onRunTest={handleRunTest}
          onRunAll={handleRunAll}
          acidRunning={acidRunning}
        />
        <main className="content">
          <AnimatePresence mode="wait">
            <motion.div key={view} variants={pageVariants} initial="initial" animate="animate" exit="exit">
              <Suspense fallback={<div className="view-loading">Loading…</div>}>
                {renderView()}
              </Suspense>
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </div>
  )
}

export default function App() {
  return (
    <ToastProvider>
      <Dashboard />
    </ToastProvider>
  )
}
