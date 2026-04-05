import { useState, useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import Particles, { initParticlesEngine } from '@tsparticles/react'
import { loadSlim } from '@tsparticles/slim'
import { ToastProvider } from './components/Toast'
import SessionBar from './components/SessionBar'
import Sidebar from './components/Sidebar'
import BootstrapView from './views/BootstrapView'
import EntityBrowser from './views/EntityBrowser'
import QueryWorkspace from './views/QueryWorkspace'
import CrudCreate from './views/CrudCreate'
import CrudRead from './views/CrudRead'
import CrudUpdate from './views/CrudUpdate'
import CrudDelete from './views/CrudDelete'
import AcidResults from './views/AcidResults'
import { api } from './api'

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

function Dashboard() {
  const [view, setView] = useState('bootstrap')
  const [session, setSession] = useState({})
  const [particlesReady, setParticlesReady] = useState(false)

  // ACID state
  const [acidResults, setAcidResults] = useState({})
  const [acidLogs, setAcidLogs] = useState({})
  const [acidStatuses, setAcidStatuses] = useState({})
  const [acidRunning, setAcidRunning] = useState(false)
  const acidRef = useRef(null)

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

  const pageVariants = {
    initial: { opacity: 0, y: 24, scale: 0.97 },
    animate: { opacity: 1, y: 0, scale: 1, transition: { duration: 0.45, ease: [0.16, 1, 0.3, 1] } },
    exit: { opacity: 0, y: -16, scale: 0.98, transition: { duration: 0.25 } },
  }

  function renderView() {
    switch (view) {
      case 'bootstrap': return <BootstrapView onSessionUpdate={handleSessionUpdate} />
      case 'entities': return <EntityBrowser />
      case 'query': return <QueryWorkspace />
      case 'create': return <CrudCreate />
      case 'read': return <CrudRead />
      case 'update': return <CrudUpdate />
      case 'delete': return <CrudDelete />
      case 'acid': return (
        <AcidResults
          ref={acidRef}
          results={acidResults}
          setResults={setAcidResults}
          logs={acidLogs}
          setLogs={setAcidLogs}
          statuses={acidStatuses}
          setStatuses={setAcidStatuses}
          running={acidRunning}
          setRunning={setAcidRunning}
        />
      )
      default: return <BootstrapView onSessionUpdate={handleSessionUpdate} />
    }
  }

  return (
    <div className="app-layout">
      {/* Ambient glow orbs */}
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
              {renderView()}
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
