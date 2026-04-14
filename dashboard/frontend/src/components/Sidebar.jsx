import { useRef, useCallback } from 'react'
import { motion } from 'framer-motion'

// ── Data-driven navigation sections ─────────────────────────────────────────
// Add new views: just add an entry here (and register component in App.jsx)
export const NAV_SECTIONS = [
  {
    label: 'Setup',
    items: [{ id: 'bootstrap', icon: '⚡', text: 'Bootstrap' }],
  },
  {
    label: 'Data',
    items: [
      { id: 'entities', icon: '📦', text: 'Entity Browser' },
      { id: 'query',    icon: '🔍', text: 'Query Workspace' },
      { id: 'history',  icon: '📜', text: 'Query History' },
    ],
  },
  {
    label: 'CRUD Operations',
    items: [
      { id: 'create', icon: '＋', text: 'Create' },
      { id: 'read',   icon: '📖', text: 'Read' },
      { id: 'update', icon: '✏️', text: 'Update' },
      { id: 'delete', icon: '🗑', text: 'Delete' },
    ],
  },
  {
    label: 'Analytics',
    items: [
      { id: 'monitoring', icon: '📊', text: 'Query Monitoring' },
      { id: 'benchmark',  icon: '⏱',  text: 'Performance Benchmark' },
      { id: 'comparative', icon: '⚖️', text: 'Comparative Analysis' },
      { id: 'analytics',  icon: '📈', text: 'Session Analytics' },
    ],
  },
]

const ACID_ORDER = ['reconstruction', 'atomicity', 'consistency', 'isolation', 'durability']
const ACID_LETTERS = { reconstruction: 'R', atomicity: 'A', consistency: 'C', isolation: 'I', durability: 'D' }

function RippleButton({ className, onClick, children, initial, animate, transition }) {
  const btnRef = useRef(null)

  const handleClick = useCallback((e) => {
    const btn = btnRef.current
    if (btn) {
      const rect = btn.getBoundingClientRect()
      const ripple = document.createElement('span')
      ripple.className = 'ripple'
      const size = Math.max(rect.width, rect.height)
      ripple.style.width = ripple.style.height = `${size}px`
      ripple.style.left = `${e.clientX - rect.left - size / 2}px`
      ripple.style.top = `${e.clientY - rect.top - size / 2}px`
      ripple.style.position = 'absolute'
      ripple.style.pointerEvents = 'none'
      ripple.style.borderRadius = '50%'
      btn.appendChild(ripple)
      setTimeout(() => ripple.remove(), 600)
    }
    onClick?.(e)
  }, [onClick])

  return (
    <motion.button
      ref={btnRef}
      className={className}
      onClick={handleClick}
      initial={initial}
      animate={animate}
      transition={transition}
    >
      {children}
    </motion.button>
  )
}

export default function Sidebar({ currentView, onNavigate, acidStatuses, onRunTest, onRunAll, acidRunning }) {
  return (
    <nav className="sidebar" role="navigation" aria-label="Main navigation">
      {NAV_SECTIONS.map((sec, si) => (
        <div key={sec.label}>
          <div className="sidebar-section">{sec.label}</div>
          {sec.items.map((item, ii) => (
            <RippleButton
              key={item.id}
              className={`sidebar-btn ${currentView === item.id ? 'active' : ''}`}
              onClick={() => onNavigate(item.id)}
              initial={{ opacity: 0, x: -24 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: si * 0.1 + ii * 0.05, duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
            >
              <span className="sidebar-icon">{item.icon}</span>
              <span>{item.text}</span>
            </RippleButton>
          ))}
        </div>
      ))}

      <div className="sidebar-section">ACID + R Tests</div>
      <div className="acid-quick-row">
        {ACID_ORDER.map(t => (
          <motion.button
            key={t}
            className={`acid-mini-btn ${acidStatuses[t] || 'pending'}`}
            onClick={() => onRunTest(t)}
            title={t.charAt(0).toUpperCase() + t.slice(1)}
            whileHover={{ scale: 1.15, y: -3 }}
            whileTap={{ scale: 0.9 }}
          >
            {ACID_LETTERS[t]}
          </motion.button>
        ))}
      </div>
      <RippleButton
        className="sidebar-btn"
        onClick={onRunAll}
        initial={{ opacity: 0, x: -24 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ delay: 0.55, duration: 0.4 }}
      >
        <span className="sidebar-icon">▶</span>
        <span>Run All Tests</span>
      </RippleButton>
      <RippleButton
        className={`sidebar-btn ${currentView === 'acid' ? 'active' : ''}`}
        onClick={() => onNavigate('acid')}
        initial={{ opacity: 0, x: -24 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ delay: 0.6, duration: 0.4 }}
      >
        <span className="sidebar-icon">🧪</span>
        <span>Test Results</span>
      </RippleButton>
    </nav>
  )
}
