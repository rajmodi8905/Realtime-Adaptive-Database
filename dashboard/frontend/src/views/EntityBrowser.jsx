import { useState, useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

/* ─── colour tokens ─────────────────────────────────────────────── */
const SQL_ACCENT   = '#0ea5e9' // sky blue
const SQL_LIGHT    = '#7dd3fc'
const SQL_BG       = 'rgba(14,165,233,0.08)'
const MONGO_ACCENT = '#10b981' // emerald
const MONGO_LIGHT  = '#6ee7b7'
const MONGO_BG     = 'rgba(16,185,129,0.08)'
const BOTH_ACCENT  = '#f59e0b'

/* ─── canvas constants ──────────────────────────────────────────── */
const CANVAS_H   = 480
const NODE_W     = 280
const NODE_GAP_X = 120
const NODE_GAP_Y = 40
const CANVAS_PAD = 32

/* ─── tiny helpers ──────────────────────────────────────────────── */
function BackendBadge({ backend }) {
  const map = {
    sql:   { label: 'SQL',   bg: 'rgba(14,165,233,0.15)',  color: SQL_LIGHT },
    mongo: { label: 'Mongo', bg: 'rgba(16,185,129,0.15)',  color: MONGO_LIGHT },
    both:  { label: 'Both',  bg: 'rgba(245,158,11,0.15)',  color: BOTH_ACCENT },
  }
  const s = map[backend] || { label: backend, bg: 'rgba(255,255,255,0.08)', color: '#94a3b8' }
  return (
    <span style={{
      display: 'inline-block', padding: '1px 7px', borderRadius: 999,
      fontSize: 10, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace",
      background: s.bg, color: s.color, letterSpacing: '0.05em',
    }}>{s.label}</span>
  )
}

/* ─── Field detail panel ────────────────────────────────────────── */
function FieldDetail({ fields, tableName, accent, isPrimary }) {
  const relevant = isPrimary ? fields : fields.filter(f => f.table_or_collection === tableName)
  if (!relevant.length) return null
  return (
    <motion.div
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
      exit={{ opacity: 0, height: 0 }}
      transition={{ duration: 0.3, ease: [0.16, 1, 0.3, 1] }}
      style={{ overflow: 'hidden' }}
    >
      <div className="custom-scrollbar" style={{
        margin: '14px 0 0', padding: '12px 14px',
        background: 'rgba(0,0,0,0.25)', borderRadius: 10,
        borderTop: `2px solid ${accent}40`,
        maxHeight: 280, overflowY: 'auto'
      }}>
        {relevant.map((f, i) => (
          <div key={i} style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '5px 0', gap: 8,
            borderBottom: i < relevant.length - 1 ? '1px solid rgba(255,255,255,0.04)' : 'none',
          }}>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#e2e8f0', flex: 1 }}>
              {f.field_path}
            </span>
            <BackendBadge backend={f.backend} />
          </div>
        ))}
      </div>
    </motion.div>
  )
}

/* ─── Node card (stateless — drag is managed by SchemaPanel) ───── */
function NodeCard({ node, accent, bg, isPrimary, fields, expanded, pos, onPointerDown, isDragging }) {
  const { name, pk, columns, embedded_paths, referenced_paths, foreignKeys = [] } = node

  const visibleCols = columns
    ? Object.entries(columns)
    : embedded_paths?.map(p => [p, 'embedded']) ?? []

  return (
    <div
      onMouseDown={(e) => onPointerDown(e, name, pos)}
      onTouchStart={(e) => onPointerDown(e, name, pos)}
      style={{
        position: 'absolute',
        left: pos.x || 0,
        top: pos.y || 0,
        width: NODE_W,
        background: expanded ? bg : 'var(--bg-card)',
        border: `1.5px solid ${expanded ? accent : 'rgba(99,102,241,0.12)'}`,
        borderRadius: 14,
        padding: '16px 18px',
        cursor: isDragging ? 'grabbing' : 'grab',
        transition: isDragging ? 'none' : 'border-color 0.25s, background 0.25s, box-shadow 0.25s',
        boxShadow: isDragging
          ? '0 8px 32px rgba(0,0,0,0.4)'
          : expanded ? `0 0 28px ${accent}22` : '0 2px 12px rgba(0,0,0,0.25)',
        zIndex: isDragging ? 50 : expanded ? 20 : 10,
        userSelect: 'none',
        transform: isDragging ? 'scale(1.03)' : 'scale(1)',
        touchAction: 'none',
      }}
    >
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
        {isPrimary && (
          <span style={{
            fontSize: 9, fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.1em',
            padding: '2px 8px', borderRadius: 999, background: `${accent}22`, color: accent,
          }}>ROOT</span>
        )}
        <span style={{
          fontFamily: "'JetBrains Mono', monospace", fontWeight: 700, fontSize: 15,
          color: expanded ? accent : '#f1f5f9', flex: 1,
        }}>{name}</span>
        <motion.span
          animate={{ rotate: expanded ? 180 : 0 }}
          transition={{ duration: 0.25 }}
          style={{ fontSize: 12, color: '#64748b' }}
        >▼</motion.span>
      </div>

      {/* Key/FK badges */}
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
        {pk && (
          <span style={{
            fontSize: 10, fontFamily: "'JetBrains Mono', monospace", fontWeight: 700,
            padding: '2px 8px', borderRadius: 999,
            background: `${accent}18`, color: accent, border: `1px solid ${accent}33`,
          }}>🔑 {pk}</span>
        )}
        {foreignKeys.map((fk, i) => (
          <span key={i} style={{
            fontSize: 10, fontFamily: "'JetBrains Mono', monospace",
            padding: '2px 8px', borderRadius: 999,
            background: 'rgba(245,158,11,0.12)', color: BOTH_ACCENT, border: '1px solid rgba(245,158,11,0.25)',
          }}>🔗 {fk.column} → {fk.references_table}</span>
        ))}
      </div>

      {/* Columns */}
      <div className="custom-scrollbar" style={{ 
        display: 'flex', flexDirection: 'column', gap: 3, 
        maxHeight: expanded ? 220 : 'none', 
        overflowY: expanded ? 'auto' : 'visible',
        paddingRight: expanded ? 4 : 0 
      }}>
        {visibleCols.slice(0, expanded ? visibleCols.length : 4).map(([col, type], i) => (
          <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#94a3b8' }}>{col}</span>
            {type !== 'embedded' && (
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#475569' }}>{type}</span>
            )}
          </div>
        ))}
        {!expanded && visibleCols.length > 4 && (
          <span style={{ fontSize: 11, color: '#475569', marginTop: 2 }}>+{visibleCols.length - 4} more…</span>
        )}
      </div>

      {/* Referenced paths */}
      {referenced_paths?.length > 0 && (
        <div style={{ marginTop: 10 }}>
          {referenced_paths.map((p, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 }}>
              <span style={{ fontSize: 10, color: MONGO_ACCENT }}>↗</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#6ee7b7' }}>{p}</span>
            </div>
          ))}
        </div>
      )}

      {/* Expanded field detail */}
      <AnimatePresence>
        {expanded && <FieldDetail fields={fields} tableName={name} accent={accent} isPrimary={isPrimary} />}
      </AnimatePresence>
    </div>
  )
}

/* ─── SVG connector ─────────────────────────────────────────────── */
function GraphConnector({ fromPos, toPos, color, label, cardinality, index = 0 }) {
  if (!fromPos || !toPos || !Number.isFinite(fromPos.x) || !Number.isFinite(toPos.x) || !Number.isFinite(fromPos.y) || !Number.isFinite(toPos.y)) return null

  const stagger = index * 28
  const nodeH = 160
  const x1 = (fromPos.x || 0) + NODE_W
  const y1 = (fromPos.y || 0) + nodeH / 2 + stagger
  const x2 = (toPos.x || 0)
  const y2 = (toPos.y || 0) + nodeH / 2 + stagger

  const goingLeft = x2 < x1
  const ax1 = goingLeft ? (fromPos.x || 0) : x1
  const ay1 = goingLeft ? (fromPos.y || 0) + nodeH / 2 + stagger : y1
  const ax2 = goingLeft ? (toPos.x || 0) + NODE_W : x2
  const ay2 = goingLeft ? (toPos.y || 0) + nodeH / 2 + stagger : y2
  const dx  = Math.abs(ax2 - ax1)
  const cpOff = Math.max(60, dx * 0.4)
  const cx1 = goingLeft ? ax1 - cpOff : ax1 + cpOff
  const cx2 = goingLeft ? ax2 + cpOff : ax2 - cpOff
  const midX = (ax1 + ax2) / 2
  const midY = (ay1 + ay2) / 2
  const gradId = `lg-${label}-${index}`

  return (
    <g>
      <defs>
        <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor={color} stopOpacity="0.6" />
          <stop offset="100%" stopColor={color} stopOpacity="0.2" />
        </linearGradient>
      </defs>
      <path d={`M${ax1},${ay1} C${cx1},${ay1} ${cx2},${ay2} ${ax2},${ay2}`}
        fill="none" stroke={color} strokeWidth="6" strokeOpacity="0.05" />
      <path d={`M${ax1},${ay1} C${cx1},${ay1} ${cx2},${ay2} ${ax2},${ay2}`}
        fill="none" stroke={`url(#${gradId})`} strokeWidth="1.5"
        strokeDasharray="6 4" strokeLinecap="round">
        <animate attributeName="stroke-dashoffset" values="20;0" dur="1.5s" repeatCount="indefinite" />
      </path>
      <rect x={midX - 20} y={midY - 10} width={40} height={20} rx={5}
        fill="var(--bg-deep)" stroke={color} strokeWidth="1" strokeOpacity="0.4" />
      <text x={midX} y={midY + 4.5} textAnchor="middle"
        fill={color} fontSize="9" fontFamily="JetBrains Mono, monospace" fontWeight="700"
        opacity="0.9">{cardinality || '1:N'}</text>
    </g>
  )
}

/* ─── Dot-grid background ───────────────────────────────────────── */
function DotGrid() {
  return (
    <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none' }}>
      <defs>
        <pattern id="dotgrid" width="24" height="24" patternUnits="userSpaceOnUse">
          <circle cx="1" cy="1" r="0.8" fill="rgba(255,255,255,0.04)" />
        </pattern>
      </defs>
      <rect width="100%" height="100%" fill="url(#dotgrid)" />
    </svg>
  )
}

/* ─── Auto layout ───────────────────────────────────────────────── */
function computeInitialPositions(nodes, relationships) {
  const positions = {}
  const childSet = new Set(relationships.map(r => r.child_table || r.child))
  const roots    = nodes.filter(n => !childSet.has(n.name))
  const children = nodes.filter(n => childSet.has(n.name))

  roots.forEach((n, i) => {
    positions[n.name] = { x: CANVAS_PAD, y: CANVAS_PAD + i * (180 + NODE_GAP_Y) }
  })
  children.forEach((n, i) => {
    positions[n.name] = { x: CANVAS_PAD + NODE_W + NODE_GAP_X, y: CANVAS_PAD + i * (180 + NODE_GAP_Y) }
  })
  nodes.forEach((n, i) => {
    if (!positions[n.name]) {
      positions[n.name] = { x: CANVAS_PAD + i * (NODE_W + NODE_GAP_X), y: CANVAS_PAD }
    }
  })
  return positions
}

function computeCanvasSize(positions, expanded = {}) {
  let maxX = 0, maxY = 0
  Object.entries(positions).forEach(([name, { x, y }]) => {
    let safeX = Number.isFinite(x) ? x : 0
    let safeY = Number.isFinite(y) ? y : 0
    const cardHeight = expanded[name] ? 650 : 250
    maxX = Math.max(maxX, safeX + NODE_W + CANVAS_PAD)
    maxY = Math.max(maxY, safeY + cardHeight + CANVAS_PAD)
  })
  return { width: Math.max(maxX, 700), height: Math.max(maxY, CANVAS_H) }
}

/* ─── Schema Panel — owns drag state & global listeners ─────────── */
function SchemaPanel({ title, icon, accent, bg, nodes, relationships, fields }) {
  const [expanded, setExpanded] = useState({})
  const [positions, setPositions] = useState(() => computeInitialPositions(nodes, relationships))
  const [draggingNode, setDraggingNode] = useState(null)
  const dragRef = useRef(null)   // { name, mx, my, ox, oy, moved }

  // One global listener pair for the whole panel — no deps so it never re-attaches
  useEffect(() => {
    function onMove(e) {
      if (!dragRef.current) return
      
      const clientX = e.clientX ?? (e.touches && e.touches.length > 0 ? e.touches[0].clientX : null)
      const clientY = e.clientY ?? (e.touches && e.touches.length > 0 ? e.touches[0].clientY : null)
      
      if (!Number.isFinite(clientX) || !Number.isFinite(clientY)) return

      const dx = clientX - dragRef.current.mx
      const dy = clientY - dragRef.current.my
      if (!dragRef.current.moved && Math.abs(dx) < 4 && Math.abs(dy) < 4) return
      if (!dragRef.current.moved) {
        dragRef.current.moved = true
        setDraggingNode(dragRef.current.name)
      }
      
      if (e.cancelable) e.preventDefault()
      
      const ox = dragRef.current.ox
      const oy = dragRef.current.oy
      const name = dragRef.current.name
      
      setPositions(prev => {
        const nx = Math.max(0, ox + dx)
        const ny = Math.max(0, oy + dy)
        if (!Number.isFinite(nx) || !Number.isFinite(ny)) return prev
        return {
          ...prev,
          [name]: { x: nx, y: ny },
        }
      })
    }
    
    function onUp() {
      if (!dragRef.current) return
      const wasDrag = dragRef.current.moved
      const nodeName = dragRef.current.name
      dragRef.current = null
      setDraggingNode(null)
      if (!wasDrag) {
        setExpanded(prev => ({ ...prev, [nodeName]: !prev[nodeName] }))
      }
    }
    
    window.addEventListener('pointermove', onMove, { passive: false })
    window.addEventListener('pointerup', onUp)
    window.addEventListener('mousemove', onMove, { passive: false })
    window.addEventListener('mouseup', onUp)
    window.addEventListener('touchmove', onMove, { passive: false })
    window.addEventListener('touchend', onUp)
    
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
      window.removeEventListener('touchmove', onMove)
      window.removeEventListener('touchend', onUp)
    }
  }, [])

  function handleNodePointerDown(e, name, nodePos) {
    if (e.button !== undefined && e.button !== 0) return
    const clientX = e.clientX ?? (e.touches && e.touches.length > 0 ? e.touches[0].clientX : null)
    const clientY = e.clientY ?? (e.touches && e.touches.length > 0 ? e.touches[0].clientY : null)
    
    if (!Number.isFinite(clientX) || !Number.isFinite(clientY)) return
    
    if (e.cancelable && !e.touches) e.preventDefault()
    
    const ox = Number.isFinite(nodePos.x) ? nodePos.x : 0
    const oy = Number.isFinite(nodePos.y) ? nodePos.y : 0
    dragRef.current = { name, mx: clientX, my: clientY, ox, oy, moved: false }
  }

  const canvasSize = computeCanvasSize(positions, expanded)

  return (
    <div>
      {/* Panel header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
        <span style={{
          width: 36, height: 36, borderRadius: 10,
          background: `${accent}18`, border: `1.5px solid ${accent}30`,
          display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18,
        }}>{icon}</span>
        <div>
          <div style={{ fontFamily: "'Outfit', sans-serif", fontWeight: 900, fontSize: 20, color: '#f1f5f9', letterSpacing: '-0.03em' }}>
            {title}
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 1 }}>
            {nodes.length} {nodes.length === 1 ? 'table' : title.includes('SQL') ? 'tables' : 'collections'} · {relationships.length} relationship{relationships.length !== 1 ? 's' : ''} · drag nodes to rearrange
          </div>
        </div>
      </div>

      {/* Relationship key */}
      {relationships.length > 0 && (
        <div style={{ marginBottom: 16, display: 'flex', gap: 16, flexWrap: 'wrap' }}>
          {relationships.map((rel, i) => (
            <div key={i} style={{
              display: 'flex', alignItems: 'center', gap: 8,
              padding: '6px 12px', borderRadius: 8,
              background: `${accent}08`, border: `1px solid ${accent}18`, fontSize: 11,
            }}>
              <span style={{ color: accent, fontWeight: 700 }}>{rel.parent_table || rel.parent}</span>
              <span style={{ color: '#475569' }}>──</span>
              <span style={{
                fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#f59e0b',
                background: 'rgba(245,158,11,0.1)', padding: '1px 6px', borderRadius: 4,
              }}>
                {rel.cardinality === 'one-to-many' ? '1:N' : rel.cardinality || 'ref'}
              </span>
              <span style={{ color: '#475569' }}>──</span>
              <span style={{ color: accent, fontWeight: 700 }}>{rel.child_table || rel.child}</span>
              {(rel.source_path || rel.via) && (
                <span style={{ color: '#475569', fontSize: 10 }}>via {rel.source_path || rel.via}</span>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Graph canvas */}
      <div style={{
        position: 'relative', width: '100%',
        height: canvasSize.height, minHeight: CANVAS_H,
        overflow: 'visible', borderRadius: 12,
        background: 'rgba(0,0,0,0.15)', border: '1px solid rgba(255,255,255,0.04)',
      }}>
        <DotGrid />

        {/* SVG connectors */}
        <svg style={{
          position: 'absolute', inset: 0,
          width: '100%', height: '100%',
          pointerEvents: 'none', overflow: 'visible', zIndex: 5,
        }}>
          {relationships.map((rel, i) => {
            const fromName = rel.parent_table || rel.parent
            const toName   = rel.child_table  || rel.child
            const fromPos  = positions[fromName]
            const toPos    = positions[toName]
            if (!fromPos || !toPos) return null
            return (
              <GraphConnector
                key={i} fromPos={fromPos} toPos={toPos} color={accent}
                label={`${fromName}-${toName}`}
                cardinality={rel.cardinality === 'one-to-many' ? '1:N' : rel.cardinality || 'ref'}
                index={i}
              />
            )
          })}
        </svg>

        {/* Node cards */}
        {nodes.map(node => (
          <NodeCard
            key={node.name}
            node={node}
            accent={accent}
            bg={bg}
            isPrimary={node.isPrimary}
            fields={fields}
            expanded={!!expanded[node.name]}
            pos={positions[node.name] || { x: CANVAS_PAD, y: CANVAS_PAD }}
            onPointerDown={handleNodePointerDown}
            isDragging={draggingNode === node.name}
          />
        ))}
      </div>
    </div>
  )
}

/* ─── Main component ────────────────────────────────────────────── */
export default function EntityBrowser() {
  const [plan, setPlan] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const toast = useToast()

  useEffect(() => {
    setLoading(true)
    api.schemaplan()
      .then(res => {
        if (res.success) setPlan(res.data)
        else setError(res.error || 'Bootstrap first to generate schema plans')
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} style={{ textAlign: 'center', paddingTop: 80 }}>
        <div className="spinner" style={{ margin: '0 auto 16px' }} />
        <div style={{ color: '#64748b', fontFamily: "'Outfit', sans-serif" }}>Loading schema plans…</div>
      </motion.div>
    )
  }

  if (error || !plan) {
    return (
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
        <h1 className="view-title">Schema Explorer</h1>
        <div className="placeholder">
          <span className="placeholder-icon">🗄️</span>
          {error || 'Bootstrap the database first to generate schema plans'}
        </div>
      </motion.div>
    )
  }

  const sqlNodes = plan.sql.tables.map(t => ({
    name: t.table_name,
    pk: t.primary_key,
    columns: t.columns,
    foreignKeys: t.foreign_keys || [],
    indexes: t.indexes || [],
    isPrimary: (t.foreign_keys || []).length === 0,
  }))

  const mongoNodes = plan.mongo.collections.map(c => ({
    name: c.collection_name,
    embedded_paths: c.embedded_paths || [],
    referenced_paths: c.referenced_paths || [],
    isPrimary: Object.keys(c.reference_collections || {}).length > 0,
  }))

  const mongoRels = []
  plan.mongo.collections.forEach(c => {
    Object.entries(c.reference_collections || {}).forEach(([path, childColl]) => {
      mongoRels.push({ parent: c.collection_name, child: childColl, cardinality: 'ref', via: path })
    })
  })

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Schema Explorer</h1>
      <p className="view-subtitle">
        Visual graph of how your data is split across MySQL tables and MongoDB collections — drag nodes to rearrange, click to expand details
      </p>

      <div className="card" style={{ marginBottom: 28 }}>
        <SchemaPanel
          title="SQL Schema (MySQL)" icon="🗄️"
          accent={SQL_ACCENT} bg={SQL_BG}
          nodes={sqlNodes}
          relationships={plan.sql.relationships || []}
          fields={plan.fields || []}
        />
      </div>

      <div className="card">
        <SchemaPanel
          title="MongoDB Collections" icon="🍃"
          accent={MONGO_ACCENT} bg={MONGO_BG}
          nodes={mongoNodes}
          relationships={mongoRels}
          fields={plan.fields || []}
        />
      </div>
    </motion.div>
  )
}
