import { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { api } from '../api'
import { useToast } from '../components/Toast'

const PAGE_SIZE = 50
const TAB_DATA = 'data'
const TAB_ER = 'er'

function toCellString(value) {
  if (value === undefined || value === null) return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function getValueByPath(row, column) {
  if (!row || typeof row !== 'object') return undefined
  if (Object.prototype.hasOwnProperty.call(row, column)) {
    return row[column]
  }
  if (!column.includes('.')) return row[column]

  const parts = column.split('.')
  let current = row
  for (const part of parts) {
    if (!current || typeof current !== 'object' || !(part in current)) {
      return undefined
    }
    current = current[part]
  }
  return current
}

function inferColumns(fields, instances) {
  if (Array.isArray(fields) && fields.length > 0) {
    return fields
  }
  if (!Array.isArray(instances) || instances.length === 0) {
    return []
  }
  return [...new Set(instances.flatMap((row) => Object.keys(row)))]
}

function prioritizeContextColumns(columns) {
  const contextPriority = ['username', 'event_id', 'id']
  const context = contextPriority.filter((key) => columns.includes(key))
  const rest = columns.filter((column) => !context.includes(column))
  return [...context, ...rest]
}

function buildLogicalER(fieldLocations = []) {
  const entitySet = new Set(['root'])
  const edgeMap = new Map()

  for (const loc of fieldLocations) {
    const path = String(loc?.field_path || '')
    if (!path) continue

    if (path.includes('.')) {
      const entity = path.split('.')[0]
      entitySet.add(entity)
    } else {
      entitySet.add('root')
    }
  }

  for (const loc of fieldLocations) {
    const path = String(loc?.field_path || '')
    if (!path || !path.includes('.')) continue

    const entity = path.split('.')[0]
    const joinKeys = Array.isArray(loc?.join_keys) ? loc.join_keys : []
    if (!joinKeys.length) continue

    for (const key of joinKeys) {
      if (!key || key === entity) continue
      const from = 'root'
      const to = entity
      const edgeKey = `${from}->${to}`
      if (!edgeMap.has(edgeKey)) {
        edgeMap.set(edgeKey, { from, to, via: new Set() })
      }
      edgeMap.get(edgeKey).via.add(key)
    }
  }

  const nodes = [...entitySet].sort()
  const edges = [...edgeMap.values()]
    .map((edge) => ({ ...edge, via: [...edge.via].sort() }))
    .sort((a, b) => `${a.from}.${a.to}`.localeCompare(`${b.from}.${b.to}`))

  return { nodes, edges }
}

function ErDiagram({ nodes, edges }) {
  if (!nodes.length) {
    return <div className="placeholder">No logical entities available</div>
  }

  return (
    <div className="er-grid">
      {nodes.map((node) => (
        <div key={node} className="er-node">
          <div className="er-node-name">{node}</div>
          <div className="er-node-type">logical entity</div>
        </div>
      ))}

      <div className="er-edges">
        <div className="er-edges-title">Relationships</div>
        {edges.length === 0 ? (
          <div className="placeholder">No explicit relationships inferred</div>
        ) : (
          <div className="er-edges-list">
            {edges.map((edge, index) => (
              <div key={`${edge.from}-${edge.to}-${index}`} className="er-edge-item">
                <span className="er-edge-entity">{edge.from}</span>
                <span className="er-edge-arrow">→</span>
                <span className="er-edge-entity">{edge.to}</span>
                {edge.via.length > 0 && (
                  <span className="er-edge-via">via {edge.via.join(', ')}</span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

export default function EntityBrowser() {
  const [entities, setEntities] = useState([])
  const [selectedEntity, setSelectedEntity] = useState('')
  const [offset, setOffset] = useState(0)
  const [loadingEntities, setLoadingEntities] = useState(true)
  const [loadingEntityData, setLoadingEntityData] = useState(false)
  const [error, setError] = useState(null)
  const [entityData, setEntityData] = useState(null)
  const [activeTab, setActiveTab] = useState(TAB_DATA)
  const [schemaPlan, setSchemaPlan] = useState(null)
  const toast = useToast()

  useEffect(() => {
    let mounted = true

    async function loadEntityList() {
      setLoadingEntities(true)
      setError(null)
      try {
        const res = await api.entities()
        if (!mounted) return

        if (!res.success) {
          setError(res.error || 'Bootstrap first to load entities')
          setEntities([])
          return
        }

        const names = Array.isArray(res.data) ? res.data : []
        setEntities(names)

        if (names.length > 0) {
          setSelectedEntity(names[0])
          setOffset(0)
        }
      } catch (err) {
        if (!mounted) return
        setError(err.message)
      } finally {
        if (mounted) setLoadingEntities(false)
      }
    }

    loadEntityList()
    return () => {
      mounted = false
    }
  }, [])

  useEffect(() => {
    let mounted = true

    async function loadSchemaPlan() {
      try {
        const res = await api.schemaplan()
        if (!mounted) return
        if (res.success) {
          setSchemaPlan(res.data)
        }
      } catch {
      }
    }

    loadSchemaPlan()
    return () => {
      mounted = false
    }
  }, [])

  useEffect(() => {
    if (!selectedEntity) {
      setEntityData(null)
      return
    }

    let mounted = true

    async function loadEntityPage() {
      setLoadingEntityData(true)
      setError(null)
      try {
        const res = await api.entity(selectedEntity, PAGE_SIZE, offset)
        if (!mounted) return

        if (!res.success) {
          setError(res.error || `Failed to load entity '${selectedEntity}'`)
          setEntityData(null)
          return
        }

        setEntityData(res.data)
      } catch (err) {
        if (!mounted) return
        setError(err.message)
        setEntityData(null)
      } finally {
        if (mounted) setLoadingEntityData(false)
      }
    }

    loadEntityPage()
    return () => {
      mounted = false
    }
  }, [selectedEntity, offset])

  function handleSelectEntity(name) {
    setSelectedEntity(name)
    setOffset(0)
  }

  const instances = entityData?.instances || []
  const columns = useMemo(() => {
    const baseColumns = inferColumns(entityData?.fields || [], instances)
    return prioritizeContextColumns(baseColumns)
  }, [entityData?.fields, instances])

  const page = Math.floor(offset / PAGE_SIZE) + 1
  const hasPrev = offset > 0
  const hasNext = instances.length === PAGE_SIZE
  const erData = useMemo(
    () => buildLogicalER(schemaPlan?.fields || []),
    [schemaPlan?.fields],
  )

  if (loadingEntities) {
    return (
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} style={{ textAlign: 'center', paddingTop: 80 }}>
        <div className="spinner" style={{ margin: '0 auto 16px' }} />
        <div style={{ color: '#64748b' }}>Loading logical entities…</div>
      </motion.div>
    )
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Entity Browser</h1>
      <p className="view-subtitle">Browse logical entities and reconstructed records without backend storage details</p>

      <div className="entity-tabs" role="tablist" aria-label="Entity browser tabs">
        <button
          type="button"
          className={`entity-tab-btn ${activeTab === TAB_DATA ? 'active' : ''}`}
          onClick={() => setActiveTab(TAB_DATA)}
        >
          Data
        </button>
        <button
          type="button"
          className={`entity-tab-btn ${activeTab === TAB_ER ? 'active' : ''}`}
          onClick={() => setActiveTab(TAB_ER)}
        >
          E-R Diagram
        </button>
      </div>

      {error && <div className="placeholder" style={{ marginBottom: 20 }}>{error}</div>}

      {activeTab === TAB_DATA ? (
      <div className="entity-layout">
        <div className="card entity-list">
          <div style={{ marginBottom: 10, fontWeight: 600 }}>Entities</div>
          {entities.length === 0 ? (
            <div className="placeholder">Bootstrap first to see entities</div>
          ) : (
            entities.map((name) => (
              <button
                key={name}
                className={`entity-name-btn ${selectedEntity === name ? 'active' : ''}`}
                onClick={() => handleSelectEntity(name)}
              >
                {name}
              </button>
            ))
          )}
        </div>

        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <div style={{ fontWeight: 600 }}>
              {entityData?.entity_name || selectedEntity || 'Select an entity'}
            </div>
            {selectedEntity && (
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                Page {page} · {instances.length} row{instances.length === 1 ? '' : 's'}
              </div>
            )}
          </div>

          {loadingEntityData ? (
            <div className="placeholder">Loading entity data…</div>
          ) : !selectedEntity ? (
            <div className="placeholder">Select an entity from the list</div>
          ) : instances.length === 0 ? (
            <div className="placeholder">No records found for this entity</div>
          ) : (
            <>
              <div className="table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      {columns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {instances.map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {columns.map((column) => {
                          const value = toCellString(getValueByPath(row, column))
                          return (
                            <td key={`${rowIndex}-${column}`} title={value}>
                              {value}
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="pagination">
                <button
                  className="btn btn-ghost btn-sm"
                  disabled={!hasPrev}
                  onClick={() => setOffset((prev) => Math.max(0, prev - PAGE_SIZE))}
                >
                  ← Prev
                </button>
                <span className="page-info">Page {page}</span>
                <button
                  className="btn btn-ghost btn-sm"
                  disabled={!hasNext}
                  onClick={() => {
                    setOffset((prev) => prev + PAGE_SIZE)
                    toast(`Loading next page for '${selectedEntity}'`, 'info')
                  }}
                >
                  Next →
                </button>
              </div>
            </>
          )}
        </div>
      </div>
      ) : (
        <div className="card">
          <div style={{ marginBottom: 12, fontWeight: 600 }}>Logical Entity Relationships</div>
          <ErDiagram nodes={erData.nodes} edges={erData.edges} />
        </div>
      )}
    </motion.div>
  )
}
