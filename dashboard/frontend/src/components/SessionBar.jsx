export default function SessionBar({ session }) {
  return (
    <header className="header">
      <div className="header-brand">
        <div className="brand-icon">◆</div>
        <span className="brand-name">Logical Dashboard</span>
      </div>
      <div className="session-pills">
        <div className="pill">
          <span className="pill-label">Schema</span>
          <span className="pill-value">{session.schema_name || '—'}</span>
        </div>
        <div className="pill">
          <span className="pill-label">Ver</span>
          <span className="pill-value">{session.version || '—'}</span>
        </div>
        <div className="pill">
          <span className="pill-label">Root</span>
          <span className="pill-value">{session.root_entity || '—'}</span>
        </div>
        <div className="pill">
          <span className="pill-label">Fields</span>
          <span className="pill-value">{session.field_count ?? '—'}</span>
        </div>
        <div className="pill">
          <span className={`status-dot ${session.mysql_connected ? 'online' : 'offline'}`} />
          <span className="pill-value">MySQL</span>
        </div>
        <div className="pill">
          <span className={`status-dot ${session.mongo_connected ? 'online' : 'offline'}`} />
          <span className="pill-value">MongoDB</span>
        </div>
      </div>
    </header>
  )
}
