import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { api } from '../api';
import { useToast } from '../components/Toast';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts';

export default function ComparativeAnalysis() {
  const [results, setResults] = useState([]);
  const [running, setRunning] = useState(false);
  const toast = useToast();

  const scenarios = [
    { id: 'retrieve_users_sql', label: 'Retrieve Users (SQL)', desc: 'Framework vs Direct SQL' },
    { id: 'access_nested_mongo', label: 'Access Nested docs (MongoDB)', desc: 'Framework vs Direct Mongo' },
    { id: 'update_multi_entity', label: 'Update Records', desc: 'Framework vs Direct Cross-DB Updates' },
    { id: 'custom_query', label: 'Custom Benchmark', desc: 'Custom Field & Runtime Filter Execution' }
  ];

  const [activeTab, setActiveTab] = useState('retrieve_users_sql');
  const [iterations, setIterations] = useState(25);
  const [warmup, setWarmup] = useState(5);
  const [availableFields, setAvailableFields] = useState([]);
  const [selectedField, setSelectedField] = useState('');
  const [currentFilterValue, setCurrentFilterValue] = useState('');
  const [customFilters, setCustomFilters] = useState({});

  useEffect(() => {
    const fetchSchema = async () => {
      const res = await api.schemaplan();
      if (res.success && res.data?.fields) {
          setAvailableFields(res.data.fields);
          if (res.data.fields.length > 0) {
              setSelectedField(res.data.fields[0].column_or_path);
          }
      }
    };
    fetchSchema();
  }, []);

  const handleAddFilter = () => {
      if (selectedField && currentFilterValue) {
          let val = currentFilterValue;
          try {
              // Try to parse so numbers and booleans are passed correctly instead of strings
              val = JSON.parse(currentFilterValue);
          } catch (e) {
              // Ignore if it's just a regular string
          }
          setCustomFilters(prev => ({ ...prev, [selectedField]: val }));
          setCurrentFilterValue('');
      }
  };

  const currentDesc = scenarios.find(s => s.id === activeTab)?.desc;

  const runBenchmark = async () => {
    setRunning(true);
    try {
      const payload = {
        type: 'comparative',
        scenario: activeTab,
        iterations: iterations,
        warmup: warmup,
        label: `Comparison: ${activeTab}`
      };
      if (activeTab === 'custom_query') {
         payload.custom_query = {
             operation: 'read',
             filters: customFilters
         };
      }
      
      const res = await api.benchmarkRun(payload);
      if (res.success) {
        toast('Comparative benchmark complete', 'success');
        setResults(prev => [res.data, ...prev]);
      } else {
        toast(res.error || 'Benchmark failed', 'error');
      }
    } catch (err) {
      toast('Benchmark failed', 'error');
    } finally {
      setRunning(false);
    }
  };

  const currentResults = results.filter(r => r.config?.scenario === activeTab);
  const latest = currentResults[0];

  let barData = [];
  let lineData = [];
  let breakdownData = [];

  if (latest && latest.results) {
    barData = [
      {
        name: 'Average Latency (ms)',
        Logical: latest.results?.logical?.avg_ms || 0,
        Direct: latest.results?.direct?.avg_ms || 0,
      }
    ];

    const logicalLatencies = latest.results?.logical?.latencies || [];
    const directLatencies = latest.results?.direct?.latencies || [];
    const maxLen = Math.max(logicalLatencies.length, directLatencies.length);

    for (let i = 0; i < maxLen; i++) {
        lineData.push({
            name: `Run ${i+1}`,
            Logical: logicalLatencies[i] || 0,
            Direct: directLatencies[i] || 0
        });
    }

    if (latest.results.avg_breakdown_ms) {
      breakdownData = [{
        name: 'Timings (ms)',
        'Metadata Lookup': latest.results.avg_breakdown_ms.metadata_lookup_ms || 0,
        'Query Plan': latest.results.avg_breakdown_ms.query_plan_ms || 0,
        'SQL Execution': latest.results.avg_breakdown_ms.sql_ms || 0,
        'Mongo Execution': latest.results.avg_breakdown_ms.mongo_ms || 0,
        'Merging': latest.results.avg_breakdown_ms.merge_ms || 0,
      }];
    }
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
      <h1 className="view-title">Comparative Analysis</h1>
      <p className="view-subtitle">Evaluate the latency overhead introduced by the logical abstraction layer versus direct database access operations.</p>
      
      <div className="card" style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', gap: '10px', marginBottom: '15px' }}>
          {scenarios.map(s => (
            <button 
              key={s.id} 
              className={`btn ${activeTab === s.id ? 'btn-primary' : ''}`}
              onClick={() => setActiveTab(s.id)}
            >
              {s.label}
            </button>
          ))}
        </div>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
             {currentDesc}
        </p>
        {activeTab === 'custom_query' && (
            <div style={{ marginTop: 20, padding: 16, background: 'var(--bg-elevated)', borderRadius: 'var(--radius-md)' }}>
                <h4 style={{ marginBottom: 12, fontSize: 13, color: 'var(--text-secondary)' }}>Configure Field Filters</h4>
                <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', marginBottom: 16 }}>
                    <div style={{ flex: 1 }}>
                        <label style={{ fontSize: 11, display: 'block', marginBottom: 6 }}>Select Indexed Field</label>
                        <select className="input" value={selectedField} onChange={e => setSelectedField(e.target.value)}>
                            {availableFields.map((f, i) => (
                                <option key={i} value={f.field_path}>[{f.backend.toUpperCase()}] {f.field_path}</option>
                            ))}
                        </select>
                    </div>
                    <div style={{ flex: 1 }}>
                        <label style={{ fontSize: 11, display: 'block', marginBottom: 6 }}>Condition / JSON Match</label>
                        <input className="input" placeholder="e.g. active, 1, or string value" value={currentFilterValue} onChange={e => setCurrentFilterValue(e.target.value)} />
                    </div>
                    <button className="btn btn-ghost" onClick={handleAddFilter}>Add Filter</button>
                </div>
                
                {Object.keys(customFilters).length > 0 && (
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(customFilters).map(([k, v]) => (
                            <div key={k} className="pill" style={{ background: 'var(--bg-card)' }}>
                                <span className="pill-label">{k}</span>
                                <span className="pill-value">{v}</span>
                                <button className="btn-remove" onClick={() => setCustomFilters(prev => { const n = {...prev}; delete n[k]; return n; })}>×</button>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        )}

        <div style={{ display: 'flex', gap: 20, marginTop: 15 }}>
            <div className="form-group">
                <label>Iterations (Samples)</label>
                <input className="input" type="number" min="1" value={iterations} onChange={e => setIterations(+e.target.value)} />
            </div>
            <div className="form-group">
                <label>Warmup Queries</label>
                <input className="input" type="number" min="0" value={warmup} onChange={e => setWarmup(+e.target.value)} />
            </div>
            <div style={{ alignSelf: 'flex-end', paddingBottom: 5 }}>
                <button className="btn btn-primary" onClick={runBenchmark} disabled={running}>
                    {running ? '⏳ Running...' : '🚀 Run Comparison'}
                </button>
            </div>
        </div>
      </div>

      {latest && latest.results && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: 20 }}>
            
            {/* Visual Charts section */}
            <div className="card">
                <div className="card-title">Average Latency Bar Chart</div>
                <div style={{ width: '100%', height: 300 }}>
                    <ResponsiveContainer>
                        <BarChart data={barData} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis dataKey="name" stroke="#ccc" />
                            <YAxis stroke="#ccc" />
                            <RechartsTooltip contentStyle={{ backgroundColor: '#111', border: '1px solid #333' }} />
                            <Legend />
                            <Bar dataKey="Logical" fill="#8b5cf6" />
                            <Bar dataKey="Direct" fill="#22d3ee" />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            <div className="card">
                 <div className="card-title">Dynamic Throughput/Latency Execution</div>
                 <div style={{ width: '100%', height: 300 }}>
                    <ResponsiveContainer>
                        <LineChart data={lineData} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                            <XAxis dataKey="name" tick={false} stroke="#ccc" />
                            <YAxis stroke="#ccc" />
                            <RechartsTooltip contentStyle={{ backgroundColor: '#111', border: '1px solid #333', color: '#fff' }} />
                            <Legend />
                            <Line type="monotone" dataKey="Logical" stroke="#8b5cf6" strokeWidth={2} dot={false} activeDot={{ r: 8 }} />
                            <Line type="monotone" dataKey="Direct" stroke="#22d3ee" strokeWidth={2} dot={false} />
                        </LineChart>
                    </ResponsiveContainer>
                 </div>
            </div>

            {/* Benchmark Query Display */}
            {latest.config?.logical_query && (
                <div className="card">
                    <div className="card-title">Benchmark Query</div>
                    <pre style={{ fontSize: 11, padding: 12, background: '#111', borderRadius: 4, overflowX: 'auto', color: '#e0e0e0', margin: 0 }}>
                        {JSON.stringify(latest.config.logical_query, null, 2)}
                    </pre>
                </div>
            )}

            {/* Breakdown Chart */}
            {breakdownData.length > 0 && (
                <div className="card">
                    <div className="card-title">Logical Breakdown (Overhead Sources)</div>
                    <div style={{ width: '100%', height: 120 }}>
                        <ResponsiveContainer>
                            <BarChart data={breakdownData} layout="vertical" margin={{ top: 0, right: 30, left: 0, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
                                <XAxis type="number" stroke="#ccc" />
                                <YAxis dataKey="name" type="category" stroke="#ccc" hide />
                                <RechartsTooltip contentStyle={{ backgroundColor: '#111', border: '1px solid #333' }} />
                                <Legend />
                                <Bar dataKey="Metadata Lookup" stackId="a" fill="#06b6d4" />
                                <Bar dataKey="Query Plan" stackId="a" fill="#3b82f6" />
                                <Bar dataKey="SQL Execution" stackId="a" fill="#10b981" />
                                <Bar dataKey="Mongo Execution" stackId="a" fill="#f59e0b" />
                                <Bar dataKey="Merging" stackId="a" fill="#ef4444" />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            )}


            
            {/* Tabular Analysis Metrics */}
            <div className="card" style={{ gridColumn: '1 / -1' }}>
                <div className="card-title">Metrics Evaluation Summary</div>
                <table className="datatable">
                    <thead>
                        <tr>
                            <th className="datatable-th">Metric</th>
                            <th className="datatable-th">Logical Framework</th>
                            <th className="datatable-th">Direct Access</th>
                            <th className="datatable-th">Performance Overhead</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr className="datatable-row">
                            <td className="datatable-td" style={{ fontWeight: 600 }}>Average Latency</td>
                            <td className="datatable-td">{latest.results.logical.avg_ms} ms</td>
                            <td className="datatable-td">{latest.results.direct.avg_ms} ms</td>
                            <td className="datatable-td" style={{ color: 'var(--warning)', fontWeight: 600 }}>+{latest.results.overhead_ms} ms</td>
                        </tr>
                        <tr className="datatable-row">
                            <td className="datatable-td">P95 Latency</td>
                            <td className="datatable-td">{latest.results.logical.latencies.sort()[Math.floor(latest.results.logical.latencies.length * 0.95)] || 0} ms</td>
                            <td className="datatable-td">{latest.results.direct.latencies.sort()[Math.floor(latest.results.direct.latencies.length * 0.95)] || 0} ms</td>
                            <td className="datatable-td">-</td>
                        </tr>
                        <tr className="datatable-row">
                            <td className="datatable-td">Query Errors</td>
                            <td className="datatable-td" style={{ color: latest.results.logical.errors > 0 ? 'var(--danger)' : '' }}>{latest.results.logical.errors}</td>
                            <td className="datatable-td" style={{ color: latest.results.direct.errors > 0 ? 'var(--danger)' : '' }}>{latest.results.direct.errors}</td>
                            <td className="datatable-td"></td>
                        </tr>
                        <tr className="datatable-row" style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
                            <td className="datatable-td" style={{ fontWeight: 600 }}>Total Relative Overhead</td>
                            <td className="datatable-td" colSpan={3} style={{ color: 'var(--warning)', fontWeight: 'bold' }}>
                                +{latest.results.overhead_pct}% framework processing time compared to direct backend connections.
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
      )}
    </motion.div>
  );
}
