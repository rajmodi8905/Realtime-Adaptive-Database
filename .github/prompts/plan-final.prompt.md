## Plan: Final Implementation Roadmap (Assignment 4 + Multi-Laptop Access)

This plan merges both drafts into one execution roadmap: first complete and harden Assignment 4 dashboard requirements with deep UI quality, then add reliable multi-laptop collaborative access through a shared API deployment model. Recommended path: use a single shared API instance for two-laptop collaboration first (fast, low-risk), and add distributed locking only if you later scale to multiple API instances.

**Steps**
1. Phase 0 - Architecture Baseline and Contract Alignment
1.1 Normalize frontend API handling in [dashboard/frontend/src/api.js](dashboard/frontend/src/api.js): consistent success/data/error shape, centralized error normalization, and endpoint mappers reused by all views.
1.2 Refactor top-level view registry in [dashboard/frontend/src/App.jsx](dashboard/frontend/src/App.jsx) and navigation grouping in [dashboard/frontend/src/components/Sidebar.jsx](dashboard/frontend/src/components/Sidebar.jsx) so new views can be added without brittle branching.
1.3 Extract reusable table/state primitives from existing read-heavy screens in [dashboard/frontend/src/views/EntityBrowser.jsx](dashboard/frontend/src/views/EntityBrowser.jsx), [dashboard/frontend/src/views/CrudRead.jsx](dashboard/frontend/src/views/CrudRead.jsx), and [dashboard/frontend/src/views/QueryWorkspace.jsx](dashboard/frontend/src/views/QueryWorkspace.jsx).
1.4 Define UI-safe logical response contract in [dashboard/api_server.py](dashboard/api_server.py): UI endpoints expose entities/fields/constraints/results only, not storage internals.

2. Phase 1 - Assignment 4 Mandatory Completion (depends on 1)
2.1 Confirm and lock baseline required flows already present: bootstrap, active session, entities, instances, field visibility, CRUD lifecycle, query execution, ACID demonstrations.
2.2 Implement query history capture in [src/a3/session_manager.py](src/a3/session_manager.py) and/or [src/a3/orchestrator.py](src/a3/orchestrator.py): bounded retention, timestamp, status, duration, payload digest, and replay payload.
2.3 Add query-history API surface in [dashboard/api_server.py](dashboard/api_server.py): list (paginated), delete item, clear all, replay payload.
2.4 Build Query History view in [dashboard/frontend/src/views](dashboard/frontend/src/views) and integrate navigation in [dashboard/frontend/src/components/Sidebar.jsx](dashboard/frontend/src/components/Sidebar.jsx).
2.5 Wire replay flow between Query History and [dashboard/frontend/src/views/QueryWorkspace.jsx](dashboard/frontend/src/views/QueryWorkspace.jsx): select history item, prefill query editor, explicit rerun.

3. Phase 2 - Logical Abstraction Hardening (depends on 2, parallel with 4)
3.1 Add backend response sanitizers in [dashboard/api_server.py](dashboard/api_server.py) for UI-consumed endpoints (/api/session, /api/query/preview, /api/query/execute, /api/schema/plan, and new history endpoints).
3.2 Ensure query preview and result payloads remain logical-only across frontend rendering paths in [dashboard/frontend/src/views/QueryWorkspace.jsx](dashboard/frontend/src/views/QueryWorkspace.jsx).
3.3 Add regression guard checks in backend tests to fail if forbidden storage keys appear in UI contracts (sql, mongo, table, collection, index).

4. Phase 3 - Query Workspace Deep UX (depends on 2)
4.1 Upgrade [dashboard/frontend/src/views/QueryWorkspace.jsx](dashboard/frontend/src/views/QueryWorkspace.jsx) to table-first output with raw JSON toggle.
4.2 Add result telemetry chips (row count, duration, status, timestamp, entity scope).
4.3 Add in-result tools: local filter, column toggle, copy row, export JSON/CSV.
4.4 Add stable renderers for nested values with robust loading/empty/error states.

5. Phase 4 - Monitoring and Comparative Evaluation (depends on 2, parallel with 5)
5.1 Add metrics instrumentation hooks in [src/a3/orchestrator.py](src/a3/orchestrator.py), [src/a3/transaction_coordinator.py](src/a3/transaction_coordinator.py), and [src/a3/concurrency_manager.py](src/a3/concurrency_manager.py): latency, throughput, error rate, lock wait.
5.2 Expose metrics APIs in [dashboard/api_server.py](dashboard/api_server.py) for snapshot and optional stream/poll.
5.3 Build Query Monitoring view in [dashboard/frontend/src/views](dashboard/frontend/src/views) with rolling charts and trend panels.
5.4 Add benchmark-run APIs in [dashboard/api_server.py](dashboard/api_server.py) and backend comparators in [src/a3](src/a3) for logical-path vs direct-path trials.
5.5 Build Performance Benchmark view in [dashboard/frontend/src/views](dashboard/frontend/src/views) with run controls, grouped comparisons, stats summary, and exports.

6. Phase 5 - Entity Inspector and Session Analytics (depends on 2, parallel with 4)
6.1 Expand [dashboard/frontend/src/views/EntityBrowser.jsx](dashboard/frontend/src/views/EntityBrowser.jsx) into deep inspector: nested field tree, searchable paths, instance compare.
6.2 Add Session Analytics view in [dashboard/frontend/src/views](dashboard/frontend/src/views): operation volume timeline, success/failure mix, latency summaries, logical entity distribution.
6.3 Add drill-down interactions from analytics cards into filtered data tables.

7. Phase 6 - Accessibility and Responsive Maturity (depends on 3/4/5)
7.1 Implement keyboard interaction model across sidebar, forms, tables, modal, history controls in [dashboard/frontend/src/components](dashboard/frontend/src/components) and [dashboard/frontend/src/views](dashboard/frontend/src/views).
7.2 Add semantic ARIA and live-region feedback for async actions/toasts in [dashboard/frontend/src/components/Toast.jsx](dashboard/frontend/src/components/Toast.jsx) and long-running views.
7.3 Improve mobile/tablet behavior in [dashboard/frontend/src/index.css](dashboard/frontend/src/index.css): collapsible sidebar, table overflow strategy/card fallback, chart container reflow.
7.4 Run contrast/focus checks and patch token issues.

8. Phase 7 - Demo, Documentation, and Packaging (depends on 6)
8.1 Add guided demo flow entry points in frontend navigation and view presets.
8.2 Add export helpers for report evidence (history, benchmarks, analytics snapshots).
8.3 Update docs in [README.md](README.md) and [dashboard/frontend/README.md](dashboard/frontend/README.md) with exact run and demo sequence.
8.4 Add reproducibility checklist: services, startup order, seed/bootstrap, verification steps.

9. Phase 8 - End-to-End Hardening and Rubric Mapping (depends on 7)
9.1 Run regression matrix across bootstrap, entities, CRUD, query, history, monitoring, benchmark, and ACID.
9.2 Run API leakage audit for all UI-consumed endpoints.
9.3 Run performance sanity checks for polling, chart render cost, and history pagination scale.
9.4 Capture screenshots and evidence map to assignment rubric items.

10. Phase 9 - Multi-Laptop Collaborative Access (after core plan; depends on 8)
10.1 Recommended MVP (single shared server): run one API server + databases on one host, have both laptops open the same host endpoint; this already allows shared read/write access if both clients hit the same backend process.
10.2 Ensure network reachability and config: API host/port and frontend API base configuration in [dashboard/api_server.py](dashboard/api_server.py), [dashboard/frontend/src/api.js](dashboard/frontend/src/api.js), [dashboard/frontend/vite.config.js](dashboard/frontend/vite.config.js), and [docker-compose.yml](docker-compose.yml).
10.3 Add explicit deployment env docs in [README.md](README.md) for LAN usage: SERVER_IP, API_PORT, allowed origins, startup commands.
10.4 Add session identity marker (header/token) and lightweight audit trail in [dashboard/api_server.py](dashboard/api_server.py) and [src/a3/session_manager.py](src/a3/session_manager.py).
10.5 Security hardening for shared use: tighten CORS, optional auth, and keep DB ports private behind API gateway.
10.6 Optional scale-up track (only if running multiple API instances): introduce shared lock backend (Redis) and distributed lock manager around current concurrency semantics in [src/a3/concurrency_manager.py](src/a3/concurrency_manager.py).

**Relevant files**
- [dashboard/frontend/src/App.jsx](dashboard/frontend/src/App.jsx) - view registry and top-level orchestration.
- [dashboard/frontend/src/components/Sidebar.jsx](dashboard/frontend/src/components/Sidebar.jsx) - navigation growth, new views, demo entry points.
- [dashboard/frontend/src/components/SessionBar.jsx](dashboard/frontend/src/components/SessionBar.jsx) - session telemetry and optional user/session marker.
- [dashboard/frontend/src/components/Toast.jsx](dashboard/frontend/src/components/Toast.jsx) - async feedback semantics and accessibility support.
- [dashboard/frontend/src/views/QueryWorkspace.jsx](dashboard/frontend/src/views/QueryWorkspace.jsx) - table-first results, replay integration, metadata chips.
- [dashboard/frontend/src/views/EntityBrowser.jsx](dashboard/frontend/src/views/EntityBrowser.jsx) - deep inspector features.
- [dashboard/frontend/src/views/CrudRead.jsx](dashboard/frontend/src/views/CrudRead.jsx) - shared table primitives alignment.
- [dashboard/frontend/src/api.js](dashboard/frontend/src/api.js) - API contract normalization and endpoint bindings.
- [dashboard/frontend/src/index.css](dashboard/frontend/src/index.css) - responsive and focus/contrast improvements.
- [dashboard/frontend/vite.config.js](dashboard/frontend/vite.config.js) - LAN/dev API proxy and host behavior.
- [dashboard/api_server.py](dashboard/api_server.py) - history/metrics/benchmark endpoints, sanitization, LAN and session headers.
- [src/a3/session_manager.py](src/a3/session_manager.py) - query history state and session-level analytics.
- [src/a3/orchestrator.py](src/a3/orchestrator.py) - instrumentation hooks and history capture integration.
- [src/a3/transaction_coordinator.py](src/a3/transaction_coordinator.py) - timing/status hooks and conflict observability.
- [src/a3/concurrency_manager.py](src/a3/concurrency_manager.py) - lock metrics and optional distributed extension.
- [docker-compose.yml](docker-compose.yml) - service exposure and optional Redis profile.
- [README.md](README.md) - end-to-end runbook and multi-laptop deployment instructions.
- [dashboard/frontend/README.md](dashboard/frontend/README.md) - frontend run/build environment guidance.

**Verification**
1. Requirement closure matrix: map each Assignment 4 item to a visible UI screen and API proof path.
2. Query history acceptance: execute query -> appears in history -> replay prefills -> execute replay -> delete/clear -> pagination stable.
3. Abstraction acceptance: payload inspections confirm no storage-internal key leakage in UI endpoints.
4. Query UX acceptance: table-first readability, nested value stability, export and copy features validated.
5. Monitoring acceptance: metrics update correctly under synthetic workload; latency/throughput/error trends are coherent.
6. Benchmark acceptance: repeated trials produce reproducible comparative summaries with documented methodology.
7. Accessibility acceptance: keyboard-only path works for critical flows; focus and live announcements behave correctly.
8. Responsive acceptance: desktop/tablet/mobile preserve critical user journeys.
9. Multi-laptop acceptance (single shared server): two devices perform read/write operations and observe consistent state via same backend.
10. Multi-instance acceptance (optional): with multiple API instances enabled, lock coordination prevents conflicting writes.

**Decisions**
- Scope includes full Plan C depth, with explicit Assignment 4 compliance gate first.
- Query history storage starts as bounded per-session in-memory and may add metadata persistence for restart resilience.
- Multi-laptop feature is feasible immediately with a single shared server topology; distributed locks are not mandatory unless scaling to multiple API instances.
- Security posture for classroom/LAN demos should be improved with restricted CORS and optional authentication before wider exposure.

**Further Considerations**
1. History persistence mode recommendation: start with bounded in-memory + optional metadata file fallback.
2. Benchmark rigor recommendation: start with fixed query suite and warmup policy, then add configurable templates.
3. Collaboration scope recommendation: start with two-laptop single-server deployment, then add Redis-backed distributed locking only if you deploy multiple API instances.