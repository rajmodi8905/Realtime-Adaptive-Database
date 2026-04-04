/* ══════════════════════════════════════════════════════════════════════════
   Logical Dashboard — Frontend Logic
   API calls, navigation, entity browser, CRUD forms, ACID runner, SSE
   ══════════════════════════════════════════════════════════════════════════ */

// ── API Client ─────────────────────────────────────────────────────────────
const API = {
    async post(url, body = {}) {
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        return res.json();
    },
    async get(url) {
        const res = await fetch(url);
        return res.json();
    },
};

// ── Toast Notifications ────────────────────────────────────────────────────
function toast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => {
        el.style.opacity = '0';
        el.style.transform = 'translateX(40px)';
        el.style.transition = '0.3s ease';
        setTimeout(() => el.remove(), 300);
    }, 4000);
}

// ── Navigation ─────────────────────────────────────────────────────────────
const sidebarBtns = document.querySelectorAll('.sidebar-btn[data-view]');
const views = document.querySelectorAll('.view');

function switchView(viewId) {
    views.forEach(v => v.classList.remove('active'));
    sidebarBtns.forEach(b => b.classList.remove('active'));
    const target = document.getElementById(`view-${viewId}`);
    if (target) target.classList.add('active');
    const btn = document.querySelector(`.sidebar-btn[data-view="${viewId}"]`);
    if (btn) btn.classList.add('active');
}

sidebarBtns.forEach(btn => {
    btn.addEventListener('click', () => switchView(btn.dataset.view));
});

// ── Session Bar ────────────────────────────────────────────────────────────
function updateSessionBar(info) {
    if (!info) return;
    document.getElementById('val-schema').textContent = info.schema_name || '—';
    document.getElementById('val-version').textContent = info.version || '—';
    document.getElementById('val-root').textContent = info.root_entity || '—';
    document.getElementById('val-fields').textContent = info.field_count ?? '—';

    const dotMysql = document.getElementById('dot-mysql');
    const dotMongo = document.getElementById('dot-mongo');
    dotMysql.className = `dot ${info.mysql_connected ? 'dot-on' : 'dot-off'}`;
    dotMongo.className = `dot ${info.mongo_connected ? 'dot-on' : 'dot-off'}`;
}

// ── Bootstrap ──────────────────────────────────────────────────────────────
const btnBootstrap = document.getElementById('btn-bootstrap');
const bootstrapSpinner = document.getElementById('bootstrap-spinner');
const bootstrapStatus = document.getElementById('bootstrap-status');
const bootstrapIcon = document.getElementById('bootstrap-icon');
const bootstrapText = document.getElementById('bootstrap-text');

btnBootstrap.addEventListener('click', async () => {
    const count = parseInt(document.getElementById('record-count').value) || 100;
    btnBootstrap.disabled = true;
    bootstrapSpinner.classList.remove('hidden');
    bootstrapStatus.classList.add('hidden');

    try {
        const res = await API.post('/api/bootstrap', { record_count: count });
        bootstrapSpinner.classList.add('hidden');
        bootstrapStatus.classList.remove('hidden');

        if (res.success) {
            bootstrapStatus.classList.remove('error');
            bootstrapIcon.textContent = '✅';
            bootstrapText.textContent = res.data.message;
            updateSessionBar(res.data.session);
            toast('Database bootstrapped successfully!', 'success');
            loadEntities();
        } else {
            bootstrapStatus.classList.add('error');
            bootstrapIcon.textContent = '❌';
            bootstrapText.textContent = res.error;
            toast(`Bootstrap failed: ${res.error}`, 'error');
        }
    } catch (err) {
        bootstrapSpinner.classList.add('hidden');
        bootstrapStatus.classList.remove('hidden');
        bootstrapStatus.classList.add('error');
        bootstrapIcon.textContent = '❌';
        bootstrapText.textContent = err.message;
        toast(`Bootstrap error: ${err.message}`, 'error');
    }
    btnBootstrap.disabled = false;
});

// ── Entity Browser ─────────────────────────────────────────────────────────
let currentEntity = null;
let entityOffset = 0;
const ENTITY_PAGE_SIZE = 50;

async function loadEntities() {
    try {
        const res = await API.get('/api/entities');
        if (!res.success) return;
        const container = document.getElementById('entity-names');
        container.innerHTML = '';
        res.data.forEach(name => {
            const btn = document.createElement('button');
            btn.className = 'entity-name-btn';
            btn.textContent = name;
            btn.addEventListener('click', () => selectEntity(name));
            container.appendChild(btn);
        });
    } catch (err) {
        console.error('loadEntities:', err);
    }
}

async function selectEntity(name) {
    currentEntity = name;
    entityOffset = 0;
    document.querySelectorAll('.entity-name-btn').forEach(b =>
        b.classList.toggle('active', b.textContent === name)
    );
    await loadEntityData();
}

async function loadEntityData() {
    if (!currentEntity) return;
    try {
        const res = await API.get(
            `/api/entities/${encodeURIComponent(currentEntity)}?limit=${ENTITY_PAGE_SIZE}&offset=${entityOffset}`
        );
        if (!res.success) {
            toast(`Error: ${res.error}`, 'error');
            return;
        }
        const entity = res.data;
        document.getElementById('entity-data-title').textContent = entity.entity_name;
        renderEntityTable(entity.fields, entity.instances);
        renderPagination(entity.instances.length);
    } catch (err) {
        toast(`Error loading entity: ${err.message}`, 'error');
    }
}

function renderEntityTable(fields, instances) {
    const container = document.getElementById('entity-table-container');
    if (!instances || instances.length === 0) {
        container.innerHTML = '<div class="placeholder-text">No instances found</div>';
        return;
    }

    // Derive columns from instances if fields are empty
    const columns = fields && fields.length > 0
        ? fields
        : [...new Set(instances.flatMap(r => Object.keys(r)))];

    let html = '<div class="table-container"><table class="data-table"><thead><tr>';
    columns.forEach(col => { html += `<th>${escHtml(col)}</th>`; });
    html += '</tr></thead><tbody>';

    instances.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            let val = row[col];
            if (val === undefined || val === null) val = '—';
            else if (typeof val === 'object') val = JSON.stringify(val);
            html += `<td title="${escHtml(String(val))}">${escHtml(String(val))}</td>`;
        });
        html += '</tr>';
    });
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

function renderPagination(returnedCount) {
    const container = document.getElementById('entity-pagination');
    const page = Math.floor(entityOffset / ENTITY_PAGE_SIZE) + 1;
    const hasPrev = entityOffset > 0;
    const hasNext = returnedCount === ENTITY_PAGE_SIZE;

    container.innerHTML = `
        <button ${hasPrev ? '' : 'disabled'} id="page-prev">← Prev</button>
        <span class="page-info">Page ${page}</span>
        <button ${hasNext ? '' : 'disabled'} id="page-next">Next →</button>
    `;
    const prevBtn = document.getElementById('page-prev');
    const nextBtn = document.getElementById('page-next');
    if (prevBtn) prevBtn.addEventListener('click', () => { entityOffset -= ENTITY_PAGE_SIZE; loadEntityData(); });
    if (nextBtn) nextBtn.addEventListener('click', () => { entityOffset += ENTITY_PAGE_SIZE; loadEntityData(); });
}

// ── Query Workspace ────────────────────────────────────────────────────────
document.getElementById('btn-preview').addEventListener('click', async () => {
    try {
        const query = JSON.parse(document.getElementById('query-input').value);
        document.getElementById('query-result-header').textContent = 'Query Plan (Preview)';
        const res = await API.post('/api/query/preview', query);
        document.getElementById('query-result').textContent = JSON.stringify(
            res.success ? res.data : { error: res.error }, null, 2
        );
    } catch (err) {
        document.getElementById('query-result').textContent = `Parse error: ${err.message}`;
        toast('Invalid JSON', 'error');
    }
});

document.getElementById('btn-execute-query').addEventListener('click', async () => {
    try {
        const query = JSON.parse(document.getElementById('query-input').value);
        document.getElementById('query-result-header').textContent = 'Execution Result';
        const res = await API.post('/api/query/execute', query);
        document.getElementById('query-result').textContent = JSON.stringify(
            res.success ? res.data : { error: res.error }, null, 2
        );
        if (res.success) toast('Query executed', 'success');
        else toast(`Error: ${res.error}`, 'error');
    } catch (err) {
        document.getElementById('query-result').textContent = `Parse error: ${err.message}`;
        toast('Invalid JSON', 'error');
    }
});

// ── CRUD: Create ───────────────────────────────────────────────────────────
document.getElementById('btn-generate-template').addEventListener('click', async () => {
    try {
        const res = await API.get('/api/schema');
        if (!res.success) return toast('Failed to load schema', 'error');
        const schema = res.data.json_schema;
        const template = generateRecordTemplate(schema);
        document.getElementById('create-input').value = JSON.stringify([template], null, 2);

        const constraints = res.data.constraints || {};
        const infoEl = document.getElementById('create-constraints');
        let info = '';
        if (constraints.not_null) info += `<strong>Required:</strong> ${constraints.not_null.join(', ')}<br>`;
        if (constraints.unique_candidates) info += `<strong>Unique:</strong> ${constraints.unique_candidates.join(', ')}`;
        infoEl.innerHTML = info;
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
});

function generateRecordTemplate(schema) {
    if (!schema || schema.type !== 'object') return {};
    const obj = {};
    for (const [key, prop] of Object.entries(schema.properties || {})) {
        const t = prop.type || 'string';
        if (t === 'object') obj[key] = generateRecordTemplate(prop);
        else if (t === 'array') {
            const items = prop.items || { type: 'string' };
            if (items.type === 'object') obj[key] = [generateRecordTemplate(items)];
            else obj[key] = [''];
        }
        else if (t === 'integer') obj[key] = 0;
        else if (t === 'number') obj[key] = 0.0;
        else if (t === 'boolean') obj[key] = false;
        else obj[key] = '';
    }
    return obj;
}

document.getElementById('btn-create').addEventListener('click', async () => {
    const resultEl = document.getElementById('create-result');
    try {
        const records = JSON.parse(document.getElementById('create-input').value);
        if (!Array.isArray(records)) {
            toast('Input must be a JSON array', 'error');
            return;
        }
        // Validate required fields
        const valid = validateRecords(records);
        if (!valid.ok) {
            toast(`Validation: ${valid.message}`, 'error');
            return;
        }

        const res = await API.post('/api/crud', { operation: 'create', records });
        resultEl.classList.remove('hidden');
        if (res.success && res.data.status === 'committed') {
            resultEl.className = 'result-section success';
            resultEl.innerHTML = `
                <div class="result-title">✅ Records Created</div>
                <div class="result-body">Status: ${res.data.status}</div>
                <div class="result-json">${escHtml(JSON.stringify(res.data, null, 2))}</div>
            `;
            toast('Records created successfully', 'success');
        } else {
            resultEl.className = 'result-section error';
            const err = res.success ? res.data.errors?.join(', ') : res.error;
            resultEl.innerHTML = `
                <div class="result-title">❌ Create Failed</div>
                <div class="result-body">${escHtml(err || 'Unknown error')}</div>
                <div class="result-json">${escHtml(JSON.stringify(res.data || {}, null, 2))}</div>
            `;
            toast('Create failed', 'error');
        }
    } catch (err) {
        resultEl.classList.remove('hidden');
        resultEl.className = 'result-section error';
        resultEl.innerHTML = `<div class="result-title">❌ Error</div><div class="result-body">${escHtml(err.message)}</div>`;
        toast(`Error: ${err.message}`, 'error');
    }
});

function validateRecords(records) {
    // Basic validation: each record must be an object
    for (let i = 0; i < records.length; i++) {
        if (typeof records[i] !== 'object' || Array.isArray(records[i])) {
            return { ok: false, message: `Record ${i} must be an object` };
        }
        // Check for username (required in our schema)
        if (!records[i].username) {
            return { ok: false, message: `Record ${i}: 'username' is required` };
        }
    }
    return { ok: true };
}

// ── CRUD: Read ─────────────────────────────────────────────────────────────
document.getElementById('btn-add-read-filter').addEventListener('click', () => addFilterRow('read-filters'));

document.getElementById('btn-read').addEventListener('click', async () => {
    const filters = getFilters('read-filters');
    const fieldsStr = document.getElementById('read-fields').value.trim();
    const fields = fieldsStr ? fieldsStr.split(',').map(f => f.trim()).filter(Boolean) : undefined;
    const limit = parseInt(document.getElementById('read-limit').value) || 10;

    const body = { operation: 'read', filters, limit };
    if (fields) body.fields = fields;

    try {
        const res = await API.post('/api/crud', body);
        const card = document.getElementById('read-result-card');
        const container = document.getElementById('read-result-table');
        card.classList.remove('hidden');

        if (res.success) {
            const records = res.data.sql_result?.records || [];
            if (records.length === 0) {
                container.innerHTML = '<div class="placeholder-text">No records found</div>';
            } else {
                const cols = [...new Set(records.flatMap(r => Object.keys(r)))];
                renderTableInto(container, cols, records);
            }
            toast(`Read: ${records.length} records`, 'success');
        } else {
            container.innerHTML = `<div class="placeholder-text" style="color: var(--danger)">${escHtml(res.error)}</div>`;
            toast(`Read error: ${res.error}`, 'error');
        }
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
});

// ── CRUD: Update ───────────────────────────────────────────────────────────
document.getElementById('btn-add-update-filter').addEventListener('click', () => addFilterRow('update-filters'));
document.getElementById('btn-add-update-field').addEventListener('click', () => addFilterRow('update-fields'));

document.getElementById('btn-update').addEventListener('click', async () => {
    const filters = getFilters('update-filters');
    const updates = getFilters('update-fields');

    if (Object.keys(filters).length === 0) {
        toast('Add at least one filter to identify records', 'error');
        return;
    }
    if (Object.keys(updates).length === 0) {
        toast('Add at least one field to update', 'error');
        return;
    }

    const resultEl = document.getElementById('update-result');
    try {
        const res = await API.post('/api/crud', { operation: 'update', filters, updates });
        resultEl.classList.remove('hidden');
        if (res.success && res.data.status === 'committed') {
            resultEl.className = 'result-section success';
            resultEl.innerHTML = `
                <div class="result-title">✅ Update Committed</div>
                <div class="result-body">Status: ${res.data.status}</div>
                <div class="result-json">${escHtml(JSON.stringify(res.data, null, 2))}</div>
            `;
            toast('Records updated', 'success');
        } else {
            resultEl.className = 'result-section error';
            resultEl.innerHTML = `
                <div class="result-title">❌ Update Failed</div>
                <div class="result-body">${escHtml(res.error || res.data?.errors?.join(', ') || 'Unknown')}</div>
            `;
            toast('Update failed', 'error');
        }
    } catch (err) {
        resultEl.classList.remove('hidden');
        resultEl.className = 'result-section error';
        resultEl.innerHTML = `<div class="result-title">❌ Error</div><div class="result-body">${escHtml(err.message)}</div>`;
        toast(`Error: ${err.message}`, 'error');
    }
});

// ── CRUD: Delete ───────────────────────────────────────────────────────────
document.getElementById('btn-add-delete-filter').addEventListener('click', () => addFilterRow('delete-filters'));

let pendingDeleteResolve = null;
const confirmModal = document.getElementById('confirm-modal');
document.getElementById('confirm-cancel').addEventListener('click', () => {
    confirmModal.classList.add('hidden');
    if (pendingDeleteResolve) pendingDeleteResolve(false);
});
document.getElementById('confirm-ok').addEventListener('click', () => {
    confirmModal.classList.add('hidden');
    if (pendingDeleteResolve) pendingDeleteResolve(true);
});

function confirmDelete(msg) {
    document.getElementById('confirm-body').textContent = msg || 'Are you sure?';
    confirmModal.classList.remove('hidden');
    return new Promise(resolve => { pendingDeleteResolve = resolve; });
}

document.getElementById('btn-delete').addEventListener('click', async () => {
    const filters = getFilters('delete-filters');
    if (Object.keys(filters).length === 0) {
        toast('Add at least one filter to identify records', 'error');
        return;
    }

    const confirmed = await confirmDelete(
        `Delete all records matching: ${JSON.stringify(filters)}?`
    );
    if (!confirmed) return;

    const resultEl = document.getElementById('delete-result');
    try {
        const res = await API.post('/api/crud', { operation: 'delete', filters });
        resultEl.classList.remove('hidden');
        if (res.success && res.data.status === 'committed') {
            resultEl.className = 'result-section success';
            resultEl.innerHTML = `
                <div class="result-title">✅ Records Deleted</div>
                <div class="result-body">Status: ${res.data.status}</div>
                <div class="result-json">${escHtml(JSON.stringify(res.data, null, 2))}</div>
            `;
            toast('Records deleted', 'success');
        } else {
            resultEl.className = 'result-section error';
            resultEl.innerHTML = `
                <div class="result-title">❌ Delete Failed</div>
                <div class="result-body">${escHtml(res.error || res.data?.errors?.join(', ') || 'Unknown')}</div>
            `;
            toast('Delete failed', 'error');
        }
    } catch (err) {
        resultEl.classList.remove('hidden');
        resultEl.className = 'result-section error';
        resultEl.innerHTML = `<div class="result-title">❌ Error</div><div class="result-body">${escHtml(err.message)}</div>`;
        toast(`Error: ${err.message}`, 'error');
    }
});

// ── ACID Test Runner ───────────────────────────────────────────────────────
const ACID_MAP = {
    atomicity: 'A',
    consistency: 'C',
    isolation: 'I',
    durability: 'D',
    reconstruction: 'R',
};

const acidResults = {};

// Individual test buttons
document.querySelectorAll('.acid-btn').forEach(btn => {
    btn.addEventListener('click', () => runSingleAcidTest(btn.dataset.test));
});

// Run All
document.getElementById('acid-run-all').addEventListener('click', runAllAcidTests);

async function runSingleAcidTest(property) {
    switchView('acid-results');
    const btn = document.querySelector(`.acid-btn[data-test="${property}"]`);
    btn.className = 'acid-btn running';

    const logPane = document.getElementById('acid-log-pane');
    const logCard = document.getElementById('acid-log-card');
    logCard.classList.remove('hidden');
    logPane.innerHTML = '';

    try {
        const es = new EventSource(`/api/acid/stream/${property}`);

        es.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'log') {
                const line = document.createElement('span');
                line.className = 'log-entry';
                line.innerHTML = `<span class="log-time">${data.time}</span> <span class="log-level-${data.level}">[${data.level}]</span> ${escHtml(data.msg)}`;
                logPane.appendChild(line);
                logPane.appendChild(document.createTextNode('\n'));
                logPane.scrollTop = logPane.scrollHeight;
            } else if (data.type === 'result') {
                acidResults[property] = data.result;
                btn.className = `acid-btn ${data.result.passed ? 'passed' : 'failed'}`;
                renderAcidResults();
                toast(
                    `${ACID_MAP[property]}: ${data.result.passed ? 'PASSED ✅' : 'FAILED ❌'}`,
                    data.result.passed ? 'success' : 'error'
                );
                es.close();
            } else if (data.type === 'error') {
                btn.className = 'acid-btn failed';
                toast(`Error: ${data.message}`, 'error');
                es.close();
            } else if (data.type === 'done') {
                es.close();
            }
        };

        es.onerror = () => {
            btn.className = 'acid-btn failed';
            es.close();
        };
    } catch (err) {
        btn.className = 'acid-btn failed';
        toast(`Error: ${err.message}`, 'error');
    }
}

async function runAllAcidTests() {
    switchView('acid-results');
    const allBtns = document.querySelectorAll('.acid-btn');
    allBtns.forEach(b => b.className = 'acid-btn running');

    const logPane = document.getElementById('acid-log-pane');
    const logCard = document.getElementById('acid-log-card');
    logCard.classList.remove('hidden');
    logPane.innerHTML = '';

    try {
        const es = new EventSource('/api/acid/stream-all');

        es.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'log') {
                const line = document.createElement('span');
                line.className = 'log-entry';
                line.innerHTML = `<span class="log-time">${data.time}</span> <span class="log-level-${data.level}">[${data.level}]</span> ${escHtml(data.msg)}`;
                logPane.appendChild(line);
                logPane.appendChild(document.createTextNode('\n'));
                logPane.scrollTop = logPane.scrollHeight;
            } else if (data.type === 'test_start') {
                const btn = document.querySelector(`.acid-btn[data-test="${data.test}"]`);
                if (btn) btn.className = 'acid-btn running';
            } else if (data.type === 'result') {
                acidResults[data.test] = data.result;
                const btn = document.querySelector(`.acid-btn[data-test="${data.test}"]`);
                if (btn) btn.className = `acid-btn ${data.result.passed ? 'passed' : 'failed'}`;
                renderAcidResults();
            } else if (data.type === 'done') {
                const allPassed = Object.values(acidResults).every(r => r.passed);
                toast(
                    allPassed ? 'All tests PASSED ✅' : 'Some tests FAILED ❌',
                    allPassed ? 'success' : 'error'
                );
                es.close();
            } else if (data.type === 'error') {
                toast(`Error: ${data.message}`, 'error');
                es.close();
            }
        };

        es.onerror = () => {
            allBtns.forEach(b => {
                if (b.className.includes('running')) b.className = 'acid-btn failed';
            });
            es.close();
        };
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
}

function renderAcidResults() {
    const container = document.getElementById('acid-results-container');
    const order = ['reconstruction', 'atomicity', 'consistency', 'isolation', 'durability'];
    const results = order.filter(p => acidResults[p]).map(p => acidResults[p]);

    if (results.length === 0) {
        container.innerHTML = '<div class="placeholder-text">Run tests using the sidebar buttons or "Run All"</div>';
        return;
    }

    // Summary bar
    const allPassed = results.every(r => r.passed);
    const passCount = results.filter(r => r.passed).length;

    let html = `
        <div class="acid-summary ${allPassed ? 'all-pass' : 'has-fail'}">
            ${allPassed ? '✅' : '⚠️'} ${passCount}/${results.length} tests passed
        </div>
    `;

    order.forEach(prop => {
        const r = acidResults[prop];
        if (!r) return;
        const letter = ACID_MAP[prop];
        const passed = r.passed;
        html += `
            <div class="acid-result-card" onclick="this.classList.toggle('expanded')">
                <div class="acid-result-header">
                    <div class="acid-result-left">
                        <div class="acid-badge ${passed ? 'pass' : 'fail'}">${letter}</div>
                        <div>
                            <div class="acid-result-name">${capitalize(prop)}</div>
                        </div>
                    </div>
                    <div class="acid-result-status">
                        <span class="${passed ? 'pass-text' : 'fail-text'}">${passed ? 'PASSED ✅' : 'FAILED ❌'}</span>
                        <span class="acid-result-duration">${r.duration_ms?.toFixed(1) || '—'}ms</span>
                    </div>
                </div>
                <div class="acid-result-details">
                    <div class="acid-result-desc">${escHtml(r.description || '')}</div>
                    <div class="acid-detail-json">${escHtml(JSON.stringify(r.details || {}, null, 2))}</div>
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
}

// ── Helpers ─────────────────────────────────────────────────────────────────
function escHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function addFilterRow(containerId) {
    const container = document.getElementById(containerId);
    const row = document.createElement('div');
    row.className = 'filter-row';
    row.innerHTML = `
        <input type="text" placeholder="Field name" class="filter-key">
        <input type="text" placeholder="Value" class="filter-value">
        <button class="btn btn-ghost btn-sm btn-remove-filter">✕</button>
    `;
    row.querySelector('.btn-remove-filter').addEventListener('click', () => row.remove());
    container.appendChild(row);
}

function getFilters(containerId) {
    const rows = document.querySelectorAll(`#${containerId} .filter-row`);
    const result = {};
    rows.forEach(row => {
        const key = row.querySelector('.filter-key')?.value?.trim();
        const val = row.querySelector('.filter-value')?.value?.trim();
        if (key && val) result[key] = val;
    });
    return result;
}

function renderTableInto(container, columns, records) {
    let html = '<div class="table-container"><table class="data-table"><thead><tr>';
    columns.forEach(col => { html += `<th>${escHtml(col)}</th>`; });
    html += '</tr></thead><tbody>';
    records.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            let val = row[col];
            if (val === undefined || val === null) val = '—';
            else if (typeof val === 'object') val = JSON.stringify(val);
            html += `<td title="${escHtml(String(val))}">${escHtml(String(val))}</td>`;
        });
        html += '</tr>';
    });
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// Wire up remove buttons for existing filter rows
document.querySelectorAll('.btn-remove-filter').forEach(btn => {
    btn.addEventListener('click', () => btn.closest('.filter-row').remove());
});

// ── Init: Try to load session on page load ─────────────────────────────────
(async function init() {
    try {
        const res = await API.get('/api/session');
        if (res.success) {
            updateSessionBar(res.data);
            loadEntities();
        }
    } catch (e) {
        // Pipeline not bootstrapped yet — that's fine
    }
})();
