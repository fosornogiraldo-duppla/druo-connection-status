document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- DRUO Connection Status v3 ---');

    // ---- DOM refs ----
    const tableBody = document.getElementById('druo-tbody');
    const descartadosBody = document.getElementById('druo-descartados-tbody');
    const conectadosBody = document.getElementById('conectados-tbody');
    const filterStatus = document.getElementById('druo-filter-status');
    const portafolioChipsEl = document.getElementById('portafolio-chips');
    const searchInput = document.getElementById('druo-search-input');
    const conectadosSearch = document.getElementById('conectados-search-input');
    const conectadosPortFilter = document.getElementById('conectados-filter-portafolio');

    const kpiTotal = document.getElementById('druo-kpi-total');
    const kpiFailed = document.getElementById('druo-kpi-failed');
    const kpiNull = document.getElementById('druo-kpi-null');
    const kpiConectados = document.getElementById('druo-kpi-conectados');
    const kpiDescartados = document.getElementById('druo-kpi-descartados');

    let druoData = [];
    let descartados = [];
    let conectadosData = [];
    let descartadosCodes = new Set();
    let selectedPortafolios = new Set();
    let pendingDiscardRow = null;

    // ----------------------------------------------------------------
    // URL params
    // ----------------------------------------------------------------
    function getURLParams() {
        const p = new URLSearchParams(window.location.search);
        return {
            status: p.get('status') || 'all',
            portafolios: p.get('portafolios') ? p.get('portafolios').split(',') : [],
            search: p.get('search') || ''
        };
    }
    function saveURLParams() {
        const params = new URLSearchParams();
        const status = filterStatus ? filterStatus.value : 'all';
        if (status !== 'all') params.set('status', status);
        if (selectedPortafolios.size > 0) params.set('portafolios', [...selectedPortafolios].join(','));
        const search = searchInput ? searchInput.value.trim() : '';
        if (search) params.set('search', search);
        window.history.replaceState({}, '', `${window.location.pathname}${params.toString() ? '?' + params.toString() : ''}`);
    }

    // ----------------------------------------------------------------
    // Supabase fetches
    // ----------------------------------------------------------------
    async function fetchDruoData() {
        const { data, error } = await window.supabaseClient.from('druo_no_conectados').select('*');
        if (error) { console.error('druo_no_conectados error:', error); return []; }
        return data || [];
    }

    async function fetchDescartados() {
        const { data, error } = await window.supabaseClient
            .from('druo_descartados').select('*').order('descartado_at', { ascending: false });
        if (error) { console.error('druo_descartados error:', error); return []; }
        return data || [];
    }

    async function fetchConectados() {
        // Try to fetch from druo_conectados table
        const { data, error } = await window.supabaseClient.from('druo_conectados').select('*');
        if (error) {
            console.warn('druo_conectados table not found or error:', error.message);
            return null; // null = table doesn't exist yet
        }
        return data || [];
    }

    async function saveDescarte(row, razon) {
        const { error } = await window.supabaseClient.from('druo_descartados').upsert([{
            codigo_inmueble: row.codigo_inmueble,
            nombre_oportunidad: row.nombre_oportunidad,
            portafolio: row.portafolio,
            druo_status: row.druo_status,
            razon_descarte: razon,
            descartado_at: new Date().toISOString()
        }], { onConflict: 'codigo_inmueble' });
        return error;
    }

    async function deleteDescarte(codigo_inmueble) {
        const { error } = await window.supabaseClient.from('druo_descartados')
            .delete().eq('codigo_inmueble', codigo_inmueble);
        return error;
    }

    // ----------------------------------------------------------------
    // KPIs
    // ----------------------------------------------------------------
    function updateKPIs() {
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        if (kpiTotal) kpiTotal.textContent = activos.length;
        if (kpiFailed) kpiFailed.textContent = activos.filter(d => d.druo_status === 'CONNECTION_FAILED').length;
        if (kpiNull) kpiNull.textContent = activos.filter(d => !d.druo_status).length;
        if (kpiConectados) kpiConectados.textContent = (conectadosData && conectadosData.length > 0) ? conectadosData.length : '—';
        if (kpiDescartados) kpiDescartados.textContent = descartados.length;
    }

    // ----------------------------------------------------------------
    // Portafolio chips (for pending view)
    // ----------------------------------------------------------------
    function buildPortafolioChips() {
        if (!portafolioChipsEl) return;
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const ports = [...new Set(activos.map(d => d.portafolio).filter(Boolean))].sort();

        portafolioChipsEl.innerHTML = '';
        ports.forEach(p => {
            const chip = document.createElement('button');
            chip.type = 'button';
            chip.textContent = p;
            chip.className = 'portafolio-chip';
            if (selectedPortafolios.has(p)) chip.classList.add('active');
            chip.addEventListener('click', () => {
                selectedPortafolios.has(p) ? selectedPortafolios.delete(p) : selectedPortafolios.add(p);
                chip.classList.toggle('active');
                saveURLParams(); renderTable(); updateChipCount();
            });
            portafolioChipsEl.appendChild(chip);
        });
        updateChipCount();
    }

    function updateChipCount() {
        const el = document.getElementById('portafolio-count');
        if (!el) return;
        el.textContent = selectedPortafolios.size === 0 ? 'Todos' : `${selectedPortafolios.size} seleccionados`;
        el.style.background = selectedPortafolios.size === 0 ? '#e2e8f0' : 'var(--color-brand-dark)';
        el.style.color = selectedPortafolios.size === 0 ? '#475569' : 'white';
    }

    // ----------------------------------------------------------------
    // Badge HTML helper
    // ----------------------------------------------------------------
    function statusBadge(status, isFailed) {
        const bg = isFailed ? '#fee2e2' : '#f1f5f9';
        const color = isFailed ? '#b91c1c' : '#475569';
        const border = isFailed ? '#fca5a5' : '#cbd5e1';
        return `<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:${bg};color:${color};border:1px solid ${border};">${status || 'null'}</span>`;
    }

    // ----------------------------------------------------------------
    // Render: Pendientes table
    // ----------------------------------------------------------------
    function renderTable() {
        if (!tableBody) return;
        const fStatus = filterStatus ? filterStatus.value : 'all';
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';

        const filtered = druoData
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => {
                const dStatus = d.druo_status || null;
                if (fStatus === 'null') return !dStatus;
                if (fStatus === 'CONNECTION_FAILED') return dStatus === 'CONNECTION_FAILED';
                return true;
            })
            .filter(d => selectedPortafolios.size === 0 || selectedPortafolios.has(d.portafolio))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt));

        const rc = document.getElementById('result-count');
        if (rc) rc.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        tableBody.innerHTML = '';
        if (filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;color:#94a3b8;">Sin resultados con los filtros actuales.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const isFailed = d.druo_status === 'CONNECTION_FAILED';
            const badge = statusBadge(d.druo_status, isFailed);
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td>${badge}</td>
                <td><button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button></td>
            `;
            row.addEventListener('click', e => { if (!e.target.closest('.btn-discard')) showDetailModal(d, badge); });
            row.querySelector('.btn-discard').addEventListener('click', e => { e.stopPropagation(); openDiscardModal(d); });
            tableBody.appendChild(row);
        });
    }

    // ----------------------------------------------------------------
    // Render: Conectados table
    // ----------------------------------------------------------------
    function renderConectados() {
        if (!conectadosBody) return;
        const searchTxt = conectadosSearch ? conectadosSearch.value.toLowerCase().trim() : '';
        const filterPort = conectadosPortFilter ? conectadosPortFilter.value : 'all';

        if (conectadosData === null) {
            conectadosBody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:50px;color:#94a3b8;">
                <div style="font-size:40px;margin-bottom:12px;">🔧</div>
                <strong style="color:#334155;display:block;margin-bottom:8px;">Tabla druo_conectados no encontrada</strong>
                <span style="font-size:13px;">Crea la tabla en Supabase con los campos: codigo_inmueble, nombre_oportunidad, portafolio, fecha_entrega, druo_status</span>
            </td></tr>`;
            return;
        }

        const filtered = conectadosData
            .filter(d => filterPort === 'all' || d.portafolio === filterPort)
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt));

        const cc = document.getElementById('conectados-count');
        if (cc) cc.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        conectadosBody.innerHTML = '';
        if (filtered.length === 0) {
            conectadosBody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:40px;color:#94a3b8;">Sin resultados.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:11px;color:#064e3b;background:#d1fae5;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td><span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#d1fae5;color:#065f46;border:1px solid #6ee7b7;">${d.druo_status || 'CONNECTED'}</span></td>
            `;
            conectadosBody.appendChild(row);
        });
    }

    function buildConectadosPortFilter() {
        if (!conectadosPortFilter || !conectadosData || conectadosData.length === 0) return;
        const ports = [...new Set(conectadosData.map(d => d.portafolio).filter(Boolean))].sort();
        conectadosPortFilter.innerHTML = '<option value="all">Todos los portafolios</option>';
        ports.forEach(p => {
            const opt = document.createElement('option');
            opt.value = p; opt.textContent = p;
            conectadosPortFilter.appendChild(opt);
        });
    }

    // ----------------------------------------------------------------
    // Render: Descartados table
    // ----------------------------------------------------------------
    function renderDescartados() {
        if (!descartadosBody) return;
        descartadosBody.innerHTML = '';
        if (descartados.length === 0) {
            descartadosBody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:#94a3b8;">No hay inmuebles descartados aún.</td></tr>';
            return;
        }
        descartados.forEach(d => {
            const isFailed = d.druo_status === 'CONNECTION_FAILED';
            const date = d.descartado_at
                ? new Date(d.descartado_at).toLocaleDateString('es-CO', { day: '2-digit', month: 'short', year: 'numeric' })
                : '-';
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${statusBadge(d.druo_status, isFailed)}</td>
                <td style="color:#475569;font-size:12px;max-width:200px;">${d.razon_descarte || '-'}</td>
                <td style="color:#94a3b8;font-size:12px;">${date}</td>
                <td><button class="btn-restore" data-code="${d.codigo_inmueble}">Restaurar</button></td>
            `;
            row.querySelector('.btn-restore').addEventListener('click', async e => {
                e.stopPropagation();
                await restoreDescarte(d.codigo_inmueble);
            });
            descartadosBody.appendChild(row);
        });
    }

    // ----------------------------------------------------------------
    // Modals
    // ----------------------------------------------------------------
    function showDetailModal(d, badge) {
        const body = document.getElementById('detail-modal-body');
        const modal = document.getElementById('detail-modal');
        if (!body || !modal) return;
        body.innerHTML = `
            <h2 style="color:var(--color-brand-dark);margin:0 0 16px 0;font-size:19px;">ℹ️ Detalle del Inmueble</h2>
            <div style="display:flex;gap:10px;align-items:center;border-bottom:1px solid #e2e8f0;padding-bottom:14px;margin-bottom:18px;">
                <strong style="font-size:15px;color:#1e293b;">${d.codigo_inmueble || 'Sin código'}</strong>
                ${badge}
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px;">
                <div style="background:#f8fafc;padding:13px;border-radius:10px;border:1px solid #f1f5f9;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:3px;">Oportunidad</div>
                    <div style="font-size:13px;color:#334155;">${d.nombre_oportunidad || '-'}</div>
                </div>
                <div style="background:#f8fafc;padding:13px;border-radius:10px;border:1px solid #f1f5f9;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:3px;">Portafolio</div>
                    <div style="font-size:13px;color:#334155;">${d.portafolio || '-'}</div>
                </div>
                <div style="background:#f8fafc;padding:13px;border-radius:10px;border:1px solid #f1f5f9;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:3px;">Fecha Entrega</div>
                    <div style="font-size:13px;color:#334155;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</div>
                </div>
            </div>
            <div style="background:#fffbeb;padding:14px;border-radius:10px;border:1px dashed #fcd34d;">
                <div style="font-size:10px;font-weight:700;color:#d97706;text-transform:uppercase;margin-bottom:7px;">💬 Remarks / Razón del Fallo</div>
                <div style="font-size:13px;color:#92400e;white-space:pre-wrap;line-height:1.6;">${d.remarks || 'No hay remarks registrados.'}</div>
            </div>
        `;
        modal.style.display = 'flex';
    }

    function openDiscardModal(row) {
        pendingDiscardRow = row;
        const subtitle = document.getElementById('discard-modal-subtitle');
        if (subtitle) subtitle.textContent = `${row.codigo_inmueble} — ${row.nombre_oportunidad || ''}`;
        const ta = document.getElementById('discard-reason');
        if (ta) { ta.value = ''; ta.style.borderColor = '#e2e8f0'; }
        document.getElementById('discard-modal').style.display = 'flex';
        setTimeout(() => ta && ta.focus(), 100);
    }

    document.getElementById('confirm-discard-btn').addEventListener('click', async () => {
        const razon = (document.getElementById('discard-reason').value || '').trim();
        if (!razon) {
            const ta = document.getElementById('discard-reason');
            ta.style.borderColor = '#ef4444';
            ta.placeholder = '⚠️  La razón es obligatoria...';
            return;
        }
        if (!pendingDiscardRow) return;
        const btn = document.getElementById('confirm-discard-btn');
        btn.disabled = true; btn.textContent = 'Guardando...';
        const error = await saveDescarte(pendingDiscardRow, razon);
        if (error) {
            alert('Error al guardar. Intenta de nuevo.');
            btn.disabled = false; btn.textContent = 'Descartar';
            return;
        }
        descartados.unshift({
            codigo_inmueble: pendingDiscardRow.codigo_inmueble,
            nombre_oportunidad: pendingDiscardRow.nombre_oportunidad,
            portafolio: pendingDiscardRow.portafolio,
            druo_status: pendingDiscardRow.druo_status,
            razon_descarte: razon,
            descartado_at: new Date().toISOString()
        });
        descartadosCodes.add(pendingDiscardRow.codigo_inmueble);
        pendingDiscardRow = null;
        closeModal('discard-modal');
        btn.disabled = false; btn.textContent = 'Descartar';
        updateKPIs(); renderTable(); renderDescartados(); buildPortafolioChips();
    });

    // ----------------------------------------------------------------
    // Restore
    // ----------------------------------------------------------------
    async function restoreDescarte(codigo_inmueble) {
        const error = await deleteDescarte(codigo_inmueble);
        if (error) { alert('Error al restaurar.'); return; }
        descartados = descartados.filter(d => d.codigo_inmueble !== codigo_inmueble);
        descartadosCodes.delete(codigo_inmueble);
        updateKPIs(); renderTable(); renderDescartados(); buildPortafolioChips();
    }

    // Helper exposed globally
    window.closeModal = id => { document.getElementById(id).style.display = 'none'; };

    // ----------------------------------------------------------------
    // Event listeners
    // ----------------------------------------------------------------
    if (filterStatus) filterStatus.addEventListener('change', () => { saveURLParams(); renderTable(); });
    if (searchInput) searchInput.addEventListener('input', () => { saveURLParams(); renderTable(); });
    if (conectadosSearch) conectadosSearch.addEventListener('input', renderConectados);
    if (conectadosPortFilter) conectadosPortFilter.addEventListener('change', renderConectados);

    const clearBtn = document.getElementById('clear-portafolios');
    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            selectedPortafolios.clear();
            document.querySelectorAll('.portafolio-chip').forEach(c => c.classList.remove('active'));
            saveURLParams(); renderTable(); updateChipCount();
        });
    }

    // ----------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------
    const urlP = getURLParams();
    if (filterStatus) filterStatus.value = urlP.status;
    if (searchInput) searchInput.value = urlP.search;
    urlP.portafolios.forEach(p => selectedPortafolios.add(p));

    // Load all data in parallel
    [druoData, descartados, conectadosData] = await Promise.all([
        fetchDruoData(),
        fetchDescartados(),
        fetchConectados()
    ]);
    descartadosCodes = new Set(descartados.map(d => d.codigo_inmueble));

    updateKPIs();
    buildPortafolioChips();
    buildConectadosPortFilter();
    renderTable();
    renderDescartados();
    renderConectados();
});
