document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- DRUO Connection Status v3 ---');

    // ---- DOM refs ----
    const tableBody = document.getElementById('druo-tbody');
    const comercialBody = document.getElementById('comercial-tbody');
    const descartadosBody = document.getElementById('druo-descartados-tbody');
    const conectadosBody = document.getElementById('conectados-tbody');
    const portfolioOverview = document.getElementById('portfolio-overview');
    const overviewTotalActivos = document.getElementById('overview-total-activos');
    const statusChipsEl = document.getElementById('status-chips');
    const portafolioChipsEl = document.getElementById('portafolio-chips');
    const searchInput = document.getElementById('druo-search-input');
    const comercialSearch = document.getElementById('comercial-search-input');
    const conectadosSearch = document.getElementById('conectados-search-input');
    const conectadosPortFilter = document.getElementById('conectados-filter-portafolio');

    const kpiFailed = document.getElementById('druo-kpi-failed');
    const kpiNull = document.getElementById('druo-kpi-null');
    const kpiConectados = document.getElementById('druo-kpi-conectados');
    const kpiDescartados = document.getElementById('druo-kpi-descartados');
    const chartTooltip = document.createElement('div');
    chartTooltip.className = 'chart-tooltip';
    document.body.appendChild(chartTooltip);

    let druoData = [];
    let descartados = [];
    let conectadosData = [];
    let descartadosCodes = new Set();
    let selectedPortafolios = new Set();
    let selectedStatuses = new Set(); // empty = all statuses
    let pendingDiscardRow = null;

    // ----------------------------------------------------------------
    // URL params
    // ----------------------------------------------------------------
    function getURLParams() {
        const p = new URLSearchParams(window.location.search);
        return {
            statuses: p.get('statuses') ? p.get('statuses').split(',') : [],
            portafolios: p.get('portafolios') ? p.get('portafolios').split(',') : [],
            search: p.get('search') || ''
        };
    }
    function saveURLParams() {
        const params = new URLSearchParams();
        if (selectedStatuses.size > 0) params.set('statuses', [...selectedStatuses].join(','));
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
    function normalizeDruoStatus(status) {
        return (status || '').toString().trim().toUpperCase();
    }

    function isConnectedStatus(status) {
        return normalizeDruoStatus(status) === 'CONNECTED';
    }

    function isDisconnectedStatus(status) {
        const normalized = normalizeDruoStatus(status);
        return normalized === 'CONNECTION_FAILED' || normalized === 'DISCONNECTED';
    }

    function isMissingInDruoStatus(status) {
        return !normalizeDruoStatus(status);
    }

    function displayStatusLabel(status) {
        if (isMissingInDruoStatus(status)) return 'No están en DRUO';
        if (isDisconnectedStatus(status)) return 'Desconectado';
        return 'Desconectado';
    }

    function getStatusFilterKey(status) {
        if (isMissingInDruoStatus(status)) return 'missing';
        return 'disconnected';
    }

    function getStatusFilterLabel(statusKey) {
        if (statusKey === 'missing') return 'No están en DRUO';
        if (statusKey === 'disconnected') return 'Desconectado';
        return statusKey;
    }

    function updateKPIs() {
        const all = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const conectados = druoData.filter(d => isConnectedStatus(d.druo_status));
        const pendientes = all.filter(d => !isConnectedStatus(d.druo_status));

        if (kpiFailed) kpiFailed.textContent = pendientes.filter(d => isDisconnectedStatus(d.druo_status)).length;
        if (kpiNull) kpiNull.textContent = pendientes.filter(d => isMissingInDruoStatus(d.druo_status)).length;
        if (kpiConectados) kpiConectados.textContent = conectados.length;
        if (kpiDescartados) kpiDescartados.textContent = descartados.length;
    }

    function isCommercialPortfolio(portafolio) {
        const raw = (portafolio || '').trim();
        if (!raw) return false;

        const normalized = raw
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase();

        if (normalized.includes('progresion')) return true;

        const match = normalized.match(/duppla beneficio\s+(\d+)/);
        if (!match) return false;

        return Number(match[1]) >= 6;
    }

    function comparePortafolios(a, b) {
        const normalize = value => (value || '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();

        const getNumericPortfolio = value => {
            const match = normalize(value).match(/duppla beneficio\s+(\d+)/);
            return match ? Number(match[1]) : null;
        };

        const aNum = getNumericPortfolio(a);
        const bNum = getNumericPortfolio(b);
        const aNamed = aNum === null;
        const bNamed = bNum === null;

        if (aNamed && !bNamed) return -1;
        if (!aNamed && bNamed) return 1;
        if (aNamed && bNamed) return a.localeCompare(b, 'es', { sensitivity: 'base' });

        return aNum - bNum || a.localeCompare(b, 'es', { sensitivity: 'base' });
    }

    function getOverviewRows() {
        const normalizePortfolioName = value => (value || '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();
        const pinnedPortfolioOrder = new Map([
            ['skandia', 0],
            ['we seed', 1],
            ['progresion', 999]
        ]);

        const byCode = new Map();
        druoData.forEach(row => {
            const key = row.codigo_inmueble || [
                row.portafolio || 'sin-portafolio',
                row.nombre_oportunidad || 'sin-nombre',
                row.fecha_entrega || 'sin-fecha'
            ].join('|');
            const current = byCode.get(key);
            if (!current) {
                byCode.set(key, row);
                return;
            }

            const currentScore = isConnectedStatus(current.druo_status)
                ? 3
                : isDisconnectedStatus(current.druo_status)
                    ? 2
                    : normalizeDruoStatus(current.druo_status)
                        ? 1
                        : 0;
            const nextScore = isConnectedStatus(row.druo_status)
                ? 3
                : isDisconnectedStatus(row.druo_status)
                    ? 2
                    : normalizeDruoStatus(row.druo_status)
                        ? 1
                        : 0;

            if (nextScore > currentScore) byCode.set(key, row);
        });

        const grouped = new Map();
        [...byCode.values()].forEach(row => {
            const portafolio = row.portafolio || 'Sin portafolio';
            if (!grouped.has(portafolio)) {
                grouped.set(portafolio, {
                    portafolio,
                    total: 0,
                    failed: 0,
                    sinIntentar: 0,
                    connected: 0,
                    discarded: 0
                });
            }

            const bucket = grouped.get(portafolio);
            bucket.total += 1;

            if (descartadosCodes.has(row.codigo_inmueble)) {
                bucket.discarded += 1;
            } else if (isConnectedStatus(row.druo_status)) {
                bucket.connected += 1;
            } else if (isDisconnectedStatus(row.druo_status)) {
                bucket.failed += 1;
            } else if (isMissingInDruoStatus(row.druo_status)) {
                bucket.sinIntentar += 1;
            } else {
                bucket.failed += 1;
            }
        });

        return [...grouped.values()].sort((a, b) => {
            const aPinned = pinnedPortfolioOrder.get(normalizePortfolioName(a.portafolio));
            const bPinned = pinnedPortfolioOrder.get(normalizePortfolioName(b.portafolio));

            if (aPinned !== undefined || bPinned !== undefined) {
                if (aPinned === undefined) return bPinned === 999 ? -1 : 1;
                if (bPinned === undefined) return aPinned === 999 ? 1 : -1;
                if (aPinned !== bPinned) return aPinned - bPinned;
            }

            return a.portafolio.localeCompare(b.portafolio, 'es', { sensitivity: 'base' });
        });
    }

    function buildOverviewSegment(label, className, value, total) {
        if (!value || !total) return '';
        const width = Math.max((value / total) * 100, 2.5);
        return `<div class="stack-segment ${className}" style="width:${Math.min(width, 100)}%" data-tooltip="${label}: ${value} de ${total} operativos"></div>`;
    }

    function moveChartTooltip(event) {
        chartTooltip.style.left = `${event.clientX + 14}px`;
        chartTooltip.style.top = `${event.clientY + 14}px`;
    }

    function hideChartTooltip() {
        chartTooltip.classList.remove('visible');
    }

    function renderPortfolioOverview() {
        if (!portfolioOverview) return;
        const rows = getOverviewRows();
        const totalActivos = rows.reduce((acc, row) => acc + row.total, 0);

        if (overviewTotalActivos) {
            overviewTotalActivos.textContent = `Total inmuebles operativos: ${totalActivos}`;
        }

        if (rows.length === 0) {
            portfolioOverview.innerHTML = '<div class="overview-empty">No hay datos para construir el resumen por portafolio.</div>';
            return;
        }

        portfolioOverview.innerHTML = rows.map(row => `
            <div class="portfolio-row">
                <div class="portfolio-name" title="${row.portafolio}: ${row.total} operativos">
                    <span>${row.portafolio}</span>
                    <span class="portfolio-total-inline">${row.total} operativos</span>
                </div>
                <div class="stack-track" aria-label="Distribucion de estados para ${row.portafolio}">
                    ${buildOverviewSegment(`${row.portafolio} · No están en DRUO`, 'stack-null', row.sinIntentar, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Desconectados`, 'stack-failed', row.failed, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Conectados`, 'stack-connected', row.connected, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Descartados`, 'stack-discarded', row.discarded, row.total)}
                </div>
            </div>
        `).join('');
    }

    // ----------------------------------------------------------------
    // Status chips (multi-select)
    // ----------------------------------------------------------------
    function buildStatusChips() {
        if (!statusChipsEl) return;
        // Only show non-CONNECTED statuses (CONNECTED has its own tab)
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble) && !isConnectedStatus(d.druo_status));
        const statuses = [...new Set(activos.map(d => getStatusFilterKey(d.druo_status)))].sort();

        statusChipsEl.innerHTML = '';
        statuses.forEach(s => {
            const chip = document.createElement('button');
            chip.type = 'button';
            chip.textContent = getStatusFilterLabel(s);
            chip.className = 'portafolio-chip';
            chip.dataset.status = s;
            if (selectedStatuses.has(s)) chip.classList.add('active');
            chip.addEventListener('click', () => {
                selectedStatuses.has(s) ? selectedStatuses.delete(s) : selectedStatuses.add(s);
                chip.classList.toggle('active');
                saveURLParams(); renderTable(); updateStatusCount();
            });
            statusChipsEl.appendChild(chip);
        });
        updateStatusCount();
    }

    function updateStatusCount() {
        const el = document.getElementById('status-count');
        if (!el) return;
        el.textContent = selectedStatuses.size === 0 ? 'Todos' : `${selectedStatuses.size} seleccionados`;
        el.style.background = selectedStatuses.size === 0 ? '#e2e8f0' : 'var(--color-brand-dark)';
        el.style.color = selectedStatuses.size === 0 ? '#475569' : 'white';
    }

    // ----------------------------------------------------------------
    // Portafolio chips (for pending view)
    // ----------------------------------------------------------------
    function buildPortafolioChips() {
        if (!portafolioChipsEl) return;
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const ports = [...new Set(activos.map(d => d.portafolio).filter(Boolean))].sort(comparePortafolios);

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
        return `<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:${bg};color:${color};border:1px solid ${border};">${displayStatusLabel(status)}</span>`;
    }

    // ----------------------------------------------------------------
    // Render: Pendientes table
    // ----------------------------------------------------------------
    function renderTable() {
        if (!tableBody) return;
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';

        const filtered = druoData
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => !isConnectedStatus(d.druo_status)) // CONNECTED goes to its own tab
            .filter(d => {
                if (selectedStatuses.size === 0) return true;
                const dStatus = getStatusFilterKey(d.druo_status);
                return selectedStatuses.has(dStatus);
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
            const isFailed = isDisconnectedStatus(d.druo_status);
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

    function renderComercial() {
        if (!comercialBody) return;
        const searchTxt = comercialSearch ? comercialSearch.value.toLowerCase().trim() : '';

        const filtered = druoData
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => !isConnectedStatus(d.druo_status))
            .filter(d => isCommercialPortfolio(d.portafolio))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt));

        const cc = document.getElementById('comercial-count');
        if (cc) cc.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        comercialBody.innerHTML = '';
        if (filtered.length === 0) {
            comercialBody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;color:#94a3b8;">No hay inmuebles comerciales pendientes con los filtros actuales.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const isFailed = isDisconnectedStatus(d.druo_status);
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
            comercialBody.appendChild(row);
        });
    }

    // ----------------------------------------------------------------
    // Render: Conectados table
    // ----------------------------------------------------------------
    function renderConectados() {
        if (!conectadosBody) return;
        const searchTxt = conectadosSearch ? conectadosSearch.value.toLowerCase().trim() : '';
        const filterPort = conectadosPortFilter ? conectadosPortFilter.value : 'all';

        // Derive conectados directly from druoData
        const filtered = druoData
            .filter(d => isConnectedStatus(d.druo_status))
            .filter(d => filterPort === 'all' || d.portafolio === filterPort)
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt));

        const cc = document.getElementById('conectados-count');
        if (cc) cc.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        conectadosBody.innerHTML = '';
        if (filtered.length === 0) {
            conectadosBody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:40px;color:#94a3b8;">No hay clientes conectados aún.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:11px;color:#064e3b;background:#d1fae5;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td><span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#d1fae5;color:#065f46;border:1px solid #6ee7b7;">CONNECTED</span></td>
            `;
            conectadosBody.appendChild(row);
        });
    }

    function buildConectadosPortFilter() {
        if (!conectadosPortFilter) return;
        const ports = [...new Set(
            druoData.filter(d => isConnectedStatus(d.druo_status)).map(d => d.portafolio).filter(Boolean)
        )].sort(comparePortafolios);
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
            const isFailed = isDisconnectedStatus(d.druo_status);
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
        updateKPIs(); renderPortfolioOverview(); renderTable(); renderDescartados(); buildPortafolioChips();
    });

    // ----------------------------------------------------------------
    // Restore
    // ----------------------------------------------------------------
    async function restoreDescarte(codigo_inmueble) {
        const error = await deleteDescarte(codigo_inmueble);
        if (error) { alert('Error al restaurar.'); return; }
        descartados = descartados.filter(d => d.codigo_inmueble !== codigo_inmueble);
        descartadosCodes.delete(codigo_inmueble);
        updateKPIs(); renderPortfolioOverview(); renderTable(); renderDescartados(); buildPortafolioChips();
    }

    // Helper exposed globally
    window.closeModal = id => { document.getElementById(id).style.display = 'none'; };

    // ----------------------------------------------------------------
    // Event listeners
    // ----------------------------------------------------------------
    if (searchInput) searchInput.addEventListener('input', () => { saveURLParams(); renderTable(); });
    if (comercialSearch) comercialSearch.addEventListener('input', renderComercial);
    if (conectadosSearch) conectadosSearch.addEventListener('input', renderConectados);
    if (conectadosPortFilter) conectadosPortFilter.addEventListener('change', renderConectados);
    if (portfolioOverview) {
        portfolioOverview.addEventListener('mousemove', event => {
            const segment = event.target.closest('.stack-segment');
            if (!segment) {
                hideChartTooltip();
                return;
            }

            chartTooltip.textContent = segment.dataset.tooltip || '';
            moveChartTooltip(event);
            chartTooltip.classList.add('visible');
        });
        portfolioOverview.addEventListener('mouseleave', hideChartTooltip);
    }

    const clearPortBtn = document.getElementById('clear-portafolios');
    if (clearPortBtn) {
        clearPortBtn.addEventListener('click', () => {
            selectedPortafolios.clear();
            document.querySelectorAll('#portafolio-chips .portafolio-chip').forEach(c => c.classList.remove('active'));
            saveURLParams(); renderTable(); updateChipCount();
        });
    }

    const clearStatusBtn = document.getElementById('clear-statuses');
    if (clearStatusBtn) {
        clearStatusBtn.addEventListener('click', () => {
            selectedStatuses.clear();
            document.querySelectorAll('#status-chips .portafolio-chip').forEach(c => c.classList.remove('active'));
            saveURLParams(); renderTable(); updateStatusCount();
        });
    }

    // ----------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------
    const urlP = getURLParams();
    if (searchInput) searchInput.value = urlP.search;
    urlP.portafolios.forEach(p => selectedPortafolios.add(p));
    urlP.statuses.forEach(s => selectedStatuses.add(s));


    // Load all data in parallel
    [druoData, descartados, conectadosData] = await Promise.all([
        fetchDruoData(),
        fetchDescartados(),
        fetchConectados()
    ]);
    descartadosCodes = new Set(descartados.map(d => d.codigo_inmueble));

    updateKPIs();
    renderPortfolioOverview();
    buildStatusChips();
    buildPortafolioChips();
    buildConectadosPortFilter();
    renderTable();
    renderComercial();
    renderDescartados();
    renderConectados();
});
