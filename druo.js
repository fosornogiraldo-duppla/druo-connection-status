document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- DRUO Connection Status v3 ---');

    // ---- DOM refs ----
    const tableBody = document.getElementById('druo-tbody');
    const entregaBody = document.getElementById('entrega-tbody');
    const comercialBody = document.getElementById('comercial-tbody');
    const escrituracionBody = document.getElementById('escrituracion-tbody');
    const descartadosBody = document.getElementById('druo-descartados-tbody');
    const conectadosBody = document.getElementById('conectados-tbody');
    const portfolioOverview = document.getElementById('portfolio-overview');
    const overviewTotalActivos = document.getElementById('overview-total-activos');
    const statusOptionsEl = document.getElementById('status-options');
    const portafolioOptionsEl = document.getElementById('portafolio-options');
    const statusTriggerText = document.getElementById('status-trigger-text');
    const portafolioTriggerText = document.getElementById('portafolio-trigger-text');
    const searchInput = document.getElementById('druo-search-input');
    const entregaSearch = document.getElementById('entrega-search-input');
    const comercialSearch = document.getElementById('comercial-search-input');
    const escrituracionSearch = document.getElementById('escrituracion-search-input');
    const conectadosSearch = document.getElementById('conectados-search-input');
    const conectadosPortFilter = document.getElementById('conectados-filter-portafolio');
    const globalSegmentFilter = document.getElementById('global-segment-filter');
    const exportCsvBtn = document.getElementById('export-csv-btn');

    const kpiOperativoFailed = document.getElementById('druo-kpi-operativo-failed');
    const kpiOperativoNull = document.getElementById('druo-kpi-operativo-null');
    const kpiOperativoConectados = document.getElementById('druo-kpi-operativo-conectados');
    const kpiOperativoDescartados = document.getElementById('druo-kpi-operativo-descartados');
    const kpiEscrituracionFailed = document.getElementById('druo-kpi-escrituracion-failed');
    const kpiEscrituracionNull = document.getElementById('druo-kpi-escrituracion-null');
    const kpiEscrituracionConectados = document.getElementById('druo-kpi-escrituracion-conectados');
    const kpiEscrituracionDescartados = document.getElementById('druo-kpi-escrituracion-descartados');
    const kpiPushNull = document.getElementById('druo-kpi-push-null');
    const kpiPushFailed = document.getElementById('druo-kpi-push-failed');
    const kpiPushConectados = document.getElementById('druo-kpi-push-conectados');
    const kpiPushDescartados = document.getElementById('druo-kpi-push-descartados');
    const chartTooltip = document.createElement('div');
    chartTooltip.className = 'chart-tooltip';
    document.body.appendChild(chartTooltip);

    let druoData = [];
    let descartados = [];
    let conectadosData = [];
    let descartadosCodes = new Set();
    let selectedPortafolios = new Set();
    let selectedStatuses = new Set(); // empty = all statuses
    let selectedSegment = 'all';
    let pendingDiscardRow = null;
    const tableSortState = {
        pendientes: { key: 'fecha_entrega', direction: 'asc' },
        entrega: { key: 'fecha_entrega', direction: 'asc' },
        comercial: { key: null, direction: 'asc' },
        escrituracion: { key: null, direction: 'asc' },
        conectados: { key: null, direction: 'asc' },
        descartados: { key: null, direction: 'asc' }
    };

    // ----------------------------------------------------------------
    // URL params
    // ----------------------------------------------------------------
    function getURLParams() {
        const p = new URLSearchParams(window.location.search);
        return {
            statuses: p.get('statuses') ? p.get('statuses').split(',') : [],
            portafolios: p.get('portafolios') ? p.get('portafolios').split(',') : [],
            search: p.get('search') || '',
            segment: p.get('segment') || 'all'
        };
    }
    function saveURLParams() {
        const params = new URLSearchParams();
        if (selectedStatuses.size > 0) params.set('statuses', [...selectedStatuses].join(','));
        if (selectedPortafolios.size > 0) params.set('portafolios', [...selectedPortafolios].join(','));
        if (selectedSegment !== 'all') params.set('segment', selectedSegment);
        const search = searchInput ? searchInput.value.trim() : '';
        if (search) params.set('search', search);
        window.history.replaceState({}, '', `${window.location.pathname}${params.toString() ? '?' + params.toString() : ''}`);
    }

    // ----------------------------------------------------------------
    // Supabase fetches
    // ----------------------------------------------------------------
    async function fetchDruoData() {
        const { data: dataV2, error: errorV2 } = await window.supabaseClient.from('druo_no_conectados_v2').select('*');
        if (!errorV2) return dataV2 || [];

        console.warn('druo_no_conectados_v2 error, fallback to druo_no_conectados:', errorV2.message);
        const { data, error } = await window.supabaseClient.from('druo_no_conectados').select('*');
        if (error) {
            console.error('druo_no_conectados fallback error:', error);
            return [];
        }
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
        const enrichedPayload = {
            codigo_inmueble: row.codigo_inmueble,
            nombre_oportunidad: row.nombre_oportunidad,
            tipo_cliente: row.tipo_cliente,
            propietario_oportunidad: row.propietario_oportunidad,
            portafolio: row.portafolio,
            druo_status: row.druo_status || row.status || null,
            fecha_entrega: row.fecha_entrega || null,
            razon_descarte: razon,
            descartado_at: new Date().toISOString()
        };
        const basePayload = {
            codigo_inmueble: row.codigo_inmueble,
            nombre_oportunidad: row.nombre_oportunidad,
            portafolio: row.portafolio,
            druo_status: row.druo_status || row.status || null,
            razon_descarte: razon,
            descartado_at: new Date().toISOString()
        };

        const firstAttempt = await window.supabaseClient
            .from('druo_descartados')
            .upsert([enrichedPayload], { onConflict: 'codigo_inmueble' });
        if (!firstAttempt.error) return null;

        // Fallback for schema/constraint drift between environments.
        const message = (firstAttempt.error.message || '').toLowerCase();
        const shouldFallback = message.includes('column')
            || message.includes('on conflict')
            || message.includes('constraint');
        if (!shouldFallback) return firstAttempt.error;

        const secondAttempt = await window.supabaseClient
            .from('druo_descartados')
            .upsert([basePayload], { onConflict: 'codigo_inmueble' });
        if (!secondAttempt.error) return null;

        const insertAttempt = await window.supabaseClient
            .from('druo_descartados')
            .insert([basePayload]);
        return insertAttempt.error || secondAttempt.error;
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

    function normalizeText(value) {
        return (value || '')
            .toString()
            .trim()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase();
    }

    function getRowStatus(row) {
        return row?.druo_status ?? row?.status ?? null;
    }

    function getNormalizedLifecycle(row) {
        const segmento = normalizeText(row?.segmento);
        if (segmento) {
            if (segmento.includes('oper')) return 'operativo';
            if (segmento.includes('escr')) return 'escrituracion';
            return 'desconocido';
        }

        const ciclo = normalizeText(row?.ciclo_de_vida);
        if (ciclo.includes('oper')) return 'operativo';
        if (ciclo.includes('escr')) return 'escrituracion';
        return 'desconocido';
    }

    function isOperativoRow(row) {
        return getNormalizedLifecycle(row) === 'operativo';
    }

    function getOperativosRows() {
        return druoData.filter(isOperativoRow);
    }

    function getEscrituracionRows() {
        return druoData.filter(row => getNormalizedLifecycle(row) === 'escrituracion');
    }

    function getRowsForSelectedSegment() {
        if (selectedSegment === 'operativo') return getOperativosRows();
        if (selectedSegment === 'escrituracion') return getEscrituracionRows();
        return druoData;
    }

    function isConnectedStatus(status) {
        return normalizeDruoStatus(status) === 'CONNECTED';
    }

    function isDisconnectedStatus(status) {
        const normalized = normalizeDruoStatus(status);
        return normalized === 'CONNECTION_FAILED' || normalized === 'DISCONNECTED';
    }

    function isMissingInDruoStatus(status) {
        const normalized = normalizeDruoStatus(status);
        return !normalized || normalized === 'SIN INSCRIPCION' || normalized === 'SIN_INSCRIPCION';
    }

    function displayStatusLabel(status) {
        if (isConnectedStatus(status)) return 'Conectado';
        if (isMissingInDruoStatus(status)) return 'No está en DRUO';
        if (isDisconnectedStatus(status)) return 'Desconectado';
        return 'Desconectado';
    }

    function getStatusFilterKey(status) {
        if (isMissingInDruoStatus(status)) return 'missing';
        if (isConnectedStatus(status)) return 'connected';
        return 'disconnected';
    }

    function matchesSelectedStatus(status) {
        if (selectedStatuses.size === 0) return true;
        return selectedStatuses.has(getStatusFilterKey(status));
    }

    function getStatusFilterLabel(statusKey) {
        if (statusKey === 'missing') return 'No está en DRUO';
        if (statusKey === 'connected') return 'Conectado';
        if (statusKey === 'disconnected') return 'Desconectado';
        return statusKey;
    }

    function updateKPIs() {
        const operativos = getOperativosRows();
        const escrituracion = getEscrituracionRows();

        const activosOperativos = operativos.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const conectadosOperativos = activosOperativos.filter(d => isConnectedStatus(getRowStatus(d)));
        const pendientesOperativos = activosOperativos.filter(d => !isConnectedStatus(getRowStatus(d)));

        const activosEscrituracion = escrituracion.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const conectadosEscrituracion = activosEscrituracion.filter(d => isConnectedStatus(getRowStatus(d)));
        const pendientesEscrituracion = activosEscrituracion.filter(d => !isConnectedStatus(getRowStatus(d)));

        const lifecycleByCode = new Map(druoData.map(row => [row.codigo_inmueble, getNormalizedLifecycle(row)]));
        const descOperativos = descartados.filter(d => (lifecycleByCode.get(d.codigo_inmueble) || getNormalizedLifecycle(d)) === 'operativo');
        const descEscrituracion = descartados.filter(d => (lifecycleByCode.get(d.codigo_inmueble) || getNormalizedLifecycle(d)) === 'escrituracion');

        const pushAll = operativos.filter(d => isCommercialPortfolio(d.portafolio));
        const pushDescartados = descOperativos.filter(d => isCommercialPortfolio(d.portafolio));
        const pushActivos = pushAll.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const pushConectados = pushActivos.filter(d => isConnectedStatus(getRowStatus(d)));
        const pushPendientes = pushActivos.filter(d => !isConnectedStatus(getRowStatus(d)));

        if (kpiOperativoFailed) kpiOperativoFailed.textContent = pendientesOperativos.filter(d => isDisconnectedStatus(getRowStatus(d))).length;
        if (kpiOperativoNull) kpiOperativoNull.textContent = pendientesOperativos.filter(d => isMissingInDruoStatus(getRowStatus(d))).length;
        if (kpiOperativoConectados) kpiOperativoConectados.textContent = conectadosOperativos.length;
        if (kpiOperativoDescartados) kpiOperativoDescartados.textContent = descOperativos.length;

        if (kpiEscrituracionFailed) kpiEscrituracionFailed.textContent = pendientesEscrituracion.filter(d => isDisconnectedStatus(getRowStatus(d))).length;
        if (kpiEscrituracionNull) kpiEscrituracionNull.textContent = pendientesEscrituracion.filter(d => isMissingInDruoStatus(getRowStatus(d))).length;
        if (kpiEscrituracionConectados) kpiEscrituracionConectados.textContent = conectadosEscrituracion.length;
        if (kpiEscrituracionDescartados) kpiEscrituracionDescartados.textContent = descEscrituracion.length;

        if (kpiPushFailed) kpiPushFailed.textContent = pushPendientes.filter(d => isDisconnectedStatus(getRowStatus(d))).length;
        if (kpiPushNull) kpiPushNull.textContent = pushPendientes.filter(d => isMissingInDruoStatus(getRowStatus(d))).length;
        if (kpiPushConectados) kpiPushConectados.textContent = pushConectados.length;
        if (kpiPushDescartados) kpiPushDescartados.textContent = pushDescartados.length;
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

        return Number(match[1]) >= 5;
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

    function compareText(a, b) {
        return (a || '').toString().localeCompare((b || '').toString(), 'es', {
            sensitivity: 'base',
            numeric: true
        });
    }

    function getRowSortValue(row, key) {
        if (key === 'segmento') return getNormalizedLifecycle(row);
        if (key === 'portafolio') return row.portafolio || 'Sin portafolio';
        if (key === 'druo_status') return displayStatusLabel(getRowStatus(row));
        if (key === 'fecha_entrega' || key === 'descartado_at') {
            if (!row[key]) return null;
            const ts = new Date(row[key]).getTime();
            return Number.isNaN(ts) ? null : ts;
        }
        return row[key] ?? '';
    }

    function compareRowsByKey(a, b, key) {
        if (key === 'portafolio') return comparePortafolios(a.portafolio || 'Sin portafolio', b.portafolio || 'Sin portafolio');

        const aValue = getRowSortValue(a, key);
        const bValue = getRowSortValue(b, key);

        if (typeof aValue === 'number' && typeof bValue === 'number') {
            return aValue - bValue;
        }

        return compareText(aValue, bValue);
    }

    function sortRows(rows, tableName) {
        const state = tableSortState[tableName];
        if (!state || !state.key) return rows;

        if (state.key === 'fecha_entrega' || state.key === 'descartado_at') {
            return [...rows].sort((a, b) => {
                const aValue = getRowSortValue(a, state.key);
                const bValue = getRowSortValue(b, state.key);
                const aMissing = aValue === null;
                const bMissing = bValue === null;

                // Keep missing dates always at the end.
                if (aMissing && bMissing) return 0;
                if (aMissing) return 1;
                if (bMissing) return -1;

                return state.direction === 'desc' ? (bValue - aValue) : (aValue - bValue);
            });
        }

        const direction = state.direction === 'desc' ? -1 : 1;
        return [...rows].sort((a, b) => direction * compareRowsByKey(a, b, state.key));
    }

    function updateSortHeaders() {
        document.querySelectorAll('th.th-sortable').forEach(th => {
            const tableName = th.dataset.table;
            const sortKey = th.dataset.sort;
            const state = tableSortState[tableName];
            if (state && state.key === sortKey) {
                th.dataset.sortDir = state.direction;
            } else {
                th.dataset.sortDir = '';
            }
        });
    }

    function getOverviewRows() {
        const normalizePortfolioName = value => (value || '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();
        const pinnedPortfolioOrder = new Map([
            ['skandia', 0],
            ['progresion', 1]
        ]);

        const byCode = new Map();
        getOperativosRows().forEach(row => {
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

            const currentStatus = getRowStatus(current);
            const nextStatus = getRowStatus(row);
            const currentScore = isConnectedStatus(currentStatus)
                ? 3
                : isDisconnectedStatus(currentStatus)
                    ? 2
                    : normalizeDruoStatus(currentStatus)
                        ? 1
                        : 0;
            const nextScore = isConnectedStatus(nextStatus)
                ? 3
                : isDisconnectedStatus(nextStatus)
                    ? 2
                    : normalizeDruoStatus(nextStatus)
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
            } else if (isConnectedStatus(getRowStatus(row))) {
                bucket.connected += 1;
            } else if (isDisconnectedStatus(getRowStatus(row))) {
                bucket.failed += 1;
            } else if (isMissingInDruoStatus(getRowStatus(row))) {
                bucket.sinIntentar += 1;
            } else {
                bucket.failed += 1;
            }
        });

        return [...grouped.values()].sort((a, b) => {
            const aPinned = pinnedPortfolioOrder.get(normalizePortfolioName(a.portafolio));
            const bPinned = pinnedPortfolioOrder.get(normalizePortfolioName(b.portafolio));

            if (aPinned !== undefined || bPinned !== undefined) {
                if (aPinned === undefined) return 1;
                if (bPinned === undefined) return -1;
                if (aPinned !== bPinned) return aPinned - bPinned;
            }

            return comparePortafolios(a.portafolio, b.portafolio);
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

    function arrangeRowsForColumnReading(rows) {
        const leftCount = Math.ceil(rows.length / 2);
        const left = rows.slice(0, leftCount);
        const right = rows.slice(leftCount);
        const arranged = [];

        for (let i = 0; i < leftCount; i += 1) {
            if (left[i]) arranged.push(left[i]);
            if (right[i]) arranged.push(right[i]);
        }

        return arranged;
    }

    function renderPortfolioOverview() {
        if (!portfolioOverview) return;
        const rows = getOverviewRows();
        const displayRows = arrangeRowsForColumnReading(rows);
        const totalActivos = rows.reduce((acc, row) => acc + row.total, 0);

        if (overviewTotalActivos) {
            overviewTotalActivos.textContent = `Total inmuebles operativos: ${totalActivos}`;
        }

        if (rows.length === 0) {
            portfolioOverview.innerHTML = '<div class="overview-empty">No hay datos para construir el resumen por portafolio.</div>';
            return;
        }

        portfolioOverview.innerHTML = displayRows.map(row => `
            <div class="portfolio-row">
                <div class="portfolio-name" title="${row.portafolio}: ${row.total} operativos">
                    <span>${row.portafolio}</span>
                    <span class="portfolio-total-inline">${row.total} operativos</span>
                </div>
                <div class="stack-track" aria-label="Distribucion de estados para ${row.portafolio}">
                    ${buildOverviewSegment(`${row.portafolio} · No está en DRUO`, 'stack-null', row.sinIntentar, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Desconectados`, 'stack-failed', row.failed, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Conectados`, 'stack-connected', row.connected, row.total)}
                    ${buildOverviewSegment(`${row.portafolio} · Descartados`, 'stack-discarded', row.discarded, row.total)}
                </div>
            </div>
        `).join('');
    }

    // ----------------------------------------------------------------
    // Status dropdown (multi-select)
    // ----------------------------------------------------------------
    function buildStatusChips() {
        if (!statusOptionsEl) return;
        const ativosBase = getRowsForSelectedSegment();
        const activos = ativosBase.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const statuses = [...new Set(activos.map(d => getStatusFilterKey(getRowStatus(d))))].sort();

        statusOptionsEl.innerHTML = '';
        if (statuses.length === 0) {
            statusOptionsEl.innerHTML = '<span style="color:#94a3b8; font-size:12px; padding:8px; display:block;">Sin opciones</span>';
        }
        statuses.forEach(s => {
            const label = document.createElement('label');
            label.className = 'multi-option';
            const checked = selectedStatuses.has(s) ? 'checked' : '';
            label.innerHTML = `<input type="checkbox" data-status="${s}" ${checked} /> <span>${getStatusFilterLabel(s)}</span>`;
            const input = label.querySelector('input');
            input.addEventListener('change', () => {
                input.checked ? selectedStatuses.add(s) : selectedStatuses.delete(s);
                saveURLParams();
                renderTable();
                renderEntrega();
                renderComercial();
                renderEscrituracionPush();
                renderDescartados();
                updateStatusCount();
            });
            statusOptionsEl.appendChild(label);
        });
        updateStatusCount();
    }

    function updateStatusCount() {
        const el = document.getElementById('status-count');
        if (!el) return;
        el.textContent = selectedStatuses.size === 0 ? 'Todos' : `${selectedStatuses.size} seleccionados`;
        el.style.background = selectedStatuses.size === 0 ? '#e2e8f0' : 'var(--color-brand-dark)';
        el.style.color = selectedStatuses.size === 0 ? '#475569' : 'white';
        if (statusTriggerText) statusTriggerText.textContent = selectedStatuses.size === 0 ? 'Todos los status' : `${selectedStatuses.size} status seleccionados`;
    }

    // ----------------------------------------------------------------
    // Portafolio dropdown (for pending view)
    // ----------------------------------------------------------------
    function buildPortafolioChips() {
        if (!portafolioOptionsEl) return;
        const activos = getRowsForSelectedSegment().filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const ports = [...new Set(activos.map(d => d.portafolio).filter(Boolean))].sort(comparePortafolios);

        portafolioOptionsEl.innerHTML = '';
        if (ports.length === 0) {
            portafolioOptionsEl.innerHTML = '<span style="color:#94a3b8; font-size:12px; padding:8px; display:block;">Sin opciones</span>';
        }
        ports.forEach(p => {
            const label = document.createElement('label');
            label.className = 'multi-option';
            const checked = selectedPortafolios.has(p) ? 'checked' : '';
            label.innerHTML = `<input type="checkbox" data-portafolio="${p.replace(/"/g, '&quot;')}" ${checked} /> <span>${p}</span>`;
            const input = label.querySelector('input');
            input.addEventListener('change', () => {
                input.checked ? selectedPortafolios.add(p) : selectedPortafolios.delete(p);
                saveURLParams(); renderTable(); updateChipCount();
            });
            portafolioOptionsEl.appendChild(label);
        });
        updateChipCount();
    }

    function updateChipCount() {
        const el = document.getElementById('portafolio-count');
        if (!el) return;
        el.textContent = selectedPortafolios.size === 0 ? 'Todos' : `${selectedPortafolios.size} seleccionados`;
        el.style.background = selectedPortafolios.size === 0 ? '#e2e8f0' : 'var(--color-brand-dark)';
        el.style.color = selectedPortafolios.size === 0 ? '#475569' : 'white';
        if (portafolioTriggerText) portafolioTriggerText.textContent = selectedPortafolios.size === 0 ? 'Todos los portafolios' : `${selectedPortafolios.size} portafolios seleccionados`;
    }

    // ----------------------------------------------------------------
    // Badge HTML helper
    // ----------------------------------------------------------------
    function statusBadge(status, isFailed) {
        if (isConnectedStatus(status)) {
            return '<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#d1fae5;color:#065f46;border:1px solid #6ee7b7;">Conectado</span>';
        }
        const bg = isFailed ? '#fee2e2' : '#f1f5f9';
        const color = isFailed ? '#b91c1c' : '#475569';
        const border = isFailed ? '#fca5a5' : '#cbd5e1';
        return `<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:${bg};color:${color};border:1px solid ${border};">${displayStatusLabel(status)}</span>`;
    }

    function lifecycleBadge(row) {
        const lifecycle = getNormalizedLifecycle(row);
        if (lifecycle === 'operativo') {
            return '<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#dcfce7;color:#166534;border:1px solid #86efac;">Operativo</span>';
        }
        if (lifecycle === 'escrituracion') {
            return '<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#ffedd5;color:#9a3412;border:1px solid #fdba74;">Escrituración</span>';
        }
        return '<span style="display:inline-block;padding:4px 9px;border-radius:6px;font-size:11px;font-weight:700;background:#f1f5f9;color:#475569;border:1px solid #cbd5e1;">Sin definir</span>';
    }

    function remarksCell(remarks) {
        const text = (remarks || '').toString().trim();
        if (!text) return '<span style="color:#cbd5e1;">-</span>';
        return `<div style="max-width:320px;font-size:12px;line-height:1.45;color:#475569;white-space:pre-wrap;word-break:break-word;">${text}</div>`;
    }

    function ownerCell(owner) {
        const text = (owner || '').toString().trim();
        if (!text) return '<span style="color:#cbd5e1;">-</span>';
        return `<div style="line-height:1.35;color:#334155;white-space:normal;word-break:break-word;">${text}</div>`;
    }

    function clientCell(clientName) {
        const text = (clientName || '').toString().trim();
        if (!text) return '-';
        const title = text.replace(/"/g, '&quot;');
        return `<div title="${title}" style="max-width:260px;line-height:1.35;color:#334155;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;text-overflow:ellipsis;white-space:normal;word-break:break-word;">${text}</div>`;
    }

    function clientTypeCell(clientType) {
        const text = (clientType || '').toString().trim();
        if (!text) return '<span style="color:#cbd5e1;">-</span>';
        return `<span style="display:inline-block;padding:3px 8px;border-radius:999px;font-size:11px;font-weight:600;background:#f8fafc;color:#475569;border:1px solid #e2e8f0;">${text}</span>`;
    }

    function isMissingOrWithinOneYear(fechaEntrega) {
        if (!fechaEntrega) return true;
        const date = new Date(fechaEntrega);
        if (Number.isNaN(date.getTime())) return false;

        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const oneYearAhead = new Date(today);
        oneYearAhead.setFullYear(oneYearAhead.getFullYear() + 1);
        return date >= today && date <= oneYearAhead;
    }

    function getFilteredPendientesRows() {
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';
        const filtered = getRowsForSelectedSegment()
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => matchesSelectedStatus(getRowStatus(d)))
            .filter(d => selectedPortafolios.size === 0 || selectedPortafolios.has(d.portafolio))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt)
                || (d.propietario_oportunidad || '').toLowerCase().includes(searchTxt));
        return sortRows(filtered, 'pendientes');
    }

    function getFilteredComercialRows() {
        const searchTxt = comercialSearch ? comercialSearch.value.toLowerCase().trim() : '';
        const filtered = getRowsForSelectedSegment()
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => !isConnectedStatus(getRowStatus(d)))
            .filter(d => isCommercialPortfolio(d.portafolio))
            .filter(d => matchesSelectedStatus(getRowStatus(d)))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt)
                || (d.propietario_oportunidad || '').toLowerCase().includes(searchTxt));
        return sortRows(filtered, 'comercial');
    }

    function getFilteredEntregaRows() {
        const searchTxt = entregaSearch ? entregaSearch.value.toLowerCase().trim() : '';
        const filtered = getRowsForSelectedSegment()
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => matchesSelectedStatus(getRowStatus(d)))
            .filter(d => isMissingOrWithinOneYear(d.fecha_entrega))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt)
                || (d.propietario_oportunidad || '').toLowerCase().includes(searchTxt));
        return sortRows(filtered, 'entrega');
    }

    function getFilteredEscrituracionRows() {
        const searchTxt = escrituracionSearch ? escrituracionSearch.value.toLowerCase().trim() : '';
        const filtered = druoData
            .filter(d => getNormalizedLifecycle(d) === 'escrituracion')
            .filter(d => !descartadosCodes.has(d.codigo_inmueble))
            .filter(d => !isConnectedStatus(getRowStatus(d)))
            .filter(d => isMissingInDruoStatus(getRowStatus(d)) || isDisconnectedStatus(getRowStatus(d)))
            .filter(d => matchesSelectedStatus(getRowStatus(d)))
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt)
                || (d.propietario_oportunidad || '').toLowerCase().includes(searchTxt));
        return sortRows(filtered, 'escrituracion');
    }

    function getFilteredDescartadosRows() {
        return sortRows(descartados.filter(d => matchesSelectedStatus(getRowStatus(d))), 'descartados');
    }

    function getActiveView() {
        const views = ['pendientes', 'entrega', 'comercial', 'escrituracion', 'descartados'];
        const active = views.find(v => {
            const el = document.getElementById(`view-${v}`);
            return el && !el.classList.contains('hidden');
        });
        return active || 'pendientes';
    }

    function csvEscape(value) {
        const text = (value ?? '').toString().replace(/\r?\n/g, ' ').trim();
        if (text.includes('"') || text.includes(',') || text.includes(';')) {
            return `"${text.replace(/"/g, '""')}"`;
        }
        return text;
    }

    function downloadCsv(filename, headers, rows) {
        const headerLine = headers.map(csvEscape).join(',');
        const bodyLines = rows.map(row => row.map(csvEscape).join(','));
        const csv = [headerLine, ...bodyLines].join('\n');
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    }

    function exportCurrentViewToCsv() {
        const view = getActiveView();
        const today = new Date().toISOString().slice(0, 10);
        const sharedHeaders = [
            'Estado inmueble',
            'Status DRUO',
            'Codigo Inmueble',
            'Oportunidad / Cliente',
            'Tipo cliente',
            'Comercial',
            'Fecha Entrega',
            'Portafolio',
            'Remarks'
        ];

        if (view === 'pendientes') {
            const rows = getFilteredPendientesRows().map(d => [
                getNormalizedLifecycle(d),
                displayStatusLabel(getRowStatus(d)),
                d.codigo_inmueble || '',
                d.nombre_oportunidad || '',
                d.tipo_cliente || '',
                d.propietario_oportunidad || '',
                d.fecha_entrega || '',
                d.portafolio || '',
                d.remarks || ''
            ]);
            return downloadCsv(`druo-pendientes-${today}.csv`, sharedHeaders, rows);
        }

        if (view === 'comercial') {
            const rows = getFilteredComercialRows().map(d => [
                getNormalizedLifecycle(d),
                displayStatusLabel(getRowStatus(d)),
                d.codigo_inmueble || '',
                d.nombre_oportunidad || '',
                d.tipo_cliente || '',
                d.propietario_oportunidad || '',
                d.fecha_entrega || '',
                d.portafolio || '',
                d.remarks || ''
            ]);
            return downloadCsv(`druo-push-5-mas-${today}.csv`, sharedHeaders, rows);
        }

        if (view === 'entrega') {
            const rows = getFilteredEntregaRows().map(d => [
                getNormalizedLifecycle(d),
                displayStatusLabel(getRowStatus(d)),
                d.codigo_inmueble || '',
                d.nombre_oportunidad || '',
                d.tipo_cliente || '',
                d.propietario_oportunidad || '',
                d.fecha_entrega || '',
                d.portafolio || '',
                d.remarks || ''
            ]);
            return downloadCsv(`druo-entrega-menos-1-anio-${today}.csv`, sharedHeaders, rows);
        }

        if (view === 'escrituracion') {
            const rows = getFilteredEscrituracionRows().map(d => [
                getNormalizedLifecycle(d),
                displayStatusLabel(getRowStatus(d)),
                d.codigo_inmueble || '',
                d.nombre_oportunidad || '',
                d.tipo_cliente || '',
                d.propietario_oportunidad || '',
                d.fecha_entrega || '',
                d.portafolio || '',
                d.remarks || ''
            ]);
            return downloadCsv(`druo-push-escrituracion-${today}.csv`, sharedHeaders, rows);
        }

        const rows = getFilteredDescartadosRows().map(d => [
            getNormalizedLifecycle(d),
            displayStatusLabel(getRowStatus(d)),
            d.codigo_inmueble || '',
            d.nombre_oportunidad || '',
            d.tipo_cliente || '',
            d.propietario_oportunidad || '',
            d.portafolio || '',
            d.remarks || '',
            d.razon_descarte || '',
            d.descartado_at || ''
        ]);
        return downloadCsv(
            `druo-descartados-${today}.csv`,
            [
                'Estado inmueble',
                'Status DRUO',
                'Codigo Inmueble',
                'Oportunidad / Cliente',
                'Tipo cliente',
                'Comercial',
                'Portafolio',
                'Remarks',
                'Razon de descarte',
                'Fecha descarte'
            ],
            rows
        );
    }

    // ----------------------------------------------------------------
    // Render: Pendientes table
    // ----------------------------------------------------------------
    function renderTable() {
        if (!tableBody) return;
        const sorted = getFilteredPendientesRows();
        const rc = document.getElementById('result-count');
        if (rc) rc.textContent = `${sorted.length} resultado${sorted.length !== 1 ? 's' : ''}`;

        tableBody.innerHTML = '';
        if (sorted.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#94a3b8;">Sin resultados con los filtros actuales.</td></tr>';
            return;
        }

        sorted.forEach(d => {
            const rowStatus = getRowStatus(d);
            const isFailed = isDisconnectedStatus(rowStatus);
            const badge = statusBadge(rowStatus, isFailed);
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td>${badge}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${clientTypeCell(d.tipo_cliente)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${remarksCell(d.remarks)}</td>
                <td><button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button></td>
            `;
            row.addEventListener('click', e => { if (!e.target.closest('.btn-discard')) showDetailModal(d, badge); });
            row.querySelector('.btn-discard').addEventListener('click', e => { e.stopPropagation(); openDiscardModal(d); });
            tableBody.appendChild(row);
        });
    }

    function renderEntrega() {
        if (!entregaBody) return;
        const sorted = getFilteredEntregaRows();
        const countEl = document.getElementById('entrega-count');
        if (countEl) countEl.textContent = `${sorted.length} resultado${sorted.length !== 1 ? 's' : ''}`;

        entregaBody.innerHTML = '';
        if (sorted.length === 0) {
            entregaBody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#94a3b8;">No hay clientes sin fecha o con fecha de entrega menor a 1 año.</td></tr>';
            return;
        }

        sorted.forEach(d => {
            const rowStatus = getRowStatus(d);
            const isFailed = isDisconnectedStatus(rowStatus);
            const badge = statusBadge(rowStatus, isFailed);
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td>${badge}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${clientTypeCell(d.tipo_cliente)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : 'Sin fecha'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${remarksCell(d.remarks)}</td>
                <td><button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button></td>
            `;
            row.addEventListener('click', e => { if (!e.target.closest('.btn-discard')) showDetailModal(d, badge); });
            row.querySelector('.btn-discard').addEventListener('click', e => { e.stopPropagation(); openDiscardModal(d); });
            entregaBody.appendChild(row);
        });
    }

    function renderComercial() {
        if (!comercialBody) return;
        const sorted = getFilteredComercialRows();
        const cc = document.getElementById('comercial-count');
        if (cc) cc.textContent = `${sorted.length} resultado${sorted.length !== 1 ? 's' : ''}`;

        comercialBody.innerHTML = '';
        if (sorted.length === 0) {
            comercialBody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#94a3b8;">No hay inmuebles comerciales pendientes con los filtros actuales.</td></tr>';
            return;
        }

        sorted.forEach(d => {
            const rowStatus = getRowStatus(d);
            const isFailed = isDisconnectedStatus(rowStatus);
            const badge = statusBadge(rowStatus, isFailed);
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td>${badge}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${clientTypeCell(d.tipo_cliente)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${remarksCell(d.remarks)}</td>
                <td><button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button></td>
            `;
            row.addEventListener('click', e => { if (!e.target.closest('.btn-discard')) showDetailModal(d, badge); });
            row.querySelector('.btn-discard').addEventListener('click', e => { e.stopPropagation(); openDiscardModal(d); });
            comercialBody.appendChild(row);
        });
    }

    function renderEscrituracionPush() {
        if (!escrituracionBody) return;
        const sorted = getFilteredEscrituracionRows();
        const countEl = document.getElementById('escrituracion-count');
        if (countEl) countEl.textContent = `${sorted.length} resultado${sorted.length !== 1 ? 's' : ''}`;

        escrituracionBody.innerHTML = '';
        if (sorted.length === 0) {
            escrituracionBody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#94a3b8;">No hay inmuebles en escrituración no conectados/fallidos.</td></tr>';
            return;
        }

        sorted.forEach(d => {
            const rowStatus = getRowStatus(d);
            const isFailed = isDisconnectedStatus(rowStatus);
            const badge = statusBadge(rowStatus, isFailed);
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td>${badge}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${clientTypeCell(d.tipo_cliente)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${remarksCell(d.remarks)}</td>
                <td><button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button></td>
            `;
            row.addEventListener('click', e => { if (!e.target.closest('.btn-discard')) showDetailModal(d, badge); });
            row.querySelector('.btn-discard').addEventListener('click', e => { e.stopPropagation(); openDiscardModal(d); });
            escrituracionBody.appendChild(row);
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
        const filtered = getRowsForSelectedSegment()
            .filter(d => isConnectedStatus(getRowStatus(d)))
            .filter(d => filterPort === 'all' || d.portafolio === filterPort)
            .filter(d => !searchTxt
                || (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt)
                || (d.propietario_oportunidad || '').toLowerCase().includes(searchTxt));

        const sorted = sortRows(filtered, 'conectados');
        const cc = document.getElementById('conectados-count');
        if (cc) cc.textContent = `${sorted.length} resultado${sorted.length !== 1 ? 's' : ''}`;

        conectadosBody.innerHTML = '';
        if (sorted.length === 0) {
            conectadosBody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:#94a3b8;">No hay clientes conectados aún.</td></tr>';
            return;
        }

        sorted.forEach(d => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
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
            getRowsForSelectedSegment().filter(d => isConnectedStatus(getRowStatus(d))).map(d => d.portafolio).filter(Boolean)
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
        const sorted = getFilteredDescartadosRows();
        descartadosBody.innerHTML = '';
        if (sorted.length === 0) {
            descartadosBody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#94a3b8;">No hay inmuebles descartados para el status seleccionado.</td></tr>';
            return;
        }
        sorted.forEach(d => {
            const rowStatus = getRowStatus(d);
            const isFailed = isDisconnectedStatus(rowStatus);
            const date = d.descartado_at
                ? new Date(d.descartado_at).toLocaleDateString('es-CO', { day: '2-digit', month: 'short', year: 'numeric' })
                : '-';
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${lifecycleBadge(d)}</td>
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${clientCell(d.nombre_oportunidad)}</td>
                <td>${clientTypeCell(d.tipo_cliente)}</td>
                <td>${ownerCell(d.propietario_oportunidad)}</td>
                <td><span style="font-size:11px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td>${statusBadge(rowStatus, isFailed)}</td>
                <td>${remarksCell(d.remarks)}</td>
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
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:3px;">Propietario Opp</div>
                    <div style="font-size:13px;color:#334155;">${d.propietario_oportunidad || '-'}</div>
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
            const detail = error.message || error.details || error.hint || '';
            alert(`Error al guardar. ${detail || 'Intenta de nuevo.'}`);
            btn.disabled = false; btn.textContent = 'Descartar';
            return;
        }
        descartados.unshift({
            codigo_inmueble: pendingDiscardRow.codigo_inmueble,
            nombre_oportunidad: pendingDiscardRow.nombre_oportunidad,
            portafolio: pendingDiscardRow.portafolio,
            druo_status: getRowStatus(pendingDiscardRow),
            razon_descarte: razon,
            descartado_at: new Date().toISOString()
        });
        descartadosCodes.add(pendingDiscardRow.codigo_inmueble);
        pendingDiscardRow = null;
        closeModal('discard-modal');
        btn.disabled = false; btn.textContent = 'Descartar';
        updateKPIs(); renderPortfolioOverview(); renderTable(); renderEntrega(); renderComercial(); renderEscrituracionPush(); renderDescartados(); buildPortafolioChips();
    });

    // ----------------------------------------------------------------
    // Restore
    // ----------------------------------------------------------------
    async function restoreDescarte(codigo_inmueble) {
        const error = await deleteDescarte(codigo_inmueble);
        if (error) { alert('Error al restaurar.'); return; }
        descartados = descartados.filter(d => d.codigo_inmueble !== codigo_inmueble);
        descartadosCodes.delete(codigo_inmueble);
        updateKPIs(); renderPortfolioOverview(); renderTable(); renderEntrega(); renderComercial(); renderEscrituracionPush(); renderDescartados(); buildPortafolioChips();
    }

    // Helper exposed globally
    window.closeModal = id => { document.getElementById(id).style.display = 'none'; };

    // ----------------------------------------------------------------
    // Event listeners
    // ----------------------------------------------------------------
    if (searchInput) searchInput.addEventListener('input', () => { saveURLParams(); renderTable(); });
    if (entregaSearch) entregaSearch.addEventListener('input', renderEntrega);
    if (comercialSearch) comercialSearch.addEventListener('input', renderComercial);
    if (escrituracionSearch) escrituracionSearch.addEventListener('input', renderEscrituracionPush);
    if (conectadosSearch) conectadosSearch.addEventListener('input', renderConectados);
    if (conectadosPortFilter) conectadosPortFilter.addEventListener('change', renderConectados);
    if (globalSegmentFilter) {
        globalSegmentFilter.addEventListener('change', () => {
            selectedSegment = globalSegmentFilter.value || 'all';
            selectedStatuses.clear();
            selectedPortafolios.clear();
            saveURLParams();
            buildStatusChips();
            buildPortafolioChips();
            buildConectadosPortFilter();
            renderTable();
            renderEntrega();
            renderComercial();
            renderEscrituracionPush();
            renderConectados();
        });
    }
    document.querySelectorAll('th.th-sortable').forEach(th => {
        th.addEventListener('click', () => {
            const tableName = th.dataset.table;
            const sortKey = th.dataset.sort;
            if (!tableName || !sortKey || !tableSortState[tableName]) return;

            const state = tableSortState[tableName];
            if (state.key === sortKey) {
                state.direction = state.direction === 'asc' ? 'desc' : 'asc';
            } else {
                state.key = sortKey;
                state.direction = 'asc';
            }

            updateSortHeaders();

            if (tableName === 'pendientes') renderTable();
            if (tableName === 'entrega') renderEntrega();
            if (tableName === 'comercial') renderComercial();
            if (tableName === 'escrituracion') renderEscrituracionPush();
            if (tableName === 'conectados') renderConectados();
            if (tableName === 'descartados') renderDescartados();
        });
    });
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
            document.querySelectorAll('#portafolio-options input[type=\"checkbox\"]').forEach(c => { c.checked = false; });
            saveURLParams();
            renderTable();
            updateChipCount();
        });
    }

    const clearStatusBtn = document.getElementById('clear-statuses');
    if (clearStatusBtn) {
        clearStatusBtn.addEventListener('click', () => {
            selectedStatuses.clear();
            document.querySelectorAll('#status-options input[type=\"checkbox\"]').forEach(c => { c.checked = false; });
            saveURLParams();
            renderTable();
            renderEntrega();
            renderComercial();
            renderEscrituracionPush();
            renderDescartados();
            updateStatusCount();
        });
    }

    if (exportCsvBtn) {
        exportCsvBtn.addEventListener('click', exportCurrentViewToCsv);
    }

    // ----------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------
    const urlP = getURLParams();
    if (searchInput) searchInput.value = urlP.search;
    selectedSegment = ['all', 'operativo', 'escrituracion'].includes(urlP.segment) ? urlP.segment : 'all';
    if (globalSegmentFilter) globalSegmentFilter.value = selectedSegment;
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
    renderEntrega();
    renderComercial();
    renderEscrituracionPush();
    renderDescartados();
    renderConectados();
    updateSortHeaders();
});
