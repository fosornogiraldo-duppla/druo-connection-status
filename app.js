// Main App logic - Refactored for global scope execution

document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- Duppla Dashboard V4.11 (Forgotten Filter: 7+ Months) ---');
    // 1. Supabase Initialization & Data Fetching
    let clients = [];

    async function fetchClients() {
        console.log('Fetching clients from Supabase...');
        const { data, error } = await window.supabaseClient
            .from('clients')
            .select('*');

        if (error) {
            console.error('Error fetching clients:', error);
            alert('Error al conectar con la base de datos.');
            return [];
        }
        const enriched = data.map(enrichClientData);
        clients = enriched;
        syncStateFromClients();
        await fetchOffRampQueue();
        return enriched;
    }

    async function fetchOffRampQueue() {
        const { data, error } = await window.supabaseClient
            .from('offramp_queue')
            .select('client_id, status, updates_log, updated_at')
            .order('created_at', { ascending: false });

        if (error) {
            console.error('Error fetching off-ramp queue:', error);
            offRampQueueIds = [];
            offRampQueueData = {};
            return;
        }

        offRampQueueIds = [...new Set((data || []).map(row => row.client_id))];
        offRampQueueData = {};
        (data || []).forEach(row => {
            offRampQueueData[row.client_id] = {
                status: row.status || 'pendiente',
                updates_log: row.updates_log || '',
                updated_at: row.updated_at || null
            };
        });
    }

    // State
    let filters = {
        group: 'all',
        segment: 'all',
        search: ''
    };
    let callFilter = 'queue'; // 'queue', 'scheduled' or 'completed'
    let callQueueIds = [];
    let offRampQueueIds = [];
    let offRampQueueData = {};
    let scheduledData = {}; // { client_id: { date: 'YYYY-MM-DD', time: 'HH:MM', prep_notes: '...', rejection_reason: '...' } }
    let completedCalls = [];
    let retentionChartInst = null;

    // 2. Enrich Data
    function enrichClientData(client) {
        // Ensure all numerical fields are treated as numbers
        const equityActual = Number(client.equity_actual_pct || 0);
        const equityInicial = Number(client.participacion_inicial_pct || 0);

        // CÁLCULO SEGURO: Siempre basado en la diferencia real
        const equityComprado = equityActual - equityInicial;

        const meses = Number(client.antiguedad_meses || 1); // Avoid div by zero
        const progresoMensual = (equityActual - equityInicial) / meses;
        const proy5a = equityActual + (progresoMensual * 60);

        const normalized = {
            ...client,
            antiguedad_meses: meses,
            participacion_inicial_pct: equityInicial,
            equity_actual_pct: equityActual,
            participacion_adquirida_pct: equityComprado,
            moratoria_promedio_pct: Number(client.moratoria_promedio_pct || 0),
            credit_score_actual: Number(client.credit_score_actual || 0),
            delta_credit_score: Number(client.delta_credit_score || 0),
            saldo_en_mora: Number(client.saldo_en_mora || 0),
            dti_actual_pct: client.dti_actual_pct !== null ? Number(client.dti_actual_pct) : null,
            equity_proyectada_5y_pct: Math.max(0, proy5a) // Proyección dinámica basada en ritmo real
        };

        return {
            ...normalized,
            grupo: window.clasificarGrupoMeta(normalized),
            isListo: window.esListoBanco(normalized),
            segmento: window.clasificarSegmentacionValeria(normalized),
            estadoLlamada: window.calcularEstadoLlamada(normalized)
        };
    }

    // Helper: Initialize state from fetched data
    function syncStateFromClients() {
        scheduledData = {};
        callQueueIds = [];
        clients.forEach(c => {
            // Priority: Scheduled > In Queue
            if (c.scheduled_date) {
                scheduledData[c.id] = {
                    date: c.scheduled_date,
                    time: c.scheduled_time || '09:00',
                    prep_notes: c.prep_notes || '',
                    rejection_reason: c.rejection_reason || ''
                };
            } else if (c.in_queue) {
                callQueueIds.push(c.id);
            }
        });

        // Keep only queue IDs that still exist in clients table.
        offRampQueueIds = offRampQueueIds.filter(id => clients.some(c => c.id === id));
        Object.keys(offRampQueueData).forEach(id => {
            if (!clients.some(c => c.id === Number(id))) {
                delete offRampQueueData[id];
            }
        });
    }

    // 3. DOM Elements
    const kpiNorthstar = document.getElementById('kpi-northstar');
    const kpiTotal = document.getElementById('kpi-total');
    const kpiOfframp = document.getElementById('kpi-offramp');
    const kpiConstruyendo = document.getElementById('kpi-construyendo');
    const kpiRisk = document.getElementById('kpi-risk');
    const kpiReach6m = document.getElementById('kpi-reach-6m'); // NEW
    const kpiForgotten = document.getElementById('kpi-forgotten'); // NEW
    const tableBody = document.getElementById('clients-tbody');
    const callsContainer = document.getElementById('calls-container');
    const offRampContainer = document.getElementById('offramp-container');
    const activeFilterHint = document.getElementById('active-filter-hint');
    const clientSearchInput = document.getElementById('client-search-input');
    const toggleFiltersBtn = document.getElementById('toggle-filters-btn');
    const filtersContent = document.getElementById('filters-content');
    const filterBtns = document.querySelectorAll('.filter-btn');
    const tabBtns = document.querySelectorAll('.tab-btn');
    const subTabBtns = document.querySelectorAll('.sub-tab-btn');
    const views = document.querySelectorAll('.view');

    // 4. Render Functions
    function updateKPIs() {
        const total = clients.length;
        const listos = clients.filter(c => c.isListo).length;
        const offramp = clients.filter(c => c.grupo === 'Off-Ramp' || c.grupo === 'off-ramp').length;
        const construyendo = clients.filter(c => c.grupo === 'Construyendo' || c.grupo === 'construyendo').length;
        const risk = clients.filter(c => c.grupo === 'En riesgo' || c.grupo === 'En Riesgo').length;

        if (kpiNorthstar) kpiNorthstar.textContent = listos; // Count only, not percentage
        if (kpiTotal) kpiTotal.textContent = total;
        if (kpiOfframp) kpiOfframp.textContent = offramp;
        if (kpiConstruyendo) kpiConstruyendo.textContent = construyendo;
        if (kpiRisk) kpiRisk.textContent = risk;

        // Diagnóstico Logic (Clientes sin mora que fallan criterios)
        const sinMora = clients.filter(c => c.saldo_en_mora === 0);

        const failEquity = sinMora.filter(c => c.equity_actual_pct < 30).length;
        const failScore = sinMora.filter(c => c.credit_score_actual < 700).length;
        // DTI failure check: not null AND > 35%
        const failDTI = sinMora.filter(c => (c.dti_actual_pct !== null && c.dti_actual_pct > 35)).length;

        // V3 NEW KPIs: 6 Month Reach
        const hoy = new Date();
        const seisMesesAtras = new Date();
        seisMesesAtras.setMonth(hoy.getMonth() - 6);

        const talkedLast6m = clients.filter(c => c.ultima_llamada && new Date(c.ultima_llamada) >= seisMesesAtras).length;
        const reachPct = total > 0 ? ((talkedLast6m / total) * 100).toFixed(1) : '0.0';

        // Forgotten: clients with 7+ months in portfolio AND no contact in last 6 months
        const forgotten = clients.filter(c => {
            const antiguedad = c.antiguedad_meses || 0;
            if (antiguedad < 7) return false; // Must be in portfolio for at least 7 months
            return !c.ultima_llamada || new Date(c.ultima_llamada) < seisMesesAtras;
        }).length;

        if (kpiReach6m) kpiReach6m.textContent = talkedLast6m;
        if (kpiForgotten) kpiForgotten.textContent = forgotten;

        const elFailEquity = document.getElementById('countFallaEquity');
        const elFailScore = document.getElementById('countFallaScore');
        const elFailDTI = document.getElementById('countFallaDTI');

        if (elFailEquity) elFailEquity.textContent = failEquity;
        if (elFailScore) elFailScore.textContent = failScore;
        if (elFailDTI) elFailDTI.textContent = failDTI;

        // V4: New Evolutionary Chart
        updateRetentionChart();
    }

    function updateActiveFilterHint() {
        if (!activeFilterHint) return;

        const hintByGroup = {
            'Off-Ramp': {
                className: 'hint-offramp',
                text: 'Off-Ramp (cumple TODAS): equity_actual_pct >= 25, equity_proyectada_5y_pct > 30, saldo_en_mora = 0, delta_credit_score > 0 y credit_score_actual > 700.'
            },
            'Construyendo': {
                className: 'hint-construyendo',
                text: 'Construyendo: cliente que NO dispara ninguna regla de En riesgo y NO cumple todas las reglas de Off-Ramp.'
            },
            'En riesgo': {
                className: 'hint-riesgo',
                text: 'En riesgo (solo si antiguedad_meses >= 12 y cumple AL MENOS UNA): saldo_en_mora > 0, moratoria_promedio_pct > 0, (delta_credit_score < 0 y credit_score_actual < 600) o equity_proyectada_5y_pct < 30.'
            },
            'Nuevos': {
                className: 'hint-nuevos',
                text: 'Nuevos (temporal): clientes con antiguedad_meses < 12.'
            }
        };

        const selected = hintByGroup[filters.group];
        if (!selected) {
            activeFilterHint.style.display = 'none';
            activeFilterHint.textContent = '';
            activeFilterHint.className = 'filter-hint';
            return;
        }

        activeFilterHint.style.display = 'block';
        activeFilterHint.className = `filter-hint ${selected.className}`;
        activeFilterHint.textContent = selected.text;
    }

    function updateRetentionChart() {
        const canvas = document.getElementById('retentionChart');
        if (!canvas) return;

        const buckets = ['Año 1', 'Año 2', 'Año 3', 'Año 4', 'Año 5+'];
        const dataMap = {
            'Off-Ramp': [0, 0, 0, 0, 0],
            'Construyendo': [0, 0, 0, 0, 0],
            'En riesgo': [0, 0, 0, 0, 0],
            'Nuevos': [0, 0, 0, 0, 0]
        };

        clients.forEach(c => {
            const months = c.antiguedad_meses || 0;
            let bucketIdx = 0;
            if (months <= 12) bucketIdx = 0;      // Año 1: 0-12 meses
            else if (months <= 24) bucketIdx = 1;  // Año 2: 13-24 meses
            else if (months <= 36) bucketIdx = 2;  // Año 3: 25-36 meses
            else if (months <= 48) bucketIdx = 3;  // Año 4: 37-48 meses
            else bucketIdx = 4;                    // Año 5+: 49+ meses

            const group = c.grupo || 'Construyendo';
            // Normalize group name
            let normalized = 'Construyendo';
            if (group.toLowerCase().includes('off-ramp')) normalized = 'Off-Ramp';
            else if (group.toLowerCase().includes('riesgo')) normalized = 'En riesgo';
            else if (group.toLowerCase().includes('nuevo')) normalized = 'Nuevos';

            if (dataMap[normalized]) {
                dataMap[normalized][bucketIdx]++;
            }
        });

        const datasets = [
            {
                label: 'Off-Ramp',
                data: dataMap['Off-Ramp'],
                backgroundColor: '#A7F3D0', // soft emerald
                borderRadius: 4,
                barPercentage: 0.6,
                categoryPercentage: 0.6
            },
            {
                label: 'Construyendo',
                data: dataMap['Construyendo'],
                backgroundColor: '#FDE68A', // soft amber
                borderRadius: 4,
                barPercentage: 0.6,
                categoryPercentage: 0.6
            },
            {
                label: 'En Riesgo',
                data: dataMap['En riesgo'],
                backgroundColor: '#FECACA', // soft rose
                borderRadius: 4,
                barPercentage: 0.6,
                categoryPercentage: 0.6
            },
            {
                label: 'Nuevos',
                data: dataMap['Nuevos'],
                backgroundColor: '#C7D2FE', // soft indigo
                borderRadius: 4,
                barPercentage: 0.6,
                categoryPercentage: 0.6
            }
        ];

        if (retentionChartInst) {
            retentionChartInst.data.datasets = datasets;
            retentionChartInst.update();
        } else {
            retentionChartInst = new Chart(canvas, {
                type: 'bar',
                data: {
                    labels: buckets,
                    datasets: datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { stacked: true, grid: { display: false } },
                        y: { stacked: true, grid: { color: '#f1f5f9' }, beginAtZero: true, max: 60 }
                    },
                    plugins: {
                        legend: { position: 'bottom', labels: { usePointStyle: true, padding: 20, font: { size: 11 } } },
                        tooltip: { backgroundColor: '#1e293b', padding: 12 }
                    }
                }
            });
        }
    }

    function renderTable() {
        if (!tableBody) return;
        tableBody.innerHTML = '';

        // Sort: Oldest first (more months)
        const sortedClients = [...clients].sort((a, b) => b.antiguedad_meses - a.antiguedad_meses);

        const filtered = sortedClients.filter(c => {
            const groupMatch = filters.group === 'all' || c.grupo === filters.group;
            // Segment check: allow partial match because segment string is long (e.g. "Avanzados confiables")
            const segmentMatch = filters.segment === 'all' || c.segmento.startsWith(filters.segment);
            const searchText = (filters.search || '').trim().toLowerCase();
            const searchMatch = searchText === '' || (c.cliente || '').toLowerCase().includes(searchText);
            return groupMatch && segmentMatch && searchMatch;
        });

        if (filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="18" style="text-align:center; padding: 20px;">No se encontraron clientes con estos filtros.</td></tr>';
            return;
        }

        filtered.forEach(c => {
            const row = document.createElement('tr');

            // Format Data
            const badgeClass = c.grupo === 'Off-Ramp' ? 'off-ramp' :
                c.grupo === 'Construyendo' ? 'construyendo' :
                    c.grupo === 'Nuevos' ? 'nuevos' : 'en-riesgo';

            const badgeText = c.grupo === 'En riesgo' ? 'En Riesgo' : c.grupo;

            const badgeGrupo = `<span class="badge ${badgeClass}" style="margin-top:4px;">${badgeText}</span>`;
            const badgeListo = c.isListo ? '<span class="badge off-ramp">Listo</span>' : '-';

            // Universal Red Highlighting Logic
            const redColor = 'color:#ff3b30;';
            const styleEquity = c.equity_actual_pct < 30 ? redColor : '';
            const styleScore = c.credit_score_actual < 700 ? redColor : '';
            const styleDti = (c.dti_actual_pct !== null && c.dti_actual_pct > 35) ? redColor : '';
            const styleComprado = ''; // Criterio pendiente
            const styleProy = c.equity_proyectada_5y_pct < 30 ? redColor : '';
            const styleDelta = c.delta_credit_score < -100 ? redColor : '';
            const styleMora = c.saldo_en_mora > 0 ? redColor : '';
            const styleMoraProm = c.moratoria_promedio_pct > 5 ? redColor : '';
            const styleHabito = c.habito_de_pago === 'Atrasado' ? redColor : '';

            row.innerHTML = `
                <td style="line-height:1.3;">
                    <strong>${c.cliente}</strong><br>
                    <span class="text-sm" style="font-size:11px;">${c.codigo_inmueble || '-'}</span><br>
                    ${badgeGrupo}
                </td>
                <td style="color:#6e6e73;">${c.antiguedad_meses} meses</td>
                <td>${badgeListo}</td>
                <td style="color:#6e6e73;">${c.segmento.split(' ')[0]}</td>
                <td>${c.participacion_inicial_pct.toFixed(1)}%</td>
                
                <!-- CORE 3 HIGHLIGHTED GROUP -->
                <td class="highlight-bg" style="font-weight:700;${styleEquity}">${c.equity_actual_pct.toFixed(1)}%</td>
                <td class="highlight-bg" style="font-weight:700;${styleScore}">${c.credit_score_actual}</td>
                <td class="highlight-bg" style="font-weight:700;${styleDti}">${c.dti_actual_pct !== null ? c.dti_actual_pct.toFixed(1) + '%' : '-'}</td>
                
                <td style="${styleComprado}">${c.participacion_adquirida_pct.toFixed(1)}%</td>
                <td style="font-weight:600;${styleProy}">${c.equity_proyectada_5y_pct.toFixed(1)}%</td>
                <td style="font-weight:600;${styleDelta}">${c.delta_credit_score > 0 ? '+' : ''}${c.delta_credit_score}</td>
                <td style="font-weight:600;${styleMora}">${c.saldo_en_mora > 0 ? '$' + c.saldo_en_mora.toLocaleString() : '-'}</td>
                <td style="${styleMoraProm}">${c.moratoria_promedio_pct.toFixed(1)}%</td>
                <td style="${styleHabito}">${c.habito_de_pago || '-'}</td>
                <td style="color:#6e6e73; font-size:11px;">${c.situacion_actual || '-'}</td>
                <td style="color:#6e6e73; font-size:11px;">${c.ultima_llamada ? new Date(c.ultima_llamada).toLocaleDateString() : 'Sin contacto'}</td>
                <td>
                    <button class="filter-btn agendar-manual-btn" data-id="${c.id}" style="padding: 4px 10px; font-size: 11px; ${callQueueIds.includes(c.id) || scheduledData[c.id] ? 'opacity:0.5; cursor:default;' : ''}">
                        ${(callQueueIds.includes(c.id) || scheduledData[c.id]) ? 'En lista' : 'Agendar'}
                    </button>
                </td>
                <td>
                    <button class="filter-btn offramp-manual-btn" data-id="${c.id}" style="padding: 4px 10px; font-size: 11px; ${offRampQueueIds.includes(c.id) ? 'opacity:0.5; cursor:default;' : ''}">
                        ${offRampQueueIds.includes(c.id) ? 'En gestión' : 'Off-Ramp'}
                    </button>
                </td>
            `;
            tableBody.appendChild(row);
        });

        // Add listeners for manual agenda
        document.querySelectorAll('.agendar-manual-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation(); // Prevent parent row clicks
                const id = parseInt(btn.dataset.id);
                if (!callQueueIds.includes(id) && !scheduledData[id]) {
                    openConfirmSchedulingModal(id);
                }
            });
        });

        document.querySelectorAll('.offramp-manual-btn').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const id = parseInt(btn.dataset.id);
                if (!offRampQueueIds.includes(id)) {
                    await addToOffRampQueue(id);
                }
            });
        });
    }

    function openConfirmSchedulingModal(id) {
        const client = clients.find(c => c.id === id);
        if (!client) return;

        const modalBody = document.getElementById('modalBody');
        modalBody.innerHTML = `
            <div style="text-align: center; padding: 20px 0;">
                <div style="font-size: 48px; margin-bottom: 20px;">📋</div>
                <h2 style="margin-bottom: 12px; color: var(--color-brand-dark);">Añadir a lista de llamadas</h2>
                <p style="color: var(--color-text-secondary); margin-bottom: 32px; font-size: 16px;">
                    ¿Quieres añadir a <strong>${client.cliente}</strong> a la lista de pendientes por agendar?
                </p>
                <div style="display: flex; justify-content: center; gap: 16px;">
                    <button class="filter-btn" style="padding: 10px 24px;" onclick="document.getElementById('infoModal').style.display='none'">Cancelar</button>
                    <button class="btn-primary" id="confirm-queue-btn" style="padding: 10px 24px;">Sí, añadir</button>
                </div>
            </div>
        `;

        document.getElementById('infoModal').style.display = 'block';

        document.getElementById('confirm-queue-btn').onclick = async () => {
            if (!callQueueIds.includes(id)) {
                // V4: Persistent update to Supabase
                const confirmBtn = document.getElementById('confirm-queue-btn');
                confirmBtn.disabled = true;
                confirmBtn.textContent = 'Añadiendo...';

                try {
                    const { error } = await window.supabaseClient
                        .from('clients')
                        .update({ in_queue: true })
                        .eq('id', id);

                    if (error) throw error;

                    callQueueIds.push(id);
                    // Update local object
                    const clientIdx = clients.findIndex(c => c.id === id);
                    if (clientIdx !== -1) clients[clientIdx].in_queue = true;

                    document.getElementById('infoModal').style.display = 'none';
                    renderTable();
                    renderCalls();
                } catch (err) {
                    console.error('Error adding to queue:', err);
                    alert('No se pudo añadir a la lista en la nube.');
                    confirmBtn.disabled = false;
                    confirmBtn.textContent = 'Sí, añadir';
                }
            }
        };
    }

    async function addToOffRampQueue(id) {
        const btn = document.querySelector(`.offramp-manual-btn[data-id="${id}"]`);
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Guardando...';
        }

        try {
            const { error } = await window.supabaseClient
                .from('offramp_queue')
                .upsert(
                    [{ client_id: id, status: 'pendiente' }],
                    { onConflict: 'client_id' }
                );

            if (error) throw error;

            if (!offRampQueueIds.includes(id)) {
                offRampQueueIds.push(id);
            }
            if (!offRampQueueData[id]) {
                offRampQueueData[id] = { status: 'pendiente', updates_log: '', updated_at: null };
            }
            renderTable();
            renderOffRamp();
            switchToTab('offramp');
        } catch (err) {
            console.error('Error adding to off-ramp queue:', err);
            alert('No se pudo añadir a gestión Off-Ramp en la nube.');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    function renderOffRamp() {
        if (!offRampContainer) return;
        offRampContainer.innerHTML = '';

        const queue = clients.filter(c => offRampQueueIds.includes(c.id));
        if (queue.length === 0) {
            offRampContainer.innerHTML = '<p class="text-secondary">No hay clientes en gestión Off-Ramp. Agrégalos desde Priorización.</p>';
            return;
        }

        queue.forEach(c => {
            const offData = offRampQueueData[c.id] || { status: 'pendiente', updates_log: '' };
            const historyItems = (offData.updates_log || '')
                .split('\n')
                .filter(Boolean)
                .map(line => `<li style="font-size:12px; color:#475569; margin-bottom:4px;">${escapeHtml(line)}</li>`)
                .join('');

            const card = document.createElement('div');
            card.className = 'call-card';
            card.style.borderLeftColor = '#16a34a';
            card.innerHTML = `
                <div style="flex: 1;">
                    <div class="font-bold">${c.cliente}</div>
                    <div class="text-sm text-secondary">${c.grupo} • ${c.segmento}</div>
                    <div class="text-sm" style="margin-top:4px; color:#166534;">Estado: Pendiente gestión legal</div>
                    <div style="margin-top:10px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px;">
                        <div style="font-size:11px; font-weight:700; color:#475569; margin-bottom:6px; text-transform:uppercase;">Updates</div>
                        ${historyItems ? `<ul style="margin-left:16px;">${historyItems}</ul>` : '<p class="text-sm text-secondary">Sin updates todavía.</p>'}
                    </div>
                    <div style="margin-top:10px; display:flex; gap:8px; align-items:flex-start;">
                        <textarea id="offramp-update-${c.id}" style="flex:1; min-height:56px; padding:8px; border-radius:8px; border:1px solid var(--color-border); font-family:inherit;" placeholder="Escribe update de estado..."></textarea>
                        <button class="btn-primary save-offramp-update-btn" data-id="${c.id}" style="padding:8px 12px;">Guardar update</button>
                    </div>
                </div>
                <div class="badge off-ramp">En seguimiento</div>
            `;
            offRampContainer.appendChild(card);
        });

        document.querySelectorAll('.save-offramp-update-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                await saveOffRampUpdate(parseInt(btn.dataset.id, 10));
            });
        });
    }

    function escapeHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    async function saveOffRampUpdate(clientId) {
        const input = document.getElementById(`offramp-update-${clientId}`);
        if (!input) return;

        const newUpdate = input.value.trim();
        if (!newUpdate) {
            alert('Escribe un update antes de guardar.');
            return;
        }

        const today = new Date().toISOString().split('T')[0];
        const entry = `${today}: ${newUpdate}`;
        const currentLog = offRampQueueData[clientId]?.updates_log || '';
        const mergedLog = currentLog ? `${currentLog}\n${entry}` : entry;
        const currentStatus = offRampQueueData[clientId]?.status || 'pendiente';

        const { error } = await window.supabaseClient
            .from('offramp_queue')
            .upsert(
                [{
                    client_id: clientId,
                    status: currentStatus,
                    updates_log: mergedLog,
                    updated_at: new Date().toISOString()
                }],
                { onConflict: 'client_id' }
            );

        if (error) {
            console.error('Error saving off-ramp update:', error);
            alert('No se pudo guardar el update en Supabase.');
            return;
        }

        offRampQueueData[clientId] = {
            status: currentStatus,
            updates_log: mergedLog,
            updated_at: new Date().toISOString()
        };
        input.value = '';
        renderOffRamp();
    }

    function renderCalls() {
        if (!callsContainer) return;
        callsContainer.innerHTML = '';

        if (callFilter === 'queue') {
            const queue = clients.filter(c => callQueueIds.includes(c.id));
            if (queue.length === 0) {
                callsContainer.innerHTML = '<p class="text-secondary">No hay clientes pendientes por agendar. Añade algunos desde la pestaña de Priorización.</p>';
            } else {
                renderQueueCards(queue);
            }
            return;
        }

        if (callFilter === 'scheduled') {
            const scheduledList = clients.filter(c => scheduledData[c.id]);
            if (scheduledList.length === 0) {
                callsContainer.innerHTML = '<p class="text-secondary">No tienes citas agendadas aún.</p>';
            } else {
                renderScheduledCards(scheduledList);
            }
            return;
        }

        if (callFilter === 'completed') {
            if (completedCalls.length === 0) {
                callsContainer.innerHTML = '<p class="text-secondary">Aún no se han registrado llamadas en esta sesión.</p>';
            } else {
                const completed = clients.filter(c => completedCalls.includes(c.id));
                renderCompletedCards(completed);
            }
            return;
        }
    }

    function renderQueueCards(clientList) {
        clientList.forEach(c => {
            const card = document.createElement('div');
            card.className = 'call-card';
            card.style.borderLeftColor = '#f59e0b'; // Warning/Yellow for queue

            card.innerHTML = `
                <div>
                    <div class="font-bold">${c.cliente}</div>
                    <div class="text-sm text-secondary">${c.grupo} • ${c.segmento}</div>
                    <div class="text-sm" style="margin-top:4px; color:#6e6e73;">Prioridad: ${c.estadoLlamada.estado}</div>
                </div>
                <button class="btn-primary schedule-date-btn" data-id="${c.id}" style="background:#f59e0b;">
                    Agendar Cita
                </button>
            `;
            callsContainer.appendChild(card);
        });
        document.querySelectorAll('.schedule-date-btn').forEach(btn => {
            btn.addEventListener('click', () => openScheduleDateModal(parseInt(btn.dataset.id)));
        });
    }

    function formatTimeAMPM(time24) {
        if (!time24) return 'Sin hora';
        const [hours, minutes] = time24.split(':');
        let h = parseInt(hours);
        const ampm = h >= 12 ? 'PM' : 'AM';
        h = h % 12;
        h = h ? h : 12; // the hour '0' should be '12'
        return `${h}:${minutes} ${ampm}`;
    }

    function renderScheduledCards(clientList) {
        clientList.forEach(c => {
            const data = scheduledData[c.id];
            const card = document.createElement('div');
            card.className = 'call-card';
            card.style.borderLeftColor = '#2563eb';

            // Format date for better readability (Spanish)
            const dateObj = new Date(data.date + 'T00:00:00');
            const options = { weekday: 'long', day: 'numeric', month: 'short' };
            const formattedDate = dateObj.toLocaleDateString('es-ES', options);
            const capitalizedDate = formattedDate.charAt(0).toUpperCase() + formattedDate.slice(1);

            card.innerHTML = `
                <div style="flex:1;">
                    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div class="font-bold" style="font-size:15px;">${c.cliente}</div>
                    </div>
                    <div style="margin-top:6px; display:flex; align-items:center; gap:6px; color:#2563eb; font-weight:600; font-size:13px;">
                        <span>📅 ${capitalizedDate}</span>
                        <span style="opacity:0.6;">•</span>
                        <span>⏰ ${formatTimeAMPM(data.time)}</span>
                    </div>
                    ${data.prep_notes ? `
                        <div style="font-size:12px; color:#6e6e73; background:#f8fafc; padding:10px; border-radius:10px; margin-top:12px; border:1px solid #e2e8f0;">
                            <span style="font-weight:700; color:#475569; display:block; margin-bottom:4px; font-size:10px; text-transform:uppercase;">Notas de preparación</span>
                            ${data.prep_notes}
                        </div>
                    ` : ''}
                </div>
                <div style="display:flex; flex-direction:column; gap:8px; min-width:130px;">
                    <button class="btn-primary registrar-btn" data-id="${c.id}" style="padding:10px; font-size:12px;">Registrar gestión</button>
                    <button class="btn-primary unschedule-btn" data-id="${c.id}" style="padding:8px; font-size:11px; background:#f1f5f9; color:#64748b;">Posponer / Quitar</button>
                </div>
            `;
            callsContainer.appendChild(card);
        });
        document.querySelectorAll('.registrar-btn').forEach(btn => {
            btn.addEventListener('click', () => openCallModal(btn.dataset.id));
        });
        document.querySelectorAll('.unschedule-btn').forEach(btn => {
            btn.addEventListener('click', () => unscheduleClient(parseInt(btn.dataset.id)));
        });
    }

    async function unscheduleClient(id) {
        if (!confirm('¿Estás seguro de que quieres quitar esta cita? (Volverá a estar pendiente por agendar)')) return;
        try {
            const { error } = await window.supabaseClient
                .from('clients')
                .update({
                    scheduled_date: null,
                    scheduled_time: null,
                    prep_notes: null,
                    in_queue: true // V4: Return to queue
                })
                .eq('id', id);

            if (error) throw error;

            delete scheduledData[id];
            callQueueIds.push(id);

            // Update local client object
            const clientIdx = clients.findIndex(c => c.id === id);
            if (clientIdx !== -1) {
                clients[clientIdx].scheduled_date = null;
                clients[clientIdx].scheduled_time = null;
                clients[clientIdx].prep_notes = null;
                clients[clientIdx].in_queue = true;
            }

            renderCalls();
            renderTable();
        } catch (err) {
            console.error('Error unscheduling:', err);
            alert('No se pudo quitar la cita de Supabase.');
        }
    }

    function renderCompletedCards(clientList) {
        clientList.forEach(c => {
            const card = document.createElement('div');
            card.className = 'call-card';
            card.style.borderLeftColor = '#10b981'; // Success/Green

            card.innerHTML = `
                <div>
                    <div class="font-bold">${c.cliente}</div>
                    <div class="text-sm text-secondary">Llamada realizada hoy</div>
                </div>
                <div class="badge off-ramp">Realizada</div>
            `;
            callsContainer.appendChild(card);
        });
    }

    function openScheduleDateModal(id) {
        const client = clients.find(c => c.id === id);
        if (!client) return;

        const modalBody = document.getElementById('modalBody');
        modalBody.innerHTML = `
            <div style="margin-bottom: 24px;">
                <h2 style="color: var(--color-brand-dark); margin-bottom: 4px;">Agendar Cita</h2>
                <p class="text-secondary">Definir fecha y hora para llamar a <strong>${client.cliente}</strong></p>
            </div>

            <div style="margin-bottom: 20px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Fecha de la cita</label>
                <input type="date" id="schedule-date" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);">
            </div>

            <div style="margin-bottom: 24px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Hora (opcional)</label>
                <input type="time" id="schedule-time" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);" value="09:00">
            </div>

            <div style="margin-bottom: 24px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Notas de preparación (V3)</label>
                <textarea id="schedule-prep-notes" style="width:100%; height:60px; padding:10px; border-radius:8px; border:1px solid var(--color-border);" placeholder="Ej: Revisar si ya pagó el seguro..."></textarea>
            </div>

            <div style="display:flex; justify-content: flex-end; gap: 12px;">
                <button class="filter-btn" onclick="document.getElementById('infoModal').style.display = 'none'">Cancelar</button>
                <button class="btn-primary" id="confirm-date-btn">Confirmar y Guardar</button>
            </div>
        `;

        document.getElementById('infoModal').style.display = 'block';

        document.getElementById('confirm-date-btn').onclick = async () => {
            const date = document.getElementById('schedule-date').value;
            const time = document.getElementById('schedule-time').value;
            const prepNotes = document.getElementById('schedule-prep-notes').value;

            if (!date) {
                alert('Por favor selecciona una fecha.');
                return;
            }

            document.getElementById('confirm-date-btn').disabled = true;
            document.getElementById('confirm-date-btn').textContent = 'Guardando...';

            try {
                // Persistent update to Supabase
                const { error } = await window.supabaseClient
                    .from('clients')
                    .update({
                        scheduled_date: date,
                        scheduled_time: time,
                        prep_notes: prepNotes,
                        in_queue: false
                    })
                    .eq('id', id);

                if (error) throw error;

                // Update local state
                callQueueIds = callQueueIds.filter(cid => cid !== id);
                scheduledData[id] = { date, time, prep_notes: prepNotes };

                // Update local client object manually to avoid full refresh if possible, 
                // but re-fetching is safer.
                const clientIdx = clients.findIndex(c => c.id === id);
                if (clientIdx !== -1) {
                    clients[clientIdx].scheduled_date = date;
                    clients[clientIdx].scheduled_time = time;
                    clients[clientIdx].prep_notes = prepNotes;
                    clients[clientIdx].in_queue = false;
                }

                document.getElementById('infoModal').style.display = 'none';
                renderCalls();
                renderTable();
            } catch (err) {
                console.error('Error saving schedule:', err);
                alert('No se pudo guardar el agendamiento en Supabase.');
                document.getElementById('confirm-date-btn').disabled = false;
                document.getElementById('confirm-date-btn').textContent = 'Confirmar y Guardar';
            }
        };
    }

    function openCallModal(id) {
        const client = clients.find(c => c.id == id);
        if (!client) return;

        const strategy = window.strategies ? window.strategies[client.segmento.split(' ')[0]] : 'Estrategia general de contacto';

        const modalBody = document.getElementById('modalBody');
        modalBody.innerHTML = `
            <div style="margin-bottom: 24px;">
                <h2 style="color: var(--color-brand-dark); margin-bottom: 4px;">Registrar Llamada</h2>
                <p class="text-secondary">${client.cliente}</p>
            </div>

            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; background: #f8fafc; padding: 16px; border-radius: 12px; margin-bottom: 24px;">
                <div>
                    <div class="text-sm font-bold">Equity Actual</div>
                    <div style="font-size: 18px;">${client.equity_actual_pct.toFixed(1)}%</div>
                </div>
                <div>
                    <div class="text-sm font-bold">Score Bancario</div>
                    <div style="font-size: 18px; ${client.credit_score_actual < 700 ? 'color: var(--color-danger);' : ''}">${client.credit_score_actual}</div>
                </div>
                <div>
                    <div class="text-sm font-bold">DTI Actual</div>
                    <div style="font-size: 18px;">${client.dti_actual_pct !== null ? client.dti_actual_pct.toFixed(1) + '%' : '-'}</div>
                </div>
                <div>
                    <div class="text-sm font-bold">Saldo en Mora</div>
                    <div style="font-size: 18px; ${client.saldo_en_mora > 0 ? 'color: var(--color-danger); font-weight: 700;' : 'color: var(--color-success);'}">${client.saldo_en_mora > 0 ? '$' + (client.saldo_en_mora / 1000).toFixed(0) + 'K' : '$0'}</div>
                </div>
                <div style="grid-column: span 2;">
                    <div class="text-sm font-bold">Segmento</div>
                    <div style="font-size: 18px; color: var(--color-accent);">${client.segmento.split(' ')[0]}</div>
                </div>
            </div>

            <div style="background: #fff9db; border: 1px solid #f9f0ab; padding: 12px; border-radius: 8px; margin-bottom: 24px;">
                <p class="text-sm font-bold" style="color: #854d0e; margin-bottom: 4px;">🎯 ¿Qué hacer con este cliente?</p>
                <p style="font-size: 13px; color: #854d0e;">${strategy || 'Consultar pestaña de Estrategias para más detalle.'}</p>
            </div>

            <!-- NUEVOS CAMPOS FINANCIEROS -->
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px;">
                <div>
                    <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Ingresos Mensuales ($)</label>
                    <input type="number" id="call-ingresos" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);" placeholder="Ej: 5000000">
                </div>
                <div>
                    <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Cuota Mensual Prom. ($)</label>
                    <input type="number" id="call-cuota" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);" placeholder="Ej: 800000">
                </div>
            </div>

            <div style="margin-bottom: 20px; border-top: 1px solid var(--color-border); padding-top: 16px;">
                <label class="text-sm font-bold" style="display:flex; align-items:center; gap:8px; cursor:pointer;">
                    <input type="checkbox" id="call-rejection" onchange="document.getElementById('rejection-reason-container').style.display = this.checked ? 'block' : 'none'">
                    ⚠️ Cliente rechaza contacto / No responde
                </label>
                <div id="rejection-reason-container" style="display:none; margin-top:12px;">
                    <select id="call-rejection-reason" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);">
                        <option value="">-- Seleccionar motivo --</option>
                        <option value="no contesta">No contesta</option>
                        <option value="telefono no funciona">Teléfono no funciona</option>
                        <option value="no quiere llamada">No quiere llamada / Rechazo</option>
                    </select>
                </div>
            </div>

            <div style="margin-bottom: 20px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Nota resumen de la llamada (Duppla)</label>
                <textarea id="call-note" style="width:100%; height:60px; padding:10px; border-radius:8px; border:1px solid var(--color-border); font-family:inherit;" placeholder="Ej: Se compromete a pagar mora el lunes..."></textarea>
            </div>

            <div style="margin-bottom: 20px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Update Situación Cliente (Master Data)</label>
                <textarea id="call-situacion" style="width:100%; height:40px; padding:10px; border-radius:8px; border:1px solid var(--color-border); font-family:inherit;" placeholder="Ej: Se mudó a Medellín, sigue interesado...">${client.situacion_actual || ''}</textarea>
            </div>

            <div style="margin-bottom: 20px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Feedback del Cliente (V3)</label>
                <textarea id="call-feedback" style="width:100%; height:40px; padding:10px; border-radius:8px; border:1px solid var(--color-border); font-family:inherit;" placeholder="Ej: No entiende el cobro del seguro..."></textarea>
            </div>

            <div style="margin-bottom: 24px;">
                <label class="text-sm font-bold" style="display:block; margin-bottom: 8px;">Adjuntar links (TLDR, Grabaciones)</label>
                <input type="text" id="call-links" style="width:100%; padding:10px; border-radius:8px; border:1px solid var(--color-border);" placeholder="https://link-a-grabacion.com">
            </div>

            <div style="display:flex; justify-content: flex-end; gap: 12px;">
                <button class="filter-btn" onclick="document.getElementById('infoModal').style.display = 'none'">Cancelar</button>
                <button class="btn-primary" id="save-call-btn">Guardar Gestión</button>
            </div>
        `;

        document.getElementById('infoModal').style.display = 'block';

        const saveBtn = document.getElementById('save-call-btn');
        saveBtn.onclick = async () => {
            const note = document.getElementById('call-note').value;
            const feedback = document.getElementById('call-feedback').value;
            const links = document.getElementById('call-links').value;
            const isRejected = document.getElementById('call-rejection').checked;
            const rejectionReason = isRejected ? document.getElementById('call-rejection-reason').value : null;

            if (isRejected && !rejectionReason) {
                alert('Por favor selecciona un motivo de rechazo.');
                return;
            }

            saveBtn.disabled = true;
            saveBtn.textContent = 'Guardando...';

            try {
                const ingresos = document.getElementById('call-ingresos').value;
                const cuota = document.getElementById('call-cuota').value;

                // 1. Insert Call Log
                const { error: callError } = await window.supabaseClient
                    .from('calls')
                    .insert([{
                        client_id: client.id,
                        note: note,
                        customer_feedback: feedback,
                        situacion_update: document.getElementById('call-situacion').value,
                        links: links,
                        ingresos: ingresos ? parseFloat(ingresos) : null,
                        cuota_mensual: cuota ? parseFloat(cuota) : null,
                        date: new Date().toISOString().split('T')[0]
                    }]);

                if (callError) throw callError;

                // 2. Update Client (Persistence & Clearing Schedule)
                const { error: clientError } = await window.supabaseClient
                    .from('clients')
                    .update({
                        ultima_llamada: new Date().toISOString().split('T')[0],
                        situacion_actual: document.getElementById('call-situacion').value,
                        scheduled_date: null,
                        scheduled_time: null,
                        prep_notes: null,
                        rejection_reason: rejectionReason
                    })
                    .eq('id', client.id);

                if (clientError) throw clientError;

                // Move from state
                delete scheduledData[client.id];
                if (!completedCalls.includes(client.id)) {
                    completedCalls.push(client.id);
                }

                alert('Gestión registrada con éxito en Supabase.');

                // 3. Refresh Data
                await fetchClients();

                document.getElementById('infoModal').style.display = 'none';
                updateKPIs();
                renderTable();
                renderCalls();

            } catch (err) {
                console.error('Error al guardar:', err);
                alert('Hubo un error al guardar en la base de datos.');
            } finally {
                saveBtn.disabled = false;
                saveBtn.textContent = 'Guardar Gestión';
            }
        };
    }

    // 5. Event Listeners

    // Info Modal Logic
    const infoContent = {
        offramp: `
                <h2 style="margin-bottom: 16px; color: #28a745;">🟢 Off-Ramp</h2>
            <p style="margin-bottom: 16px; font-size: 14px;"><strong>Criterios actuales</strong> (debe cumplir todos):</p>
            <ul style="line-height: 1.8; margin-left: 20px; font-size: 14px;">
                <li><strong>Equity actual ≥ 25%</strong></li>
                <li><strong>Proyección a 5 años > 30%</strong></li>
                <li><strong>Sin mora</strong> (saldo_en_mora = 0)</li>
                <li><strong>Delta score positivo</strong> (delta_credit_score > 0)</li>
                <li><strong>Score actual > 700</strong></li>
            </ul>
            `,
        construyendo: `
                <h2 style="margin-bottom: 16px; color: #ffa500;">🟡 Construyendo</h2>
            <p style="margin-bottom: 16px; font-size: 14px;"><strong>Estado intermedio:</strong> no cumple Off-Ramp pero tampoco dispara señales de riesgo.</p>
            <ul style="line-height: 1.8; margin-left: 20px; font-size: 14px;">
                <li><strong>Sin mora actual</strong></li>
                <li><strong>Sin alertas críticas</strong> (mora, caída score, DTI alto, etc.)</li>
                <li><strong>Aún en progreso</strong> en equity/score/DTI</li>
            </ul>
            `,
        riesgo: `
                <h2 style="margin-bottom: 16px; color: #ff3b30;">🔴 En riesgo</h2>
            <p style="margin-bottom: 16px; font-size: 14px;"><strong>Atención prioritaria:</strong> aplica desde 12 meses de antigüedad y basta con cumplir una condición.</p>
            <ul style="line-height: 1.8; margin-left: 20px; font-size: 14px;">
                <li><strong>Antigüedad:</strong> ≥ 12 meses</li>
                <li><strong>Mora > 0</strong></li>
                <li><strong>Moratoria promedio > 0</strong></li>
                <li><strong>Caída de score + score bajo:</strong> delta_credit_score < 0 y credit_score_actual < 600</li>
                <li><strong>Proyección a 5 años < 30%</strong></li>
            </ul>
            `,
        listos: `
                <h2 style="margin-bottom: 16px; color: #28a745;">✅ Listos para Banco</h2>
            <p style="margin-bottom: 16px; font-size: 14px;"><strong>Criterios bancarios ESTRICTOS</strong> (100% aprobación):</p>
            <ul style="line-height: 1.8; margin-left: 20px; font-size: 14px;">
                <li><strong>Equity ≥ 30%</strong> (mínimo bancario)</li>
                <li><strong>Score ≥ 700</strong> (perfil crediticio excelente)</li>
                <li><strong>DTI ≤ 10%</strong> (capacidad de pago óptima)</li>
                <li><strong>Sin mora</strong> (saldo_en_mora = 0)</li>
            </ul>
            `,
        avanzados: `
            <h2 style="margin-bottom: 12px; color: #0071e3;">Avanzados</h2>
            <p style="font-size: 14px; margin-bottom: 8px;"><strong>Clientes Top Performer:</strong></p>
            <ul style="line-height: 1.6; margin-left: 20px; font-size: 14px; margin-bottom: 12px;">
                <li>Equity alto / Maduros</li>
                <li>Excelente hábito de pago</li>
                <li>Avance significativo</li>
            </ul>
            <div style="border-top: 1px solid #e5e5ea; padding-top: 12px; margin-top: 12px;">
                <p style="font-size: 12px; color: #6e6e73; margin-bottom: 6px;"><strong>Clasificación Técnica:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Participación Actual ≥ 28%</li>
                    <li>Mora Promedio ≤ 5%</li>
                    <li>Hábito: "A tiempo" o "Anticipado"</li>
                    <li>Antigüedad ≥ 5 meses</li>
                </ul>
                <p style="font-size: 12px; color: #6e6e73; margin-top: 8px; margin-bottom: 6px;"><strong>Campos Usados:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Equity Actual (%)</li>
                    <li>Moratoria Promedio (%)</li>
                    <li>Antigüedad</li>
                </ul>
            </div>
        `,
        estables: `
            <h2 style="margin-bottom: 12px; color: #28a745;">Estables</h2>
            <p style="font-size: 14px; margin-bottom: 8px;"><strong>Progreso Constante:</strong></p>
            <ul style="line-height: 1.6; margin-left: 20px; font-size: 14px; margin-bottom: 12px;">
                <li>Avance parcial en participación</li>
                <li>Puntuales o retraso ligero</li>
            </ul>
            <div style="border-top: 1px solid #e5e5ea; padding-top: 12px; margin-top: 12px;">
                <p style="font-size: 12px; color: #6e6e73; margin-bottom: 6px;"><strong>Clasificación Técnica:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Participación Actual ≥ 20%</li>
                    <li>Mora Promedio ≤ 15%</li>
                    <li>Antigüedad ≥ 3 meses</li>
                </ul>
                <p style="font-size: 12px; color: #6e6e73; margin-top: 8px; margin-bottom: 6px;"><strong>Campos Usados:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Equity Actual (%)</li>
                    <li>Moratoria Promedio (%)</li>
                </ul>
            </div>
        `,
        estancados: `
            <h2 style="margin-bottom: 12px; color: #ff9500;">Estancados</h2>
            <p style="font-size: 14px; margin-bottom: 8px;"><strong>Poco avance, historial rescatable:</strong></p>
            <ul style="line-height: 1.6; margin-left: 20px; font-size: 14px; margin-bottom: 12px;">
                <li>Buen pago de arriendo</li>
                <li>Poca compra de equity</li>
            </ul>
            <div style="border-top: 1px solid #e5e5ea; padding-top: 12px; margin-top: 12px;">
                <p style="font-size: 12px; color: #6e6e73; margin-bottom: 6px;"><strong>Clasificación Técnica:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Comprado ≤ 3%</li>
                    <li>Mora Promedio ≤ 15%</li>
                    <li>Antigüedad ≥ 4 meses</li>
                </ul>
                <p style="font-size: 12px; color: #6e6e73; margin-top: 8px; margin-bottom: 6px;"><strong>Campos Usados:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Participación Adquirida (%)</li>
                    <li>Moratoria Promedio (%)</li>
                </ul>
            </div>
        `,
        inestables: `
            <h2 style="margin-bottom: 12px; color: #ff3b30;">Inestables</h2>
            <p style="font-size: 14px; margin-bottom: 8px;"><strong>Riesgo Operativo:</strong></p>
            <ul style="line-height: 1.6; margin-left: 20px; font-size: 14px; margin-bottom: 12px;">
                <li>Moras recurrentes</li>
                <li>Sin avance en participación</li>
            </ul>
            <div style="border-top: 1px solid #e5e5ea; padding-top: 12px; margin-top: 12px;">
                <p style="font-size: 12px; color: #6e6e73; margin-bottom: 6px;"><strong>Clasificación Técnica:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Mora Promedio ≥ 25%</li>
                    <li>O Comprado ≤ 1%</li>
                </ul>
                <p style="font-size: 12px; color: #6e6e73; margin-top: 8px; margin-bottom: 6px;"><strong>Campos Usados:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Moratoria Promedio (%)</li>
                    <li>Participación Adquirida (%)</li>
                </ul>
            </div>
        `,
        nuevos: `
            <h2 style="margin-bottom: 12px; color: #6e6e73;">Nuevos</h2>
            <p style="font-size: 14px; margin-bottom: 8px;"><strong>Sin trazabilidad suficiente:</strong></p>
            <ul style="line-height: 1.6; margin-left: 20px; font-size: 14px; margin-bottom: 12px;">
                <li>Clientes recién ingresados</li>
                <li>Información incompleta</li>
            </ul>
            <div style="border-top: 1px solid #e5e5ea; padding-top: 12px; margin-top: 12px;">
                <p style="font-size: 12px; color: #6e6e73; margin-bottom: 6px;"><strong>Clasificación Técnica:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Antigüedad ≤ 4 meses</li>
                </ul>
                <p style="font-size: 12px; color: #6e6e73; margin-top: 8px; margin-bottom: 6px;"><strong>Campos Usados:</strong></p>
                <ul style="font-size: 12px; color: #6e6e73; margin-left: 20px; line-height: 1.5;">
                    <li>Antigüedad (meses)</li>
                </ul>
            </div>
        `,
        capa2: `
                <h2 style="margin-bottom: 16px;">📋 Segmentación Valeria</h2>
            <p style="margin-bottom: 16px; font-size: 14px;">Define el <strong>tipo de llamada</strong>:</p>
            <ul style="line-height: 1.8; margin-left: 20px; font-size: 14px;">
                <li><strong>Avanzados:</strong> Equity alto, buen pago.</li>
                <li><strong>Estables:</strong> Progreso constante.</li>
                <li><strong>Estancados:</strong> Poca compra, pero pagan bien.</li>
                <li><strong>Inestables:</strong> Mora alta o equity muy bajo.</li>
            </ul>
            `
    };

    document.querySelectorAll('.info-icon').forEach(icon => {
        icon.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent filter click
            const tipo = e.target.dataset.info;
            document.getElementById('modalBody').innerHTML = infoContent[tipo] || 'Información no disponible';
            document.getElementById('infoModal').style.display = 'block';
        });
    });

    document.querySelector('.close').onclick = () => document.getElementById('infoModal').style.display = 'none';
    window.onclick = (e) => { if (e.target.id === 'infoModal') e.target.style.display = 'none'; };

    subTabBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            const value = e.target.dataset.subtab;
            subTabBtns.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');

            callFilter = value;
            renderCalls();
        });
    });

    filterBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            const type = e.target.dataset.type; // 'group' or 'segment'
            const value = e.target.dataset.filter;

            // 1. Update UI: Remove active only from siblings in the same group
            const siblings = e.target.parentElement.querySelectorAll('.filter-btn');
            siblings.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');

            // 2. Update State
            if (type) {
                filters[type] = value;
            } else {
                // Fallback for old buttons if any (though we replaced HTML)
                // assumes group if no type
                filters.group = value;
            }

            renderTable();
            updateActiveFilterHint();
        });
    });

    if (clientSearchInput) {
        clientSearchInput.addEventListener('input', (e) => {
            filters.search = e.target.value || '';
            renderTable();
        });
    }

    if (toggleFiltersBtn && filtersContent) {
        toggleFiltersBtn.addEventListener('click', () => {
            const collapsed = filtersContent.classList.toggle('is-collapsed');
            toggleFiltersBtn.textContent = collapsed ? 'Mostrar filtros' : 'Ocultar filtros';
        });
    }

    function switchToTab(tab) {
        tabBtns.forEach(b => b.classList.remove('active'));
        const activeBtn = document.querySelector(`.tab-btn[data-tab="${tab}"]`);
        if (activeBtn) activeBtn.classList.add('active');

        views.forEach(v => v.classList.remove('active'));
        const view = document.getElementById(`view-${tab}`);
        if (view) {
            view.classList.add('active');
        } else {
            console.error('View not found for tab:', tab);
        }

        // Re-render relevant data if needed
        if (tab === 'priorizacion') renderTable();
        if (tab === 'llamadas') renderCalls();
        if (tab === 'offramp') renderOffRamp();
    }

    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            console.log('Switching to tab:', tab);
            switchToTab(tab);
        });
    });

    // Init
    clients = await fetchClients();
    updateKPIs();
    renderTable();
    renderCalls();
    renderOffRamp();
    updateActiveFilterHint();
});
