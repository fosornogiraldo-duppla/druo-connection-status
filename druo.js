document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- DRUO Connection Status v2 ---');

    // ---- DOM refs ----
    const tableBody = document.getElementById('druo-tbody');
    const descartadosBody = document.getElementById('druo-descartados-tbody');
    const filterStatus = document.getElementById('druo-filter-status');
    const portafolioChips = document.getElementById('portafolio-chips');
    const searchInput = document.getElementById('druo-search-input');

    const kpiTotal = document.getElementById('druo-kpi-total');
    const kpiFailed = document.getElementById('druo-kpi-failed');
    const kpiNull = document.getElementById('druo-kpi-null');
    const kpiDescartados = document.getElementById('druo-kpi-descartados');

    let druoData = [];   // all from druo_no_conectados
    let descartados = [];   // all from druo_descartados
    let descartadosCodes = new Set(); // Set of codigo_inmueble that are discarded
    let selectedPortafolios = new Set();
    let pendingDiscardRow = null; // row object to be discarded

    // ----------------------------------------------------------------
    // URL Params persistence
    // ----------------------------------------------------------------
    function getURLParams() {
        const p = new URLSearchParams(window.location.search);
        return {
            status: p.get('status') || 'all',
            portafolios: p.get('portafolios') ? p.get('portafolios').split(',') : [],
            search: p.get('search') || '',
            view: p.get('view') || 'pendientes'
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
        const { data, error } = await window.supabaseClient
            .from('druo_no_conectados').select('*');
        if (error) { console.error(error); return []; }
        return data || [];
    }

    async function fetchDescartados() {
        const { data, error } = await window.supabaseClient
            .from('druo_descartados').select('*').order('descartado_at', { ascending: false });
        if (error) { console.error(error); return []; }
        return data || [];
    }

    async function saveDescarte(row, razon) {
        const { error } = await window.supabaseClient
            .from('druo_descartados')
            .upsert([{
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
        const { error } = await window.supabaseClient
            .from('druo_descartados')
            .delete()
            .eq('codigo_inmueble', codigo_inmueble);
        return error;
    }

    // ----------------------------------------------------------------
    // KPIs
    // ----------------------------------------------------------------
    function updateKPIs() {
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const failed = activos.filter(d => d.druo_status === 'CONNECTION_FAILED').length;
        const nulls = activos.filter(d => !d.druo_status).length;

        if (kpiTotal) kpiTotal.textContent = activos.length;
        if (kpiFailed) kpiFailed.textContent = failed;
        if (kpiNull) kpiNull.textContent = nulls;
        if (kpiDescartados) kpiDescartados.textContent = descartados.length;
    }

    // ----------------------------------------------------------------
    // Portafolio chips
    // ----------------------------------------------------------------
    function buildPortafolioChips() {
        if (!portafolioChips) return;
        const activos = druoData.filter(d => !descartadosCodes.has(d.codigo_inmueble));
        const ports = [...new Set(activos.map(d => d.portafolio).filter(Boolean))].sort();

        portafolioChips.innerHTML = '';
        ports.forEach(p => {
            const chip = document.createElement('button');
            chip.type = 'button';
            chip.textContent = p;
            chip.className = 'portafolio-chip';
            if (selectedPortafolios.has(p)) chip.classList.add('active');
            chip.addEventListener('click', () => {
                selectedPortafolios.has(p) ? selectedPortafolios.delete(p) : selectedPortafolios.add(p);
                chip.classList.toggle('active');
                saveURLParams();
                renderTable();
                updateChipCount();
            });
            portafolioChips.appendChild(chip);
        });
        updateChipCount();
    }

    function updateChipCount() {
        const el = document.getElementById('portafolio-count');
        if (!el) return;
        if (selectedPortafolios.size === 0) {
            el.textContent = 'Todos';
            el.style.background = '#e2e8f0';
            el.style.color = '#475569';
        } else {
            el.textContent = `${selectedPortafolios.size} seleccionados`;
            el.style.background = 'var(--color-brand-dark)';
            el.style.color = 'white';
        }
    }

    // ----------------------------------------------------------------
    // Render: Pending table
    // ----------------------------------------------------------------
    function renderTable() {
        if (!tableBody) return;
        const fStatus = filterStatus ? filterStatus.value : 'all';
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';

        const filtered = druoData
            .filter(d => !descartadosCodes.has(d.codigo_inmueble)) // exclude discarded
            .filter(d => {
                const dStatus = d.druo_status || null;
                let ok = true;
                if (fStatus === 'null') ok = !dStatus;
                if (fStatus === 'CONNECTION_FAILED') ok = dStatus === 'CONNECTION_FAILED';
                return ok;
            })
            .filter(d => {
                if (selectedPortafolios.size === 0) return true;
                return selectedPortafolios.has(d.portafolio);
            })
            .filter(d => {
                if (!searchTxt) return true;
                return (d.codigo_inmueble || '').toLowerCase().includes(searchTxt)
                    || (d.nombre_oportunidad || '').toLowerCase().includes(searchTxt);
            });

        const rc = document.getElementById('result-count');
        if (rc) rc.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        tableBody.innerHTML = '';
        if (filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;color:#94a3b8;">No hay registros con los filtros actuales.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';

            const isFailed = d.druo_status === 'CONNECTION_FAILED';
            const badgeBg = isFailed ? '#fee2e2' : '#f1f5f9';
            const badgeColor = isFailed ? '#b91c1c' : '#475569';
            const badgeBorder = isFailed ? '#fca5a5' : '#cbd5e1';
            const statusText = d.druo_status || 'null';
            const badgeHtml = `<span style="display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:700;background:${badgeBg};color:${badgeColor};border:1px solid ${badgeBorder};">${statusText}</span>`;

            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td style="color:#334155;">${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:12px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td>${badgeHtml}</td>
                <td>
                    <button class="btn-discard" data-code="${d.codigo_inmueble}">Descartar</button>
                </td>
            `;

            // Click row → detail modal (but not on the button)
            row.addEventListener('click', (e) => {
                if (e.target.closest('.btn-discard')) return;
                showDetailModal(d, badgeHtml);
            });

            // Discard button
            row.querySelector('.btn-discard').addEventListener('click', (e) => {
                e.stopPropagation();
                openDiscardModal(d);
            });

            tableBody.appendChild(row);
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
            const row = document.createElement('tr');
            const isFailed = d.druo_status === 'CONNECTION_FAILED';
            const badgeBg = isFailed ? '#fee2e2' : '#f1f5f9';
            const badgeColor = isFailed ? '#b91c1c' : '#475569';
            const statusText = d.druo_status || 'null';

            const date = d.descartado_at
                ? new Date(d.descartado_at).toLocaleDateString('es-CO', { day: '2-digit', month: 'short', year: 'numeric' })
                : '-';

            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td style="color:#334155;">${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:12px;color:#64748b;background:#f1f5f9;padding:2px 8px;border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td><span style="display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:700;background:${badgeBg};color:${badgeColor};">${statusText}</span></td>
                <td style="color:#475569;font-size:13px;max-width:220px;">${d.razon_descarte || '-'}</td>
                <td style="color:#94a3b8;font-size:12px;">${date}</td>
                <td>
                    <button class="btn-restore" data-code="${d.codigo_inmueble}">Restaurar</button>
                </td>
            `;

            row.querySelector('.btn-restore').addEventListener('click', async (e) => {
                e.stopPropagation();
                await restoreDescarte(d.codigo_inmueble);
            });

            descartadosBody.appendChild(row);
        });
    }

    // ----------------------------------------------------------------
    // Modals
    // ----------------------------------------------------------------
    function showDetailModal(d, badgeHtml) {
        const body = document.getElementById('detail-modal-body');
        const modal = document.getElementById('detail-modal');
        if (!body || !modal) return;

        body.innerHTML = `
            <h2 style="color:var(--color-brand-dark);margin:0 0 16px 0;font-size:20px;">ℹ️ Detalle del Inmueble</h2>
            <div style="display:flex;gap:10px;align-items:center;border-bottom:1px solid #e2e8f0;padding-bottom:16px;margin-bottom:20px;">
                <strong style="font-size:15px;color:#1e293b;">${d.codigo_inmueble || 'Sin código'}</strong>
                ${badgeHtml}
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:16px;">
                <div style="background:#f8fafc;padding:14px;border-radius:10px;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:4px;">Oportunidad</div>
                    <div style="font-size:14px;color:#334155;">${d.nombre_oportunidad || '-'}</div>
                </div>
                <div style="background:#f8fafc;padding:14px;border-radius:10px;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:4px;">Portafolio</div>
                    <div style="font-size:14px;color:#334155;">${d.portafolio || '-'}</div>
                </div>
                <div style="background:#f8fafc;padding:14px;border-radius:10px;">
                    <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:4px;">Fecha Entrega</div>
                    <div style="font-size:14px;color:#334155;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</div>
                </div>
            </div>
            <div style="background:#fffbeb;padding:16px;border-radius:10px;border:1px dashed #fcd34d;">
                <div style="font-size:10px;font-weight:700;color:#d97706;text-transform:uppercase;margin-bottom:8px;">💬 Remarks / Razón del Fallo</div>
                <div style="font-size:14px;color:#92400e;white-space:pre-wrap;line-height:1.6;">${d.remarks || 'No hay remarks registrados.'}</div>
            </div>
        `;
        modal.style.display = 'flex';
    }

    function openDiscardModal(row) {
        pendingDiscardRow = row;
        const subtitle = document.getElementById('discard-modal-subtitle');
        if (subtitle) subtitle.textContent = `${row.codigo_inmueble} — ${row.nombre_oportunidad || ''}`;
        const textarea = document.getElementById('discard-reason');
        if (textarea) textarea.value = '';
        document.getElementById('discard-modal').style.display = 'flex';
        setTimeout(() => textarea && textarea.focus(), 100);
    }

    // Confirm discard
    document.getElementById('confirm-discard-btn').addEventListener('click', async () => {
        const razon = document.getElementById('discard-reason').value.trim();
        if (!razon) {
            document.getElementById('discard-reason').style.borderColor = '#ef4444';
            document.getElementById('discard-reason').placeholder = '⚠️  La razón es obligatoria...';
            return;
        }
        if (!pendingDiscardRow) return;

        const btn = document.getElementById('confirm-discard-btn');
        btn.disabled = true;
        btn.textContent = 'Guardando...';

        const error = await saveDescarte(pendingDiscardRow, razon);
        if (error) {
            console.error('Error saving descarte:', error);
            alert('Error al guardar en Supabase. Intenta de nuevo.');
            btn.disabled = false;
            btn.textContent = 'Descartar';
            return;
        }

        // Update local state
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

        document.getElementById('discard-modal').style.display = 'none';
        btn.disabled = false;
        btn.textContent = 'Descartar';

        updateKPIs();
        renderTable();
        renderDescartados();
    });

    // ----------------------------------------------------------------
    // Restore
    // ----------------------------------------------------------------
    async function restoreDescarte(codigo_inmueble) {
        const error = await deleteDescarte(codigo_inmueble);
        if (error) {
            console.error('Error restoring:', error);
            alert('Error al restaurar. Intenta de nuevo.');
            return;
        }
        descartados = descartados.filter(d => d.codigo_inmueble !== codigo_inmueble);
        descartadosCodes.delete(codigo_inmueble);

        updateKPIs();
        renderTable();
        renderDescartados();
        buildPortafolioChips();
    }

    // ----------------------------------------------------------------
    // Events
    // ----------------------------------------------------------------
    if (filterStatus) filterStatus.addEventListener('change', () => { saveURLParams(); renderTable(); });
    if (searchInput) searchInput.addEventListener('input', () => { saveURLParams(); renderTable(); });

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

    [druoData, descartados] = await Promise.all([fetchDruoData(), fetchDescartados()]);
    descartadosCodes = new Set(descartados.map(d => d.codigo_inmueble));

    updateKPIs();
    buildPortafolioChips();
    renderTable();
    renderDescartados();
});
