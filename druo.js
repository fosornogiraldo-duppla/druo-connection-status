document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- Initializing DRUO Connection Status ---');

    const tableBody = document.getElementById('druo-tbody');
    const filterStatus = document.getElementById('druo-filter-status');
    const portafolioContainer = document.getElementById('portafolio-chips');
    const searchInput = document.getElementById('druo-search-input');

    // KPIs
    const kpiTotal = document.getElementById('druo-kpi-total');
    const kpiFailed = document.getElementById('druo-kpi-failed');
    const kpiNull = document.getElementById('druo-kpi-null');

    let druoData = [];
    let selectedPortafolios = new Set(); // Set of selected portafolio names

    // ---- URL Params persistence ----
    function getURLParams() {
        const params = new URLSearchParams(window.location.search);
        return {
            status: params.get('status') || 'all',
            portafolios: params.get('portafolios') ? params.get('portafolios').split(',') : [],
            search: params.get('search') || ''
        };
    }

    function saveURLParams() {
        const params = new URLSearchParams();
        const status = filterStatus ? filterStatus.value : 'all';
        if (status !== 'all') params.set('status', status);
        if (selectedPortafolios.size > 0) params.set('portafolios', [...selectedPortafolios].join(','));
        const search = searchInput ? searchInput.value.trim() : '';
        if (search) params.set('search', search);

        const newURL = `${window.location.pathname}${params.toString() ? '?' + params.toString() : ''}`;
        window.history.replaceState({}, '', newURL);
    }

    // ---- Supabase Fetch ----
    async function fetchDruoData() {
        console.log('Fetching DRUO data from Supabase...');
        const { data, error } = await window.supabaseClient
            .from('druo_no_conectados')
            .select('*');

        if (error) {
            console.error('Error fetching DRUO data:', error);
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding:40px; color:#b91c1c;">Error al cargar datos de Supabase.</td></tr>';
            return [];
        }
        return data || [];
    }

    // ---- KPIs ----
    function updateKPIs(data) {
        const total = data.length;
        const failed = data.filter(d => d.druo_status === 'CONNECTION_FAILED').length;
        const nulls = data.filter(d => !d.druo_status).length;

        if (kpiTotal) kpiTotal.textContent = total;
        if (kpiFailed) kpiFailed.textContent = failed;
        if (kpiNull) kpiNull.textContent = nulls;
    }

    // ---- Portafolio chips multi-select ----
    function buildPortafolioChips(data) {
        if (!portafolioContainer) return;
        const portafolios = [...new Set(data.map(d => d.portafolio).filter(Boolean))].sort();

        portafolioContainer.innerHTML = '';

        portafolios.forEach(p => {
            const chip = document.createElement('button');
            chip.type = 'button';
            chip.textContent = p;
            chip.setAttribute('data-portafolio', p);
            chip.className = 'portafolio-chip filter-btn';
            if (selectedPortafolios.has(p)) chip.classList.add('active');

            chip.addEventListener('click', () => {
                if (selectedPortafolios.has(p)) {
                    selectedPortafolios.delete(p);
                    chip.classList.remove('active');
                } else {
                    selectedPortafolios.add(p);
                    chip.classList.add('active');
                }
                saveURLParams();
                renderTable();
                updateFilterCount();
            });

            portafolioContainer.appendChild(chip);
        });

        updateFilterCount();
    }

    function updateFilterCount() {
        const countEl = document.getElementById('portafolio-count');
        if (!countEl) return;
        if (selectedPortafolios.size === 0) {
            countEl.textContent = 'Todos';
            countEl.style.background = '#e2e8f0';
            countEl.style.color = '#475569';
        } else {
            countEl.textContent = `${selectedPortafolios.size} seleccionados`;
            countEl.style.background = 'var(--color-brand-dark)';
            countEl.style.color = 'white';
        }
    }

    // ---- Render Table ----
    function renderTable() {
        if (!tableBody) return;
        tableBody.innerHTML = '';

        const fStatus = filterStatus ? filterStatus.value : 'all';
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';

        const filtered = druoData.filter(d => {
            // Status filter
            const dStatus = d.druo_status || null;
            let matchStatus = true;
            if (fStatus === 'null') matchStatus = !dStatus;
            else if (fStatus === 'CONNECTION_FAILED') matchStatus = dStatus === 'CONNECTION_FAILED';

            // Multi portafolio filter
            let matchPortafolio = true;
            if (selectedPortafolios.size > 0) {
                matchPortafolio = selectedPortafolios.has(d.portafolio);
            }

            // Search
            let matchSearch = true;
            if (searchTxt) {
                const cod = (d.codigo_inmueble || '').toLowerCase();
                const nom = (d.nombre_oportunidad || '').toLowerCase();
                matchSearch = cod.includes(searchTxt) || nom.includes(searchTxt);
            }

            return matchStatus && matchPortafolio && matchSearch;
        });

        // Update result count
        const resultCount = document.getElementById('result-count');
        if (resultCount) resultCount.textContent = `${filtered.length} resultado${filtered.length !== 1 ? 's' : ''}`;

        if (filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 40px; color:#94a3b8;">No se encontraron registros con los filtros actuales.</td></tr>';
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

            const badgeHtml = `<span style="display:inline-block; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 700; background: ${badgeBg}; color: ${badgeColor}; border: 1px solid ${badgeBorder};">${statusText}</span>`;

            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td><span style="font-size:12px; color:#64748b; background:#f1f5f9; padding:2px 8px; border-radius:4px;">${d.portafolio || '-'}</span></td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString('es-CO') : '-'}</td>
                <td>${badgeHtml}</td>
            `;

            row.addEventListener('click', () => showRemarksModal(d, badgeHtml));
            tableBody.appendChild(row);
        });
    }

    // ---- Modal ----
    function showRemarksModal(data, badgeHtml) {
        const modalBody = document.getElementById('modalBody');
        const infoModal = document.getElementById('infoModal');
        if (!modalBody || !infoModal) return;

        modalBody.innerHTML = `
            <div>
                <h2 style="color: var(--color-brand-dark); margin-bottom: 12px; font-size: 20px;">ℹ️ Detalle de Inmueble DRUO</h2>

                <div style="display: flex; gap: 12px; margin-bottom: 24px; align-items: center; border-bottom: 1px solid #e2e8f0; padding-bottom: 16px;">
                    <span style="font-size: 16px; font-weight: bold; color: #334155;">${data.codigo_inmueble || 'Sin código'}</span>
                    ${badgeHtml}
                </div>

                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px;">
                    <div style="background: #f8fafc; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;">Oportunidad</div>
                        <div style="font-size: 14px; color: #334155;">${data.nombre_oportunidad || '-'}</div>
                    </div>
                    <div style="background: #f8fafc; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;">Portafolio</div>
                        <div style="font-size: 14px; color: #334155;">${data.portafolio || '-'}</div>
                    </div>
                    <div style="background: #f8fafc; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;">Fecha de Entrega</div>
                        <div style="font-size: 14px; color: #334155;">${data.fecha_entrega ? new Date(data.fecha_entrega).toLocaleDateString('es-CO') : '-'}</div>
                    </div>
                </div>

                <div style="background: #fffbeb; padding: 16px; border-radius: 8px; border: 1px dashed #fcd34d;">
                    <div style="font-size: 11px; font-weight: 700; color: #d97706; text-transform: uppercase; margin-bottom: 8px;">💬 Remarks / Razón del Fallo</div>
                    <div style="font-size: 14px; color: #92400e; white-space: pre-wrap; line-height: 1.6;">${data.remarks || 'No hay remarks registrados para este inmueble.'}</div>
                </div>
            </div>
        `;

        infoModal.style.display = 'flex';
    }

    // ---- Modal close ----
    const closeBtn = document.getElementById('modal-close');
    if (closeBtn) {
        closeBtn.addEventListener('click', () => {
            const infoModal = document.getElementById('infoModal');
            if (infoModal) infoModal.style.display = 'none';
        });
    }
    window.addEventListener('click', (event) => {
        const infoModal = document.getElementById('infoModal');
        if (event.target === infoModal) infoModal.style.display = 'none';
    });

    // ---- Filter events ----
    if (filterStatus) {
        filterStatus.addEventListener('change', () => {
            saveURLParams();
            renderTable();
        });
    }
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            saveURLParams();
            renderTable();
        });
    }

    // ---- "Clear portafolios" button ----
    const clearPortafolios = document.getElementById('clear-portafolios');
    if (clearPortafolios) {
        clearPortafolios.addEventListener('click', () => {
            selectedPortafolios.clear();
            document.querySelectorAll('.portafolio-chip').forEach(c => c.classList.remove('active'));
            saveURLParams();
            renderTable();
            updateFilterCount();
        });
    }

    // ---- Init: restore from URL ----
    const urlParams = getURLParams();
    if (filterStatus) filterStatus.value = urlParams.status;
    if (searchInput) searchInput.value = urlParams.search;
    if (urlParams.portafolios.length > 0) {
        urlParams.portafolios.forEach(p => selectedPortafolios.add(p));
    }

    // ---- Fetch & render ----
    druoData = await fetchDruoData();
    updateKPIs(druoData);
    buildPortafolioChips(druoData);
    renderTable();
});
