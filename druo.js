document.addEventListener('DOMContentLoaded', async () => {
    console.log('--- Initializing DRUO Connection Status ---');

    const tableBody = document.getElementById('druo-tbody');
    const filterStatus = document.getElementById('druo-filter-status');
    const filterPortafolio = document.getElementById('druo-filter-portafolio');
    const searchInput = document.getElementById('druo-search-input');

    // KPIs
    const kpiTotal = document.getElementById('druo-kpi-total');
    const kpiFailed = document.getElementById('druo-kpi-failed');
    const kpiNull = document.getElementById('druo-kpi-null');

    let druoData = [];

    async function fetchDruoData() {
        console.log('Fetching DRUO data from Supabase...');
        const { data, error } = await window.supabaseClient
            .from('druo_no_conectados')
            .select('*');

        if (error) {
            console.error('Error fetching DRUO data:', error);
            return [];
        }
        return data || [];
    }

    function updateKPIs(data) {
        const total = data.length;
        const failed = data.filter(d => d.druo_status === 'CONNECTION_FAILED').length;
        // Tratamos como null tanto el valor explícito null como el string "null"
        const nulls = data.filter(d => !d.druo_status || d.druo_status === 'null').length;

        if (kpiTotal) kpiTotal.textContent = total;
        if (kpiFailed) kpiFailed.textContent = failed;
        if (kpiNull) kpiNull.textContent = nulls;
    }

    function populatePortafolioFilter(data) {
        const portafolios = [...new Set(data.map(d => d.portafolio).filter(Boolean))];
        portafolios.sort();

        if (!filterPortafolio) return;
        filterPortafolio.innerHTML = '<option value="all">Todos</option>';
        portafolios.forEach(p => {
            const option = document.createElement('option');
            option.value = p;
            option.textContent = p;
            filterPortafolio.appendChild(option);
        });
    }

    function renderTable() {
        if (!tableBody) return;
        tableBody.innerHTML = '';

        const fStatus = filterStatus ? filterStatus.value : 'all';
        const fPortafolio = filterPortafolio ? filterPortafolio.value : 'all';
        const searchTxt = searchInput ? searchInput.value.toLowerCase().trim() : '';

        const filtered = druoData.filter(d => {
            // Evaluamos estado (null string, falsey o null real)
            const dStatus = d.druo_status || 'null';

            let matchStatus = true;
            if (fStatus === 'null') {
                matchStatus = dStatus === 'null';
            } else if (fStatus === 'CONNECTION_FAILED') {
                matchStatus = dStatus === 'CONNECTION_FAILED';
            }

            // Portafolio
            let matchPortafolio = true;
            if (fPortafolio !== 'all') {
                matchPortafolio = d.portafolio === fPortafolio;
            }

            // Búsqueda (código inmueble o nombre oportunidad)
            let matchSearch = true;
            if (searchTxt) {
                const cod = (d.codigo_inmueble || '').toLowerCase();
                const nom = (d.nombre_oportunidad || '').toLowerCase();
                matchSearch = cod.includes(searchTxt) || nom.includes(searchTxt);
            }

            return matchStatus && matchPortafolio && matchSearch;
        });

        if (filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 20px;">No se encontraron registros activos bajo estos filtros.</td></tr>';
            return;
        }

        filtered.forEach(d => {
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';

            // Hover styling via JS para la fila (el dashboard ya tiene estilos hover en .table-container tbody tr)
            // No need to inject since styles.css affects to #clients-table. We gave #druo-table for our id. Wait! 
            // the main table uses `table { width: 100% ... tbody tr:hover }`. So it inherits automatically.

            const isFailed = d.druo_status === 'CONNECTION_FAILED';
            const badgeBg = isFailed ? '#fee2e2' : '#f1f5f9';
            const badgeColor = isFailed ? '#b91c1c' : '#475569';
            const statusText = d.druo_status || 'null';

            // Custom badge design matching constraints
            const badgeHtml = `<span style="display:inline-block; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 700; background: ${badgeBg}; color: ${badgeColor}; border: 1px solid ${isFailed ? '#fca5a5' : '#cbd5e1'};">${statusText}</span>`;

            row.innerHTML = `
                <td><strong>${d.codigo_inmueble || '-'}</strong></td>
                <td>${d.nombre_oportunidad || '-'}</td>
                <td>${d.portafolio || '-'}</td>
                <td style="color:#6e6e73;">${d.fecha_entrega ? new Date(d.fecha_entrega).toLocaleDateString() : '-'}</td>
                <td>${badgeHtml}</td>
            `;

            row.addEventListener('click', () => {
                showRemarksModal(d, badgeHtml);
            });

            tableBody.appendChild(row);
        });
    }

    function showRemarksModal(data, badgeHtml) {
        const modalBody = document.getElementById('modalBody');
        const infoModal = document.getElementById('infoModal');
        if (!modalBody || !infoModal) return;

        modalBody.innerHTML = `
            <div style="margin-bottom: 24px;">
                <h2 style="color: var(--color-brand-dark); margin-bottom: 12px; font-size: 20px;">ℹ️ Detalle de Inmueble DRUO</h2>
                
                <div style="display: flex; gap: 12px; margin-bottom: 24px; align-items: center; border-bottom: 1px solid #e2e8f0; padding-bottom: 16px;">
                    <span style="font-size: 16px; font-weight: bold; color: #334155;">${data.codigo_inmueble || 'Sin código'}</span>
                    ${badgeHtml}
                </div>
                
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px;">
                    <div style="background: #f8fafc; padding: 16px; border-radius: 8px; border: 1px solid #f1f5f9;">
                        <div style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;">Oportunidad</div>
                        <div style="font-size: 14px; color: #334155;">${data.nombre_oportunidad || '-'}</div>
                    </div>

                    <div style="background: #f8fafc; padding: 16px; border-radius: 8px; border: 1px solid #f1f5f9;">
                        <div style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;">Portafolio</div>
                        <div style="font-size: 14px; color: #334155;">${data.portafolio || '-'}</div>
                    </div>
                </div>

                <div style="background: #fffbeb; padding: 16px; border-radius: 8px; border: 1px dashed #fcd34d; margin-bottom: 16px;">
                    <div style="font-size: 11px; font-weight: 700; color: #d97706; text-transform: uppercase; margin-bottom: 8px;">Remarks / Razón del Fallo</div>
                    <div style="font-size: 14px; color: #92400e; white-space: pre-wrap; line-height: 1.5;">${data.remarks || 'No hay remarks registrados para este inmueble.'}</div>
                </div>
            </div>
        `;

        infoModal.style.display = 'block';
    }

    const closeBtn = document.getElementById('modal-close');
    if (closeBtn) {
        closeBtn.addEventListener('click', () => {
            const infoModal = document.getElementById('infoModal');
            if (infoModal) infoModal.style.display = 'none';
        });
    }

    // Modal behavior for clicking outside content
    window.addEventListener('click', (event) => {
        const infoModal = document.getElementById('infoModal');
        if (event.target === infoModal) {
            infoModal.style.display = 'none';
        }
    });

    // Events for filters
    if (filterStatus) filterStatus.addEventListener('change', renderTable);
    if (filterPortafolio) filterPortafolio.addEventListener('change', renderTable);
    if (searchInput) searchInput.addEventListener('input', renderTable);

    // Fetch and populate data on load
    druoData = await fetchDruoData();
    populatePortafolioFilter(druoData);
    updateKPIs(druoData);
    renderTable();
});
