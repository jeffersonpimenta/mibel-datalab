/* ==========================================================================
   MIBEL Bid Analyzer - Core JavaScript
   ========================================================================== */

// ============================================================================
// API Helpers
// ============================================================================

async function apiGet(url) {
    const response = await fetch(url);
    return response.json();
}

async function apiPost(url, data) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return response.json();
}

async function apiPut(url, data) {
    const response = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return response.json();
}

async function apiDelete(url) {
    const response = await fetch(url, {
        method: 'DELETE'
    });
    return response.json();
}

// ============================================================================
// Toast Notifications
// ============================================================================

function toast(msg, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toastEl = document.createElement('div');
    toastEl.className = `toast toast-${type}`;
    toastEl.textContent = msg;

    container.appendChild(toastEl);

    setTimeout(() => {
        toastEl.style.animation = 'slideIn 0.2s ease reverse';
        setTimeout(() => toastEl.remove(), 200);
    }, 3000);
}

// ============================================================================
// Tab Navigation
// ============================================================================

function switchTab(tabId) {
    // Update nav buttons
    document.querySelectorAll('nav button').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabId);
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabId}`);
    });

    // Trigger tab load event
    const event = new CustomEvent('tabLoad', { detail: { tabId } });
    document.dispatchEvent(event);
}

// ============================================================================
// Health Check
// ============================================================================

async function checkHealth() {
    try {
        const data = await apiGet('/api/health');
        const dot = document.getElementById('status-dot');
        const text = document.getElementById('status-text');

        if (dot && text) {
            dot.className = 'status-dot ' + (data.clickhouse ? 'online' : 'offline');
            text.textContent = data.clickhouse ? 'ClickHouse Online' : 'ClickHouse Offline';
        }
    } catch (e) {
        console.error('Health check failed:', e);
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function downloadJson(data, filename) {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// ============================================================================
// Constants
// ============================================================================

const REGIMES = ['PRE', 'PRO', 'CONSUMO', 'COMERCIALIZADOR', 'GENERICA', 'PORFOLIO'];

const CATEGORIAS = {
    PRE: ['SOLAR_FOT', 'SOLAR_TER', 'EOLICA', 'EOLICA_MARINA', 'HIDRICA',
          'TERMICA_RENOV', 'TERMICA_NREN', 'GEOTERMICA', 'HIBRIDA_RENOV',
          'RE_TARIFA_CUR', 'RE_OUTRO', 'ARMAZENAMENTO_VENDA'],
    PRO: ['CICLO_COMBINADO', 'NUCLEAR', 'HIDRICA_PRO', 'BOMBEO_PURO_PRO', 'CARVAO', 'GAS'],
    CONSUMO: ['BOMBEO_CONSUMO', 'ARMAZENAMENTO_COMPRA', 'CONS_DIRECTO',
              'CONS_AUXILIARES', 'CONS_PRODUTOR'],
    COMERCIALIZADOR: ['COMERC', 'COMERC_NR', 'COMERC_EXT', 'COMERC_ULT_REC'],
    GENERICA: ['GENERICA', 'GENERICA_VENDA'],
    PORFOLIO: ['PORTF_PROD', 'PORTF_COMERC']
};

// ============================================================================
// Classificacao Tab (Tab 1)
// ============================================================================

const ClassificacaoTab = {
    data: [],
    excecoes: [],
    editingIndex: null,
    filterText: '',
    filterRegime: '',

    async init() {
        await this.loadData();
        this.render();
        this.bindEvents();
    },

    async loadData() {
        try {
            const [classData, excData] = await Promise.all([
                apiGet('/api/classificacao'),
                apiGet('/api/excecoes')
            ]);
            this.data = classData;
            this.excecoes = excData;
            this.updateCounters();
        } catch (e) {
            toast('Erro ao carregar dados: ' + e.message, 'error');
        }
    },

    updateCounters() {
        const counter = document.getElementById('classificacao-counter');
        if (counter) {
            counter.textContent = `${this.data.length} tecnologias mapeadas · ${this.excecoes.length} excepções`;
        }
    },

    getFilteredData() {
        return this.data.filter((item, index) => {
            const matchText = !this.filterText ||
                item.tecnologia.toLowerCase().includes(this.filterText.toLowerCase());
            const matchRegime = !this.filterRegime || item.regime === this.filterRegime;
            return matchText && matchRegime;
        });
    },

    render() {
        this.renderClassificacaoTable();
        this.renderExcecoesTable();
    },

    renderClassificacaoTable() {
        const tbody = document.getElementById('classificacao-tbody');
        if (!tbody) return;

        const filtered = this.getFilteredData();

        // Build add row
        let html = `
            <tr class="add-row">
                <td>#</td>
                <td>
                    <input type="text" id="add-tecnologia" placeholder="Nome da tecnologia OMIE" list="tecnologias-list">
                    <datalist id="tecnologias-list">
                        ${[...new Set(this.data.map(d => d.tecnologia))].map(t =>
                            `<option value="${escapeHtml(t)}">`
                        ).join('')}
                    </datalist>
                </td>
                <td>
                    <select id="add-regime" onchange="ClassificacaoTab.onRegimeChange('add')">
                        <option value="">Seleccionar...</option>
                        ${REGIMES.map(r => `<option value="${r}">${r}</option>`).join('')}
                    </select>
                </td>
                <td>
                    <select id="add-categoria" disabled>
                        <option value="">Seleccionar regime primeiro</option>
                    </select>
                </td>
                <td class="table-actions">
                    <button class="btn btn-primary btn-sm" onclick="ClassificacaoTab.add()">Adicionar</button>
                </td>
            </tr>
        `;

        // Build data rows
        filtered.forEach((item, filteredIndex) => {
            const realIndex = this.data.indexOf(item);
            const isEditing = this.editingIndex === realIndex;

            if (isEditing) {
                html += `
                    <tr class="editing" data-index="${realIndex}">
                        <td>${realIndex + 1}</td>
                        <td><input type="text" id="edit-tecnologia" value="${escapeHtml(item.tecnologia)}"></td>
                        <td>
                            <select id="edit-regime" onchange="ClassificacaoTab.onRegimeChange('edit')">
                                ${REGIMES.map(r =>
                                    `<option value="${r}" ${r === item.regime ? 'selected' : ''}>${r}</option>`
                                ).join('')}
                            </select>
                        </td>
                        <td>
                            <select id="edit-categoria">
                                ${(CATEGORIAS[item.regime] || []).map(c =>
                                    `<option value="${c}" ${c === item.categoria ? 'selected' : ''}>${c}</option>`
                                ).join('')}
                            </select>
                        </td>
                        <td class="table-actions">
                            <button class="btn btn-success btn-sm" onclick="ClassificacaoTab.saveEdit(${realIndex})">Guardar</button>
                            <button class="btn btn-secondary btn-sm" onclick="ClassificacaoTab.cancelEdit()">Cancelar</button>
                        </td>
                    </tr>
                `;
            } else {
                html += `
                    <tr data-index="${realIndex}">
                        <td>${realIndex + 1}</td>
                        <td onclick="ClassificacaoTab.startEdit(${realIndex})" style="cursor:pointer">${escapeHtml(item.tecnologia)}</td>
                        <td onclick="ClassificacaoTab.startEdit(${realIndex})" style="cursor:pointer">
                            <span class="badge badge-primary">${item.regime}</span>
                        </td>
                        <td onclick="ClassificacaoTab.startEdit(${realIndex})" style="cursor:pointer">${item.categoria}</td>
                        <td class="table-actions">
                            <button class="btn btn-secondary btn-sm btn-icon" onclick="ClassificacaoTab.startEdit(${realIndex})" title="Editar">✎</button>
                            <button class="btn btn-danger btn-sm btn-icon" onclick="ClassificacaoTab.remove(${realIndex})" title="Remover">×</button>
                        </td>
                    </tr>
                `;
            }
        });

        if (filtered.length === 0) {
            html += `
                <tr>
                    <td colspan="5" class="text-center text-muted" style="padding:2rem">
                        ${this.filterText || this.filterRegime ? 'Nenhum resultado encontrado' : 'Nenhuma classificação registada'}
                    </td>
                </tr>
            `;
        }

        tbody.innerHTML = html;
    },

    renderExcecoesTable() {
        const tbody = document.getElementById('excecoes-tbody');
        if (!tbody) return;

        let html = `
            <tr class="add-row">
                <td>
                    <input type="text" id="add-exc-codigo" placeholder="Código unidade (ex: ACCGV02)">
                </td>
                <td>
                    <input type="text" id="add-exc-categoria-zona" placeholder="Categoria_Zona (ex: SOLAR_FOT_PT)">
                </td>
                <td>
                    <input type="text" id="add-exc-motivo" placeholder="Motivo (opcional)">
                </td>
                <td class="table-actions">
                    <button class="btn btn-primary btn-sm" onclick="ClassificacaoTab.addExcecao()">Adicionar</button>
                </td>
            </tr>
        `;

        this.excecoes.forEach(exc => {
            html += `
                <tr>
                    <td><code>${escapeHtml(exc.codigo)}</code></td>
                    <td>${escapeHtml(exc.categoria_zona)}</td>
                    <td class="text-muted">${escapeHtml(exc.motivo || '-')}</td>
                    <td class="table-actions">
                        <button class="btn btn-danger btn-sm btn-icon" onclick="ClassificacaoTab.removeExcecao('${escapeHtml(exc.codigo)}')" title="Remover">×</button>
                    </td>
                </tr>
            `;
        });

        if (this.excecoes.length === 0) {
            html += `
                <tr>
                    <td colspan="4" class="text-center text-muted" style="padding:2rem">
                        Nenhuma excepção registada
                    </td>
                </tr>
            `;
        }

        tbody.innerHTML = html;
    },

    bindEvents() {
        const searchInput = document.getElementById('filter-tecnologia');
        const regimeSelect = document.getElementById('filter-regime');

        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.filterText = e.target.value;
                this.renderClassificacaoTable();
            });
        }

        if (regimeSelect) {
            regimeSelect.addEventListener('change', (e) => {
                this.filterRegime = e.target.value;
                this.renderClassificacaoTable();
            });
        }
    },

    onRegimeChange(prefix) {
        const regimeSelect = document.getElementById(`${prefix}-regime`);
        const categoriaSelect = document.getElementById(`${prefix}-categoria`);

        if (!regimeSelect || !categoriaSelect) return;

        const regime = regimeSelect.value;
        const categorias = CATEGORIAS[regime] || [];

        categoriaSelect.disabled = !regime;
        categoriaSelect.innerHTML = regime
            ? categorias.map(c => `<option value="${c}">${c}</option>`).join('')
            : '<option value="">Seleccionar regime primeiro</option>';
    },

    async add() {
        const tecnologia = document.getElementById('add-tecnologia')?.value.trim();
        const regime = document.getElementById('add-regime')?.value;
        const categoria = document.getElementById('add-categoria')?.value;

        if (!tecnologia || !regime || !categoria) {
            toast('Preencha todos os campos', 'warning');
            return;
        }

        try {
            const result = await apiPost('/api/classificacao', { tecnologia, regime, categoria });
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Classificação adicionada', 'success');
                await this.loadData();
                this.render();
                // Clear inputs
                document.getElementById('add-tecnologia').value = '';
                document.getElementById('add-regime').value = '';
                document.getElementById('add-categoria').innerHTML = '<option value="">Seleccionar regime primeiro</option>';
                document.getElementById('add-categoria').disabled = true;
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    startEdit(index) {
        this.editingIndex = index;
        this.renderClassificacaoTable();
    },

    cancelEdit() {
        this.editingIndex = null;
        this.renderClassificacaoTable();
    },

    async saveEdit(index) {
        const tecnologia = document.getElementById('edit-tecnologia')?.value.trim();
        const regime = document.getElementById('edit-regime')?.value;
        const categoria = document.getElementById('edit-categoria')?.value;

        if (!tecnologia || !regime || !categoria) {
            toast('Preencha todos os campos', 'warning');
            return;
        }

        try {
            const result = await apiPut(`/api/classificacao/${index}`, { tecnologia, regime, categoria });
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Classificação actualizada', 'success');
                this.editingIndex = null;
                await this.loadData();
                this.render();
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    async remove(index) {
        const item = this.data[index];
        if (!confirm(`Remover classificação "${item.tecnologia}"?`)) return;

        try {
            const result = await apiDelete(`/api/classificacao/${index}`);
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Classificação removida', 'success');
                await this.loadData();
                this.render();
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    async addExcecao() {
        const codigo = document.getElementById('add-exc-codigo')?.value.trim();
        const categoria_zona = document.getElementById('add-exc-categoria-zona')?.value.trim();
        const motivo = document.getElementById('add-exc-motivo')?.value.trim();

        if (!codigo || !categoria_zona) {
            toast('Preencha código e categoria_zona', 'warning');
            return;
        }

        try {
            const result = await apiPost('/api/excecoes', { codigo, categoria_zona, motivo });
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Excepção adicionada', 'success');
                await this.loadData();
                this.render();
                // Clear inputs
                document.getElementById('add-exc-codigo').value = '';
                document.getElementById('add-exc-categoria-zona').value = '';
                document.getElementById('add-exc-motivo').value = '';
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    async removeExcecao(codigo) {
        if (!confirm(`Remover excepção "${codigo}"?`)) return;

        try {
            const result = await apiDelete(`/api/excecoes/${encodeURIComponent(codigo)}`);
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Excepção removida', 'success');
                await this.loadData();
                this.render();
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    exportJson() {
        downloadJson(this.data, 'classificacao.json');
        toast('Ficheiro exportado', 'success');
    }
};

// ============================================================================
// Initialize Application
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Health check
    checkHealth();
    setInterval(checkHealth, 30000);

    // Tab navigation
    document.querySelectorAll('nav button[data-tab]').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Initialize first tab
    ClassificacaoTab.init();

    // Tab load events
    document.addEventListener('tabLoad', (e) => {
        if (e.detail.tabId === 'classificacao') {
            ClassificacaoTab.init();
        }
        // Other tabs will be initialized in future tasks
    });
});
