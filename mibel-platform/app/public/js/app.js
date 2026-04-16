/* ==========================================================================
   MIBEL Bid Analyzer - Core JavaScript
   ========================================================================== */

// ============================================================================
// API Helpers
// ============================================================================

async function parseJsonResponse(response) {
    const text = await response.text();
    try {
        return JSON.parse(text);
    } catch (_) {
        // Server returned non-JSON (PHP error page, nginx 502, etc.)
        const preview = text.slice(0, 200).replace(/<[^>]+>/g, ' ').trim();
        throw new Error(`Resposta inesperada do servidor (HTTP ${response.status}): ${preview}`);
    }
}

async function apiGet(url) {
    const response = await fetch(url);
    return parseJsonResponse(response);
}

async function apiPost(url, data) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return parseJsonResponse(response);
}

async function apiPut(url, data) {
    const response = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return parseJsonResponse(response);
}

async function apiDelete(url) {
    const response = await fetch(url, { method: 'DELETE' });
    return parseJsonResponse(response);
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

function switchTab(tabId, jobId = null) {
    // Update nav buttons
    document.querySelectorAll('nav button').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabId);
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabId}`);
    });

    // Trigger tab load event
    const event = new CustomEvent('tabLoad', { detail: { tabId, jobId } });
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
// Parametros Tab (Tab 2)
// ============================================================================

const ParametrosTab = {
    params: {},
    loaded: false,
    collapsedRegimes: {},

    async init() {
        if (!this.loaded) {
            await this.loadData();
            this.loaded = true;
        }
        this.render();
    },

    async loadData() {
        try {
            this.params = await apiGet('/api/parametros');
        } catch (e) {
            toast('Erro ao carregar parâmetros: ' + e.message, 'error');
        }
    },

    render() {
        const container = document.getElementById('parametros-container');
        if (!container) return;

        let html = '';

        REGIMES.forEach(regime => {
            const categorias = this.params[regime] || {};
            const categoriasKeys = Object.keys(categorias).sort();
            const isCollapsed = this.collapsedRegimes[regime] || false;

            html += `
                <div class="card card-collapsible ${isCollapsed ? 'collapsed' : ''}" data-regime="${regime}">
                    <div class="card-header" onclick="ParametrosTab.toggleRegime('${regime}')">
                        <h2>
                            <span class="collapse-icon">▼</span>
                            ${regime}
                            <span class="badge">${categoriasKeys.length} categorias</span>
                        </h2>
                    </div>
                    <div class="card-body">
                        ${categoriasKeys.length === 0 ?
                            '<p class="text-muted text-center">Nenhuma categoria definida</p>' :
                            categoriasKeys.map(catZona => this.renderCategoria(regime, catZona, categorias[catZona])).join('')
                        }
                    </div>
                </div>
            `;
        });

        container.innerHTML = html;
    },

    renderCategoria(regime, categoriaZona, config) {
        const isPRE = regime === 'PRE';
        const escala = config.escala || 1.0;
        const escaloes = config.escaloes || [];
        const hasDelta = config.delta_preco !== undefined;
        const deltaPreco = config.delta_preco || 0;

        let html = `
            <div class="categoria-item" data-categoria="${categoriaZona}">
                <div class="categoria-header">
                    <h4>${categoriaZona}</h4>
                    <button class="btn btn-danger btn-sm btn-icon" onclick="ParametrosTab.removeCategoria('${regime}', '${categoriaZona}')" title="Remover">×</button>
                </div>
                <div class="categoria-content">
                    <div class="param-field">
                        <label>Escala de volume</label>
                        <input type="number"
                               step="0.001"
                               min="0.01"
                               value="${escala}"
                               data-regime="${regime}"
                               data-categoria="${categoriaZona}"
                               data-field="escala"
                               onchange="ParametrosTab.onFieldChange(this)">
                    </div>
        `;

        // Escaloes only for PRE regime
        if (isPRE) {
            html += `
                    <div class="escaloes-container">
                        <h5>Escalões de Preço</h5>
                        <table class="escaloes-table">
                            <thead>
                                <tr>
                                    <th>Preço (€/MWh)</th>
                                    <th>% Volume</th>
                                    <th></th>
                                </tr>
                            </thead>
                            <tbody id="escaloes-${regime}-${categoriaZona}">
                                ${escaloes.map((esc, idx) => `
                                    <tr data-index="${idx}">
                                        <td>
                                            <input type="number" step="0.1" min="0" value="${esc.preco}"
                                                   data-regime="${regime}" data-categoria="${categoriaZona}"
                                                   data-field="escalao-preco" data-index="${idx}"
                                                   onchange="ParametrosTab.onEscalaoChange(this)">
                                        </td>
                                        <td>
                                            <input type="number" step="0.01" min="0" max="1" value="${esc.pct_bids}"
                                                   data-regime="${regime}" data-categoria="${categoriaZona}"
                                                   data-field="escalao-pct" data-index="${idx}"
                                                   onchange="ParametrosTab.onEscalaoChange(this)">
                                        </td>
                                        <td>
                                            <button class="btn btn-danger btn-sm btn-icon"
                                                    onclick="ParametrosTab.removeEscalao('${regime}', '${categoriaZona}', ${idx})">−</button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                        <div class="escaloes-sum ${this.getEscaloesSum(escaloes) === 1 ? 'valid' : 'invalid'}"
                             id="sum-${regime}-${categoriaZona}">
                            Soma: ${(this.getEscaloesSum(escaloes) * 100).toFixed(1)}%
                            ${this.getEscaloesSum(escaloes) === 1 ? '✓' : '(deve ser 100%)'}
                        </div>
                        <button class="btn btn-secondary btn-sm btn-add-escalao"
                                onclick="ParametrosTab.addEscalao('${regime}', '${categoriaZona}')">
                            + Escalão
                        </button>
                    </div>
            `;
        }

        // Delta price (optional for all)
        html += `
                    <div class="delta-field">
                        <label>
                            <input type="checkbox"
                                   ${hasDelta ? 'checked' : ''}
                                   data-regime="${regime}"
                                   data-categoria="${categoriaZona}"
                                   data-field="has-delta"
                                   onchange="ParametrosTab.onDeltaToggle(this)">
                            Delta preço
                        </label>
                        <input type="number" step="0.1" value="${deltaPreco}"
                               ${!hasDelta ? 'disabled' : ''}
                               data-regime="${regime}"
                               data-categoria="${categoriaZona}"
                               data-field="delta_preco"
                               onchange="ParametrosTab.onFieldChange(this)">
                    </div>
                </div>
            </div>
        `;

        return html;
    },

    getEscaloesSum(escaloes) {
        if (!escaloes || escaloes.length === 0) return 0;
        const sum = escaloes.reduce((acc, e) => acc + (parseFloat(e.pct_bids) || 0), 0);
        return Math.round(sum * 100) / 100;
    },

    toggleRegime(regime) {
        this.collapsedRegimes[regime] = !this.collapsedRegimes[regime];
        const card = document.querySelector(`.card-collapsible[data-regime="${regime}"]`);
        if (card) {
            card.classList.toggle('collapsed', this.collapsedRegimes[regime]);
        }
    },

    onFieldChange(input) {
        const { regime, categoria, field } = input.dataset;
        const value = parseFloat(input.value);

        if (!this.params[regime]) this.params[regime] = {};
        if (!this.params[regime][categoria]) this.params[regime][categoria] = { escala: 1.0 };

        this.params[regime][categoria][field] = value;
    },

    onEscalaoChange(input) {
        const { regime, categoria, field, index } = input.dataset;
        const idx = parseInt(index);
        const value = parseFloat(input.value);

        if (!this.params[regime]?.[categoria]?.escaloes) return;

        if (field === 'escalao-preco') {
            this.params[regime][categoria].escaloes[idx].preco = value;
        } else if (field === 'escalao-pct') {
            this.params[regime][categoria].escaloes[idx].pct_bids = value;
        }

        // Update sum display
        this.updateEscaloesSum(regime, categoria);
    },

    updateEscaloesSum(regime, categoria) {
        const escaloes = this.params[regime]?.[categoria]?.escaloes || [];
        const sum = this.getEscaloesSum(escaloes);
        const sumEl = document.getElementById(`sum-${regime}-${categoria}`);
        if (sumEl) {
            sumEl.className = `escaloes-sum ${sum === 1 ? 'valid' : 'invalid'}`;
            sumEl.innerHTML = `Soma: ${(sum * 100).toFixed(1)}% ${sum === 1 ? '✓' : '(deve ser 100%)'}`;
        }
    },

    onDeltaToggle(checkbox) {
        const { regime, categoria } = checkbox.dataset;
        const deltaInput = document.querySelector(
            `input[data-regime="${regime}"][data-categoria="${categoria}"][data-field="delta_preco"]`
        );

        if (checkbox.checked) {
            deltaInput.disabled = false;
            this.params[regime][categoria].delta_preco = parseFloat(deltaInput.value) || 0;
        } else {
            deltaInput.disabled = true;
            delete this.params[regime][categoria].delta_preco;
        }
    },

    addEscalao(regime, categoria) {
        if (!this.params[regime]?.[categoria]) return;

        if (!this.params[regime][categoria].escaloes) {
            this.params[regime][categoria].escaloes = [];
        }

        this.params[regime][categoria].escaloes.push({ preco: 0, pct_bids: 0 });
        this.render();
    },

    removeEscalao(regime, categoria, index) {
        if (!this.params[regime]?.[categoria]?.escaloes) return;

        this.params[regime][categoria].escaloes.splice(index, 1);
        this.render();
    },

    toggleAddCategoria() {
        const card = document.getElementById('add-categoria-card');
        const btn = document.getElementById('btn-add-categoria');
        if (card.style.display === 'none') {
            card.style.display = 'block';
            btn.style.display = 'none';
        } else {
            card.style.display = 'none';
            btn.style.display = 'inline-flex';
        }
    },

    addCategoria() {
        const regime = document.getElementById('add-cat-regime').value;
        const nome = document.getElementById('add-cat-nome').value.trim().toUpperCase();

        if (!nome) {
            toast('Preencha o nome da categoria', 'warning');
            return;
        }

        if (!this.params[regime]) {
            this.params[regime] = {};
        }

        if (this.params[regime][nome]) {
            toast('Categoria já existe neste regime', 'error');
            return;
        }

        // Create with defaults
        this.params[regime][nome] = { escala: 1.0 };
        if (regime === 'PRE') {
            this.params[regime][nome].escaloes = [{ preco: 0, pct_bids: 1.0 }];
        }

        toast('Categoria adicionada', 'success');
        this.toggleAddCategoria();
        document.getElementById('add-cat-nome').value = '';
        this.render();
    },

    removeCategoria(regime, categoria) {
        if (!confirm(`Remover categoria "${categoria}"?`)) return;

        if (this.params[regime]?.[categoria]) {
            delete this.params[regime][categoria];
            toast('Categoria removida', 'success');
            this.render();
        }
    },

    async saveAll() {
        try {
            const result = await apiPut('/api/parametros', this.params);
            if (result.error) {
                toast(result.error, 'error');
            } else {
                toast('Parâmetros guardados com sucesso', 'success');
            }
        } catch (e) {
            toast('Erro ao guardar: ' + e.message, 'error');
        }
    },

    async resetDefaults() {
        if (!confirm('Repor todos os parâmetros para os valores padrão? Esta acção não pode ser desfeita.')) {
            return;
        }

        // Default parameters (same as TASK-02 migrate.php)
        this.params = {
            "PRE": {
                "SOLAR_FOT_ES": {"escala": 2.2297, "escaloes": [{"preco": 0.0, "pct_bids": 0.30}, {"preco": 20.0, "pct_bids": 0.30}, {"preco": 35.0, "pct_bids": 0.40}]},
                "SOLAR_FOT_PT": {"escala": 3.6879, "escaloes": [{"preco": 0.0, "pct_bids": 0.30}, {"preco": 20.0, "pct_bids": 0.30}, {"preco": 35.0, "pct_bids": 0.40}]},
                "SOLAR_TER_ES": {"escala": 2.0869, "escaloes": [{"preco": 40.0, "pct_bids": 1.00}]},
                "EOLICA_ES": {"escala": 1.8857, "escaloes": [{"preco": 50.0, "pct_bids": 0.50}, {"preco": 70.0, "pct_bids": 0.50}]},
                "EOLICA_PT": {"escala": 2.1379, "escaloes": [{"preco": 50.0, "pct_bids": 0.50}, {"preco": 70.0, "pct_bids": 0.50}]},
                "EOLICA_MARINA_ES": {"escala": 1.0, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]},
                "HIDRICA_ES": {"escala": 1.0, "escaloes": [{"preco": 15.0, "pct_bids": 1.00}]},
                "TERMICA_RENOV_ES": {"escala": 1.4, "escaloes": [{"preco": 10.0, "pct_bids": 1.00}]},
                "TERMICA_RENOV_PT": {"escala": 1.5348, "escaloes": [{"preco": 10.0, "pct_bids": 1.00}]},
                "TERMICA_NREN_ES": {"escala": 1.0, "escaloes": [{"preco": 60.0, "pct_bids": 1.00}]},
                "GEOTERMICA_ES": {"escala": 1.0, "escaloes": [{"preco": 30.0, "pct_bids": 1.00}]},
                "HIBRIDA_RENOV_ES": {"escala": 1.0, "escaloes": [{"preco": 45.0, "pct_bids": 1.00}]},
                "RE_TARIFA_CUR_ES": {"escala": 1.7, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]},
                "RE_TARIFA_CUR_PT": {"escala": 1.7, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]},
                "RE_OUTRO_ES": {"escala": 1.0, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]},
                "ARMAZENAMENTO_VENDA_ES": {"escala": 1.0, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]},
                "ARMAZENAMENTO_VENDA_PT": {"escala": 1.0, "escaloes": [{"preco": 0.0, "pct_bids": 1.00}]}
            },
            "PRO": {
                "BOMBEO_PURO_PRO_ES": {"escala": 1.0},
                "CARVAO_ES": {"escala": 1.0},
                "CICLO_COMBINADO_ES": {"escala": 1.0},
                "CICLO_COMBINADO_PT": {"escala": 0.7143},
                "GAS_ES": {"escala": 1.0},
                "HIDRICA_PRO_ES": {"escala": 1.0},
                "HIDRICA_PRO_PT": {"escala": 1.0},
                "NUCLEAR_ES": {"escala": 0.447}
            },
            "CONSUMO": {
                "ARMAZENAMENTO_COMPRA_ES": {"escala": 1.0},
                "ARMAZENAMENTO_COMPRA_PT": {"escala": 1.0},
                "BOMBEO_CONSUMO_ES": {"escala": 1.6926},
                "BOMBEO_CONSUMO_PT": {"escala": 1.3871},
                "CONS_AUXILIARES_ES": {"escala": 1.0},
                "CONS_DIRECTO_ES": {"escala": 1.6926},
                "CONS_DIRECTO_EXT": {"escala": 1.6926},
                "CONS_PRODUTOR_ES": {"escala": 1.0}
            },
            "COMERCIALIZADOR": {
                "COMERC_ES": {"escala": 1.3871},
                "COMERC_EXT": {"escala": 1.0},
                "COMERC_NR_EXT": {"escala": 1.0},
                "COMERC_PT": {"escala": 1.6926},
                "COMERC_ULT_REC_ES": {"escala": 1.0},
                "COMERC_ULT_REC_PT": {"escala": 1.0}
            },
            "GENERICA": {
                "GENERICA_ES": {"escala": 1.3871},
                "GENERICA_PT": {"escala": 1.6926},
                "GENERICA_VENDA_ES": {"escala": 1.3871},
                "GENERICA_VENDA_PT": {"escala": 1.6926}
            },
            "PORFOLIO": {
                "PORTF_COMERC_ES": {"escala": 1.0},
                "PORTF_PROD_ES": {"escala": 1.0},
                "PORTF_PROD_PT": {"escala": 1.0}
            }
        };

        this.render();
        toast('Parâmetros repostos para valores padrão', 'info');
    }
};

// ============================================================================
// Estudos Tab (Tab 3)
// ============================================================================

const EstudosTab = {
    estudos: [],
    pollingTimer: null,

    async init() {
        await this.loadData();
        this.render();
        this.startPollingIfNeeded();
    },

    async loadData() {
        try {
            const data = await apiGet('/api/estudos');
            if (data.error) {
                toast('Erro ao carregar estudos: ' + data.error, 'error');
                return;
            }
            this.estudos = Array.isArray(data) ? data : [];
        } catch (e) {
            toast('Erro ao carregar estudos: ' + e.message, 'error');
        }
    },

    render() {
        this.renderTable();
    },

    renderTable() {
        const tbody = document.getElementById('estudos-tbody');
        if (!tbody) return;

        if (this.estudos.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="text-center text-muted" style="padding:2rem">
                        Nenhum estudo registado. Lance o primeiro estudo acima.
                    </td>
                </tr>
            `;
            return;
        }

        let html = '';
        this.estudos.forEach(job => {
            const tipoBadge = job.tipo === 'otimizacao'
                ? '<span class="badge badge-primary">Optimização</span>'
                : '<span class="badge">Substituição</span>';

            const periodo = `${job.data_inicio} → ${job.data_fim}`;
            const statusBadge = this.renderStatusBadge(job.status);
            const createdAt = job.created_at ? job.created_at.substring(0, 16) : '-';
            const obs = escapeHtml(job.observacoes || '-');

            const isDone = job.status === 'DONE';
            const canCancel = job.status === 'RUNNING';
            const canDelete = job.status === 'PENDING';

            let actions = '';
            if (isDone) {
                actions += `<button class="btn btn-success btn-sm" onclick="EstudosTab.verResultados('${job.id}')">Ver resultados</button> `;
            }
            actions += `<button class="btn btn-secondary btn-sm" onclick="EstudosTab.verLog('${job.id}')">Log</button> `;
            if (canCancel || canDelete) {
                actions += `<button class="btn btn-danger btn-sm btn-icon" onclick="EstudosTab.cancelarOuRemover('${job.id}', '${job.status}')" title="Cancelar">×</button>`;
            }

            html += `
                <tr data-job-id="${job.id}">
                    <td>${tipoBadge}</td>
                    <td><code>${escapeHtml(periodo)}</code></td>
                    <td class="text-muted">${obs}</td>
                    <td>${statusBadge}</td>
                    <td>${escapeHtml(createdAt)}</td>
                    <td class="table-actions">${actions}</td>
                </tr>
            `;
        });

        tbody.innerHTML = html;
    },

    renderStatusBadge(status) {
        switch (status) {
            case 'PENDING':
                return '<span class="badge status-pending">PENDING</span>';
            case 'RUNNING':
                return '<span class="badge status-running">RUNNING</span>';
            case 'DONE':
                return '<span class="badge status-done">DONE</span>';
            case 'FAILED':
                return '<span class="badge status-failed">FAILED</span>';
            default:
                return `<span class="badge">${escapeHtml(status)}</span>`;
        }
    },

    startPollingIfNeeded() {
        const hasActive = this.estudos.some(j => j.status === 'RUNNING' || j.status === 'PENDING');

        if (hasActive && !this.pollingTimer) {
            this.pollingTimer = setInterval(async () => {
                await this.loadData();
                this.render();

                const stillActive = this.estudos.some(j => j.status === 'RUNNING' || j.status === 'PENDING');
                if (!stillActive) {
                    clearInterval(this.pollingTimer);
                    this.pollingTimer = null;
                }
            }, 5000);
        } else if (!hasActive && this.pollingTimer) {
            clearInterval(this.pollingTimer);
            this.pollingTimer = null;
        }
    },

    async lancarEstudo() {
        const tipo = document.querySelector('input[name="estudo-tipo"]:checked')?.value;
        const dataInicio = document.getElementById('estudo-data-inicio')?.value;
        const dataFim = document.getElementById('estudo-data-fim')?.value;
        const observacoes = document.getElementById('estudo-observacoes')?.value.trim();
        const workersN = parseInt(document.getElementById('estudo-workers')?.value || '4');

        if (!tipo) { toast('Seleccione o tipo de estudo', 'warning'); return; }
        if (!dataInicio) { toast('Seleccione a data de início', 'warning'); return; }
        if (!dataFim) { toast('Seleccione a data de fim', 'warning'); return; }
        if (dataFim < dataInicio) { toast('Data fim deve ser posterior à data início', 'warning'); return; }

        try {
            const result = await apiPost('/api/estudos', {
                tipo,
                data_inicio: dataInicio,
                data_fim: dataFim,
                observacoes: observacoes || '',
                workers_n: workersN
            });

            if (result.error) {
                toast('Erro: ' + result.error, 'error');
                return;
            }

            toast('Estudo lançado! ID: ' + result.job_id.substring(0, 8) + '…', 'success');
            await this.loadData();
            this.render();
            this.startPollingIfNeeded();
        } catch (e) {
            toast('Erro ao lançar estudo: ' + e.message, 'error');
        }
    },

    async refresh() {
        await this.loadData();
        this.render();
        this.startPollingIfNeeded();
        toast('Lista actualizada', 'info');
    },

    async verLog(jobId) {
        const modal = document.getElementById('modal-log');
        const modalJobId = document.getElementById('modal-job-id');
        const modalContent = document.getElementById('modal-log-content');

        if (!modal || !modalContent) return;

        if (modalJobId) modalJobId.textContent = jobId.substring(0, 8) + '…';
        modalContent.textContent = 'A carregar log…';
        modal.showModal();

        try {
            const data = await apiGet(`/api/estudos/${jobId}`);
            if (data.error) {
                modalContent.textContent = 'Erro: ' + data.error;
                return;
            }
            const lines = data.log || [];
            modalContent.textContent = lines.length > 0 ? lines.join('\n') : '(sem linhas de log)';
            modalContent.scrollTop = modalContent.scrollHeight;
        } catch (e) {
            modalContent.textContent = 'Erro ao carregar log: ' + e.message;
        }
    },

    verResultados(jobId) {
        switchTab('resultados', jobId);
    },

    async cancelarOuRemover(jobId, status) {
        const action = status === 'RUNNING' ? 'cancelar' : 'remover';
        if (!confirm(`Confirma ${action} este estudo?`)) return;

        try {
            let result;
            if (status === 'RUNNING') {
                result = await apiPost(`/api/estudos/${jobId}/cancelar`, {});
            } else {
                result = await apiDelete(`/api/estudos/${jobId}`);
            }

            if (result?.error) {
                toast('Erro: ' + result.error, 'error');
                return;
            }

            toast(`Estudo ${action}do com sucesso`, 'success');
            await this.loadData();
            this.render();
            this.startPollingIfNeeded();
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    }
};

// ============================================================================
// Resultados Tab (Tab 4)
// ============================================================================

const ResultadosTab = {
    jobId: null,
    tipo: 'substituicao',
    pais: '',
    chartSerie: null,
    chartDelta: null,
    tabelaOffset: 0,
    tabelaTotal: 0,
    PAGE_SIZE: 50,

    load(jobId) {
        this.jobId = jobId;
        this.pais = '';
        this.tabelaOffset = 0;

        const csvLink  = document.getElementById('res-export-csv');
        const jsonLink = document.getElementById('res-export-json');
        if (csvLink)  csvLink.href  = `/api/resultados/${jobId}/exportar?formato=csv`;
        if (jsonLink) jsonLink.href = `/api/resultados/${jobId}/exportar?formato=json`;

        document.getElementById('res-empty').hidden   = true;
        document.getElementById('res-content').hidden = false;

        this.loadStats();
    },

    async loadStats() {
        if (!this.jobId) return;
        try {
            const data = await apiGet(`/api/resultados/${this.jobId}/stats`);
            if (data.error) { toast('Erro ao carregar estatísticas: ' + data.error, 'error'); return; }

            const { job, stats } = data;
            this.tipo = stats.tipo || (job?.tipo === 'otimizacao' ? 'otimizacao' : 'substituicao');
            const nPeriodos = parseInt(stats.n_periodos || 0);

            // Header
            const titulo = document.getElementById('res-titulo');
            if (titulo) {
                titulo.textContent = `Resultados — ${this.tipo === 'otimizacao' ? 'Optimização' : 'Substituição'}`;
            }

            const badges = document.getElementById('res-badges');
            if (badges) {
                const obs = job?.observacoes ? `<span class="badge">${escapeHtml(job.observacoes)}</span>` : '';
                badges.innerHTML = `
                    <span class="badge badge-primary">${escapeHtml(job?.data_inicio || '')} → ${escapeHtml(job?.data_fim || '')}</span>
                    <span class="badge">${nPeriodos.toLocaleString('pt-PT')} períodos</span>
                    ${obs}
                `;
            }

            if (nPeriodos === 0) {
                document.getElementById('res-no-data').hidden        = false;
                document.getElementById('res-charts-section').hidden = true;
                document.getElementById('res-tabela-section').hidden = true;
                document.getElementById('res-stat-cards').innerHTML  = '';
                return;
            }

            document.getElementById('res-no-data').hidden        = true;
            document.getElementById('res-charts-section').hidden = false;
            document.getElementById('res-tabela-section').hidden = false;

            this._updateTableHeaders();
            this._updateChartTitles();
            this.renderStatCards(stats);
            await Promise.all([this.loadCharts(), this.loadTabela(0)]);
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    _updateTableHeaders() {
        const thead = document.getElementById('res-tabela-thead');
        if (!thead) return;
        if (this.tipo === 'otimizacao') {
            thead.innerHTML = `<tr>
                <th>Data</th><th>Hora</th><th>País</th>
                <th class="text-right">P.Orig (€/MWh)</th>
                <th class="text-right">P.Base (€/MWh)</th>
                <th class="text-right">P.Óptimo (€/MWh)</th>
                <th class="text-right">Δ Preço</th>
                <th class="text-right">Lucro Base (€)</th>
                <th class="text-right">Lucro Opt (€)</th>
                <th class="text-right">Δ Lucro (€)</th>
                <th class="text-right">Bids Rem.</th>
            </tr>`;
        } else {
            thead.innerHTML = `<tr>
                <th>Data</th><th>Hora</th><th>País</th>
                <th class="text-right">P.Orig (€/MWh)</th>
                <th class="text-right">P.Sim (€/MWh)</th>
                <th class="text-right">Δ Preço</th>
                <th class="text-right">Vol.Orig (MWh)</th>
                <th class="text-right">Vol.Sim (MWh)</th>
                <th class="text-right">Bids Sub.</th>
            </tr>`;
        }
    },

    _updateChartTitles() {
        const t1 = document.getElementById('res-chart-serie-titulo');
        const t2 = document.getElementById('res-chart-delta-titulo');
        if (this.tipo === 'otimizacao') {
            if (t1) t1.textContent = 'Evolução do Preço de Clearing (Base vs Óptimo)';
            if (t2) t2.textContent = 'Delta de Lucro PRE médio por Hora do Dia (€)';
        } else {
            if (t1) t1.textContent = 'Evolução do Preço de Clearing';
            if (t2) t2.textContent = 'Delta Médio de Preço por Hora do Dia (€/MWh)';
        }
    },

    renderStatCards(stats) {
        const container = document.getElementById('res-stat-cards');
        if (!container) return;

        if (this.tipo === 'otimizacao') {
            const dL = parseFloat(stats.delta_lucro_total || 0);
            const dLClass = dL > 0 ? 'positive' : (dL < 0 ? 'negative' : '');
            container.innerHTML = `
                <div class="stat-card">
                    <div class="stat-card-label">Preço médio base</div>
                    <div class="stat-card-value">${this.fmtNum(stats.preco_orig_medio, 2)}</div>
                    <div class="stat-card-unit">€/MWh</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Preço médio óptimo</div>
                    <div class="stat-card-value">${this.fmtNum(stats.preco_sim_medio, 2)}</div>
                    <div class="stat-card-unit">€/MWh</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Lucro PRE base</div>
                    <div class="stat-card-value">${this.fmtNum(stats.lucro_base_total, 0)}</div>
                    <div class="stat-card-unit">€ total</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Lucro PRE óptimo</div>
                    <div class="stat-card-value">${this.fmtNum(stats.lucro_opt_total, 0)}</div>
                    <div class="stat-card-unit">€ total</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Delta lucro PRE</div>
                    <div class="stat-card-value ${dLClass}">${dL >= 0 ? '+' : ''}${this.fmtNum(dL, 0)}</div>
                    <div class="stat-card-unit">€</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Bids PRE removidos</div>
                    <div class="stat-card-value">${this.fmtNum(stats.total_bids_rem, 0)}</div>
                    <div class="stat-card-unit">total</div>
                </div>
            `;
        } else {
            const delta = parseFloat(stats.delta_medio || 0);
            const dClass = delta < 0 ? 'negative' : (delta > 0 ? 'positive' : '');
            container.innerHTML = `
                <div class="stat-card">
                    <div class="stat-card-label">Preço médio original</div>
                    <div class="stat-card-value">${this.fmtNum(stats.preco_orig_medio, 2)}</div>
                    <div class="stat-card-unit">€/MWh</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Preço médio simulado</div>
                    <div class="stat-card-value">${this.fmtNum(stats.preco_sim_medio, 2)}</div>
                    <div class="stat-card-unit">€/MWh</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Delta médio</div>
                    <div class="stat-card-value ${dClass}">${delta >= 0 ? '+' : ''}${this.fmtNum(delta, 2)}</div>
                    <div class="stat-card-unit">€/MWh · min ${this.fmtNum(stats.delta_min, 2)} / max ${this.fmtNum(stats.delta_max, 2)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Bids substituídos</div>
                    <div class="stat-card-value">${this.fmtNum(stats.total_bids_sub, 0)}</div>
                    <div class="stat-card-unit">total</div>
                </div>
            `;
        }
    },

    async loadCharts() {
        if (!this.jobId) return;
        try {
            const data = await apiGet(`/api/resultados/${this.jobId}/serie`);
            if (data.error) return;
            this.renderChartSerie(data);
            this.renderChartDelta(data);
        } catch (e) {
            toast('Erro ao carregar gráficos: ' + e.message, 'error');
        }
    },

    renderChartSerie(data) {
        const rows = data.rows || [];
        if (!rows.length) return;

        // Build ordered list of unique time-points and group rows by (data, hora_num)
        const ptMap   = new Map();  // label -> { MI: row, ES: row, PT: row }
        const ptOrder = [];
        for (const r of rows) {
            const key = `${r.data} H${r.hora_num}`;
            if (!ptMap.has(key)) { ptMap.set(key, {}); ptOrder.push(key); }
            ptMap.get(key)[r.pais] = r;
        }

        const n     = ptOrder.length;
        const origMI = Array(n).fill(null);
        const simMI  = Array(n).fill(null);
        const origES = Array(n).fill(null);
        const simES  = Array(n).fill(null);
        const origPT = Array(n).fill(null);
        const simPT  = Array(n).fill(null);
        const isSep  = Array(n).fill(false);

        ptOrder.forEach((key, i) => {
            const pt = ptMap.get(key);
            if (pt.MI) {
                origMI[i] = parseFloat(pt.MI.preco_orig);
                simMI[i]  = parseFloat(pt.MI.preco_sim);
            }
            if (pt.ES) {
                origES[i] = parseFloat(pt.ES.preco_orig);
                simES[i]  = parseFloat(pt.ES.preco_sim);
                isSep[i]  = true;
            }
            if (pt.PT) {
                origPT[i] = parseFloat(pt.PT.preco_orig);
                simPT[i]  = parseFloat(pt.PT.preco_sim);
                isSep[i]  = true;
            }
        });

        // Transition connectors: at separation boundaries bridge MI ↔ ES/PT
        // so the curves visually diverge/converge at those points.
        for (let i = 0; i < n; i++) {
            // Entry: last unified point → first separated point
            if (i > 0 && isSep[i] && !isSep[i - 1] && origMI[i - 1] !== null) {
                origES[i - 1] = origMI[i - 1];
                simES[i - 1]  = simMI[i - 1];
                origPT[i - 1] = origMI[i - 1];
                simPT[i - 1]  = simMI[i - 1];
            }
            // Exit: last separated point → first unified point
            if (i > 0 && !isSep[i] && isSep[i - 1] && origMI[i] !== null) {
                origES[i] = origMI[i];
                simES[i]  = simMI[i];
                origPT[i] = origMI[i];
                simPT[i]  = simMI[i];
            }
        }

        // Subsample to ≤ 500 points for performance
        let lbls = ptOrder.slice();
        let [oMI, sMI, oES, sES, oPT, sPT] = [origMI, simMI, origES, simES, origPT, simPT];
        if (lbls.length > 500) {
            const step = Math.ceil(lbls.length / 500);
            const keep = (_, i) => i % step === 0;
            lbls = lbls.filter(keep);
            oMI  = origMI.filter(keep);
            sMI  = simMI.filter(keep);
            oES  = origES.filter(keep);
            sES  = simES.filter(keep);
            oPT  = origPT.filter(keep);
            sPT  = simPT.filter(keep);
        }

        if (this.chartSerie) { this.chartSerie.destroy(); this.chartSerie = null; }
        const ctx = document.getElementById('chart-serie');
        if (!ctx) return;

        const hasES  = oES.some(v => v !== null);
        const hasPT  = oPT.some(v => v !== null);
        const simLbl = this.tipo === 'otimizacao' ? 'Óptimo' : 'Simulado';

        const BASE = { tension: 0.1, pointRadius: 0, backgroundColor: 'transparent', spanGaps: false };
        const datasets = [
            { ...BASE, label: 'MIBEL Original',    data: oMI, borderColor: '#1f6fb5', borderWidth: 1.8 },
            { ...BASE, label: `MIBEL ${simLbl}`,   data: sMI, borderColor: '#7eb8e8', borderWidth: 1.4, borderDash: [5, 3] },
        ];
        if (hasES) {
            datasets.push(
                { ...BASE, label: 'ES Original',   data: oES, borderColor: '#d04e1a', borderWidth: 1.8 },
                { ...BASE, label: `ES ${simLbl}`,  data: sES, borderColor: '#f0a07e', borderWidth: 1.4, borderDash: [5, 3] }
            );
        }
        if (hasPT) {
            datasets.push(
                { ...BASE, label: 'PT Original',   data: oPT, borderColor: '#2a9d45', borderWidth: 1.8 },
                { ...BASE, label: `PT ${simLbl}`,  data: sPT, borderColor: '#80cb96', borderWidth: 1.4, borderDash: [5, 3] }
            );
        }

        this.chartSerie = new Chart(ctx, {
            type: 'line',
            data: { labels: lbls, datasets },
            options: {
                responsive: true,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { ticks: { maxTicksLimit: 20 } },
                    y: { title: { display: true, text: '€/MWh' } }
                }
            }
        });
    },

    renderChartDelta(data) {
        // Optimização: usa delta_lucro; substituição: usa delta (preço)
        const isOpt    = this.tipo === 'otimizacao';
        const deltas   = (isOpt ? data.delta_lucro : data.delta) || [];
        const horaNums = data.hora_num || [];
        const yLabel   = isOpt ? '€' : '€/MWh';
        const barLabel = isOpt ? 'Δ Lucro PRE médio (€)' : 'Δ Preço médio (€/MWh)';

        const sums   = Array(24).fill(0);
        const counts = Array(24).fill(0);
        deltas.forEach((d, i) => {
            const h = parseInt(horaNums[i] || 0);
            if (h >= 1 && h <= 24) {
                sums[h - 1]   += parseFloat(d || 0);
                counts[h - 1] += 1;
            }
        });
        const deltasPorHora = sums.map((s, i) => counts[i] > 0 ? s / counts[i] : null);
        // Optimização: verde quando lucro sobe (> 0); substituição: verde quando preço cai (< 0)
        const bgColors = deltasPorHora.map(v =>
            v === null ? '#94a3b8' : (isOpt ? (v > 0 ? '#16a34a' : '#dc2626') : (v < 0 ? '#16a34a' : '#dc2626'))
        );

        if (this.chartDelta) { this.chartDelta.destroy(); this.chartDelta = null; }
        const ctx2 = document.getElementById('chart-delta');
        if (!ctx2) return;

        this.chartDelta = new Chart(ctx2, {
            type: 'bar',
            data: {
                labels: Array.from({ length: 24 }, (_, i) => `H${i + 1}`),
                datasets: [{ label: barLabel, data: deltasPorHora, backgroundColor: bgColors }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { y: { title: { display: true, text: yLabel } } }
            }
        });
    },

    async loadTabela(offset = 0) {
        if (!this.jobId) return;
        this.tabelaOffset = offset;
        const colspan = this.tipo === 'otimizacao' ? 11 : 9;

        const tbody = document.getElementById('res-tabela-tbody');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="${colspan}" class="loading"><span class="spinner"></span> A carregar...</td></tr>`;
        }

        try {
            const data = await apiGet(`/api/resultados/${this.jobId}/tabela?limit=${this.PAGE_SIZE}&offset=${offset}`);
            if (data.error) { toast('Erro na tabela: ' + data.error, 'error'); return; }

            this.tabelaTotal = parseInt(data.total || 0);
            this.renderTabelaRows(data.rows || []);
            this.updatePaginacao();
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    renderTabelaRows(rows) {
        const tbody = document.getElementById('res-tabela-tbody');
        if (!tbody) return;
        const colspan = this.tipo === 'otimizacao' ? 11 : 9;

        if (rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-muted" style="padding:2rem">Sem dados</td></tr>`;
            return;
        }

        if (this.tipo === 'otimizacao') {
            tbody.innerHTML = rows.map(r => {
                const dP = parseFloat(r.delta_preco || 0);
                const dL = parseFloat(r.delta_lucro || 0);
                const dPStyle = dP < 0 ? 'color:#dc2626;font-weight:600' : (dP > 0 ? 'color:#16a34a;font-weight:600' : '');
                const dLStyle = dL > 0 ? 'color:#16a34a;font-weight:600' : (dL < 0 ? 'color:#dc2626;font-weight:600' : '');
                return `<tr>
                    <td>${escapeHtml(r.data || '')}</td>
                    <td>${escapeHtml(r.hora_raw || String(r.hora_num || ''))}</td>
                    <td>${escapeHtml(r.pais || '')}</td>
                    <td class="text-right">${this.fmtNum(r.preco_orig, 2)}</td>
                    <td class="text-right">${this.fmtNum(r.preco_base, 2)}</td>
                    <td class="text-right">${this.fmtNum(r.preco_sim, 2)}</td>
                    <td class="text-right" style="${dPStyle}">${dP >= 0 ? '+' : ''}${this.fmtNum(dP, 2)}</td>
                    <td class="text-right">${this.fmtNum(r.lucro_pre_base, 0)}</td>
                    <td class="text-right">${this.fmtNum(r.lucro_pre_opt, 0)}</td>
                    <td class="text-right" style="${dLStyle}">${dL >= 0 ? '+' : ''}${this.fmtNum(dL, 0)}</td>
                    <td class="text-right">${r.n_bids_sub ?? 0}</td>
                </tr>`;
            }).join('');
            return;
        }

        // Substituição
        tbody.innerHTML = rows.map(r => {
            const delta = parseFloat(r.delta_preco || 0);
            const dStyle = delta < 0
                ? 'color:#16a34a;font-weight:600'
                : (delta > 0 ? 'color:#dc2626;font-weight:600' : '');
            const dStr = (delta >= 0 ? '+' : '') + this.fmtNum(delta, 2);

            return `<tr>
                <td>${escapeHtml(r.data || '')}</td>
                <td>${escapeHtml(r.hora_raw || String(r.hora_num || ''))}</td>
                <td>${escapeHtml(r.pais || '')}</td>
                <td class="text-right">${this.fmtNum(r.preco_orig, 2)}</td>
                <td class="text-right">${this.fmtNum(r.preco_sim, 2)}</td>
                <td class="text-right" style="${dStyle}">${dStr}</td>
                <td class="text-right">${this.fmtVol(r.volume_clearing_orig)}</td>
                <td class="text-right">${this.fmtVol(r.volume_sim)}</td>
                <td class="text-right">${r.n_bids_sub ?? 0}</td>
            </tr>`;
        }).join('');
    },

    updatePaginacao() {
        const info    = document.getElementById('res-tabela-info');
        const btnPrev = document.getElementById('res-btn-prev');
        const btnNext = document.getElementById('res-btn-next');

        const from = this.tabelaTotal === 0 ? 0 : this.tabelaOffset + 1;
        const to   = Math.min(this.tabelaOffset + this.PAGE_SIZE, this.tabelaTotal);

        if (info)    info.textContent  = `${from}–${to} de ${this.tabelaTotal.toLocaleString('pt-PT')}`;
        if (btnPrev) btnPrev.disabled  = this.tabelaOffset === 0;
        if (btnNext) btnNext.disabled  = (this.tabelaOffset + this.PAGE_SIZE) >= this.tabelaTotal;
    },

    prevPage() {
        if (this.tabelaOffset === 0) return;
        this.loadTabela(Math.max(0, this.tabelaOffset - this.PAGE_SIZE));
    },

    nextPage() {
        if ((this.tabelaOffset + this.PAGE_SIZE) >= this.tabelaTotal) return;
        this.loadTabela(this.tabelaOffset + this.PAGE_SIZE);
    },

    async setPais(pais) {
        this.pais = pais;
        this.tabelaOffset = 0;
        await Promise.all([this.loadCharts(), this.loadTabela(0)]);
    },

    fmtNum(v, decimals = 2) {
        const n = parseFloat(v);
        if (isNaN(n)) return '—';
        return n.toLocaleString('pt-PT', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals,
        });
    },

    fmtVol(v) {
        const n = parseFloat(v);
        if (isNaN(n)) return '—';
        return n.toLocaleString('pt-PT', {
            minimumFractionDigits: 1,
            maximumFractionDigits: 1,
        });
    },
};

/**
 * Entry point called by switchTab('resultados', jobId) from EstudosTab
 */
function loadResultados(jobId) {
    ResultadosTab.load(jobId);
}

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
        const { tabId, jobId } = e.detail;
        if (tabId === 'classificacao') {
            ClassificacaoTab.init();
        } else if (tabId === 'parametros') {
            ParametrosTab.init();
        } else if (tabId === 'estudos') {
            EstudosTab.init();
        } else if (tabId === 'resultados') {
            const id = jobId || window.resultadosJobId;
            if (id) {
                window.resultadosJobId = id;
                ResultadosTab.load(id);
            }
        } else if (tabId === 'ingestao') {
            IngestaoTab.init();
        } else if (tabId === 'explorador') {
            ExploradorTab.init();
        }
    });
});

// ============================================================================
// Ingestão de Dados Tab (Tab 5)
// ============================================================================

const IngestaoTab = {
    // queue items: { file, status, error, progress, jobId, jobStatus }
    // status: waiting|uploading|ingesting|done|error|invalid
    queue: [],
    _pollTimer: null,

    // ------------------------------------------------------------------
    // Initialisation
    // ------------------------------------------------------------------
    init() {
        this._bindDropzone();
        this.loadSummary();
    },

    // ------------------------------------------------------------------
    // Drag-and-drop + file input
    // ------------------------------------------------------------------
    _bindDropzone() {
        const zone  = document.getElementById('ingestao-dropzone');
        const input = document.getElementById('ingestao-file-input');
        if (!zone || zone._bound) return;
        zone._bound = true;

        zone.addEventListener('click', (e) => {
            if (e.target.tagName !== 'LABEL') input.click();
        });
        input.addEventListener('change', () => {
            this._enqueue([...input.files]);
            input.value = '';
        });

        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            zone.classList.add('drag-over');
        });
        ['dragleave', 'dragend'].forEach(evt =>
            zone.addEventListener(evt, () => zone.classList.remove('drag-over'))
        );
        zone.addEventListener('drop', (e) => {
            e.preventDefault();
            zone.classList.remove('drag-over');
            const files = [...(e.dataTransfer.files || [])].filter(f => f.name.endsWith('.zip'));
            if (files.length === 0) {
                toast('Apenas ficheiros .zip são aceites.', 'warning');
                return;
            }
            this._enqueue(files);
        });
    },

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    _formatBytes(bytes) {
        if (bytes < 1024)        return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    },

    _formatRows(n) {
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + ' M';
        if (n >= 1_000)     return (n / 1_000).toFixed(0) + ' K';
        return String(n);
    },

    _isValidName(name) {
        return /^curva_pbc_uof_\d{6}\.zip$/.test(name);
    },

    // ------------------------------------------------------------------
    // Queue management
    // ------------------------------------------------------------------
    _enqueue(files) {
        for (const f of files) {
            const valid = this._isValidName(f.name);
            this.queue.push({
                file:      f,
                status:    valid ? 'waiting' : 'invalid',
                error:     valid ? null : `Nome inválido: "${f.name}". Esperado: curva_pbc_uof_YYYYMM.zip`,
                progress:  0,
                jobId:     null,
                jobStatus: null,
            });
        }
        this._renderQueue();
        this._processQueue();
    },

    clearQueue() {
        this.queue = this.queue.filter(i => i.status === 'uploading' || i.status === 'ingesting');
        this._renderQueue();
        if (this.queue.length === 0 && this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
    },

    // ------------------------------------------------------------------
    // Upload processing (serial uploads, parallel ingestion)
    // ------------------------------------------------------------------
    _uploading: false,

    async _processQueue() {
        if (this._uploading) return;
        const next = this.queue.find(i => i.status === 'waiting');
        if (!next) return;

        this._uploading = true;
        next.status = 'uploading';
        this._renderQueue();

        try {
            const fd  = new FormData();
            fd.append('file', next.file);

            const xhr = new XMLHttpRequest();
            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    next.progress = Math.round((e.loaded / e.total) * 100);
                    this._renderQueue();
                }
            };

            await new Promise((resolve) => {
                xhr.open('POST', '/api/ingestao');
                xhr.onload = () => {
                    let json;
                    try { json = JSON.parse(xhr.responseText); } catch (_) { json = {}; }
                    if (xhr.status === 201 && json.job_id) {
                        next.status    = 'ingesting';
                        next.jobId     = json.job_id;
                        next.jobStatus = 'RUNNING';
                        // Start polling
                        this._startPolling();
                    } else {
                        next.status = 'error';
                        next.error  = json.error || `HTTP ${xhr.status}`;
                    }
                    resolve();
                };
                xhr.onerror = () => {
                    next.status = 'error';
                    next.error  = 'Erro de rede.';
                    resolve();
                };
                xhr.send(fd);
            });
        } catch (err) {
            next.status = 'error';
            next.error  = err.message;
        }

        this._uploading = false;
        this._renderQueue();
        this._processQueue();
    },

    // ------------------------------------------------------------------
    // Poll ingestion job status
    // ------------------------------------------------------------------
    _startPolling() {
        if (this._pollTimer) return;
        this._pollTimer = setInterval(() => this._pollJobs(), 2000);
    },

    async _pollJobs() {
        const ingesting = this.queue.filter(i => i.status === 'ingesting' && i.jobId);
        if (ingesting.length === 0) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
            return;
        }

        for (const item of ingesting) {
            try {
                const data = await apiGet(`/api/estudos/${item.jobId}`);
                const job  = data.job || data;
                item.jobStatus = job.status;

                if (job.status === 'DONE') {
                    item.status = 'done';
                    // Refresh ClickHouse summary
                    this.loadSummary();
                } else if (job.status === 'FAILED') {
                    item.status = 'error';
                    item.error  = job.erro || 'Worker falhou';
                }
            } catch (_) {
                // Ignore transient network errors
            }
        }
        this._renderQueue();
    },

    // ------------------------------------------------------------------
    // Render upload/ingestão queue
    // ------------------------------------------------------------------
    _renderQueue() {
        const card = document.getElementById('ingestao-queue-card');
        const list = document.getElementById('ingestao-queue-list');
        if (!card || !list) return;

        if (this.queue.length === 0) {
            card.style.display = 'none';
            return;
        }
        card.style.display = '';

        list.innerHTML = this.queue.map((item) => {
            const sizeStr = this._formatBytes(item.file.size);
            let statusHtml = '';
            switch (item.status) {
                case 'waiting':
                    statusHtml = `<span class="iq-status-waiting">A aguardar</span>`;
                    break;
                case 'uploading': {
                    const pct = item.progress ?? 0;
                    statusHtml = `
                        <div style="flex:1;min-width:140px">
                            <div class="flex items-center gap-1">
                                <span class="spinner"></span>
                                <span class="iq-status-uploading">A enviar… ${pct}%</span>
                            </div>
                            <div class="ingestao-progress">
                                <div class="ingestao-progress-bar" style="width:${pct}%"></div>
                            </div>
                        </div>`;
                    break;
                }
                case 'ingesting':
                    statusHtml = `
                        <div class="flex items-center gap-1">
                            <span class="spinner"></span>
                            <span class="iq-status-uploading">A ingerir no ClickHouse…</span>
                        </div>`;
                    break;
                case 'done':
                    statusHtml = `<span class="iq-status-done">&#10003; Ingerido</span>`;
                    break;
                case 'error':
                    statusHtml = item.error
                        ? `<span class="iq-status-error">&#10007; ${escapeHtml(item.error)}</span>`
                        : `<span class="iq-status-error">&#10007; Erro desconhecido</span>`;
                    break;
                case 'invalid':
                    statusHtml = item.error
                        ? `<span class="iq-status-invalid">&#9888; ${escapeHtml(item.error)}</span>`
                        : `<span class="iq-status-invalid">&#9888; Nome inválido</span>`;
                    break;
            }

            return `
                <div class="ingestao-queue-item">
                    <span class="ingestao-queue-name">${escapeHtml(item.file.name)}</span>
                    <span class="ingestao-queue-size">${sizeStr}</span>
                    <span class="ingestao-queue-status">${statusHtml}</span>
                </div>`;
        }).join('');
    },

    // ------------------------------------------------------------------
    // ClickHouse data summary
    // ------------------------------------------------------------------
    async loadSummary() {
        const content = document.getElementById('ingestao-ch-content');
        const counter = document.getElementById('ingestao-counter');
        if (!content) return;

        try {
            const data  = await apiGet('/api/ingestao');
            const meses = data.meses || [];

            if (counter) {
                const total = data.total_rows || 0;
                counter.textContent = `${meses.length} mês(es) · ${this._formatRows(total)} bids`;
            }

            if (data.ch_error) {
                content.innerHTML = `<div class="text-muted" style="padding:1.5rem">
                    ClickHouse indisponível: ${escapeHtml(data.ch_error)}
                </div>`;
                return;
            }

            if (meses.length === 0) {
                content.innerHTML = `<div style="padding:2rem;text-align:center;color:var(--color-text-muted)">
                    <p style="font-size:1.5rem;margin:0 0 0.5rem">&#128190;</p>
                    <p style="margin:0">Nenhum dado ingerido ainda.</p>
                    <p style="margin:0.25rem 0 0;font-size:0.875rem">
                        Arraste um ficheiro <code>curva_pbc_uof_YYYYMM.zip</code> para a zona acima.
                    </p>
                </div>`;
                return;
            }

            // Render month cards
            const cardsHtml = meses.map(m => `
                <div class="ingestao-month-card">
                    <div class="ingestao-month-label">${escapeHtml(m.mes_label)}</div>
                    <div class="ingestao-month-rows">${this._formatRows(m.n_rows)} <span class="ingestao-month-unit">bids</span></div>
                    <div class="ingestao-month-meta">
                        ${m.n_dias} dia(s) &nbsp;·&nbsp; ${escapeHtml(m.data_min)} a ${escapeHtml(m.data_max)}
                    </div>
                    <div class="ingestao-month-actions">
                        <button class="btn btn-danger btn-sm"
                            onclick="IngestaoTab.deleteMes('${escapeHtml(m.mes)}', '${escapeHtml(m.mes_label)}')">
                            Eliminar
                        </button>
                    </div>
                </div>
            `).join('');

            content.innerHTML = `<div class="ingestao-months-grid">${cardsHtml}</div>`;

        } catch (e) {
            if (content) content.innerHTML = `<div class="text-muted" style="padding:1rem">
                Erro ao carregar resumo: ${escapeHtml(e.message)}
            </div>`;
            if (counter) counter.textContent = 'Erro';
        }
    },

    async deleteMes(yyyymm, label) {
        if (!confirm(`Eliminar TODOS os dados do mês ${label} (${yyyymm}) do ClickHouse?\n\nEsta acção não pode ser desfeita.`)) return;
        try {
            const data = await apiDelete(`/api/ingestao/mes/${encodeURIComponent(yyyymm)}`);
            if (data.success) {
                toast(`Mês ${label} eliminado (${this._formatRows(data.deleted || 0)} bids removidos).`, 'success');
                this.loadSummary();
            } else {
                toast(data.error || 'Erro ao eliminar.', 'error');
            }
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },
};

// ============================================================================
// Explorador de Dados Tab (Tab 6)
// ============================================================================

const ExploradorTab = {
    _charts: {},
    init() {
        // Não carrega automaticamente — aguarda o utilizador seleccionar período
    },

    // -------------------------------------------------------------------------
    // Filtros
    // -------------------------------------------------------------------------

    _getQs() {
        const params = new URLSearchParams();
        const de   = document.getElementById('exp-de')?.value   || '';
        const ate  = document.getElementById('exp-ate')?.value  || '';
        const pais = document.getElementById('exp-pais')?.value || '';
        const tipo = document.getElementById('exp-tipo')?.value || '';
        if (de)   params.set('de',   de);
        if (ate)  params.set('ate',  ate);
        if (pais) params.set('pais', pais);
        if (tipo) params.set('tipo', tipo);
        const qs = params.toString();
        return qs ? '?' + qs : '';
    },

    async aplicarFiltros() {
        const deEl  = document.getElementById('exp-de');
        const ateEl = document.getElementById('exp-ate');
        const de    = deEl?.value  || '';
        const ate   = ateEl?.value || '';

        // Validação visual: destacar campos em falta
        if (deEl)  deEl.style.borderColor  = de  ? '' : 'var(--color-error)';
        if (ateEl) ateEl.style.borderColor = ate ? '' : 'var(--color-error)';

        const errorDiv = document.getElementById('exp-error-state');
        const infoDiv  = document.getElementById('exp-empty-state');

        if (!de || !ate) {
            if (errorDiv) { errorDiv.style.display = 'flex'; }
            if (infoDiv)  { infoDiv.style.display  = 'none'; }
            if (!de) deEl?.focus();
            return;
        }

        // Esconder mensagens de estado, mostrar conteúdo
        if (errorDiv) errorDiv.style.display = 'none';
        if (infoDiv)  infoDiv.style.display  = 'none';
        const content = document.getElementById('exp-main-content');
        if (content) content.style.display = '';

        await Promise.all([
            this.loadOverview(),
            this.loadDistribuicao(),
            this.loadPerfilHorario(),
            this.loadHistograma(),
            this.loadCategorias(),
            this.loadTendenciaMensal(),
            this.loadTopUnidades(),
        ]);
    },

    limparFiltros() {
        ['exp-de', 'exp-ate', 'exp-pais', 'exp-tipo'].forEach(id => {
            const el = document.getElementById(id);
            if (el) { el.value = ''; el.style.borderColor = ''; }
        });
        // Voltar ao estado inicial
        const infoDiv  = document.getElementById('exp-empty-state');
        const errorDiv = document.getElementById('exp-error-state');
        const content  = document.getElementById('exp-main-content');
        if (infoDiv)  infoDiv.style.display  = 'flex';
        if (errorDiv) errorDiv.style.display = 'none';
        if (content)  content.style.display  = 'none';
        // Destruir gráficos para não persistir dados antigos
        Object.keys(this._charts).forEach(k => this._destroyChart(k));
    },

    // -------------------------------------------------------------------------
    // Helpers de formatação
    // -------------------------------------------------------------------------

    _fmtNum(v, dec = 0) {
        const n = parseFloat(v);
        if (isNaN(n)) return '—';
        return n.toLocaleString('pt-PT', {
            minimumFractionDigits: dec,
            maximumFractionDigits: dec,
        });
    },

    _fmtBig(v) {
        const n = parseFloat(v);
        if (isNaN(n)) return '—';
        if (n >= 1e9) return (n / 1e9).toFixed(1) + ' G';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + ' M';
        if (n >= 1e3) return (n / 1e3).toFixed(0) + ' K';
        return String(Math.round(n));
    },

    _destroyChart(key) {
        if (this._charts[key]) {
            this._charts[key].destroy();
            this._charts[key] = null;
        }
    },

    // -------------------------------------------------------------------------
    // Overview — KPI Cards
    // -------------------------------------------------------------------------

    async loadOverview() {
        try {
            const d = await apiGet('/api/explorador/overview' + this._getQs());
            if (d.error) return;
            const wrap = document.getElementById('exp-kpi-cards');
            if (!wrap) return;

            const dP = (v, dec) => this._fmtNum(v, dec);
            const dB = (v)       => this._fmtBig(v);

            wrap.innerHTML = `
                <div class="stat-card">
                    <div class="stat-card-label">Total de Bids</div>
                    <div class="stat-card-value">${dB(d.total_bids)}</div>
                    <div class="stat-card-unit">${d.n_meses || '—'} meses disponíveis</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Unidades Distintas</div>
                    <div class="stat-card-value">${dP(d.n_unidades, 0)}</div>
                    <div class="stat-card-unit">unidades geradoras</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Energia Total</div>
                    <div class="stat-card-value">${dB(d.total_energia)}</div>
                    <div class="stat-card-unit">MWh ofertados</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Preço Médio</div>
                    <div class="stat-card-value">${dP(d.preco_medio, 2)}</div>
                    <div class="stat-card-unit">€/MWh</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Período</div>
                    <div class="stat-card-value" style="font-size:1rem">${escapeHtml(d.data_inicio || '—')}</div>
                    <div class="stat-card-unit">até ${escapeHtml(d.data_fim || '—')}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-label">Bids Venda / Compra</div>
                    <div class="stat-card-value" style="font-size:1rem">${dB(d.n_bids_venda)}</div>
                    <div class="stat-card-unit">V · ${dB(d.n_bids_compra)} C</div>
                </div>
            `;
        } catch (e) {
            const wrap = document.getElementById('exp-kpi-cards');
            if (wrap) wrap.innerHTML = `<span class="text-muted text-sm">Erro ao carregar KPIs: ${escapeHtml(e.message)}</span>`;
        }
    },

    // -------------------------------------------------------------------------
    // Distribuição por País e Tipo — Doughnut
    // -------------------------------------------------------------------------

    async loadDistribuicao() {
        try {
            const data = await apiGet('/api/explorador/distribuicao' + this._getQs());
            this._renderDistrib(data.por_pais || []);
        } catch (e) {
            console.error('loadDistribuicao', e);
        }
    },

    _renderDistrib(rows) {
        this._destroyChart('distrib');
        const ctx = document.getElementById('exp-chart-distrib');
        if (!ctx) return;

        if (rows.length === 0) {
            ctx.parentElement.innerHTML = '<p class="text-muted text-center" style="padding:2rem">Sem dados</p>';
            return;
        }

        const palette = {
            'MI V': '#f59e0b', 'MI C': '#fcd34d',
            'ES V': '#2563eb', 'ES C': '#93c5fd',
            'PT V': '#16a34a', 'PT C': '#86efac',
        };

        const labels = rows.map(r => `${r.pais} ${r.tipo_oferta === 'V' ? 'Venda' : 'Compra'}`);
        const energias = rows.map(r => parseFloat(r.total_energia || 0));
        const bgColors = rows.map(r => {
            const key = `${r.pais} ${r.tipo_oferta}`;
            return palette[key] || '#94a3b8';
        });

        this._charts.distrib = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{
                    data: energias,
                    backgroundColor: bgColors,
                    borderWidth: 2,
                    borderColor: '#fff',
                }],
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { position: 'right', labels: { font: { size: 12 } } },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const v = ctx.raw;
                                const tot = ctx.dataset.data.reduce((a, b) => a + b, 0);
                                const pct = tot > 0 ? ((v / tot) * 100).toFixed(1) : 0;
                                return ` ${(v / 1e6).toFixed(1)} M MWh  (${pct}%)`;
                            },
                        },
                    },
                },
            },
        });
    },

    // -------------------------------------------------------------------------
    // Perfil Horário — Line chart H1-H24
    // -------------------------------------------------------------------------

    async loadPerfilHorario() {
        try {
            const data = await apiGet('/api/explorador/perfil-horario' + this._getQs());
            this._renderHorario(Array.isArray(data) ? data : []);
        } catch (e) {
            console.error('loadPerfilHorario', e);
        }
    },

    _renderHorario(rows) {
        this._destroyChart('horario');
        const ctx = document.getElementById('exp-chart-horario');
        if (!ctx) return;

        const paises = [...new Set(rows.map(r => r.pais))].sort();
        const horas  = Array.from({ length: 24 }, (_, i) => i + 1);
        const colors = { MI: '#f59e0b', ES: '#2563eb', PT: '#16a34a' };

        const datasets = paises.map(p => ({
            label: p,
            data: horas.map(h => {
                const r = rows.find(x => x.pais === p && parseInt(x.hora_num) === h);
                return r ? parseFloat(r.preco_medio) : null;
            }),
            borderColor: colors[p] || '#94a3b8',
            backgroundColor: 'transparent',
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 3,
            spanGaps: true,
        }));

        this._charts.horario = new Chart(ctx, {
            type: 'line',
            data: { labels: horas.map(h => `H${h}`), datasets },
            options: {
                responsive: true,
                plugins: { legend: { position: 'top' } },
                scales: { y: { title: { display: true, text: '€/MWh' } } },
            },
        });
    },

    // -------------------------------------------------------------------------
    // Histograma de Preços — Bar chart
    // -------------------------------------------------------------------------

    async loadHistograma() {
        try {
            const data = await apiGet('/api/explorador/histograma' + this._getQs());
            this._renderHistograma(Array.isArray(data) ? data : []);
        } catch (e) {
            console.error('loadHistograma', e);
        }
    },

    _renderHistograma(rows) {
        this._destroyChart('histograma');
        const ctx = document.getElementById('exp-chart-histograma');
        if (!ctx) return;

        const bgColors = rows.map(r => {
            if (r.faixa === 'Negativo') return '#94a3b8';
            const o = parseInt(r.ordem);
            if (o <= 2)  return '#86efac'; // barato
            if (o <= 5)  return '#fcd34d'; // médio
            if (o <= 8)  return '#fb923c'; // caro
            return '#f87171';              // muito caro
        });

        this._charts.histograma = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: rows.map(r => r.faixa),
                datasets: [{
                    label: 'Nº Bids',
                    data: rows.map(r => parseInt(r.n_bids)),
                    backgroundColor: bgColors,
                }],
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { title: { display: true, text: '€/MWh' } },
                    y: { title: { display: true, text: 'Nº Bids' } },
                },
            },
        });
    },

    // -------------------------------------------------------------------------
    // Volume por Categoria — Horizontal bar
    // -------------------------------------------------------------------------

    async loadCategorias() {
        try {
            const data = await apiGet('/api/explorador/categorias' + this._getQs());
            this._renderCategorias(Array.isArray(data) ? data : []);
        } catch (e) {
            console.error('loadCategorias', e);
        }
    },

    _renderCategorias(rows) {
        this._destroyChart('categorias');
        const ctx = document.getElementById('exp-chart-categorias');
        if (!ctx) return;

        const top = rows.slice(0, 14);
        const palette = [
            '#2563eb', '#16a34a', '#f59e0b', '#dc2626', '#0ea5e9', '#7c3aed',
            '#db2777', '#059669', '#d97706', '#1d4ed8', '#15803d', '#b91c1c',
            '#0284c7', '#6d28d9',
        ];

        this._charts.categorias = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: top.map(r => r.categoria),
                datasets: [{
                    label: 'Energia (M MWh)',
                    data: top.map(r => parseFloat(r.total_energia) / 1e6),
                    backgroundColor: palette,
                }],
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { x: { title: { display: true, text: 'M MWh' } } },
            },
        });
    },

    // -------------------------------------------------------------------------
    // Tendência Mensal — Mixed bar+line
    // -------------------------------------------------------------------------

    async loadTendenciaMensal() {
        try {
            const data = await apiGet('/api/explorador/tendencia-mensal' + this._getQs());
            this._renderMensal(Array.isArray(data) ? data : []);
        } catch (e) {
            console.error('loadTendenciaMensal', e);
        }
    },

    _renderMensal(rows) {
        this._destroyChart('mensal');
        const ctx = document.getElementById('exp-chart-mensal');
        if (!ctx) return;

        this._charts.mensal = new Chart(ctx, {
            data: {
                labels: rows.map(r => r.mes),
                datasets: [
                    {
                        type: 'bar',
                        label: 'Energia (M MWh)',
                        data: rows.map(r => parseFloat(r.total_energia) / 1e6),
                        backgroundColor: '#93c5fd',
                        yAxisID: 'yE',
                    },
                    {
                        type: 'line',
                        label: 'Preço Médio (€/MWh)',
                        data: rows.map(r => parseFloat(r.preco_medio)),
                        borderColor: '#dc2626',
                        backgroundColor: 'transparent',
                        tension: 0.3,
                        pointRadius: 4,
                        borderWidth: 2,
                        yAxisID: 'yP',
                    },
                ],
            },
            options: {
                responsive: true,
                plugins: { legend: { position: 'top' } },
                scales: {
                    yE: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'M MWh' },
                    },
                    yP: {
                        type: 'linear',
                        position: 'right',
                        title: { display: true, text: '€/MWh' },
                        grid: { drawOnChartArea: false },
                    },
                },
            },
        });
    },

    // -------------------------------------------------------------------------
    // Top Unidades Geradoras — Table
    // -------------------------------------------------------------------------

    async loadTopUnidades() {
        const qs      = this._getQs();
        const sort    = document.getElementById('exp-top-sort')?.value  || 'energia';
        const limit   = document.getElementById('exp-top-limit')?.value || '25';
        const sep     = qs ? '&' : '?';
        const url     = `/api/explorador/top-unidades${qs}${sep}sort=${sort}&limit=${limit}`;
        const tbody   = document.getElementById('exp-top-tbody');

        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="11" class="loading"><span class="spinner"></span> A carregar...</td></tr>`;
        }

        try {
            const data = await apiGet(url);
            if (data && data.error) {
                if (tbody) tbody.innerHTML = `<tr><td colspan="11" class="text-muted" style="padding:1rem">Erro do servidor: ${escapeHtml(data.error)}</td></tr>`;
                return;
            }
            const rows = Array.isArray(data) ? data : [];
            this._renderTopUnidades(rows);
        } catch (e) {
            if (tbody) {
                tbody.innerHTML = `<tr><td colspan="11" class="text-muted" style="padding:1rem">Erro: ${escapeHtml(e.message)}</td></tr>`;
            }
        }
    },

    _renderTopUnidades(rows) {
        const tbody = document.getElementById('exp-top-tbody');
        if (!tbody) return;

        if (!Array.isArray(rows) || rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="11" class="text-center text-muted" style="padding:2rem">Sem dados</td></tr>`;
            return;
        }

        tbody.innerHTML = rows.map((r, i) => `
            <tr>
                <td class="text-muted" style="font-size:0.8125rem">${i + 1}</td>
                <td><code style="font-size:0.8125rem">${escapeHtml(r.unidade || '')}</code></td>
                <td class="text-muted" style="font-size:0.8125rem;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                    ${escapeHtml(r.descricao || '—')}
                </td>
                <td><span class="badge" style="font-size:0.75rem">${escapeHtml(r.regime || '—')}</span></td>
                <td class="text-muted" style="font-size:0.8125rem">${escapeHtml(r.categoria || '—')}</td>
                <td style="font-size:0.8125rem">${escapeHtml(r.zona_frontera || '—')}</td>
                <td class="text-right">${this._fmtBig(r.n_bids)}</td>
                <td class="text-right">${this._fmtBig(r.total_energia)}</td>
                <td class="text-right">${this._fmtNum(r.preco_medio, 2)}</td>
                <td class="text-right" style="color:var(--color-success)">${this._fmtNum(r.preco_min, 2)}</td>
                <td class="text-right" style="color:var(--color-error)">${this._fmtNum(r.preco_max, 2)}</td>
            </tr>
        `).join('');
    },

    // -------------------------------------------------------------------------
    // Console SQL
    // -------------------------------------------------------------------------

    SQL_PRESETS: {
        overview: `SELECT
    pais,
    tipo_oferta,
    count()              AS n_bids,
    round(sum(energia))  AS total_energia_mwh,
    round(avg(precio),2) AS preco_medio,
    round(min(precio),2) AS preco_min,
    round(max(precio),2) AS preco_max
FROM mibel.bids_raw
GROUP BY pais, tipo_oferta
ORDER BY pais, tipo_oferta`,

        horas_caras: `SELECT
    hora_num,
    pais,
    round(avg(precio), 2)    AS preco_medio,
    count()                  AS n_bids,
    round(sum(energia))      AS total_mwh
FROM mibel.bids_raw
WHERE tipo_oferta = 'V'
GROUP BY hora_num, pais
ORDER BY preco_medio DESC
LIMIT 20`,

        unidade_preco: `SELECT
    b.unidade,
    any(u.categoria)         AS categoria,
    any(u.regime)            AS regime,
    round(avg(b.precio), 2)  AS preco_medio,
    round(sum(b.energia))    AS total_mwh,
    count()                  AS n_bids
FROM mibel.bids_raw b
LEFT JOIN mibel.unidades u ON b.unidade = u.codigo
WHERE b.tipo_oferta = 'V'
GROUP BY b.unidade
ORDER BY total_mwh DESC
LIMIT 20`,

        curva_oferta: `SELECT
    round(precio / 10) * 10  AS faixa_preco,
    round(sum(energia))      AS total_mwh,
    count()                  AS n_bids
FROM mibel.bids_raw
WHERE tipo_oferta = 'V'
GROUP BY faixa_preco
ORDER BY faixa_preco
LIMIT 30`,

        bids_hora_dia: `SELECT
    toDayOfWeek(data_ficheiro)       AS dia_semana,
    hora_num,
    round(avg(precio), 2)            AS preco_medio,
    round(avg(energia), 2)           AS energia_media
FROM mibel.bids_raw
WHERE tipo_oferta = 'V'
GROUP BY dia_semana, hora_num
ORDER BY dia_semana, hora_num`,
    },

    setPreset(name) {
        const sql = this.SQL_PRESETS[name];
        if (!sql) return;
        const ta = document.getElementById('exp-sql-input');
        if (ta) ta.value = sql;
    },

    async runQuery() {
        const ta     = document.getElementById('exp-sql-input');
        const status = document.getElementById('exp-sql-status');
        const result = document.getElementById('exp-sql-result');
        const sql    = ta?.value?.trim() || '';

        if (!sql) { toast('Introduza uma query SQL.', 'warning'); return; }
        if (status) status.textContent = 'A executar…';
        if (result) result.style.display = 'none';

        try {
            const t0   = Date.now();
            const data = await apiPost('/api/explorador/query', { sql });
            const ms   = Date.now() - t0;

            if (data.error) {
                if (status) status.textContent = 'Erro: ' + data.error;
                toast('Erro SQL: ' + data.error, 'error');
                return;
            }

            if (status) status.textContent = `${data.count} linha(s) · ${ms} ms`;
            this._renderSqlResult(data.rows || []);
            if (result) result.style.display = '';
        } catch (e) {
            if (status) status.textContent = 'Erro: ' + e.message;
            toast('Erro: ' + e.message, 'error');
        }
    },

    _renderSqlResult(rows) {
        const thead = document.getElementById('exp-sql-thead');
        const tbody = document.getElementById('exp-sql-tbody');
        if (!thead || !tbody) return;

        if (rows.length === 0) {
            thead.innerHTML = '';
            tbody.innerHTML = `<tr><td class="text-muted" style="padding:1rem">Sem resultados.</td></tr>`;
            return;
        }

        const cols   = Object.keys(rows[0]);
        thead.innerHTML = `<tr>${cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr>`;
        tbody.innerHTML = rows.map(r =>
            `<tr>${cols.map(c => `<td>${escapeHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`
        ).join('');
    },
};
