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

        const paisSel = document.getElementById('res-pais-filter');
        if (paisSel) paisSel.value = '';

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
            const nPeriodos = parseInt(stats.n_periodos || 0);

            // Header
            const titulo = document.getElementById('res-titulo');
            if (titulo) {
                const tipo = job?.tipo === 'otimizacao' ? 'Optimização' : 'Substituição';
                titulo.textContent = `Resultados — ${tipo}`;
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
                document.getElementById('res-no-data').hidden         = false;
                document.getElementById('res-charts-section').hidden  = true;
                document.getElementById('res-tabela-section').hidden  = true;
                document.getElementById('res-stat-cards').innerHTML   = '';
                return;
            }

            document.getElementById('res-no-data').hidden        = true;
            document.getElementById('res-charts-section').hidden = false;
            document.getElementById('res-tabela-section').hidden = false;

            this.renderStatCards(stats);
            await Promise.all([this.loadCharts(), this.loadTabela(0)]);
        } catch (e) {
            toast('Erro: ' + e.message, 'error');
        }
    },

    renderStatCards(stats) {
        const container = document.getElementById('res-stat-cards');
        if (!container) return;

        const delta = parseFloat(stats.delta_medio || 0);
        const deltaClass = delta < 0 ? 'negative' : (delta > 0 ? 'positive' : '');

        container.innerHTML = `
            <div class="stat-card">
                <div class="stat-card-label">Preço médio original</div>
                <div class="stat-card-value">${this.fmtNum(stats.preco_orig_medio, 2)}</div>
                <div class="stat-card-unit">€/MWh</div>
            </div>
            <div class="stat-card">
                <div class="stat-card-label">Preço médio simulado</div>
                <div class="stat-card-value">${this.fmtNum(stats.preco_sub_medio, 2)}</div>
                <div class="stat-card-unit">€/MWh</div>
            </div>
            <div class="stat-card">
                <div class="stat-card-label">Delta médio</div>
                <div class="stat-card-value ${deltaClass}">${delta >= 0 ? '+' : ''}${this.fmtNum(stats.delta_medio, 2)}</div>
                <div class="stat-card-unit">€/MWh · min ${this.fmtNum(stats.delta_min, 2)} / max ${this.fmtNum(stats.delta_max, 2)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-card-label">Bids substituídos</div>
                <div class="stat-card-value">${this.fmtNum(stats.total_bids_sub, 0)}</div>
                <div class="stat-card-unit">total</div>
            </div>
        `;
    },

    async loadCharts() {
        if (!this.jobId) return;
        try {
            const paisParam = this.pais ? `?pais=${this.pais}` : '';
            const data = await apiGet(`/api/resultados/${this.jobId}/serie${paisParam}`);
            if (data.error) return;
            this.renderChartSerie(data);
            this.renderChartDelta(data);
        } catch (e) {
            toast('Erro ao carregar gráficos: ' + e.message, 'error');
        }
    },

    renderChartSerie(data) {
        let labels = data.labels    || [];
        let orig   = data.preco_orig || [];
        let sub    = data.preco_sub  || [];

        // Subsample to ≤ 500 points for performance
        if (labels.length > 500) {
            const step = Math.ceil(labels.length / 500);
            labels = labels.filter((_, i) => i % step === 0);
            orig   = orig.filter((_, i)   => i % step === 0);
            sub    = sub.filter((_, i)    => i % step === 0);
        }

        if (this.chartSerie) { this.chartSerie.destroy(); this.chartSerie = null; }

        const ctx = document.getElementById('chart-serie');
        if (!ctx) return;

        this.chartSerie = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    { label: 'Original', data: orig, borderColor: '#2563eb', backgroundColor: 'transparent', tension: 0.1, pointRadius: 0, borderWidth: 1.5 },
                    { label: 'Simulado', data: sub,  borderColor: '#ea580c', backgroundColor: 'transparent', tension: 0.1, pointRadius: 0, borderWidth: 1.5 },
                ]
            },
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
        const deltas   = data.delta    || [];
        const horaNums = data.hora_num || [];

        // Aggregate by hora_num (1–24): compute mean delta per hour
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
        const bgColors = deltasPorHora.map(v => v === null ? '#94a3b8' : (v < 0 ? '#16a34a' : '#dc2626'));

        if (this.chartDelta) { this.chartDelta.destroy(); this.chartDelta = null; }

        const ctx2 = document.getElementById('chart-delta');
        if (!ctx2) return;

        this.chartDelta = new Chart(ctx2, {
            type: 'bar',
            data: {
                labels: Array.from({ length: 24 }, (_, i) => `H${i + 1}`),
                datasets: [{
                    label: 'Delta médio (€/MWh)',
                    data: deltasPorHora,
                    backgroundColor: bgColors,
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    y: { title: { display: true, text: '€/MWh' } }
                }
            }
        });
    },

    async loadTabela(offset = 0) {
        if (!this.jobId) return;
        this.tabelaOffset = offset;

        const tbody = document.getElementById('res-tabela-tbody');
        if (tbody) {
            tbody.innerHTML = '<tr><td colspan="9" class="loading"><span class="spinner"></span> A carregar...</td></tr>';
        }

        try {
            const paisParam = this.pais ? `&pais=${this.pais}` : '';
            const data = await apiGet(`/api/resultados/${this.jobId}/tabela?limit=${this.PAGE_SIZE}&offset=${offset}${paisParam}`);
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

        if (rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" class="text-center text-muted" style="padding:2rem">Sem dados</td></tr>';
            return;
        }

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
                <td class="text-right">${this.fmtNum(r.preco_clearing_orig, 2)}</td>
                <td class="text-right">${this.fmtNum(r.preco_clearing_sub, 2)}</td>
                <td class="text-right" style="${dStyle}">${dStr}</td>
                <td class="text-right">${this.fmtVol(r.volume_clearing_orig)}</td>
                <td class="text-right">${this.fmtVol(r.volume_clearing_sub)}</td>
                <td class="text-right">${r.n_bids_substituidos ?? 0}</td>
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
