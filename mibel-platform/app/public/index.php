<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MIBEL Bid Analyzer</title>
    <link rel="stylesheet" href="/css/app.css">
</head>
<body>
    <header>
        <h1><span>MIBEL</span> Bid Analyzer</h1>
        <div class="status-indicator">
            <span id="status-dot" class="status-dot"></span>
            <span id="status-text">A verificar...</span>
        </div>
    </header>

    <nav>
        <button data-tab="classificacao" class="active">Classificação</button>
        <button data-tab="parametros">Parâmetros</button>
        <button data-tab="estudos">Estudos</button>
        <button data-tab="resultados">Resultados</button>
    </nav>

    <main>
        <!-- ================================================================
             TAB 1: Classificação de Unidades
             ================================================================ -->
        <div id="tab-classificacao" class="tab-content active">
            <!-- Header with counter and export -->
            <div class="flex justify-between items-center mb-3">
                <div>
                    <h2 style="margin:0 0 0.25rem">Classificação de Unidades</h2>
                    <span id="classificacao-counter" class="badge">A carregar...</span>
                </div>
                <button class="btn btn-secondary" onclick="ClassificacaoTab.exportJson()">
                    Exportar JSON
                </button>
            </div>

            <!-- Main Classification Table -->
            <div class="card">
                <div class="card-header">
                    <h2>Mapeamento Tecnologia → Categoria</h2>
                </div>
                <div class="card-body" style="padding:0.75rem 1.25rem">
                    <!-- Filters -->
                    <div class="filters">
                        <div class="filter-group">
                            <label for="filter-tecnologia">Pesquisar:</label>
                            <input type="text" id="filter-tecnologia" class="form-input form-input-sm search-input" placeholder="Filtrar por tecnologia...">
                        </div>
                        <div class="filter-group">
                            <label for="filter-regime">Regime:</label>
                            <select id="filter-regime" class="form-select form-select-sm" style="min-width:150px">
                                <option value="">Todos</option>
                                <option value="PRE">PRE</option>
                                <option value="PRO">PRO</option>
                                <option value="CONSUMO">CONSUMO</option>
                                <option value="COMERCIALIZADOR">COMERCIALIZADOR</option>
                                <option value="GENERICA">GENERICA</option>
                                <option value="PORFOLIO">PORFOLIO</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:50px">#</th>
                                <th>Tecnologia (OMIE)</th>
                                <th style="width:150px">Regime</th>
                                <th style="width:200px">Categoria</th>
                                <th style="width:120px;text-align:right">Acções</th>
                            </tr>
                        </thead>
                        <tbody id="classificacao-tbody">
                            <tr>
                                <td colspan="5" class="loading">
                                    <span class="spinner"></span> A carregar...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Exceptions Table -->
            <div class="card">
                <div class="card-header">
                    <h2>Excepções por Código de Unidade</h2>
                </div>
                <div class="card-body" style="padding-bottom:0">
                    <div class="note note-info">
                        As excepções sobrepõem a classificação automática por tecnologia.
                        Use para corrigir unidades mal classificadas pelo OMIE (ex: unidades PT registadas como ES).
                    </div>
                </div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:150px">Código</th>
                                <th>Categoria Zona</th>
                                <th>Motivo</th>
                                <th style="width:80px;text-align:right">Acções</th>
                            </tr>
                        </thead>
                        <tbody id="excecoes-tbody">
                            <tr>
                                <td colspan="4" class="loading">
                                    <span class="spinner"></span> A carregar...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ================================================================
             TAB 2: Parâmetros de Substituição
             ================================================================ -->
        <div id="tab-parametros" class="tab-content">
            <!-- Header with actions -->
            <div class="flex justify-between items-center mb-3 flex-wrap gap-2">
                <div>
                    <h2 style="margin:0 0 0.25rem">Parâmetros de Substituição</h2>
                    <span class="text-muted text-sm">Configure escalas e escalões de preço por categoria</span>
                </div>
                <div class="flex gap-2">
                    <button class="btn btn-secondary" onclick="ParametrosTab.resetDefaults()">
                        Repor Padrões
                    </button>
                    <button class="btn btn-primary" onclick="ParametrosTab.saveAll()">
                        Guardar Tudo
                    </button>
                </div>
            </div>

            <!-- Add Category Form -->
            <div class="card mb-3" id="add-categoria-card" style="display:none">
                <div class="card-header">
                    <h2>Adicionar Nova Categoria</h2>
                    <button class="btn btn-secondary btn-sm" onclick="ParametrosTab.toggleAddCategoria()">Cancelar</button>
                </div>
                <div class="card-body">
                    <div class="form-row">
                        <div class="form-group">
                            <label class="form-label">Regime</label>
                            <select id="add-cat-regime" class="form-select">
                                <option value="PRE">PRE</option>
                                <option value="PRO">PRO</option>
                                <option value="CONSUMO">CONSUMO</option>
                                <option value="COMERCIALIZADOR">COMERCIALIZADOR</option>
                                <option value="GENERICA">GENERICA</option>
                                <option value="PORFOLIO">PORFOLIO</option>
                            </select>
                        </div>
                        <div class="form-group" style="flex:2">
                            <label class="form-label">Categoria Zona (ex: SOLAR_FOT_ES)</label>
                            <input type="text" id="add-cat-nome" class="form-input" placeholder="CATEGORIA_ZONA">
                        </div>
                        <div class="form-group" style="flex:0 0 auto">
                            <label class="form-label">&nbsp;</label>
                            <button class="btn btn-success" onclick="ParametrosTab.addCategoria()">Adicionar</button>
                        </div>
                    </div>
                </div>
            </div>

            <button class="btn btn-secondary mb-3" id="btn-add-categoria" onclick="ParametrosTab.toggleAddCategoria()">
                + Adicionar Categoria
            </button>

            <!-- Regime Cards Container -->
            <div id="parametros-container">
                <div class="loading">
                    <span class="spinner"></span> A carregar parâmetros...
                </div>
            </div>
        </div>

        <!-- ================================================================
             TAB 3: Estudos
             ================================================================ -->
        <div id="tab-estudos" class="tab-content">
            <!-- Header -->
            <div class="flex justify-between items-center mb-3">
                <div>
                    <h2 style="margin:0 0 0.25rem">Gestão de Estudos</h2>
                    <span class="text-muted text-sm">Lance e monitorize estudos de substituição ou optimização</span>
                </div>
            </div>

            <!-- New Study Form -->
            <div class="card mb-3">
                <div class="card-header">
                    <h2>Novo Estudo</h2>
                </div>
                <div class="card-body">
                    <div class="form-row mb-3">
                        <div class="form-group">
                            <label class="form-label">Tipo de Estudo</label>
                            <div class="radio-group">
                                <label class="radio-label">
                                    <input type="radio" name="estudo-tipo" value="substituicao" checked>
                                    Substituição
                                </label>
                                <label class="radio-label">
                                    <input type="radio" name="estudo-tipo" value="otimizacao">
                                    Optimização
                                </label>
                            </div>
                        </div>
                    </div>
                    <div class="form-row mb-3">
                        <div class="form-group">
                            <label class="form-label">Data Início</label>
                            <input type="date" id="estudo-data-inicio" class="form-input">
                        </div>
                        <div class="form-group">
                            <label class="form-label">Data Fim</label>
                            <input type="date" id="estudo-data-fim" class="form-input">
                        </div>
                        <div class="form-group" style="flex:0 0 120px">
                            <label class="form-label">Workers</label>
                            <input type="number" id="estudo-workers" class="form-input" value="4" min="1" max="16">
                        </div>
                    </div>
                    <div class="form-group mb-3">
                        <label class="form-label">Observações (opcional)</label>
                        <textarea id="estudo-observacoes" class="form-input" rows="2" placeholder="Notas para identificar este estudo..."></textarea>
                    </div>
                    <div class="flex justify-between items-center">
                        <span class="text-xs text-muted">Workers: controla o paralelismo do processamento (1-16)</span>
                        <button class="btn btn-primary" onclick="EstudosTab.lancarEstudo()">
                            Lançar Estudo
                        </button>
                    </div>
                </div>
            </div>

            <!-- Studies List -->
            <div class="card">
                <div class="card-header">
                    <h2>Lista de Estudos</h2>
                    <button class="btn btn-secondary btn-sm" onclick="EstudosTab.refresh()">
                        Actualizar
                    </button>
                </div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:120px">Tipo</th>
                                <th style="width:180px">Período</th>
                                <th>Observações</th>
                                <th style="width:100px">Status</th>
                                <th style="width:150px">Criado em</th>
                                <th style="width:150px;text-align:right">Acções</th>
                            </tr>
                        </thead>
                        <tbody id="estudos-tbody">
                            <tr>
                                <td colspan="6" class="loading">
                                    <span class="spinner"></span> A carregar...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ================================================================
             TAB 4: Resultados
             ================================================================ -->
        <div id="tab-resultados" class="tab-content">
            <div class="placeholder">
                <h3>Visualização de Resultados</h3>
                <p class="text-muted">Em construção — será implementado na TASK-07</p>
            </div>
        </div>
    </main>

    <!-- Toast notification container -->
    <div id="toast-container"></div>

    <!-- Log Modal -->
    <dialog id="modal-log">
        <div class="modal-content">
            <div class="modal-header">
                <h3>Log do Estudo</h3>
                <span id="modal-job-id" class="badge badge-primary"></span>
            </div>
            <div class="modal-body">
                <pre id="modal-log-content"></pre>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="document.getElementById('modal-log').close()">Fechar</button>
            </div>
        </div>
    </dialog>

    <script src="/js/app.js"></script>
</body>
</html>
