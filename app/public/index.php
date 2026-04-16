<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analizador de Mercado</title>
    <link rel="stylesheet" href="/css/app.css">
</head>
<body>
    <header>
        <h1><span>MIBEL</span> Analizador de Mercado</h1>
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
        <button data-tab="ingestao">Ingestão de Dados</button>
        <button data-tab="explorador">Explorador</button>
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

            <!-- Empty state — no job selected yet -->
            <div id="res-empty" class="empty-state">
                <h3>Seleccione um estudo concluído</h3>
                <p class="text-muted">Clique em "Ver resultados" na Aba 3 para visualizar.</p>
            </div>

            <!-- Content — populated by JS when a job is loaded -->
            <div id="res-content" hidden>

                <!-- Header row -->
                <div class="flex justify-between items-center mb-3 flex-wrap gap-2">
                    <div>
                        <h2 id="res-titulo" style="margin:0 0 0.25rem">Resultados</h2>
                        <div id="res-badges" class="flex gap-2 flex-wrap"></div>
                    </div>
                    <div class="flex gap-2 flex-wrap items-center">
                        <a id="res-export-csv"  class="btn btn-secondary" download>Exportar CSV</a>
                        <a id="res-export-json" class="btn btn-secondary" download>Exportar JSON</a>
                    </div>
                </div>

                <!-- Stat cards -->
                <div class="stat-cards mb-3" id="res-stat-cards"></div>

                <!-- No-data message (hidden unless 0 rows) -->
                <div id="res-no-data" class="card mb-3" hidden>
                    <div class="card-body text-center text-muted" style="padding:3rem">
                        Sem dados para este estudo.
                    </div>
                </div>

                <!-- Charts -->
                <div id="res-charts-section">
                    <div class="card mb-3">
                        <div class="card-header">
                            <h2 id="res-chart-serie-titulo">Evolução do Preço de Clearing</h2>
                        </div>
                        <div class="card-body">
                            <canvas id="chart-serie" height="100"></canvas>
                        </div>
                    </div>
                    <div class="card mb-3">
                        <div class="card-header">
                            <h2 id="res-chart-delta-titulo">Delta Médio por Hora do Dia</h2>
                        </div>
                        <div class="card-body">
                            <canvas id="chart-delta" height="80"></canvas>
                        </div>
                    </div>
                </div>

                <!-- Paginated data table -->
                <div class="card" id="res-tabela-section">
                    <div class="card-header">
                        <h2>Dados por Período</h2>
                        <div class="flex gap-2 items-center">
                            <span id="res-tabela-info" class="text-sm text-muted"></span>
                            <button class="btn btn-secondary btn-sm" id="res-btn-prev" onclick="ResultadosTab.prevPage()">← Anterior</button>
                            <button class="btn btn-secondary btn-sm" id="res-btn-next" onclick="ResultadosTab.nextPage()">Próximo →</button>
                        </div>
                    </div>
                    <div class="table-container">
                        <table>
                            <thead id="res-tabela-thead">
                                <tr>
                                    <th>Data</th>
                                    <th>Hora</th>
                                    <th>País</th>
                                    <th class="text-right">P.Orig (€/MWh)</th>
                                    <th class="text-right">P.Sim (€/MWh)</th>
                                    <th class="text-right">Δ Preço</th>
                                    <th class="text-right">Vol.Orig (MWh)</th>
                                    <th class="text-right">Vol.Sim (MWh)</th>
                                    <th class="text-right">Bids Sub.</th>
                                </tr>
                            </thead>
                            <tbody id="res-tabela-tbody">
                                <tr>
                                    <td colspan="9" class="loading">
                                        <span class="spinner"></span> A carregar...
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>

            </div><!-- /#res-content -->
        </div>

        <!-- ================================================================
             TAB 5: Ingestão de Dados
             ================================================================ -->
        <div id="tab-ingestao" class="tab-content">

            <div class="flex justify-between items-center mb-3">
                <div>
                    <h2 style="margin:0 0 0.25rem">Ingestão de Dados</h2>
                    <span class="text-muted text-sm">Carregue ZIPs mensais de bids OMIE — os dados são ingeridos directamente no ClickHouse</span>
                </div>
                <span id="ingestao-counter" class="badge">A carregar...</span>
            </div>

            <!-- Drop Zone -->
            <div class="card mb-3">
                <div class="card-body" style="padding:0">
                    <div id="ingestao-dropzone" class="ingestao-dropzone">
                        <div class="ingestao-dropzone-icon">&#8679;</div>
                        <p class="ingestao-dropzone-label">
                            Arraste ficheiros ZIP para aqui ou
                            <label for="ingestao-file-input" class="ingestao-browse-link">seleccione do disco</label>
                        </p>
                        <p class="ingestao-dropzone-hint">
                            Formato: <code>curva_pbc_uof_YYYYMM.zip</code> &nbsp;·&nbsp; Máx. 512 MB por ficheiro
                        </p>
                        <input type="file" id="ingestao-file-input" accept=".zip" multiple style="display:none">
                    </div>
                </div>
            </div>

            <!-- Upload + Ingestão Queue -->
            <div id="ingestao-queue-card" class="card mb-3" style="display:none">
                <div class="card-header">
                    <h2>Fila de Upload / Ingestão</h2>
                    <button class="btn btn-secondary btn-sm" onclick="IngestaoTab.clearQueue()">Limpar concluídos</button>
                </div>
                <div id="ingestao-queue-list" class="ingestao-queue-list"></div>
            </div>

            <!-- Dados no ClickHouse -->
            <div class="card">
                <div class="card-header">
                    <h2>Dados em ClickHouse <span class="text-muted text-sm" style="font-weight:400">(mibel.bids_raw)</span></h2>
                    <button class="btn btn-secondary btn-sm" onclick="IngestaoTab.loadSummary()">Actualizar</button>
                </div>
                <div id="ingestao-ch-content">
                    <div class="loading" style="padding:2rem">
                        <span class="spinner"></span> A consultar ClickHouse...
                    </div>
                </div>
            </div>

        </div><!-- /#tab-ingestao -->

        <!-- ================================================================
             TAB 6: Explorador de Dados
             ================================================================ -->
        <div id="tab-explorador" class="tab-content">

            <div class="flex justify-between items-center mb-3 flex-wrap gap-2">
                <div>
                    <h2 style="margin:0 0 0.25rem">Explorador de Dados</h2>
                    <span class="text-muted text-sm">Analise distribuição de bids, unidades geradoras, perfis horários e padrões do mercado MIBEL</span>
                </div>
                <button class="btn btn-secondary btn-sm" onclick="ExploradorTab.aplicarFiltros()">
                    Actualizar
                </button>
            </div>

            <!-- Filter bar -->
            <div class="card mb-3">
                <div class="card-body" style="padding:0.75rem 1.25rem">
                    <div class="filters" style="flex-wrap:wrap;gap:0.75rem;align-items:flex-end">
                        <div class="filter-group">
                            <label for="exp-de">De:</label>
                            <input type="date" id="exp-de" class="form-input form-input-sm">
                        </div>
                        <div class="filter-group">
                            <label for="exp-ate">Até:</label>
                            <input type="date" id="exp-ate" class="form-input form-input-sm">
                        </div>
                        <div class="filter-group">
                            <label for="exp-pais">País:</label>
                            <select id="exp-pais" class="form-select form-select-sm" style="min-width:90px">
                                <option value="">Todos</option>
                                <option value="MI">MI</option>
                                <option value="ES">ES</option>
                                <option value="PT">PT</option>
                            </select>
                        </div>
                        <div class="filter-group">
                            <label for="exp-tipo">Tipo:</label>
                            <select id="exp-tipo" class="form-select form-select-sm" style="min-width:110px">
                                <option value="">Todos</option>
                                <option value="V">V (Venda)</option>
                                <option value="C">C (Compra)</option>
                            </select>
                        </div>
                        <button class="btn btn-primary btn-sm" onclick="ExploradorTab.aplicarFiltros()">
                            Aplicar Filtros
                        </button>
                        <button class="btn btn-secondary btn-sm" onclick="ExploradorTab.limparFiltros()">
                            Limpar
                        </button>
                    </div>
                </div>
            </div>

            <!-- Validation / empty state message -->
            <div id="exp-empty-state" class="note note-info mb-3" style="display:flex;align-items:center;gap:0.75rem;padding:1rem 1.25rem">
                <span style="font-size:1.5rem;line-height:1">&#128269;</span>
                <span>Preencha <strong>De</strong> e <strong>Até</strong> e clique em <strong>Aplicar Filtros</strong> para consultar.</span>
            </div>
            <div id="exp-error-state" class="note note-warning mb-3" style="display:none;align-items:center;gap:0.75rem;padding:1rem 1.25rem">
                <span style="font-size:1.25rem;line-height:1">&#9888;</span>
                <span id="exp-error-msg">Preencha os campos "De" e "Até" antes de consultar.</span>
            </div>

            <!-- Main content — hidden until first query runs -->
            <div id="exp-main-content" style="display:none">

            <!-- KPI Cards -->
            <div id="exp-kpi-cards" class="stat-cards mb-3"></div>

            <!-- Charts row 1: Distribuição + Perfil Horário -->
            <div class="exp-charts-row mb-3">
                <div class="card">
                    <div class="card-header">
                        <h2>Distribuição por País e Tipo</h2>
                    </div>
                    <div class="card-body">
                        <canvas id="exp-chart-distrib" height="240"></canvas>
                    </div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <h2>Perfil Horário — Preço Médio (€/MWh)</h2>
                    </div>
                    <div class="card-body">
                        <canvas id="exp-chart-horario" height="240"></canvas>
                    </div>
                </div>
            </div>

            <!-- Charts row 2: Histograma de Preços + Volume por Categoria -->
            <div class="exp-charts-row mb-3">
                <div class="card">
                    <div class="card-header">
                        <h2>Histograma de Preços (€/MWh)</h2>
                    </div>
                    <div class="card-body">
                        <canvas id="exp-chart-histograma" height="240"></canvas>
                    </div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <h2>Volume por Categoria (M MWh)</h2>
                    </div>
                    <div class="card-body">
                        <canvas id="exp-chart-categorias" height="240"></canvas>
                    </div>
                </div>
            </div>

            <!-- Tendência Mensal (full width) -->
            <div class="card mb-3">
                <div class="card-header">
                    <h2>Tendência Mensal — Energia &amp; Preço Médio</h2>
                </div>
                <div class="card-body">
                    <canvas id="exp-chart-mensal" height="90"></canvas>
                </div>
            </div>

            <!-- Top Unidades Geradoras -->
            <div class="card mb-3">
                <div class="card-header">
                    <h2>Top Unidades Geradoras</h2>
                    <div class="flex gap-2 items-center">
                        <select id="exp-top-sort" class="form-select form-select-sm"
                                onchange="ExploradorTab.loadTopUnidades()">
                            <option value="energia">Por Energia (MWh)</option>
                            <option value="bids">Por Nº Bids</option>
                        </select>
                        <select id="exp-top-limit" class="form-select form-select-sm"
                                onchange="ExploradorTab.loadTopUnidades()">
                            <option value="10">Top 10</option>
                            <option value="25" selected>Top 25</option>
                            <option value="50">Top 50</option>
                        </select>
                    </div>
                </div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:36px">#</th>
                                <th>Unidade</th>
                                <th>Descrição</th>
                                <th style="width:90px">Regime</th>
                                <th style="width:160px">Categoria</th>
                                <th style="width:50px">Zona</th>
                                <th class="text-right" style="width:90px">Bids</th>
                                <th class="text-right" style="width:110px">Energia (MWh)</th>
                                <th class="text-right" style="width:100px">P.Médio (€)</th>
                                <th class="text-right" style="width:80px">P.Min (€)</th>
                                <th class="text-right" style="width:80px">P.Max (€)</th>
                            </tr>
                        </thead>
                        <tbody id="exp-top-tbody">
                            <tr>
                                <td colspan="11" class="loading">
                                    <span class="spinner"></span> A carregar...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            </div><!-- /#exp-main-content -->

            <!-- Console SQL — sempre visível, não precisa de filtros de data -->
            <div class="card mt-3">
                <div class="card-header">
                    <h2>Console SQL ClickHouse</h2>
                    <span class="badge badge-warning">Apenas SELECT</span>
                </div>
                <div class="card-body">
                    <div class="note note-info mb-2">
                        Execute queries <code>SELECT</code> directamente no ClickHouse (<code>mibel.*</code>).
                        Sem <code>LIMIT</code>, é aplicado automaticamente um limite de 200 linhas.
                    </div>
                    <div class="exp-sql-presets mb-2">
                        <span class="text-sm text-muted">Exemplos rápidos:</span>
                        <button class="btn btn-secondary btn-sm"
                                onclick="ExploradorTab.setPreset('overview')">Resumo geral</button>
                        <button class="btn btn-secondary btn-sm"
                                onclick="ExploradorTab.setPreset('horas_caras')">Horas mais caras</button>
                        <button class="btn btn-secondary btn-sm"
                                onclick="ExploradorTab.setPreset('unidade_preco')">Preço por unidade</button>
                        <button class="btn btn-secondary btn-sm"
                                onclick="ExploradorTab.setPreset('curva_oferta')">Curva de oferta</button>
                        <button class="btn btn-secondary btn-sm"
                                onclick="ExploradorTab.setPreset('bids_hora_dia')">Bids por hora/dia</button>
                    </div>
                    <textarea id="exp-sql-input" class="exp-sql-textarea"
                              rows="6"
                              placeholder="SELECT pais, count() AS n FROM mibel.bids_raw GROUP BY pais"></textarea>
                    <div class="flex justify-between items-center mt-2">
                        <span id="exp-sql-status" class="text-sm text-muted"></span>
                        <button class="btn btn-primary" onclick="ExploradorTab.runQuery()">
                            Executar
                        </button>
                    </div>
                    <div id="exp-sql-result" class="mt-3" style="display:none">
                        <div class="exp-sql-result-container">
                            <table id="exp-sql-table">
                                <thead id="exp-sql-thead"></thead>
                                <tbody id="exp-sql-tbody"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

        </div><!-- /#tab-explorador -->

    </main>

    <!-- Toast notification container -->
    <div id="toast-container"></div>

    <!-- Log Modal -->
    <dialog id="modal-log">
        <div class="modal-content" style="width:720px;max-width:95vw">
            <div class="modal-header">
                <h3>Log do Estudo</h3>
                <div class="flex gap-2 items-center">
                    <span id="modal-log-status" class="badge" style="display:none"></span>
                    <span id="modal-log-live" class="badge status-running" style="display:none">ao vivo</span>
                    <span id="modal-job-id" class="badge badge-primary"></span>
                </div>
            </div>
            <div class="modal-body" style="padding:0">
                <pre id="modal-log-content" style="margin:0;max-height:60vh;overflow-y:auto;padding:1rem;font-size:0.78rem;line-height:1.5"></pre>
            </div>
            <div class="modal-footer">
                <label class="flex items-center gap-2 text-sm text-muted" style="margin-right:auto">
                    <input type="checkbox" id="modal-log-autoscroll" checked>
                    Auto-scroll
                </label>
                <button class="btn btn-secondary" onclick="EstudosTab.fecharModalLog()">Fechar</button>
            </div>
        </div>
    </dialog>

    <!-- Edit Observations Modal -->
    <dialog id="modal-edit-obs">
        <div class="modal-content" style="width:480px;max-width:95vw">
            <div class="modal-header">
                <h3>Editar Observações</h3>
                <span id="modal-edit-job-id" class="badge badge-primary"></span>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Observações</label>
                    <textarea id="modal-edit-obs-input" class="form-input" rows="4"
                        placeholder="Notas para identificar este estudo..."></textarea>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="document.getElementById('modal-edit-obs').close()">Cancelar</button>
                <button class="btn btn-primary" onclick="EstudosTab.guardarObservacoes()">Guardar</button>
            </div>
        </div>
    </dialog>

    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <script src="/js/app.js?v=<?= filemtime(__DIR__ . '/js/app.js') ?>"></script>
</body>
</html>
