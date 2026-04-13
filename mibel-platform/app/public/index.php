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
            <div class="placeholder">
                <h3>Parâmetros de Substituição</h3>
                <p class="text-muted">Em construção — será implementado na TASK-05</p>
            </div>
        </div>

        <!-- ================================================================
             TAB 3: Estudos
             ================================================================ -->
        <div id="tab-estudos" class="tab-content">
            <div class="placeholder">
                <h3>Gestão de Estudos</h3>
                <p class="text-muted">Em construção — será implementado na TASK-06</p>
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

    <script src="/js/app.js"></script>
</body>
</html>
