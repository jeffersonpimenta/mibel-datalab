# MIBEL Platform

Plataforma de analise do Mercado Iberico de Eletricidade (MIBEL), desenvolvida no ambito de uma dissertacao de Mestrado em Engenharia Eletrotecnica e de Computadores da Faculdade de Engenharia da Universidade do Porto (FEUP).

A plataforma permite a ingestao, classificacao e analise de ofertas do mercado diario de eletricidade do MIBEL, com foco em estudos de substituicao e otimizacao de ofertas de produtores em regime especial (PRE).

## Arquitetura

```
                    ┌──────────────┐
                    │   Browser    │
                    └──────┬───────┘
                           │ :8080
                    ┌──────┴───────┐
                    │    Nginx     │
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              │                         │
       /api/*                    ficheiros estaticos
              │                         │
      ┌───────┴────────┐       ┌────────┴───────┐
      │  PHP 8.3-FPM   │       │   index.php    │
      │  (REST API)    │       │   app.js/css   │
      └───┬────────┬───┘       └────────────────┘
          │        │
   ┌──────┴──┐  ┌──┴──────────────┐
   │ SQLite  │  │   ClickHouse    │
   │ (jobs)  │  │   (analytics)   │
   └─────────┘  └────────┬────────┘
                         │
                ┌────────┴────────┐
                │  Python Workers │
                │  (docker exec)  │
                └─────────────────┘
```

**Stack tecnologico:**

| Componente | Tecnologia |
|---|---|
| Frontend | PHP 8.3 (HTML) + Vanilla JavaScript |
| API REST | PHP 8.3-FPM |
| Base de dados analitica | ClickHouse 24 |
| Fila de trabalhos | SQLite |
| Workers de calculo | Python 3.12 (pandas, numpy) |
| Orquestracao | Docker Compose |

## Funcionalidades

### Ingestao de Dados
Upload e processamento de ficheiros ZIP do OMIE (curvas de oferta `curva_pbc_uof_YYYYMM.zip`). Os dados sao inseridos na tabela `bids_raw` do ClickHouse, particionada por mes.

### Classificacao de Unidades
Mapeamento automatico das unidades do OMIE para categorias tecnologicas (solar fotovoltaica, eolica, nuclear, ciclo combinado, etc.) distribuidas por 7 regimes: PRE, PRO, CONSUMO, COMERCIALIZADOR, GENERICA, PORFOLIO. Inclui suporte para excecoes manuais por codigo de unidade.

### Estudos de Substituicao
Aplica escaloes de preco configurados para modificar as ofertas de venda de produtores PRE e recalcular o clearing do mercado. Permite avaliar o impacto no preco de equilibrio da substituicao de ofertas instrumentais por ofertas a custo de oportunidade.

### Estudos de Otimizacao
Determina o volume otimo de remocao de ofertas PRE para maximizar a receita dos produtores em regime especial, iterando sobre cenarios de remocao e calculando o lucro resultante.

### Explorador de Dados
Painel com 8 visualizacoes interativas: distribuicao de ofertas, histogramas, perfis horarios, top unidades, categorias tecnologicas, tendencias mensais e diagramas de dispersao. Inclui consola SQL para queries personalizadas.

### Exportacao de Resultados
Exportacao dos resultados de estudos em formato Excel.

## Estrutura do Projeto

```
mibel-platform/
├── app/
│   ├── public/                  # Web root
│   │   ├── index.php            # Interface principal (SPA)
│   │   ├── js/app.js            # Frontend (~109 KB)
│   │   └── css/app.css          # Estilos (~28 KB)
│   └── src/
│       ├── api/                 # Handlers da API REST
│       │   ├── router.php       # Router central
│       │   ├── classificacao.php
│       │   ├── parametros.php
│       │   ├── estudos.php
│       │   ├── resultados.php
│       │   ├── ingestao.php
│       │   └── explorador.php
│       ├── Config.php           # Gestao de configuracao JSON
│       ├── Database.php         # Cliente HTTP ClickHouse
│       ├── Jobs.php             # Fila de trabalhos SQLite
│       ├── migrate.php          # Inicializacao da base de dados
│       └── schema_clickhouse.sql
├── workers/                     # Workers Python
│   ├── clearing.py              # Algoritmo de clearing
│   ├── ingestao_worker.py       # Ingestao de ZIPs
│   ├── substituicao_worker.py   # Estudo de substituicao
│   ├── otimizacao_worker.py     # Estudo de otimizacao
│   └── utils.py                 # Utilitarios partilhados
├── scripts/
│   └── unidades/                # Classificacao de unidades OMIE
│       ├── LISTA_UNIDADES.csv
│       ├── carrega_unidades_ch.py
│       └── classificacao_pre.py
├── data/
│   ├── config/                  # Configuracao editavel via UI
│   │   ├── parametros.json      # Escaloes por regime/categoria
│   │   ├── classificacao.json   # Tecnologia -> regime/categoria
│   │   └── excecoes.json        # Excecoes por unidade
│   ├── bids/                    # ZIPs OMIE uploadados
│   ├── outputs/                 # Logs de execucao
│   └── jobs.db                  # Base de dados SQLite
├── docker/
│   ├── nginx/default.conf
│   ├── php/Dockerfile
│   └── python/Dockerfile
└── docker-compose.yml
```

## Requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/install/)

## Instalacao e Execucao

```bash
# Clonar o repositorio
git clone <url-do-repositorio>
cd mibel-platform

# Iniciar todos os servicos
docker compose up -d

# Verificar que os servicos estao a correr
docker compose ps
```

A plataforma fica acessivel em **http://localhost:8080**.

Na primeira execucao, o sistema:
1. Cria a base de dados `mibel` no ClickHouse com todas as tabelas
2. Inicializa a base de dados SQLite para a fila de trabalhos
3. Carrega o registo de unidades do OMIE (`LISTA_UNIDADES.csv`) para o ClickHouse

## Utilizacao

### 1. Ingestao de dados
No separador **Ingestao de Dados**, carregar ficheiros ZIP mensais do OMIE (`curva_pbc_uof_YYYYMM.zip`). O sistema processa e insere os dados automaticamente.

### 2. Classificacao
No separador **Classificacao**, verificar e ajustar o mapeamento de tecnologias para regimes e categorias. Adicionar excecoes para unidades mal classificadas.

### 3. Parametros
No separador **Parametros**, configurar os escaloes de preco e fatores de escala para cada categoria/zona. Estes parametros controlam como as ofertas sao transformadas nos estudos de substituicao.

### 4. Estudos
No separador **Estudos**, criar estudos de substituicao ou otimizacao selecionando o intervalo de datas e numero de workers paralelos.

### 5. Resultados
No separador **Resultados**, consultar os resultados dos estudos concluidos com series temporais, tabelas detalhadas e estatisticas agregadas.

### 6. Explorador
No separador **Explorador**, explorar os dados de ofertas com visualizacoes interativas e queries SQL personalizadas.

## API REST

| Metodo | Endpoint | Descricao |
|---|---|---|
| GET | `/api/health` | Health check |
| GET | `/api/classificacao` | Listar classificacoes |
| POST | `/api/classificacao` | Criar classificacao |
| PUT | `/api/classificacao/{id}` | Atualizar classificacao |
| DELETE | `/api/classificacao/{id}` | Remover classificacao |
| GET | `/api/excecoes` | Listar excecoes |
| POST | `/api/excecoes` | Criar excecao |
| DELETE | `/api/excecoes/{codigo}` | Remover excecao |
| GET | `/api/parametros` | Obter parametros |
| GET | `/api/parametros/categorias` | Listar categorias |
| PUT | `/api/parametros` | Atualizar parametros |
| GET | `/api/estudos` | Listar estudos |
| POST | `/api/estudos` | Criar estudo |
| GET | `/api/estudos/{id}` | Detalhe do estudo |
| POST | `/api/estudos/{id}/cancelar` | Cancelar estudo |
| DELETE | `/api/estudos/{id}` | Remover estudo |
| GET | `/api/resultados/{id}/serie` | Serie temporal |
| GET | `/api/resultados/{id}/tabela` | Tabela detalhada |
| GET | `/api/resultados/{id}/stats` | Estatisticas |
| GET | `/api/resultados/{id}/logs` | Logs de execucao |
| GET | `/api/resultados/{id}/exportar` | Exportar Excel |
| GET | `/api/ingestao` | Estado da ingestao |
| POST | `/api/ingestao` | Upload ZIP |
| DELETE | `/api/ingestao/mes/{YYYYMM}` | Remover mes |
| GET | `/api/explorador/overview` | Visao geral |
| GET | `/api/explorador/distribuicao` | Distribuicao |
| GET | `/api/explorador/histograma` | Histograma |
| GET | `/api/explorador/perfil-horario` | Perfil horario |
| GET | `/api/explorador/top-unidades` | Top unidades |
| GET | `/api/explorador/categorias` | Por categoria |
| GET | `/api/explorador/tendencia-mensal` | Tendencia mensal |
| GET | `/api/explorador/dispersao` | Dispersao |
| POST | `/api/explorador/query` | Query SQL |

## Modelo de Dados (ClickHouse)

| Tabela | Descricao |
|---|---|
| `bids_raw` | Ofertas brutas do OMIE, particionadas por mes |
| `clearing_substituicao` | Resultados de estudos de substituicao (por hora/pais) |
| `clearing_substituicao_logs` | Detalhe das ofertas substituidas |
| `clearing_otimizacao` | Resultados de estudos de otimizacao |
| `clearing_otimizacao_logs` | Cenarios testados na otimizacao |
| `unidades` | Registo de unidades OMIE com classificacao |
| `worker_logs` | Logs de execucao dos workers |

## Configuracao

Os ficheiros de configuracao em `data/config/` sao editaveis via interface web:

- **`parametros.json`** -- Escaloes de preco e fatores de escala por regime e categoria/zona
- **`classificacao.json`** -- Mapeamento de tecnologias OMIE para regime e categoria
- **`excecoes.json`** -- Excecoes manuais de classificacao por codigo de unidade

## Variaveis de Ambiente

Definidas no `docker-compose.yml`:

| Variavel | Servico | Default | Descricao |
|---|---|---|---|
| `CLICKHOUSE_HOST` | python-worker | `clickhouse` | Host do ClickHouse |
| `CLICKHOUSE_PORT` | python-worker | `9000` | Porta nativa do ClickHouse |
