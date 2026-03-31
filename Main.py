# =============================================================================
# api/main.py — FastAPI: ETL + API REST
# Lab01_PART1_5479786 — PNCP Contratos Públicos
#
# Arquitetura:
#   FastAPI (ETL + API) → PostgreSQL (Model) → Superset (View)
#
# Responsabilidades:
#   - Rotas /pipeline/* : disparam as etapas do ETL em background
#   - Rotas /q*         : expõem as 13 queries de negócio
#   - Rotas /busca/*    : busca livre
#   - Rotas /resumo     : KPIs do dashboard principal
#
# Documentação: http://localhost:8000/docs
# =============================================================================
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:postgres@localhost:5432/pncp_db'
)
CACHE_TTL = int(os.environ.get('CACHE_TTL_SECONDS', 300))

engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)

app = FastAPI(
    title       = 'PNCP API',
    description = (
        '**FastAPI** como camada ETL + Controller do pipeline PNCP.\n\n'
        'Arquitetura: `FastAPI` → `PostgreSQL` → `Superset`\n\n'
        'Lab01_PART1_5479786 — Hercules Ramos Veloso de Freitas'
    ),
    version  = '1.0.0',
    docs_url = '/docs',
    redoc_url= '/redoc',
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# ---------------------------------------------------------------------------
# Cache em memória
# ---------------------------------------------------------------------------
_cache: Dict[str, Dict] = {}
_pipeline_status: Dict[str, Any] = {}


def _cached(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry and (time.time() - entry['ts']) < CACHE_TTL:
        return entry['data']
    return None


def _set_cache(key: str, data: Any) -> None:
    _cache[key] = {'data': data, 'ts': time.time()}


def _query(sql: str, params: dict = None) -> List[Dict]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


# ===========================================================================
# SISTEMA
# ===========================================================================

@app.get('/health', tags=['Sistema'])
def health():
    """Verifica conectividade com o PostgreSQL."""
    try:
        _query('SELECT 1')
        return {
            'status':    'ok',
            'banco':     'conectado',
            'timestamp': datetime.now().isoformat(),
            'cache_entries': len(_cache),
        }
    except Exception as e:
        raise HTTPException(502, f'Banco indisponível: {e}')


@app.get('/cache/stats', tags=['Sistema'])
def cache_stats():
    """Estatísticas do cache em memória."""
    agora = time.time()
    return {
        'entradas':     len(_cache),
        'ttl_segundos': CACHE_TTL,
        'chaves': [
            {'chave': k, 'idade_s': round(agora - v['ts'], 1)}
            for k, v in _cache.items()
        ],
    }


@app.delete('/cache', tags=['Sistema'])
def limpar_cache():
    """Limpa todo o cache em memória."""
    _cache.clear()
    return {'status': 'cache limpo', 'timestamp': datetime.now().isoformat()}


# ===========================================================================
# PIPELINE ETL — dispara etapas em background
# ===========================================================================

def _run_silver():
    """Executa silver.py em background."""
    import subprocess
    _pipeline_status['silver'] = {
        'status': 'running', 'start': datetime.now().isoformat()
    }
    try:
        result = subprocess.run(
            ['python', '/app/data/../silver.py', '--tudo'],
            capture_output=True, text=True, timeout=7200
        )
        _pipeline_status['silver'] = {
            'status': 'done' if result.returncode == 0 else 'error',
            'end':    datetime.now().isoformat(),
            'stdout': result.stdout[-2000:],
            'stderr': result.stderr[-1000:],
        }
    except Exception as e:
        _pipeline_status['silver'] = {'status': 'error', 'error': str(e)}


def _run_gold_load():
    """Executa gold_load.py em background."""
    import subprocess
    _pipeline_status['gold_load'] = {
        'status': 'running', 'start': datetime.now().isoformat()
    }
    try:
        result = subprocess.run(
            ['python', '/app/data/../gold_load.py'],
            capture_output=True, text=True, timeout=3600
        )
        _pipeline_status['gold_load'] = {
            'status': 'done' if result.returncode == 0 else 'error',
            'end':    datetime.now().isoformat(),
            'stdout': result.stdout[-2000:],
            'stderr': result.stderr[-1000:],
        }
    except Exception as e:
        _pipeline_status['gold_load'] = {'status': 'error', 'error': str(e)}


@app.post('/pipeline/silver', tags=['Pipeline ETL'])
def trigger_silver(background_tasks: BackgroundTasks):
    """
    Dispara o tratamento Silver em background.
    Lê os JSONs Bronze e gera os Parquets Silver.
    """
    if _pipeline_status.get('silver', {}).get('status') == 'running':
        return {'status': 'já rodando', 'info': _pipeline_status['silver']}
    background_tasks.add_task(_run_silver)
    return {'status': 'iniciado', 'endpoint_status': '/pipeline/status'}


@app.post('/pipeline/gold', tags=['Pipeline ETL'])
def trigger_gold(background_tasks: BackgroundTasks):
    """
    Dispara a carga Gold em background.
    Carrega os Parquets Silver → PostgreSQL.
    """
    if _pipeline_status.get('gold_load', {}).get('status') == 'running':
        return {'status': 'já rodando', 'info': _pipeline_status['gold_load']}
    background_tasks.add_task(_run_gold_load)
    return {'status': 'iniciado', 'endpoint_status': '/pipeline/status'}


@app.get('/pipeline/status', tags=['Pipeline ETL'])
def pipeline_status():
    """Status atual das etapas do pipeline ETL."""
    return _pipeline_status or {'info': 'Nenhum pipeline executado ainda.'}


# ===========================================================================
# QUERIES DE NEGÓCIO
# ===========================================================================

@app.get('/resumo', tags=['Dashboard'])
def resumo():
    """KPIs gerais para o dashboard principal."""
    key = 'resumo'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            COUNT(*)                                         AS total_contratos,
            ROUND(SUM(valor_global)::NUMERIC / 1e9, 2)     AS valor_total_bilhoes,
            ROUND(AVG(valor_global)::NUMERIC, 2)            AS ticket_medio,
            COUNT(DISTINCT cnpj_contratada)                 AS fornecedores_distintos,
            COUNT(DISTINCT orgao_entidade_id)               AS orgaos_distintos,
            MIN(data_assinatura)                             AS data_inicio,
            MAX(data_assinatura)                             AS data_fim
        FROM fato_contratos
        WHERE valor_global > 0
    """)
    result = data[0] if data else {}
    _set_cache(key, result)
    return result


@app.get('/q1/evolucao-modalidade', tags=['Queries de Negócio'])
def q1(ano_inicio: int = 2021, ano_fim: int = 2026):
    """Q1 — Evolução anual do valor por modalidade de contrato."""
    key = f'q1_{ano_inicio}_{ano_fim}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR FROM f.data_assinatura)::INT    AS ano,
            COALESCE(m.nome_modalidade, 'Não informado') AS modalidade,
            COUNT(*)                                     AS qtd_contratos,
            ROUND(SUM(f.valor_global)::NUMERIC / 1e9, 4) AS valor_bilhoes,
            ROUND(AVG(f.valor_global)::NUMERIC, 2)       AS valor_medio
        FROM fato_contratos f
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE f.valor_global > 0
          AND f.data_assinatura IS NOT NULL
          AND EXTRACT(YEAR FROM f.data_assinatura) BETWEEN :ai AND :af
        GROUP BY ano, modalidade
        ORDER BY ano, valor_bilhoes DESC
    """, {'ai': ano_inicio, 'af': ano_fim})
    _set_cache(key, data)
    return data


@app.get('/q2/top-orgaos', tags=['Queries de Negócio'])
def q2(n: int = Query(10, ge=1, le=100)):
    """Q2 — Top N órgãos por volume financeiro."""
    key = f'q2_{n}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            COALESCE(o.nome_orgao, f.orgao_entidade_id) AS orgao,
            COALESCE(o.nome_unidade, '')                 AS unidade,
            COUNT(*)                                     AS qtd_contratos,
            ROUND(SUM(f.valor_global)::NUMERIC / 1e9, 4) AS valor_bilhoes,
            ROUND(AVG(f.valor_global)::NUMERIC, 2)       AS ticket_medio
        FROM fato_contratos f
        LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        WHERE f.valor_global > 0
        GROUP BY o.nome_orgao, f.orgao_entidade_id, o.nome_unidade
        ORDER BY valor_bilhoes DESC
        LIMIT :n
    """, {'n': n})
    _set_cache(key, data)
    return data


@app.get('/q3/pareto-fornecedores', tags=['Queries de Negócio'])
def q3(n: int = Query(20, ge=5, le=100)):
    """Q3 — Curva de Pareto: concentração de mercado por fornecedor."""
    key = f'q3_{n}'
    if hit := _cached(key):
        return hit
    data = _query("""
        WITH ranking AS (
            SELECT
                COALESCE(forn.nome_contratada, f.cnpj_contratada) AS nome,
                f.cnpj_contratada,
                COUNT(*)            AS qtd,
                SUM(f.valor_global) AS valor_total
            FROM fato_contratos f
            LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
            WHERE f.valor_global > 0
            GROUP BY f.cnpj_contratada, forn.nome_contratada
            ORDER BY valor_total DESC LIMIT :n
        ),
        total AS (SELECT SUM(valor_total) AS gt FROM ranking)
        SELECT
            ROW_NUMBER() OVER (ORDER BY r.valor_total DESC) AS rank,
            r.nome, r.cnpj_contratada, r.qtd,
            ROUND(r.valor_total::NUMERIC / 1e6, 2)          AS valor_milhoes,
            ROUND(SUM(r.valor_total) OVER (ORDER BY r.valor_total DESC)
                  * 100.0 / NULLIF(t.gt, 0), 2)             AS pct_acumulado
        FROM ranking r, total t ORDER BY rank
    """, {'n': n})
    _set_cache(key, data)
    return data


@app.get('/q4/sazonalidade', tags=['Queries de Negócio'])
def q4(ano_inicio: int = 2021, ano_fim: int = 2025):
    """Q4 — Sazonalidade mensal de contratos por ano."""
    key = f'q4_{ano_inicio}_{ano_fim}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR  FROM data_assinatura)::INT    AS ano,
            EXTRACT(MONTH FROM data_assinatura)::INT    AS mes,
            COUNT(*)                                    AS qtd_contratos,
            ROUND(SUM(valor_global)::NUMERIC / 1e6, 2) AS valor_milhoes,
            ROUND(AVG(valor_global)::NUMERIC, 2)        AS valor_medio
        FROM fato_contratos
        WHERE valor_global > 0 AND data_assinatura IS NOT NULL
          AND EXTRACT(YEAR FROM data_assinatura) BETWEEN :ai AND :af
        GROUP BY ano, mes ORDER BY ano, mes
    """, {'ai': ano_inicio, 'af': ano_fim})
    _set_cache(key, data)
    return data


@app.get('/q5/compromisso-ativo', tags=['Queries de Negócio'])
def q5():
    """Q5 — Estoque de contratos vigentes e valor comprometido."""
    key = 'q5'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT         AS ano,
            COUNT(*)                                         AS qtd_total,
            ROUND(SUM(valor_global)::NUMERIC / 1e9, 4)     AS valor_bilhoes,
            COUNT(*) FILTER (WHERE data_vigencia_fim > CURRENT_DATE)
                                                             AS contratos_ativos,
            ROUND(COALESCE(SUM(valor_global) FILTER (
                WHERE data_vigencia_fim > CURRENT_DATE), 0
            )::NUMERIC / 1e9, 4)                             AS valor_ativo_bilhoes
        FROM fato_contratos
        WHERE valor_global > 0 AND data_assinatura IS NOT NULL
        GROUP BY ano ORDER BY ano DESC
    """)
    _set_cache(key, data)
    return data


@app.get('/q6/universidades', tags=['Queries de Negócio'])
def q6(n: int = Query(20, ge=1, le=200)):
    """Q6 — Contratos de universidades e institutos federais."""
    key = f'q6_{n}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            o.nome_orgao,
            COUNT(*)                                          AS qtd_contratos,
            ROUND(SUM(f.valor_global)::NUMERIC / 1e6, 2)    AS valor_milhoes,
            ROUND(AVG(f.valor_global)::NUMERIC, 2)           AS valor_medio,
            MIN(f.data_assinatura)                            AS primeiro_contrato,
            MAX(f.data_assinatura)                            AS ultimo_contrato
        FROM fato_contratos f
        JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        WHERE f.valor_global > 0
          AND (o.nome_orgao ILIKE '%universidade%'
            OR o.nome_orgao ILIKE '%instituto federal%'
            OR o.nome_orgao ILIKE '%CEFET%'
            OR o.nome_orgao ILIKE '%centro universitário%')
        GROUP BY o.nome_orgao
        ORDER BY valor_milhoes DESC LIMIT :n
    """, {'n': n})
    _set_cache(key, data)
    return data


@app.get('/q7/usp', tags=['Queries de Negócio'])
def q7(n: int = Query(100, ge=1, le=500)):
    """Q7 — Top N maiores contratos da Universidade de São Paulo."""
    key = f'q7_{n}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY f.valor_global DESC)  AS ranking,
            o.nome_unidade,
            f.id_contrato_pncp,
            f.numero_contrato,
            f.categoria_processo,
            COALESCE(m.nome_modalidade, 'Não informado')       AS modalidade,
            forn.nome_contratada                               AS fornecedor,
            f.data_assinatura,
            f.data_vigencia_fim,
            ROUND(f.valor_global::NUMERIC, 2)                  AS valor_global
        FROM fato_contratos f
        JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        LEFT JOIN dim_modalidades m   ON f.id_modalidade = m.id_modalidade
        LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        WHERE f.valor_global > 0
          AND o.nome_orgao ILIKE '%universidade de são paulo%'
        ORDER BY f.valor_global DESC LIMIT :n
    """, {'n': n})
    _set_cache(key, data)
    return data


@app.get('/q8/aditivos', tags=['Queries de Negócio'])
def q8():
    """Q8 — Contratos com variação entre valor inicial e global (aditivos)."""
    key = 'q8'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT      AS ano,
            COALESCE(m.nome_modalidade,'Não informado')  AS modalidade,
            COUNT(*)                                     AS total,
            COUNT(*) FILTER (
                WHERE valor_global > valor_inicial AND valor_inicial > 0
            )                                            AS com_aditivo,
            ROUND(AVG(CASE WHEN valor_inicial > 0
                THEN (valor_global - valor_inicial) / valor_inicial * 100
                END)::NUMERIC, 2)                        AS variacao_media_pct
        FROM fato_contratos f
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE valor_global > 0 AND data_assinatura IS NOT NULL
        GROUP BY ano, modalidade ORDER BY ano, com_aditivo DESC
    """)
    _set_cache(key, data)
    return data


@app.get('/q9/delay-publicacao', tags=['Queries de Negócio'])
def q9():
    """Q9 — Dias entre assinatura e publicação no PNCP (transparência)."""
    key = 'q9'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT                  AS ano,
            COUNT(*)                                                   AS total,
            ROUND(AVG(data_publicacao - data_assinatura)::NUMERIC, 1) AS delay_medio,
            MAX(data_publicacao - data_assinatura)                     AS delay_maximo,
            COUNT(*) FILTER (
                WHERE data_publicacao - data_assinatura > 20
            )                                                          AS publicacao_tardia
        FROM fato_contratos
        WHERE data_assinatura IS NOT NULL AND data_publicacao IS NOT NULL
          AND data_publicacao >= data_assinatura
          AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
        GROUP BY ano ORDER BY ano
    """)
    _set_cache(key, data)
    return data


@app.get('/q10/mediana-modalidade', tags=['Queries de Negócio'])
def q10():
    """Q10 — Mediana e perfil financeiro por modalidade."""
    key = 'q10'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            COALESCE(m.nome_modalidade,'Não informado')   AS modalidade,
            COUNT(*)                                       AS qtd_contratos,
            ROUND(AVG(f.valor_global)::NUMERIC, 2)        AS media,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY f.valor_global)                   AS mediana,
            ROUND(MIN(f.valor_global)::NUMERIC, 2)        AS minimo,
            ROUND(MAX(f.valor_global)::NUMERIC, 2)        AS maximo,
            ROUND(SUM(f.valor_global)::NUMERIC / 1e9, 2) AS total_bilhoes
        FROM fato_contratos f
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE f.valor_global > 0
        GROUP BY modalidade ORDER BY mediana DESC
    """)
    _set_cache(key, data)
    return data


@app.get('/q11/fracionamento', tags=['Queries de Negócio'])
def q11(min_contratos: int = Query(5, ge=2)):
    """Q11 — Múltiplos contratos com mesmo fornecedor/mês (possível fracionamento)."""
    key = f'q11_{min_contratos}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            f.cnpj_contratada, forn.nome_contratada,
            o.nome_orgao, f.ano_mes_coleta AS ano_mes,
            COUNT(*)                                       AS qtd_contratos_mes,
            ROUND(SUM(f.valor_global)::NUMERIC, 2)        AS valor_total_mes
        FROM fato_contratos f
        LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        WHERE f.valor_global > 0 AND f.cnpj_contratada IS NOT NULL
        GROUP BY f.cnpj_contratada, forn.nome_contratada,
                 o.nome_orgao, f.ano_mes_coleta
        HAVING COUNT(*) >= :min_c
        ORDER BY qtd_contratos_mes DESC LIMIT 50
    """, {'min_c': min_contratos})
    _set_cache(key, data)
    return data


@app.get('/q12/ticket-medio', tags=['Queries de Negócio'])
def q12():
    """Q12 — Evolução do ticket médio por categoria de contrato."""
    key = 'q12'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT   AS ano,
            categoria_processo,
            COUNT(*)                                  AS qtd,
            ROUND(AVG(valor_global)::NUMERIC, 2)     AS ticket_medio,
            ROUND(SUM(valor_global)::NUMERIC / 1e9, 2) AS total_bilhoes
        FROM fato_contratos
        WHERE valor_global > 0 AND data_assinatura IS NOT NULL
          AND categoria_processo IS NOT NULL
          AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
        GROUP BY ano, categoria_processo
        ORDER BY categoria_processo, ano
    """)
    _set_cache(key, data)
    return data


@app.get('/q13/concentracao-orgaos', tags=['Queries de Negócio'])
def q13(n: int = Query(15, ge=5, le=50)):
    """Q13 — Concentração de gastos por esfera administrativa (CNPJ raiz)."""
    key = f'q13_{n}'
    if hit := _cached(key):
        return hit
    data = _query("""
        SELECT
            SUBSTRING(f.orgao_entidade_id, 1, 8)          AS cnpj_raiz,
            o.nome_orgao,
            COUNT(*)                                        AS total_contratos,
            ROUND(SUM(f.valor_global)::NUMERIC / 1e9, 2)  AS total_bilhoes,
            COUNT(DISTINCT f.cnpj_contratada)              AS fornecedores_distintos
        FROM fato_contratos f
        JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        WHERE f.valor_global > 0
        GROUP BY cnpj_raiz, o.nome_orgao
        ORDER BY total_bilhoes DESC LIMIT :n
    """, {'n': n})
    _set_cache(key, data)
    return data


# ---------------------------------------------------------------------------
# Busca livre
# ---------------------------------------------------------------------------
@app.get('/busca/orgao', tags=['Busca'])
def busca_orgao(
    nome: str = Query(..., min_length=3),
    n:    int = Query(50, ge=1, le=500),
):
    """Busca contratos de órgãos pelo nome (parcial, case-insensitive)."""
    return _query("""
        SELECT
            o.nome_orgao, o.nome_unidade,
            f.id_contrato_pncp, f.numero_contrato,
            f.categoria_processo,
            COALESCE(m.nome_modalidade, 'Não informado') AS modalidade,
            forn.nome_contratada                          AS fornecedor,
            f.data_assinatura,
            ROUND(f.valor_global::NUMERIC, 2)             AS valor_global
        FROM fato_contratos f
        JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        LEFT JOIN dim_modalidades m   ON f.id_modalidade = m.id_modalidade
        LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        WHERE f.valor_global > 0 AND o.nome_orgao ILIKE :nome
        ORDER BY f.valor_global DESC LIMIT :n
    """, {'nome': f'%{nome}%', 'n': n})


@app.get('/busca/fornecedor', tags=['Busca'])
def busca_fornecedor(
    nome: str = Query(..., min_length=3),
    n:    int = Query(50, ge=1, le=500),
):
    """Busca contratos de fornecedores pelo nome (parcial, case-insensitive)."""
    return _query("""
        SELECT
            forn.nome_contratada AS fornecedor,
            forn.cnpj_contratada,
            o.nome_orgao,
            f.id_contrato_pncp,
            f.categoria_processo,
            COALESCE(m.nome_modalidade, 'Não informado') AS modalidade,
            f.data_assinatura,
            ROUND(f.valor_global::NUMERIC, 2)             AS valor_global
        FROM fato_contratos f
        JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE f.valor_global > 0 AND forn.nome_contratada ILIKE :nome
        ORDER BY f.valor_global DESC LIMIT :n
    """, {'nome': f'%{nome}%', 'n': n})
