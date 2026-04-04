#!/usr/bin/env python3
# =============================================================================
# superset_setup.py — Configura o Superset via API REST
# Lab01_PART1_5479786 — PNCP Contratos Públicos
#
# Executa APÓS os containers estarem rodando (docker compose up -d).
# Registra o banco pncp_db e cria os datasets das tabelas Gold.
#
# Uso:
#   python superset_setup.py
# =============================================================================
import sys
import time

import requests

SUPERSET_URL = 'http://127.0.0.1:8088'
ADMIN_USER   = 'admin'
ADMIN_PASS   = 'admin123'

# Tabelas Gold que viram datasets no Superset
DATASETS = [
    'fato_contratos',
    'dim_fornecedores',
    'dim_orgaos',
    'dim_modalidades',
    'dim_tempo',
]


# ---------------------------------------------------------------------------
# Helpers HTTP
# ---------------------------------------------------------------------------
def _login(session: requests.Session) -> str:
    """Autentica e retorna o token JWT."""
    resp = session.post(
        f'{SUPERSET_URL}/api/v1/security/login',
        json={
            'username': ADMIN_USER,
            'password': ADMIN_PASS,
            'provider': 'db',
            'refresh':  True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()['access_token']
    session.headers.update({'Authorization': f'Bearer {token}'})
    return token


def _csrf(session: requests.Session) -> str:
    """Obtém o token CSRF."""
    resp = session.get(f'{SUPERSET_URL}/api/v1/security/csrf_token/',
                       timeout=15)
    resp.raise_for_status()
    token = resp.json()['result']
    session.headers.update({'X-CSRFToken': token})
    return token


def _aguardar_superset(max_tentativas: int = 20) -> None:
    """Aguarda o Superset inicializar."""
    print('⏳ Aguardando Superset inicializar', end='', flush=True)
    for _ in range(max_tentativas):
        try:
            r = requests.get(f'{SUPERSET_URL}/health', timeout=50)
            if r.status_code == 200:
                print(' ✓')
                return
        except requests.exceptions.ConnectionError:
            pass
        print('.', end='', flush=True)
        time.sleep(5)
    print()
    print('❌ Superset não respondeu. Verifique: docker compose ps')
    sys.exit(1)


# ---------------------------------------------------------------------------
# Registra o banco pncp_db
# ---------------------------------------------------------------------------
def registrar_banco(session: requests.Session) -> int:
    """Registra o banco pncp_db no Superset. Retorna o database_id."""

    # Verifica se já existe
    resp = session.get(f'{SUPERSET_URL}/api/v1/database/', timeout=15)
    resp.raise_for_status()
    for db in resp.json().get('result', []):
        if db['database_name'] == 'PNCP Gold':
            print(f'  ℹ️  Banco "PNCP Gold" já existe (id={db["id"]})')
            return db['id']

    payload = {
        'database_name': 'PNCP Gold',
        'sqlalchemy_uri': (
            'postgresql://postgres:postgres@pncp_postgres:5432/pncp_db'
        ),
        'expose_in_sqllab':         True,
        'allow_run_async':          True,
        'allow_ctas':               False,
        'allow_cvas':               False,
        'allow_dml':                False,
        'allow_file_upload':        False,
        'cache_timeout':            300,
        'extra': '{"metadata_params":{},"engine_params":{},'
                 '"metadata_cache_timeout":{},"schemas_allowed_for_file_upload":[]}',
    }

    resp = session.post(
        f'{SUPERSET_URL}/api/v1/database/',
        json=payload,
        timeout=30,
    )

    if resp.status_code == 201:
        db_id = resp.json()['id']
        print(f'  ✓ Banco "PNCP Gold" registrado (id={db_id})')
        return db_id
    else:
        print(f'  ❌ Erro ao registrar banco: {resp.status_code} — {resp.text}')
        sys.exit(1)


# ---------------------------------------------------------------------------
# Cria datasets (tabelas Gold)
# ---------------------------------------------------------------------------
def criar_datasets(session: requests.Session, db_id: int) -> None:
    """Cria um dataset no Superset para cada tabela Gold."""

    # Datasets já existentes
    resp = session.get(f'{SUPERSET_URL}/api/v1/dataset/', timeout=15)
    resp.raise_for_status()
    existentes = {d['table_name'] for d in resp.json().get('result', [])}

    for tabela in DATASETS:
        if tabela in existentes:
            print(f'  ℹ️  Dataset "{tabela}" já existe')
            continue

        payload = {
            'database':   db_id,
            'schema':     'public',
            'table_name': tabela,
        }
        resp = session.post(
            f'{SUPERSET_URL}/api/v1/dataset/',
            json=payload,
            timeout=30,
        )
        if resp.status_code == 201:
            print(f'  ✓ Dataset "{tabela}" criado')
        else:
            print(f'  ⚠️  "{tabela}": {resp.status_code} — {resp.text[:120]}')


# ---------------------------------------------------------------------------
# Cria métricas customizadas no dataset fato_contratos
# ---------------------------------------------------------------------------
def criar_metricas(session: requests.Session) -> None:
    """Adiciona métricas pré-calculadas ao dataset fato_contratos."""

    # Busca o id do dataset fato_contratos
    resp = session.get(
        f'{SUPERSET_URL}/api/v1/dataset/',
        params={'q': '(filters:!((col:table_name,opr:eq,val:fato_contratos)))'},
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json().get('result', [])
    if not results:
        print('  ⚠️  fato_contratos não encontrado para métricas')
        return

    dataset_id = results[0]['id']

    metricas = [
        {
            'metric_name':  'total_contratos',
            'expression':   'COUNT(*)',
            'metric_type':  'count',
            'verbose_name': 'Total de Contratos',
            'd3format':     ',d',
        },
        {
            'metric_name':  'valor_global_sum',
            'expression':   'SUM(valor_global)',
            'metric_type':  'sum',
            'verbose_name': 'Valor Total (R$)',
            'd3format':     'R$,.2f',
        },
        {
            'metric_name':  'valor_global_avg',
            'expression':   'AVG(valor_global)',
            'metric_type':  'avg',
            'verbose_name': 'Ticket Médio (R$)',
            'd3format':     'R$,.2f',
        },
        {
            'metric_name':  'valor_global_median',
            'expression':   'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_global)',
            'metric_type':  'avg',
            'verbose_name': 'Mediana Valor (R$)',
            'd3format':     'R$,.2f',
        },
        {
            'metric_name':  'fornecedores_distintos',
            'expression':   'COUNT(DISTINCT cnpj_contratada)',
            'metric_type':  'count_distinct',
            'verbose_name': 'Fornecedores Distintos',
            'd3format':     ',d',
        },
        {
            'metric_name':  'orgaos_distintos',
            'expression':   'COUNT(DISTINCT orgao_entidade_id)',
            'metric_type':  'count_distinct',
            'verbose_name': 'Órgãos Distintos',
            'd3format':     ',d',
        },
    ]

    resp = session.put(
        f'{SUPERSET_URL}/api/v1/dataset/{dataset_id}',
        json={'metrics': metricas},
        timeout=30,
    )
    if resp.status_code in (200, 201):
        print(f'  ✓ {len(metricas)} métricas criadas em fato_contratos')
    else:
        print(f'  ⚠️  Métricas: {resp.status_code} — {resp.text[:120]}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print('\n' + '=' * 60)
    print('  SUPERSET SETUP — PNCP Analytics')
    print('  Lab01_PART1_5479786')
    print('=' * 60 + '\n')

    _aguardar_superset()

    session = requests.Session()
    session.headers.update({'Content-Type': 'application/json'})

    print('🔑 Autenticando...')
    _login(session)
    _csrf(session)
    print('  ✓ Autenticado\n')

    print('🗄️  Registrando banco de dados...')
    db_id = registrar_banco(session)

    print('\n📊 Criando datasets...')
    criar_datasets(session, db_id)

    print('\n📐 Criando métricas...')
    criar_metricas(session)

    print(f'\n{"=" * 60}')
    print('✅ Setup concluído!')
    print(f'   Acesse: {SUPERSET_URL}')
    print(f'   Usuário: {ADMIN_USER}')
    print(f'   Senha:   {ADMIN_PASS}')
    print('=' * 60)
    print('\nPróximos passos no Superset:')
    print('  1. Charts → + Chart → selecione um dataset')
    print('  2. Dashboards → + Dashboard → arraste os charts')
    print('  3. SQL Lab → escreva queries ad-hoc contra o pncp_db')


if __name__ == '__main__':
    main()
