# =============================================================================
# gold_graficos.py — Gráficos Analíticos da Camada Gold
# Lab01_PART1_5479786 — PNCP Contratos Públicos
# Hercules Ramos Veloso de Freitas
#
# Conecta ao PostgreSQL e gera gráficos a partir das queries de negócio.
# Execute após gold_load.py:
#   python gold_graficos.py
# =============================================================================

import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine, text

# Ignora avisos de depreciação para manter o log limpo
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# ---------------------------------------------------------------------------
# Configuração de Ambiente
# ---------------------------------------------------------------------------
DB_URL       = 'postgresql://postgres:postgres@localhost:5432/pncp_db'
GRAFICOS_DIR = Path('data/graficos')
GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid")
PALETA = 'viridis'

def _get_engine():
    try:
        eng = create_engine(DB_URL)
        return eng
    except Exception as e:
        print(f'❌ Erro ao criar engine: {e}')
        sys.exit(1)

# ---------------------------------------------------------------------------
# G1 — Evolução Anual por Modalidade
# ---------------------------------------------------------------------------
def g1_evolucao_modalidade(engine):
    print('  G1: Evolução por modalidade...')
    sql = text("""
        SELECT
            EXTRACT(YEAR FROM f.data_assinatura)::INT AS ano,
            COALESCE(m.nome_modalidade, 'N/I') AS modalidade,
            ROUND(SUM(f.valor_global) / 1e9, 4) AS valor_bi
        FROM fato_contratos f
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE f.valor_global > 0
          AND f.data_assinatura IS NOT NULL
          AND EXTRACT(YEAR FROM f.data_assinatura) BETWEEN 2021 AND 2026
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    if df.empty: return
    df['modalidade'] = df['modalidade'].astype(str)
    
    pivot = df.pivot_table(index='ano', columns='modalidade', values='valor_bi', fill_value=0)
    fig, ax = plt.subplots(figsize=(14, 7))
    pivot.plot(kind='bar', stacked=True, ax=ax, colormap=PALETA, edgecolor='white')
    ax.set_title('G1 — Evolução Anual do Valor por Modalidade', fontsize=14)
    ax.set_ylabel('R$ Bilhões')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g1_evolucao_modalidade.png', dpi=150)
    plt.close()

# ---------------------------------------------------------------------------
# G2 — Top 10 Órgãos
# ---------------------------------------------------------------------------
def g2_top_orgaos(engine):
    print('  G2: Top 10 órgãos...')
    sql = text("""
        SELECT
            COALESCE(o.nome_orgao, f.orgao_entidade_id, 'ÓRGÃO N/I') AS orgao,
            ROUND(SUM(f.valor_global) / 1e9, 2) AS valor_bi
        FROM fato_contratos f
        LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        WHERE f.valor_global > 0
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn).sort_values('valor_bi')
    
    df['orgao'] = df['orgao'].astype(str).str[:40] # Trunca nomes longos
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x='valor_bi', y='orgao', palette=PALETA)
    plt.title('G2 — Top 10 Órgãos Contratantes (R$ Bi)')
    plt.xlabel('Valor Total (R$ Bilhões)')
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g2_top_orgaos.png')
    plt.close()

# ---------------------------------------------------------------------------
# G3 — Pareto Fornecedores (Blindado contra Float Error)
# ---------------------------------------------------------------------------
def g3_pareto_fornecedores(engine):
    print('  G3: Pareto de fornecedores...')
    sql = text("""
        SELECT
            COALESCE(forn.nome_contratada, f.cnpj_contratada, 'NÃO IDENTIFICADO') AS nome,
            SUM(f.valor_global) / 1e6 AS valor_mi
        FROM fato_contratos f
        LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        WHERE f.valor_global > 0
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    df['nome'] = df['nome'].astype(str).str[:30] # Garante String e trunca
    df['pct_acum'] = df['valor_mi'].cumsum() / df['valor_mi'].sum() * 100
    
    fig, ax1 = plt.subplots(figsize=(14, 7))
    ax2 = ax1.twinx()
    
    sns.barplot(x=df['nome'], y=df['valor_mi'], ax=ax1, palette=PALETA, alpha=0.7)
    ax2.plot(df['nome'], df['pct_acum'], color='red', marker='o', linewidth=2)
    
    ax1.tick_params(axis='x', rotation=45)
    ax1.set_ylabel('Valor Total (R$ Milhões)')
    ax2.set_ylabel('% Acumulado')
    ax2.set_ylim(0, 110)
    plt.title('G3 — Curva de Pareto: Concentração de Mercado (Top 15)')
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g3_pareto_fornecedores.png')
    plt.close()

# ---------------------------------------------------------------------------
# G4 — Sazonalidade (Filtro 2021-2026)
# ---------------------------------------------------------------------------
def g4_sazonalidade(engine):
    print('  G4: Sazonalidade...')
    sql = text("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT AS ano,
            EXTRACT(MONTH FROM data_assinatura)::INT AS mes,
            SUM(valor_global) / 1e6 AS valor_mi
        FROM fato_contratos
        WHERE data_assinatura BETWEEN '2021-01-01' AND '2026-12-31'
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='mes', y='valor_mi', hue='ano', marker='o', palette=PALETA)
    plt.title('G4 — Sazonalidade Mensal de Contratos (R$ Milhões)')
    plt.xticks(range(1, 13))
    plt.savefig(GRAFICOS_DIR / 'g4_sazonalidade.png')
    plt.close()

# ---------------------------------------------------------------------------
# G5 — Boxplot Modalidade
# ---------------------------------------------------------------------------
def g5_boxplot_modalidade(engine):
    print('  G5: Boxplot por modalidade...')
    sql = text("""
        SELECT COALESCE(m.nome_modalidade, 'N/I') AS modalidade, f.valor_global
        FROM fato_contratos f
        LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
        WHERE f.valor_global BETWEEN 10 AND 1e7
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    df['modalidade'] = df['modalidade'].astype(str)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='modalidade', y='valor_global', palette=PALETA)
    plt.yscale('log')
    plt.xticks(rotation=45)
    plt.title('G5 — Distribuição de Valores por Modalidade (Log Scale)')
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g5_boxplot_modalidade.png')
    plt.close()

# ---------------------------------------------------------------------------
# G6 — Delay Publicação
# ---------------------------------------------------------------------------
def g6_delay_publicacao(engine):
    print('  G6: Delay de publicação...')
    sql = text("""
        SELECT EXTRACT(YEAR FROM data_assinatura)::INT AS ano,
               (data_publicacao - data_assinatura) AS delay
        FROM fato_contratos
        WHERE data_publicacao >= data_assinatura 
          AND (data_publicacao - data_assinatura) BETWEEN 0 AND 180
          AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    plt.figure(figsize=(12, 6))
    sns.violinplot(data=df, x='ano', y='delay', palette=PALETA)
    plt.title('G6 — Delay de Transparência (Dias p/ Publicação)')
    plt.savefig(GRAFICOS_DIR / 'g6_delay_publicacao.png')
    plt.close()

# ---------------------------------------------------------------------------
# G7 — Top Fornecedores USP
# ---------------------------------------------------------------------------
def g7_usp_fornecedores(engine):
    print('  G7: Top fornecedores USP...')
    # Usamos % no lugar de 'São' para pegar 'SAO' ou 'SÃO'
    sql = text("""
        SELECT
            COALESCE(forn.nome_contratada, f.cnpj_contratada, 'N/I') AS fornecedor,
            COUNT(*) AS qtd,
            SUM(f.valor_global) / 1e6 AS valor_mi
        FROM fato_contratos f
        JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
        LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
        WHERE o.nome_orgao ILIKE '%Universidade%S%o%Paulo%'
        GROUP BY 1 ORDER BY 3 DESC LIMIT 10
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    if df.empty:
        print("  ⚠️ Nenhum dado encontrado para USP (verifique o nome no banco).")
        return

    df['fornecedor'] = df['fornecedor'].astype(str).str[:35]
    plt.figure(figsize=(12, 7))
    sns.barplot(data=df, x='valor_mi', y='fornecedor', palette=PALETA)
    plt.title('G7 — Top 10 Fornecedores USP (R$ Milhões)')
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g7_usp_fornecedores.png')
    plt.close()
    print("    ✓ g7_usp_fornecedores.png gerado!")


# ---------------------------------------------------------------------------
# G8 — Heatmap Ticket Médio (Incluso)
# ---------------------------------------------------------------------------
def g8_heatmap_categoria(engine):
    print('  G8: Heatmap de categorias...')
    sql = text("""
        SELECT
            EXTRACT(YEAR FROM data_assinatura)::INT AS ano,
            LEFT(COALESCE(categoria_processo, 'N/I'), 20) AS categoria,
            AVG(valor_global) AS ticket_medio
        FROM fato_contratos
        WHERE data_assinatura BETWEEN '2021-01-01' AND '2026-12-31'
        GROUP BY 1, 2
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    if df.empty: return
    pivot = df.pivot_table(index='categoria', columns='ano', values='ticket_medio')
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot / 1e3, annot=True, fmt='.0f', cmap='YlGnBu', cbar_kws={'label': 'R$ Mil'})
    plt.title('G8 — Ticket Médio por Categoria (R$ Mil)')
    plt.tight_layout()
    plt.savefig(GRAFICOS_DIR / 'g8_ticket_medio_heatmap.png')
    plt.close()

# ---------------------------------------------------------------------------
# Execução Principal
# ---------------------------------------------------------------------------
def main():
    engine = _get_engine()
    print("\n" + "="*50)
    print("🚀 Gerando Dashboard Analítico Auditado")
    print("="*50)
    
    g1_evolucao_modalidade(engine)
    g2_top_orgaos(engine)
    g3_pareto_fornecedores(engine)
    g4_sazonalidade(engine)
    g5_boxplot_modalidade(engine)
    g6_delay_publicacao(engine)
    g7_usp_fornecedores(engine)
    g8_heatmap_categoria(engine)
    
    print(f"\n✅ Dashboard concluído em: {GRAFICOS_DIR.resolve()}")

if __name__ == "__main__":
    main()
