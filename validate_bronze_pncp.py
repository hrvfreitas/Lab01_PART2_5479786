import json
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

# ---------------------------------------------------------------------------
# CONFIGURAÇÕES E SETUP
# ---------------------------------------------------------------------------
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Caminhos baseados no seu repositório
RAW_DIR = Path('data/raw')
GX_OUTPUT = Path('data/gx/reports')
GX_OUTPUT.mkdir(parents=True, exist_ok=True)

def print_header():
    print(f"\n{'='*75}")
    print(f" PIPELINE DE QUALIDADE PNCP — CAMADA BRONZE (UNIFICADO)")
    print(f" Lab01_PART2_5479786 | Engine: GX {gx.__version__}")
    print(f"{'='*75}\n")

# ---------------------------------------------------------------------------
# PASSO 1: EXTRAÇÃO E FLATTENING (Lógica do validate_bronze_pncp.py)
# ---------------------------------------------------------------------------
def get_bronze_dataframe(limit_months=3):
    """Lê os JSONs brutos e realiza o flattening para o formato tabular."""
    logger.info(f"Lendo camada Bronze (Amostra: últimos {limit_months} meses)...")
    
    dirs_mes = sorted(RAW_DIR.glob('????_??'))
    if not dirs_mes:
        logger.error(f"Diretório {RAW_DIR} não encontrado ou vazio.")
        sys.exit(1)

    dirs_amostra = dirs_mes[-limit_months:]
    registros_brutos = []

    for d in dirs_amostra:
        for arq in sorted(d.glob('pagina_*.json')):
            try:
                with open(arq, encoding='utf-8') as f:
                    dados = json.load(f)
                    if isinstance(dados, list):
                        registros_brutos.extend(dados)
            except Exception as e:
                logger.warning(f"Falha ao ler {arq.name}: {e}")

    if not registros_brutos:
        logger.error("Nenhum registro encontrado.")
        sys.exit(1)

    # Flattening otimizado
    def _flatten(r: dict) -> dict:
        orgao = r.get('orgaoEntidade') or {}
        unidade = r.get('unidadeOrgao') or {}
        return {
            'numeroControlePNCP': r.get('numeroControlePNCP') or r.get('numeroControlePncpCompra'),
            'orgaoEntidade_cnpj': orgao.get('cnpj'),
            'orgaoEntidade_razaoSocial': orgao.get('razaoSocial'),
            'unidadeOrgao_ufSigla': unidade.get('ufSigla'),
            'valorGlobal': r.get('valorGlobal'),
            'valorInicial': r.get('valorInicial'),
            'dataAssinatura': r.get('dataAssinatura'),
            'tipoPessoa': r.get('tipoPessoa'),
            'esferaId': orgao.get('esferaId'),
            'anoContrato': r.get('anoContrato')
        }

    df = pd.DataFrame([_flatten(r) for r in registros_brutos])
    logger.info(f"DataFrame carregado: {len(df):,} linhas.")
    return df

# ---------------------------------------------------------------------------
# PASSO 2: VALIDAÇÃO GX 1.x E GERAÇÃO DE HTML
# ---------------------------------------------------------------------------
def run_validation(df):
    """Configura o GX 1.x, executa expectativas e gera Data Docs HTML."""
    logger.info("Configurando motor GX 1.x (Fluent API)...")
    
    context = gx.get_context(mode='ephemeral')
    
    # CORREÇÃO: No GX 1.x usa-se context.data_sources (plural)
    datasource_name = "pncp_datasource"
    # Adiciona ou recupera o datasource
    datasource = context.data_sources.add_pandas(name=datasource_name)
    
    # Adiciona o Asset e a definição do Batch (Obrigatório na v1)
    asset_name = "bronze_asset"
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe("batch_total")
    
    # Criando Suite
    suite_name = "pncp_bronze_suite"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # Obtendo o Validator
    validator = context.get_validator(
        batch_definition=batch_definition,
        batch_parameters={"dataframe": df},
        expectation_suite_name=suite_name
    )

    # --- DEFINIÇÃO DAS EXPECTATIVAS ---
    logger.info("Aplicando expectativas de qualidade...")
    
    validator.expect_column_values_to_not_be_null("numeroControlePNCP")
    validator.expect_column_values_to_match_regex("orgaoEntidade_cnpj", regex=r"^\d{14}$", mostly=0.98)
    validator.expect_column_values_to_be_between("valorGlobal", min_value=0, max_value=1e11)
    validator.expect_column_values_to_be_in_set("tipoPessoa", ["PJ", "PF"])
    validator.expect_column_values_to_be_between("anoContrato", min_value=2020, max_value=2026)

    # Execução
    result = validator.validate()

    # --- GERAÇÃO DO HTML (Data Docs Manual) ---
    logger.info("Renderizando relatório HTML...")
    renderer = ValidationResultsPageRenderer()
    rendered_content = renderer.render(result)
    html_content = DefaultJinjaPageView().render(rendered_content)
    
    report_file = GX_OUTPUT / f"report_pncp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    return result, report_file

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print_header()
    
    # 1. Extração (Amostra de 3 meses para não travar o tirinto)
    df = get_bronze_dataframe(limit_months=3)
    
    # 2. Validação e HTML
    result, html_path = run_validation(df)
    
    # 3. Sumário Sóbrio
    print(f"\n{'-'*75}")
    print(f" STATUS FINAL: {'✅ PASSOU' if result.success else '❌ FALHOU'}")
    print(f" TAXA DE SUCESSO: {result.statistics.get('success_percent', 0):.2f}%")
    print(f" RELATÓRIO HTML: {html_path.resolve()}")
    print(f"{'-'*75}\n")

if __name__ == "__main__":
    main()
