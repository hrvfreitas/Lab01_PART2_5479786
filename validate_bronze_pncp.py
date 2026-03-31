import json
import sys
import warnings
from datetime import datetime
from pathlib import Path
import pandas as pd

warnings.filterwarnings('ignore')

try:
    import great_expectations as gx
except ImportError:
    print('X Great Expectations não instalado. Execute: pip install great-expectations')
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configurações de Diretórios
# ---------------------------------------------------------------------------
RAW_DIR     = Path('data/raw')
GX_ROOT     = Path('data/gx')
GX_DOCS_DIR = GX_ROOT / 'gx_docs'
GX_ROOT.mkdir(parents=True, exist_ok=True)
GX_DOCS_DIR.mkdir(parents=True, exist_ok=True)

print(f'\n{"="*62}')
print('  GREAT EXPECTATIONS 1.x — Camada Bronze (PNCP)')
print(f'  Lote de Processamento: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
print(f'{"="*62}\n')

# ===========================================================================
# PASSO 1 — Leitura e Flattening
# ===========================================================================
print('[1/5] Lendo JSONs brutos...')
dirs_mes = sorted(RAW_DIR.glob('????_??'))
if not dirs_mes:
    print(f'X Nenhum dado encontrado em {RAW_DIR}')
    sys.exit(1)

dirs_amostra = dirs_mes[-3:]
registros_brutos = []
for d in dirs_amostra:
    for arq in sorted(d.glob('pagina_*.json')):
        with open(arq, encoding='utf-8') as f:
            dados = json.load(f)
            if isinstance(dados, list):
                registros_brutos.extend(dados)

def _flatten(r: dict) -> dict:
    orgao   = r.get('orgaoEntidade')     or {}
    unidade = r.get('unidadeOrgao')      or {}
    tipo    = r.get('tipoContrato')      or {}
    categ   = r.get('categoriaProcesso') or {}
    return {
        'numeroControlePNCP':      r.get('numeroControlePNCP') or r.get('numeroControlePncpCompra'),
        'orgaoEntidade_cnpj':      orgao.get('cnpj'),
        'valorGlobal':             r.get('valorGlobal'),
        'valorInicial':            r.get('valorInicial'),
        'dataAssinatura':          r.get('dataAssinatura'),
        'dataPublicacaoPncp':      r.get('dataPublicacaoPncp'),
        'tipoPessoa':              r.get('tipoPessoa'),
        'orgaoEntidade_esferaId':  orgao.get('esferaId'),
        'anoContrato':             r.get('anoContrato'),
        'niFornecedor':            r.get('niFornecedor'),
        'tipoContrato_nome':       tipo.get('nome'),
        'categoriaProcesso_nome':  categ.get('nome'),
    }

df = pd.DataFrame([_flatten(r) for r in registros_brutos])
print(f'  → DataFrame pronto: {len(df):,} linhas.')

# ===========================================================================
# PASSO 2 — Contexto e Datasource
# ===========================================================================
print('\n[2/5] Configurando Contexto GX...')

context = gx.get_context(mode='ephemeral')

ds_name = "pncp_datasource"
as_name = "bronze_asset"

datasource       = context.data_sources.add_pandas(name=ds_name)
asset            = datasource.add_dataframe_asset(name=as_name)
batch_definition = asset.add_batch_definition_whole_dataframe("batch_total")

print('  → Definições de dados prontas.')

# ===========================================================================
# PASSO 3 — Suite e Validator
# ===========================================================================
print('\n[3/5] Criando Expectation Suite...')

suite_name = "pncp_bronze_suite"
try:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
except Exception:
    suite = context.add_expectation_suite(expectation_suite_name=suite_name)

batch_request = batch_definition.build_batch_request(batch_parameters={"dataframe": df})
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# ── Expectations ────────────────────────────────────────────────────────────
# numeroControlePNCP
validator.expect_column_to_exist("numeroControlePNCP")
validator.expect_column_values_to_not_be_null("numeroControlePNCP")
validator.expect_column_values_to_be_unique("numeroControlePNCP")

# orgaoEntidade_cnpj
validator.expect_column_to_exist("orgaoEntidade_cnpj")
validator.expect_column_values_to_not_be_null("orgaoEntidade_cnpj")

# valorGlobal
validator.expect_column_to_exist("valorGlobal")
validator.expect_column_values_to_not_be_null("valorGlobal")
validator.expect_column_values_to_be_between("valorGlobal", min_value=0)

# valorInicial
validator.expect_column_to_exist("valorInicial")
validator.expect_column_values_to_be_between("valorInicial", min_value=0)

# dataAssinatura
validator.expect_column_to_exist("dataAssinatura")
validator.expect_column_values_to_not_be_null("dataAssinatura")
validator.expect_column_values_to_match_regex("dataAssinatura", r"^\d{4}-\d{2}-\d{2}")

# dataPublicacaoPncp
validator.expect_column_to_exist("dataPublicacaoPncp")
validator.expect_column_values_to_not_be_null("dataPublicacaoPncp")

# tipoPessoa
validator.expect_column_values_to_be_in_set("tipoPessoa", ["PF", "PJ"])

# tipoContrato_nome
validator.expect_column_to_exist("tipoContrato_nome")
validator.expect_column_values_to_not_be_null("tipoContrato_nome")

# categoriaProcesso_nome
validator.expect_column_to_exist("categoriaProcesso_nome")
validator.expect_column_values_to_not_be_null("categoriaProcesso_nome")

# anoContrato
validator.expect_column_values_to_be_between("anoContrato", min_value=2021, max_value=2030)

# niFornecedor
validator.expect_column_to_exist("niFornecedor")
validator.expect_column_values_to_not_be_null("niFornecedor")

print(f'  → Validator conectado ao DataFrame ({len(df):,} linhas).')

# ===========================================================================
# PASSO 4 — Execução da Validação
# ===========================================================================
print('\n[4/5] Executando Validação...')
validation_result = validator.validate()

success = validation_result.success
stats   = validation_result.statistics

total  = stats.get("evaluated_expectations", 0)
passou = stats.get("successful_expectations", 0)
falhou = total - passou
pct    = (passou / total * 100) if total > 0 else 0

print(f'\n  {"="*40}')
print(f'  STATUS: {"✅ PASSOU" if success else "❌ FALHOU"}')
print(f'  Expectations: {total}')
print(f'  Passou: {passou}')
print(f'  Taxa de Sucesso: {pct:.2f}%')
print(f'  {"="*40}\n')

# ===========================================================================
# PASSO 5 — Geração do Relatório HTML
# ===========================================================================
print('[5/5] Gerando Relatório HTML...')

# ── Coleta dos detalhes de cada expectativa ──────────────────────────────────
# GX 1.x: ExpectationConfiguration usa .type (não .expectation_type)
#          e .kwargs pode ser um objeto com .get() ou um dict direto.
detalhes = []
for res in validation_result.results:
    cfg = res.expectation_config

    # ── nome do tipo (GX 1.x usa .type; fallback para versões mistas)
    exp_type = getattr(cfg, "type", None) or getattr(cfg, "expectation_type", "unknown")

    # ── coluna: kwargs pode ser dict ou objeto Pydantic
    raw_kwargs = getattr(cfg, "kwargs", {})
    if hasattr(raw_kwargs, "get"):
        column = raw_kwargs.get("column") or "—"
    elif hasattr(raw_kwargs, "__dict__"):
        column = getattr(raw_kwargs, "column", "—") or "—"
    else:
        column = str(raw_kwargs) if raw_kwargs else "—"

    ok          = res.success
    result_dict = res.result or {}
    falhas_val  = result_dict.get("unexpected_count", 0) or 0
    total_rows  = result_dict.get("element_count", len(df)) or len(df)
    detalhes.append({
        "coluna":       column,
        "exp":          exp_type,
        "sucesso":      ok,
        "falhas":       int(falhas_val),
        "total_linhas": int(total_rows),
    })

# ── Mapa de descrições amigáveis ─────────────────────────────────────────────
DESC_MAP = {
    "expect_column_to_exist":                  "Coluna existe no DataFrame",
    "expect_column_values_to_not_be_null":     "Sem valores nulos",
    "expect_column_values_to_be_unique":       "Valores únicos (sem duplicatas)",
    "expect_column_value_lengths_to_equal":    "Comprimento fixo esperado",
    "expect_column_values_to_be_between":      "Valores dentro do intervalo esperado",
    "expect_column_values_to_match_regex":     "Formato regex válido",
    "expect_column_values_to_be_in_set":       "Valores dentro do conjunto permitido",
}

def desc(exp_type):
    return DESC_MAP.get(exp_type, exp_type)

# ── Agrupamento por coluna para os cards ────────────────────────────────────
from collections import defaultdict
grouped = defaultdict(lambda: {"pass": 0, "fail": 0, "total_falhas": 0, "total_rows": len(df), "exps": []})
for d in detalhes:
    col = d["coluna"]
    if d["sucesso"]:
        grouped[col]["pass"] += 1
    else:
        grouped[col]["fail"] += 1
    grouped[col]["total_falhas"] += d["falhas"]
    grouped[col]["total_rows"]    = d["total_linhas"]
    grouped[col]["exps"].append(d)

# ── Helpers de template ──────────────────────────────────────────────────────
def chip_class(d):
    if d["sucesso"]:   return "pass"
    if d["falhas"] < 200: return "warn"
    return "fail"

def card_class(g):
    if g["fail"] == 0:           return "pass"
    if g["total_falhas"] < 200:  return "warn"
    return "fail"

def card_pct(g):
    if g["fail"] == 0: return 100
    return max(0, 100 - (g["total_falhas"] / g["total_rows"] * 100))

ts_fmt = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

# ── Build das linhas da tabela ───────────────────────────────────────────────
table_rows_html = ""
for i, d in enumerate(detalhes, 1):
    cls  = chip_class(d)
    icon = "✓ PASS" if d["sucesso"] else "✗ FAIL"
    fval = f'<span class="fail-count">{d["falhas"]:,}</span>' if d["falhas"] else '<span class="fail-count zero">0</span>'
    table_rows_html += f"""
      <tr>
        <td style="color:var(--muted);font-family:var(--font-mono);font-size:11px">{str(i).zfill(2)}</td>
        <td class="col-name">{d['coluna']}</td>
        <td class="exp-type">{d['exp']}</td>
        <td><span class="chip {cls}">{icon}</span></td>
        <td>{fval}</td>
        <td style="color:var(--muted);font-size:12px">{desc(d['exp'])}</td>
      </tr>"""

# ── Build dos detail cards ───────────────────────────────────────────────────
detail_cards_html = ""
for col, g in grouped.items():
    cls  = card_class(g)
    pct_bar = card_pct(g)
    n_exps = len(g["exps"])
    detail_cards_html += f"""
      <div class="detail-card {cls}">
        <div class="dc-col">{col}</div>
        <div class="dc-exp">{n_exps} expectativa{"s" if n_exps > 1 else ""} avaliada{"s" if n_exps > 1 else ""}</div>
        <div class="dc-stats">
          <span>Passou: <strong style="color:var(--green)">{g['pass']}</strong></span>
          <span>Falhou: <strong style="color:{'var(--red)' if g['fail'] else 'var(--muted)'}">{g['fail']}</strong></span>
          <span>Violações: <strong style="color:{'var(--amber)' if g['total_falhas'] else 'var(--muted)'}">{g['total_falhas']:,}</strong></span>
        </div>
        <div class="mini-bar">
          <div class="mini-track">
            <div class="mini-fill {cls}" style="width:{pct_bar:.1f}%"></div>
          </div>
        </div>
      </div>"""

# ── Banner dinâmico ──────────────────────────────────────────────────────────
if pct >= 100:
    banner_class    = "pass"
    banner_icon     = "✅"
    banner_title    = "Validação Concluída — PASSOU"
    banner_subtitle = "Todas as expectativas foram atendidas na camada Bronze do PNCP."
elif pct >= 75:
    banner_class    = "warn"
    banner_icon     = "⚠️"
    banner_title    = "Validação Concluída — PASSOU PARCIALMENTE"
    banner_subtitle = f"{falhou} expectativa(s) com violações — revisar antes da camada Silver."
else:
    banner_class    = "fail"
    banner_icon     = "❌"
    banner_title    = "Validação Concluída — FALHOU"
    banner_subtitle = f"{falhou} expectativa(s) críticas com violações — pipeline interrompido."

# ===========================================================================
# HTML FINAL
# ===========================================================================
html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>GX Validation Report — PNCP Bronze</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --bg:        #0b0f1a;
    --surface:   #111827;
    --surface2:  #1a2235;
    --border:    #1f2d45;
    --green:     #00e5a0;
    --green-dim: #00a372;
    --red:       #ff4d6d;
    --amber:     #ffb347;
    --blue:      #4facfe;
    --text:      #e2e8f0;
    --muted:     #64748b;
    --font-mono: 'Space Mono', monospace;
    --font-body: 'DM Sans', sans-serif;
  }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-body);
    min-height: 100vh;
    overflow-x: hidden;
  }}
  body::before {{
    content: '';
    position: fixed; inset: 0;
    background-image:
      linear-gradient(rgba(0,229,160,.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(0,229,160,.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none; z-index: 0;
  }}
  body::after {{
    content: '';
    position: fixed; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent, var(--green), transparent);
    opacity: .25;
    animation: scan 4s linear infinite;
    pointer-events: none; z-index: 1;
  }}
  @keyframes scan {{ from {{ top: -2px; }} to {{ top: 100vh; }} }}
  .wrap {{ position: relative; z-index: 2; max-width: 1100px; margin: 0 auto; padding: 0 24px 60px; }}

  header {{
    padding: 48px 0 32px;
    border-bottom: 1px solid var(--border);
    display: flex; flex-direction: column; gap: 6px;
  }}
  .badge {{
    display: inline-flex; align-items: center; gap: 8px;
    font-family: var(--font-mono); font-size: 11px; letter-spacing: 2px;
    text-transform: uppercase; color: var(--green);
    background: rgba(0,229,160,.08);
    border: 1px solid rgba(0,229,160,.2);
    padding: 4px 12px; border-radius: 2px; width: fit-content;
    animation: fadein .6s ease;
  }}
  .badge::before {{ content: '●'; animation: blink 1.2s step-start infinite; }}
  @keyframes blink {{ 50% {{ opacity: 0; }} }}
  h1 {{
    font-family: var(--font-mono);
    font-size: clamp(22px, 4vw, 34px); font-weight: 700;
    color: #fff; letter-spacing: -1px;
    animation: fadein .6s .1s ease both;
  }}
  h1 span {{ color: var(--green); }}
  .meta {{
    font-family: var(--font-mono); font-size: 12px; color: var(--muted);
    display: flex; gap: 24px; flex-wrap: wrap;
    animation: fadein .6s .2s ease both;
  }}
  @keyframes fadein {{ from {{ opacity:0; transform:translateY(10px); }} to {{ opacity:1; transform:none; }} }}

  .status-banner {{
    margin: 32px 0;
    display: flex; align-items: center; gap: 20px;
    background: var(--surface);
    border: 1px solid;
    border-left-width: 4px;
    border-radius: 4px;
    padding: 20px 28px;
    animation: fadein .6s .3s ease both;
  }}
  .status-banner.pass {{ border-color: var(--green); }}
  .status-banner.fail {{ border-color: var(--red); }}
  .status-banner.warn {{ border-color: var(--amber); }}
  .status-icon {{ font-size: 36px; line-height: 1; }}
  .status-text h2 {{ font-size: 18px; font-weight: 700; color: #fff; }}
  .status-text p  {{ font-size: 13px; color: var(--muted); margin-top: 2px; }}
  .status-pct {{
    margin-left: auto;
    font-family: var(--font-mono);
    font-size: clamp(32px, 5vw, 52px); font-weight: 700; line-height: 1;
  }}
  .status-banner.pass .status-pct {{ color: var(--green); }}
  .status-banner.fail .status-pct {{ color: var(--red); }}
  .status-banner.warn .status-pct {{ color: var(--amber); }}

  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px; margin-bottom: 36px;
    animation: fadein .6s .35s ease both;
  }}
  .card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 22px 20px;
    position: relative; overflow: hidden;
    transition: transform .2s, border-color .2s;
  }}
  .card:hover {{ transform: translateY(-3px); border-color: var(--green-dim); }}
  .card::after {{
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--accent-color, var(--green));
  }}
  .card-label {{
    font-size: 11px; font-family: var(--font-mono);
    text-transform: uppercase; letter-spacing: 1.5px;
    color: var(--muted); margin-bottom: 10px;
  }}
  .card-value {{
    font-family: var(--font-mono); font-size: 32px; font-weight: 700;
    color: var(--accent-color, var(--green)); line-height: 1;
  }}
  .card-sub {{ font-size: 12px; color: var(--muted); margin-top: 6px; }}

  .section-head {{
    display: flex; align-items: center; gap: 12px;
    margin: 36px 0 16px;
    font-family: var(--font-mono); font-size: 13px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 2px; color: var(--muted);
  }}
  .section-head::after {{
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(90deg, var(--border), transparent);
  }}

  .progress-wrap {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 18px 20px; margin-bottom: 28px;
    animation: fadein .6s .4s ease both;
  }}
  .progress-label {{
    display: flex; justify-content: space-between;
    font-size: 12px; color: var(--muted); margin-bottom: 10px;
    font-family: var(--font-mono);
  }}
  .progress-track {{ background: var(--border); border-radius: 2px; height: 8px; overflow: hidden; }}
  .progress-fill {{
    height: 100%; border-radius: 2px;
    background: linear-gradient(90deg, var(--green), #00ffc3);
    box-shadow: 0 0 12px rgba(0,229,160,.4);
    width: 0; transition: width 1.4s cubic-bezier(.22,.61,.36,1);
  }}

  .suite-box {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 20px 24px;
    font-family: var(--font-mono); font-size: 12px;
    line-height: 2; color: var(--muted);
    animation: fadein .6s .45s ease both;
  }}
  .suite-box b {{ color: var(--text); }}

  .table-wrap {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; overflow: hidden;
    animation: fadein .6s .45s ease both;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead {{ background: var(--surface2); }}
  th {{
    padding: 12px 16px; text-align: left;
    font-family: var(--font-mono); font-size: 10px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1.5px; color: var(--muted);
    border-bottom: 1px solid var(--border);
  }}
  td {{
    padding: 13px 16px; font-size: 13px;
    border-bottom: 1px solid rgba(31,45,69,.6); vertical-align: middle;
  }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(255,255,255,.02); }}
  .col-name {{ font-family: var(--font-mono); font-size: 12px; color: var(--blue); }}
  .exp-type {{ font-family: var(--font-mono); font-size: 11px; color: var(--muted); }}
  .chip {{
    display: inline-flex; align-items: center; gap: 5px;
    font-family: var(--font-mono); font-size: 11px; font-weight: 700;
    padding: 3px 10px; border-radius: 2px; border: 1px solid;
  }}
  .chip.pass {{ color: var(--green); border-color: rgba(0,229,160,.3); background: rgba(0,229,160,.08); }}
  .chip.fail {{ color: var(--red);   border-color: rgba(255,77,109,.3); background: rgba(255,77,109,.08); }}
  .chip.warn {{ color: var(--amber); border-color: rgba(255,179,71,.3); background: rgba(255,179,71,.08); }}
  .fail-count {{ font-family: var(--font-mono); font-size: 12px; color: var(--red); }}
  .fail-count.zero {{ color: var(--muted); }}

  .detail-grid {{
    display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 14px; margin-top: 8px;
    animation: fadein .6s .5s ease both;
  }}
  .detail-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 16px 18px; position: relative;
    transition: border-color .2s;
  }}
  .detail-card.pass {{ border-left: 3px solid var(--green); }}
  .detail-card.fail {{ border-left: 3px solid var(--red); }}
  .detail-card.warn {{ border-left: 3px solid var(--amber); }}
  .detail-card:hover {{ border-color: var(--green-dim); }}
  .dc-col {{ font-family: var(--font-mono); font-size: 12px; color: var(--blue); margin-bottom: 4px; }}
  .dc-exp {{ font-size: 12px; color: var(--muted); margin-bottom: 10px; }}
  .dc-stats {{ display: flex; gap: 16px; font-family: var(--font-mono); font-size: 11px; }}
  .dc-stats span {{ color: var(--muted); }}
  .dc-stats strong {{ color: var(--text); }}
  .mini-bar {{ margin-top: 10px; }}
  .mini-track {{ background: var(--border); border-radius: 1px; height: 4px; }}
  .mini-fill {{
    height: 100%; border-radius: 1px;
    transition: width 1.6s cubic-bezier(.22,.61,.36,1) .3s;
  }}
  .mini-fill.pass {{ background: var(--green); }}
  .mini-fill.fail {{ background: var(--red); }}
  .mini-fill.warn {{ background: var(--amber); }}

  footer {{
    margin-top: 48px; padding-top: 20px;
    border-top: 1px solid var(--border);
    display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px;
    font-family: var(--font-mono); font-size: 11px; color: var(--muted);
    animation: fadein .6s .6s ease both;
  }}
  @media (max-width: 600px) {{
    .status-pct {{ font-size: 28px; }}
    .cards {{ grid-template-columns: 1fr 1fr; }}
  }}
</style>
</head>
<body>
<div class="wrap">

  <header>
    <div class="badge">Great Expectations 1.x &nbsp;·&nbsp; Bronze Layer</div>
    <h1>PNCP <span>Validation</span> Report</h1>
    <div class="meta">
      <span>📁 Suite: <b>{suite_name}</b></span>
      <span>🗄️ Source: <b>{str(RAW_DIR)}</b></span>
      <span>📅 <b>{ts_fmt}</b></span>
      <span>👤 Hercules Freitas · PECE USP</span>
    </div>
  </header>

  <div class="status-banner {banner_class}">
    <div class="status-icon">{banner_icon}</div>
    <div class="status-text">
      <h2>{banner_title}</h2>
      <p>{banner_subtitle}</p>
    </div>
    <div class="status-pct">{pct:.2f}%</div>
  </div>

  <div class="cards">
    <div class="card" style="--accent-color:var(--blue)">
      <div class="card-label">Registros Validados</div>
      <div class="card-value">{len(df):,}</div>
      <div class="card-sub">linhas no DataFrame</div>
    </div>
    <div class="card" style="--accent-color:var(--muted)">
      <div class="card-label">Expectations Avaliadas</div>
      <div class="card-value">{total}</div>
      <div class="card-sub">regras executadas</div>
    </div>
    <div class="card" style="--accent-color:var(--green)">
      <div class="card-label">Passaram</div>
      <div class="card-value">{passou}</div>
      <div class="card-sub">✓ sem violações</div>
    </div>
    <div class="card" style="--accent-color:var(--red)">
      <div class="card-label">Falharam</div>
      <div class="card-value">{falhou}</div>
      <div class="card-sub">⚠ requerem atenção</div>
    </div>
    <div class="card" style="--accent-color:var(--amber)">
      <div class="card-label">Meses Ingeridos</div>
      <div class="card-value">{len(dirs_amostra)}</div>
      <div class="card-sub">lotes mais recentes</div>
    </div>
    <div class="card" style="--accent-color:var(--blue)">
      <div class="card-label">Colunas Mapeadas</div>
      <div class="card-value">{len(df.columns)}</div>
      <div class="card-sub">campos PNCP</div>
    </div>
  </div>

  <div class="progress-wrap">
    <div class="progress-label">
      <span>Taxa de Sucesso Global</span>
      <span>{pct:.2f}%</span>
    </div>
    <div class="progress-track">
      <div class="progress-fill" style="width:{pct:.2f}%"></div>
    </div>
  </div>

  <div class="section-head">Suite &amp; Contexto</div>
  <div class="suite-box">
    <b>Contexto:</b> ephemeral &nbsp;|&nbsp;
    <b>Datasource:</b> {ds_name} (pandas) &nbsp;|&nbsp;
    <b>Asset:</b> {as_name} &nbsp;|&nbsp;
    <b>Batch:</b> batch_total &nbsp;|&nbsp;
    <b>Suite:</b> {suite_name}<br>
    <b>Diretório RAW:</b> {RAW_DIR}/ &nbsp;|&nbsp;
    <b>Período:</b> últimos {len(dirs_amostra)} meses de páginas JSON &nbsp;|&nbsp;
    <b>Flatten:</b> estrutura aninhada → {len(df.columns)} colunas planas
  </div>

  <div class="section-head">Detalhes por Expectativa</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>#</th><th>Coluna</th><th>Expectativa</th>
          <th>Resultado</th><th>Falhas</th><th>Descrição</th>
        </tr>
      </thead>
      <tbody>
        {table_rows_html}
      </tbody>
    </table>
  </div>

  <div class="section-head">Visão por Coluna</div>
  <div class="detail-grid">
    {detail_cards_html}
  </div>

  <footer>
    <span>Lab01_PART2_5479786 — PNCP Bronze · Escola Politécnica USP · PECE Big Data</span>
    <span>Prof. Dr. Pedro L. P. Corrêa &amp; Profa. Dra. Jeaneth Machicao · Março/2026</span>
  </footer>
</div>
</body>
</html>"""

# ── Salvar HTML ──────────────────────────────────────────────────────────────
html_path = GX_DOCS_DIR / "index.html"
with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)

# ── Salvar JSON de fallback ──────────────────────────────────────────────────
resumo_json = GX_ROOT / "relatorio_final_gx.json"
relatorio_simplificado = {
    "projeto":   "Lab01_PART2_5479786 — PNCP Bronze",
    "executor":  "Hercules Freitas - PECE USP",
    "timestamp": datetime.now().isoformat(),
    "estatisticas": {
        "total_registros_validados": len(df),
        "expectativas_avaliadas":    total,
        "expectativas_sucesso":      passou,
        "porcentagem_sucesso":       round(pct, 2),
    },
    "detalhes_por_coluna": [
        {
            "expectativa": d["exp"],
            "coluna":      d["coluna"],
            "sucesso":     d["sucesso"],
            "falhas_encontradas": d["falhas"],
        }
        for d in detalhes
    ],
}
with open(resumo_json, 'w', encoding='utf-8') as f:
    json.dump(relatorio_simplificado, f, indent=4, ensure_ascii=False)

print(f'\n  ✅ Relatório HTML salvo em: {html_path}')
print(f'  ✅ Relatório JSON salvo em: {resumo_json}')
print(f'\n{"="*62}')
print('  Pipeline Bronze concluído.')
print(f'{"="*62}\n')
