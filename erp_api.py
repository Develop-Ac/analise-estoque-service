# -*- coding: utf-8 -*-
"""Cliente da erp-firebird-api + estoque realtime (com fallback OPENQUERY)."""
import os
from infra_db import get_sql_connection

# Leitura do ERP pela erp-firebird-api (sem SQL Server/OPENQUERY no meio).
# Sem ERP_API_URL configurada, tudo segue pelo OPENQUERY como sempre; com ela,
# o OPENQUERY vira plano B quando a API não responder.
# =============================================================================
ERP_API_URL = (os.getenv('ERP_API_URL') or '').strip().rstrip('/')
ERP_API_TOKEN = (os.getenv('ERP_API_TOKEN') or '').strip()
ERP_API_TIMEOUT_S = float(os.getenv('ERP_API_TIMEOUT_MS') or 15000) / 1000.0
ERP_API_MAX_EM = 500  # teto do filtro `em` do lado da API

# Log de subida: diz de cara qual caminho está ativo, para ninguém precisar
# adivinhar pelo comportamento (mesma convenção do ErpApiService do compras).
if ERP_API_URL:
    print(f"[ERP-API] leitura do ERP habilitada em {ERP_API_URL}")
else:
    print("[ERP-API] ERP_API_URL nao configurada — estoque realtime segue 100% pelo OPENQUERY")


def _erp_api_consulta(recurso, corpo, permitir_truncado=False):
    """POST /erp/<recurso>/consulta na erp-firebird-api. Levanta exceção em
    qualquer falha (rede, HTTP != 2xx, resposta truncada) — quem chama decide
    o fallback. permitir_truncado=True é para paginação por watermark, onde a
    página cheia (linhas == limite) é o caso normal, não um corte."""
    import json as _json
    import urllib.request as _urlreq
    req = _urlreq.Request(
        f"{ERP_API_URL}/erp/{recurso}/consulta",
        data=_json.dumps(corpo).encode('utf-8'),
        headers={
            'content-type': 'application/json',
            'x-app-token': ERP_API_TOKEN,
            # O relatório /health/n1 do outro lado é por serviço: sem este
            # header o consumo aparece como "desconhecido".
            'x-servico': 'analise-estoque-service',
        },
        method='POST',
    )
    with _urlreq.urlopen(req, timeout=ERP_API_TIMEOUT_S) as resp:
        payload = _json.loads(resp.read().decode('utf-8'))
    meta = payload.get('meta') or {}
    if meta.get('truncado') and not permitir_truncado:
        raise RuntimeError(
            f"consulta {recurso} truncada em {meta.get('linhas')} linhas — resultado incompleto"
        )
    return payload.get('dados') or []


def fornecedores_nomes_via_api(empresa=3):
    """{for_codigo:int -> FOR_NOME} do cadastro vivo, tabela inteira da empresa.
    limite = teto da tabela (10k); estourar o teto vira exceção — melhor cair
    no plano B do que montar grupo de fornecedores com nome faltando."""
    dados = _erp_api_consulta('fornecedores', {
        'empresa': empresa,
        'campos': ['FOR_CODIGO', 'FOR_NOME'],
        'limite': 10_000,
    })
    out = {}
    for row in dados:
        cod = row.get('FOR_CODIGO')
        if cod is None:
            continue
        try:
            out[int(cod)] = str(row.get('FOR_NOME') or '')
        except (TypeError, ValueError):
            continue
    return out


def todos_estoques_via_api(empresa=3):
    """{pro_codigo(str) -> estoque_disponivel} de TODOS os produtos
    comercializáveis, paginado por watermark de PRO_CODIGO (a API limita 5000
    linhas por consulta e não tem offset). INATIVO não existe no catálogo da
    API: produto inativo entra no mapa e é inofensivo — todo consumo é lookup
    pontual por código, nunca varredura do mapa."""
    m = {}
    ultimo = 0
    while True:
        dados = _erp_api_consulta('produtos', {
            'empresa': empresa,
            'campos': ['PRO_CODIGO', 'ESTOQUE_DISPONIVEL'],
            'filtros': [
                {'campo': 'COMERCIALIZAVEL', 'op': 'igual', 'valor': 'S'},
                {'campo': 'PRO_CODIGO', 'op': 'maior', 'valor': ultimo},
            ],
            'ordenar': [{'campo': 'PRO_CODIGO', 'dir': 'asc'}],
            'limite': 5000,
        }, permitir_truncado=True)
        if not dados:
            break
        for row in dados:
            cod = row.get('PRO_CODIGO')
            if cod is None:
                continue
            qty = row.get('ESTOQUE_DISPONIVEL')
            m[str(cod).strip()] = float(qty) if qty is not None else 0.0
            try:
                ultimo = max(ultimo, int(cod))
            except (TypeError, ValueError):
                pass
        if len(dados) < 5000:
            break
    return m


def vendas_diarias_via_api(codigos, data_ini, empresa=3):
    """Σ QUANTIDADE por DATA de saída (ORIGEM NFS/EVF/EFD) dos códigos desde
    data_ini (YYYY-MM-DD). A agregação roda no Firebird; volta no máximo uma
    linha por dia com as chaves DATA e QTD."""
    lote = []
    for c in codigos:
        try:
            lote.append(int(str(c).strip()))
        except (TypeError, ValueError):
            continue
    lote = sorted(set(lote))
    if not lote:
        return []
    if len(lote) > ERP_API_MAX_EM:
        raise RuntimeError(
            f"vendas mensais: {len(lote)} códigos excede o teto de {ERP_API_MAX_EM} do filtro em"
        )
    return _erp_api_consulta('lanctos-estoque', {
        'empresa': empresa,
        'filtros': [
            {'campo': 'ORIGEM', 'op': 'em', 'valor': ['NFS', 'EVF', 'EFD']},
            {'campo': 'PRO_CODIGO', 'op': 'em', 'valor': lote},
            {'campo': 'DATA', 'op': 'maior_igual', 'valor': data_ini},
        ],
        'agrupar': ['DATA'],
        'agregar': [{'fn': 'somar', 'campo': 'QUANTIDADE', 'como': 'QTD'}],
        'limite': 5000,
    })


def _realtime_stocks_via_api(pro_codes):
    """Saldo por produto pela erp-firebird-api (PRO_CODIGO é inteiro no catálogo)."""
    codigos = []
    for c in pro_codes:
        try:
            codigos.append(int(str(c).strip()))
        except (TypeError, ValueError):
            continue
    codigos = sorted(set(codigos))

    stock_map = {}
    for i in range(0, len(codigos), ERP_API_MAX_EM):
        lote = codigos[i:i + ERP_API_MAX_EM]
        dados = _erp_api_consulta('produtos', {
            'empresa': 3,
            'campos': ['PRO_CODIGO', 'ESTOQUE_DISPONIVEL'],
            'filtros': [{'campo': 'PRO_CODIGO', 'op': 'em', 'valor': lote}],
            # +1 de folga: pedir o tamanho exato do lote marcaria toda
            # consulta completa como truncada.
            'limite': len(lote) + 1,
        })
        for row in dados:
            cod = row.get('PRO_CODIGO')
            if cod is None:
                continue
            qty = row.get('ESTOQUE_DISPONIVEL')
            stock_map[str(cod).strip()] = float(qty) if qty is not None else 0.0
    return stock_map


def _realtime_stocks_openquery(pro_codes):
    """Plano B: saldo via OPENQUERY no SQL Server (Linked Server CONSULTA).
    Com timeout obrigatório: sem ele, um travamento do linked server segura a
    thread para sempre e derruba o /analise inteiro."""
    safe_codes = [str(c).replace("'", "") for c in pro_codes]

    # Dentro do OPENQUERY as aspas simples são dobradas: IN (''COD1'', ''COD2'')
    formatted_codes_list = [f"''{c}''" for c in safe_codes]
    in_clause_inner = ", ".join(formatted_codes_list)

    inner_query = f"SELECT pro_codigo, estoque_disponivel FROM produtos WHERE pro_codigo IN ({in_clause_inner}) AND empresa = 3"
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner_query}')"

    conn = get_sql_connection()
    try:
        conn.timeout = int(os.getenv("STOCK_RT_TIMEOUT_S") or 15)
    except Exception:
        pass
    stock_map = {}

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            # row[0] = pro_codigo, row[1] = estoque_disponivel
            if row[0]:
                code = str(row[0]).strip()
                qty = float(row[1]) if row[1] is not None else 0.0
                stock_map[code] = qty

    except Exception as e:
        print(f"Erro no SQL Server: {e}")
        raise e
    finally:
        conn.close()

    return stock_map


def get_realtime_stocks(pro_codes):
    """
    Busca o estoque atual (saldo) para uma lista de códigos de produto.
    Caminho preferido: erp-firebird-api (leitura direta do Firebird, com
    timeout). Plano B: OPENQUERY no SQL Server, como sempre foi.
    """
    if not pro_codes:
        return {}

    if ERP_API_URL:
        try:
            import time as _t
            _ini = _t.monotonic()
            mapa = _realtime_stocks_via_api(pro_codes)
            print(f"[ERP-API] estoque realtime: {len(mapa)}/{len(pro_codes)} codigos via api em {int((_t.monotonic() - _ini) * 1000)}ms")
            return mapa
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível ({e}) — caindo para o OPENQUERY")

    print(f"[OPENQUERY] estoque realtime: {len(pro_codes)} codigos via linked server CONSULTA")
    return _realtime_stocks_openquery(pro_codes)

# ==========================================
# MODELOS
# ==========================================
