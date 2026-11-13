# -*- coding: utf-8 -*-
"""Análise de compra de uma NF-e de entrada: GET /analise/nota/{chave}.

Lê o XML completo da intranet (com_nfe_conciliacao), a entrada da nota na
EMPRESA 3 (erp-firebird-api, OPENQUERY como plano B) e o contas a pagar
(só OPENQUERY — a tabela não existe na erp-firebird-api). As contas moram em
analise_nota.py; aqui só se lê e se monta a resposta. Nada é gravado.
"""
import base64
import datetime
import gzip
import hmac
import os
from decimal import Decimal

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text

import analise_nota as an
from analise_grupo import _SNAPSHOT, _in, _lit, _openquery
from erp_api import ERP_API_MAX_EM, ERP_API_URL, _erp_api_consulta, _erp_api_post, get_realtime_stocks
from fornecedor_info import _expandir_fornecedores, get_compras_historico
from infra_db import get_db_connection, get_sql_connection
from sugestao_compra import _gerar_sugestao_compra

router = APIRouter()

CUSTO_CAIXA_TAXA_MES = float(os.getenv("CUSTO_CAIXA_TAXA_MES") or 0.0217)
CUSTO_CAIXA_DIAS_TETO = int(os.getenv("CUSTO_CAIXA_DIAS_TETO") or 365)

_ERRO_CP = "Não foi possível ler o contas a pagar do ERP agora."
_ERRO_ENTRADA = "Não foi possível ler a entrada da nota no ERP agora."


def _data(v):
    """date de datetime/Timestamp/date/texto ISO; vazio ou NaT -> None."""
    if v is None or v != v:
        return None
    if isinstance(v, datetime.datetime):
        return v.date()
    if isinstance(v, datetime.date):
        return v
    return datetime.date.fromisoformat(str(v)[:10])


def _oq(inner):
    """OPENQUERY no linked server CONSULTA com timeout (sem ele, um travamento segura a thread)."""
    conn = get_sql_connection()
    try:
        conn.timeout = int(os.getenv("STOCK_RT_TIMEOUT_S") or 15)
        return _openquery(conn, inner)
    finally:
        conn.close()


def _nota_pg(chave):
    """NF-e lida (an.ler_xml) do XML completo guardado na intranet (o OPENQUERY corta o XML).
    Sem XML, ou conteúdo que não é uma NF-e completa: 404."""
    if len(chave) != 44 or not chave.isdigit():
        raise HTTPException(status_code=404, detail="Nota sem XML completo.")
    with get_db_connection() as pg:
        xml = pg.execute(text("SELECT xml_completo FROM com_nfe_conciliacao WHERE chave_nfe = :chave"),
                         {"chave": chave}).scalar()
    xml = str(xml or "").strip()
    if not xml:
        raise HTTPException(status_code=404, detail="Nota sem XML completo.")
    # A conciliação guarda o docZip da distribuição (gzip em base64); XML puro passa direto.
    try:
        if not xml.startswith("<"):
            xml = gzip.decompress(base64.b64decode(xml)).decode("utf-8")
        return an.ler_xml(xml)
    except (ValueError, OSError, EOFError):
        # base64/gzip inválido (binascii.Error, BadGzipFile, EOFError), XML malformado ou resumo resNFe.
        raise HTTPException(status_code=404, detail="Nota sem XML completo.")


def _entrada_emp3(chave):
    """Entrada concluída (STATUS 1) da nota na EMPRESA 3: {nfe, dt_entrada, for_codigo} ou None.
    Lançada só na empresa 1 não conta. Mais de uma linha -> a de maior NFE."""
    linhas = None
    if ERP_API_URL:
        try:
            linhas = _erp_api_post("nf-entrada/por-chaves", {"chaves": [chave], "empresa": 3})
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível p/ entrada da nota ({e}) — caindo para o OPENQUERY")
    if linhas is None:
        try:
            df = _oq("SELECT nfe, dt_entrada, for_codigo FROM nf_entrada "
                     f"WHERE empresa = 3 AND chave_nfe = {_lit(chave)} AND status = 1")
        except Exception as e:
            print(f"Erro no OPENQUERY da entrada da nota: {e}")
            raise HTTPException(status_code=502, detail=_ERRO_ENTRADA)
        linhas = df.to_dict("records")
    linhas = [r for r in linhas if r.get("NFE") is not None]
    if not linhas:
        return None
    r = max(linhas, key=lambda r: int(r["NFE"]))
    return {"nfe": int(r["NFE"]), "dt_entrada": _data(r.get("DT_ENTRADA")), "for_codigo": r.get("FOR_CODIGO")}


def _titulos_da_nota(nfe):
    """Títulos do contas a pagar da nota lançada, no formato de vencimentos do fluxo."""
    try:
        df = _oq("SELECT cp.documento, cp.vencimento, cp.valor, cp.data_pagto FROM contas_pagar cp "
                 f"WHERE cp.empresa = 3 AND cp.status = 0 AND cp.nfe = {int(nfe)}")
    except Exception as e:
        print(f"Erro no OPENQUERY do contas a pagar (títulos): {e}")
        raise HTTPException(status_code=502, detail=_ERRO_CP)
    return [{"numero": str(r["DOCUMENTO"]).strip() if r["DOCUMENTO"] is not None else None,
             "vencimento": _data(r["VENCIMENTO"]), "valor": float(r["VALOR"] or 0),
             "pago_em": _data(r["DATA_PAGTO"])}
            for r in df.to_dict("records") if _data(r["VENCIMENTO"])]


def _comprometido(inicio, fim, nfe_excluir):
    """{date: valor} em aberto no contas a pagar entre inicio e fim, sem os títulos da própria nota."""
    excluir = f" AND (cp.nfe IS NULL OR cp.nfe <> {int(nfe_excluir)})" if nfe_excluir else ""
    try:
        df = _oq("SELECT cp.vencimento, SUM(cp.valor) AS valor FROM contas_pagar cp "
                 "WHERE cp.empresa = 3 AND cp.status = 0 AND cp.data_pagto IS NULL "
                 f"AND cp.vencimento >= '{inicio.isoformat()}' AND cp.vencimento <= '{fim.isoformat()}'"
                 f"{excluir} GROUP BY cp.vencimento")
    except Exception as e:
        print(f"Erro no OPENQUERY do contas a pagar (comprometido): {e}")
        raise HTTPException(status_code=502, detail=_ERRO_CP)
    out = {}
    for r in df.to_dict("records"):
        d = _data(r["VENCIMENTO"])
        if d:
            out[d] = out.get(d, 0.0) + float(r["VALOR"] or 0)
    return out


def _api_ou_oq(rotulo, api, oq=None):
    """Linhas do ERP pela API (se configurada) e, falhando, pelo OPENQUERY. Tudo falhou -> []
    (o item só cai no próximo passo da identificação do produto)."""
    if ERP_API_URL:
        try:
            return api()
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível p/ {rotulo} ({e})")
    if oq:
        try:
            return oq().to_dict("records")
        except Exception as e:
            print(f"Erro no OPENQUERY de {rotulo}: {e}")
    return []


def _itens_entrada(nfe):
    """{item: {PRO_CODIGO, QUANTIDADE, CFOP_NOTA, FATOR_CONVERSAO}} da entrada na empresa 3."""
    campos = ["ITEM", "PRO_CODIGO", "CFOP_NOTA", "QUANTIDADE", "FATOR_CONVERSAO"]
    linhas = _api_ou_oq(
        "itens da entrada",
        lambda: _erp_api_consulta("nfe-itens", {"empresa": 3, "campos": campos, "limite": 1000,
                                                "filtros": [{"campo": "NFE", "op": "igual", "valor": int(nfe)}]}),
        lambda: _oq(f"SELECT {', '.join(campos).lower()} FROM nfe_itens WHERE empresa = 3 AND nfe = {int(nfe)}"))
    return {int(r["ITEM"]): r for r in linhas if r.get("ITEM") is not None}


def _pg_linhas(rotulo, sql, params):
    try:
        with get_db_connection() as pg:
            return [dict(r._mapping) for r in pg.execute(text(sql), params)]
    except Exception as e:
        print(f"Erro ao ler {rotulo} no Postgres: {e}")
        return []


def _conciliacao_itens(chave):
    """Itens da Verificação do Produto da nota."""
    return _pg_linhas("com_nfe_conciliacao_item",
                      "SELECT n_item, cod_prod_fornecedor, for_codigo, pro_codigo "
                      "FROM com_nfe_conciliacao_item WHERE chave_nfe = :chave", {"chave": chave})


def _vinculos(chave):
    """Itens vinculados a pedido (vínculo confirmado e não rejeitado) da nota."""
    return _pg_linhas("com_pedido_nfe_vinculo_item",
                      "SELECT i.cprod_xml, i.pro_codigo, v.for_codigo "
                      "FROM com_pedido_nfe_vinculo_item i JOIN com_pedido_nfe_vinculo v ON v.id = i.vinculo_id "
                      "WHERE v.chave_nfe = :chave AND v.confirmado AND NOT v.rejeitado AND i.tipo = 'vinculado'",
                      {"chave": chave})


def _for_codigo_cnpj(cnpj):
    """FOR_CODIGO do emitente pelo CNPJ (cadastro da empresa 1, onde o compras grava o de-para)."""
    if not cnpj:
        return None
    linhas = _api_ou_oq("fornecedor pelo CNPJ", lambda: _erp_api_consulta("fornecedores", {
        "empresa": 1, "campos": ["FOR_CODIGO"], "limite": 10,
        "filtros": [{"campo": "CNPJ_DIGITOS", "op": "igual", "valor": cnpj}]}))
    return next((r["FOR_CODIGO"] for r in linhas if r.get("FOR_CODIGO") is not None), None)


def _prod_fornecedor(for_codigo):
    """{cProd normalizado: [{PRO_CODIGO, FATOR_CONVERSAO}]} do de-para PRODUTOS_FORNECEDOR_NFE (empresa 1).
    Traz o fornecedor inteiro: COD_PROD_FORNECEDOR tem espaço à direita e casa normalizado aqui."""
    if for_codigo is None:
        return {}
    linhas = _api_ou_oq("produtos do fornecedor", lambda: _erp_api_consulta("produtos-fornecedor", {
        "empresa": 1, "campos": ["COD_PROD_FORNECEDOR", "PRO_CODIGO", "FATOR_CONVERSAO"], "limite": 5000,
        "filtros": [{"campo": "FOR_CODIGO", "op": "igual", "valor": int(for_codigo)}]}))
    out = {}
    for r in linhas:
        if r.get("PRO_CODIGO") is not None:
            out.setdefault(an.norm_cprod(r.get("COD_PROD_FORNECEDOR")), []).append(r)
    return out


def _cadastro(pro_codigos):
    """{pro_codigo(str): {PRO_CODIGO, PRO_DESCRICAO, COMERCIALIZAVEL}} da empresa 3."""
    codigos = sorted({int(p) for p in pro_codigos if str(p).strip().isdigit()})
    campos = ["PRO_CODIGO", "PRO_DESCRICAO", "COMERCIALIZAVEL"]
    linhas = []
    for i in range(0, len(codigos), ERP_API_MAX_EM):
        lote = codigos[i:i + ERP_API_MAX_EM]
        linhas += _api_ou_oq(
            "cadastro de produtos",
            lambda: _erp_api_consulta("produtos", {"empresa": 3, "campos": campos, "limite": len(lote) + 1,
                                                   "filtros": [{"campo": "PRO_CODIGO", "op": "em", "valor": lote}]}),
            lambda: _oq(f"SELECT {', '.join(campos).lower()} FROM produtos "
                        f"WHERE empresa = 3 AND pro_codigo IN ({_in(lote)})"))
    return {str(r["PRO_CODIGO"]).strip(): r for r in linhas if r.get("PRO_CODIGO") is not None}


def _resolver(chave, cab, itens_xml, entrada):
    """Lê as quatro fontes do produto de cada item e separa analisáveis de não analisados."""
    lancada = entrada is not None
    itens_entrada = _itens_entrada(entrada["nfe"]) if lancada else {}
    conc = _conciliacao_itens(chave)
    vinc = _vinculos(chave)
    # Fornecedor do de-para: o da entrada; sem entrada, conciliação -> vínculo -> CNPJ do emitente.
    for_codigo = entrada["for_codigo"] if lancada else None
    for_codigo = (for_codigo or next((r["for_codigo"] for r in conc if r.get("for_codigo")), None)
                  or next((r["for_codigo"] for r in vinc if r.get("for_codigo")), None))
    if for_codigo is None and not lancada:
        for_codigo = _for_codigo_cnpj(cab["cnpj"])
    try:
        for_codigo = int(str(for_codigo).strip()) if for_codigo is not None else None
    except ValueError:
        for_codigo = None
    pf = _prod_fornecedor(for_codigo)

    conciliacao = {int(r["n_item"]): r["pro_codigo"] for r in conc if r.get("pro_codigo")}
    vinculos = {an.norm_cprod(r["cprod_xml"]): r["pro_codigo"] for r in vinc
                if r.get("cprod_xml") and r.get("pro_codigo") is not None}
    candidatos = ({r.get("PRO_CODIGO") for r in itens_entrada.values()} | set(conciliacao.values())
                  | set(vinculos.values()) | {c["PRO_CODIGO"] for lst in pf.values() for c in lst})
    cadastro = _cadastro(c for c in candidatos if c is not None)
    return an.resolver_itens(itens_xml, itens_entrada, conciliacao, vinculos, pf, cadastro, lancada)


def _rodada(pro_codigos):
    """Última rodada (com_fifo_completo) dos produtos da nota e de todos os membros dos grupos
    deles: ({pro_codigo: linha}, data_processamento). Máximos com CEIL, como nas outras telas."""
    pros = sorted({str(p) for p in pro_codigos})
    if not pros:
        return {}, None
    with get_db_connection() as pg:
        existentes = {r[0] for r in pg.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name='com_fifo_completo'"))}

        def opt(c):
            return c if c in existentes else f"NULL AS {c}"

        sel = ("SELECT pro_codigo::text AS pro_codigo, pro_descricao AS descricao, grupo_chave, grupo_qtd_itens, "
               "CEIL(grupo_estoque_max) AS grupo_estoque_max, grupo_demanda_dia, "
               "CEIL(estoque_max_sugerido) AS estoque_max_sugerido, "
               f"{opt('demanda_real_dia')}, {opt('sob_encomenda')}, data_processamento "
               f"FROM com_fifo_completo WHERE {_SNAPSHOT} AND ")
        linhas = [dict(r._mapping) for r in pg.execute(text(sel + "pro_codigo::text = ANY(:pros)"), {"pros": pros})]
        chaves = sorted({r["grupo_chave"] for r in linhas if r["grupo_chave"] and (r["grupo_qtd_itens"] or 0) > 1})
        if chaves:
            linhas += [dict(r._mapping) for r in pg.execute(text(sel + "grupo_chave = ANY(:chaves)"), {"chaves": chaves})]
    rodada = {}
    for r in linhas:
        rodada[r["pro_codigo"].strip()] = {k: float(v) if isinstance(v, Decimal) else v for k, v in r.items()}
    data = next((_data(r["data_processamento"]) for r in rodada.values()), None)
    return rodada, data


def _nomes_fornecedor(cnpj):
    """Razões sociais do emitente no cadastro da EMPRESA 3 — é desse cadastro que sai o nome
    gravado no histórico de compra, por onde a sugestão de compra filtra o fornecedor."""
    if not cnpj or not cnpj.isdigit():
        return []
    linhas = _api_ou_oq(
        "nome do fornecedor",
        lambda: _erp_api_consulta("fornecedores", {
            "empresa": 3, "campos": ["FOR_NOME"], "limite": 10,
            "filtros": [{"campo": "CNPJ_DIGITOS", "op": "igual", "valor": cnpj}]}),
        # CPF_CNPJ é gravado com máscara.
        lambda: _oq("SELECT for_nome FROM fornecedores WHERE empresa = 3 AND "
                    f"REPLACE(REPLACE(REPLACE(cpf_cnpj, '.', ''), '/', ''), '-', '') = {_lit(cnpj)}"))
    return [str(r["FOR_NOME"]).strip() for r in linhas if r.get("FOR_NOME")]


def _nao_comprados(nomes, pros_da_nota):
    """Sugestão de compra de hoje do fornecedor (e relacionados), sem os produtos informados.
    `nomes` = nomes candidatos do fornecedor; vale o primeiro com histórico de compra.
    É um bloco de apoio: falha aqui não derruba a análise, o bloco sai vazio com aviso."""
    try:
        conhecidos = {n.strip().upper() for forns in get_compras_historico().values() for n, _ in forns}
        nome = next((n for n in nomes
                     if any(m in conhecidos for m in _expandir_fornecedores([n]))), None)
        if not nome:
            return an.montar_nao_comprados({}, pros_da_nota, None,
                                           "Fornecedor sem histórico de compra na Análise de Estoque.")
        return an.montar_nao_comprados(_gerar_sugestao_compra(fornecedor=nome), pros_da_nota, nome)
    except Exception as e:
        print(f"Erro ao montar os itens não comprados do fornecedor: {e}")
        return an.montar_nao_comprados({}, pros_da_nota, None, "Não foi possível montar esta lista agora.")


def _exigir_tela(tela, recusa):
    """Dependência de acesso: só pelo proxy da intranet (x-app-token igual ao APP_TOKEN; vazio =
    tudo recusado) e para quem visualiza `tela` em sis_permissoes; senão 403 com `recusa`."""
    def dep(x_app_token: str | None = Header(None), x_user_id: str | None = Header(None)):
        esperado = os.getenv("APP_TOKEN") or ""
        if not esperado or not x_app_token or not hmac.compare_digest(x_app_token, esperado):
            raise HTTPException(status_code=403, detail="Esta rota só responde pela intranet.")
        if not x_user_id:
            raise HTTPException(status_code=401, detail="Usuário não identificado.")
        with get_db_connection() as pg:
            pode = pg.execute(text("SELECT 1 FROM sis_permissoes WHERE usuario_id = :u AND tela = :t "
                                   "AND visualizar LIMIT 1"), {"u": x_user_id, "t": tela}).scalar()
        if not pode:
            raise HTTPException(status_code=403, detail=recusa)
    return dep


# A resposta traz títulos da nota e o contas a pagar da empresa: só para quem visualiza /fiscal/nfe.
_so_quem_ve_notas = _exigir_tela("/fiscal/nfe", "Sem permissão para ver notas fiscais.")


@router.get("/analise/nota/{chave}", dependencies=[Depends(_so_quem_ve_notas)])
def analise_nota(chave: str):
    """Análise de compra da NF-e de entrada: unidades (grupo/produto) com estoque, cobertura e
    excesso, custo de caixa, não analisados e fluxo a pagar. Só leitura."""
    chave = chave.strip()
    xml = _nota_pg(chave)
    cab = xml["cabecalho"]
    hoje = an.hoje_cuiaba()

    entrada = _entrada_emp3(chave)
    lancada = entrada is not None
    if lancada:
        vencimentos = _titulos_da_nota(entrada["nfe"])
    else:
        vencimentos = [{**d, "pago_em": None} for d in xml["duplicatas"]]

    base_prazo = "entrada" if lancada and entrada["dt_entrada"] else "hoje"
    base = entrada["dt_entrada"] if base_prazo == "entrada" else hoje
    _, fim = an.janela_semanas(vencimentos, hoje)
    comprometido = _comprometido(hoje, fim, entrada["nfe"] if lancada else None)

    itens = an.ratear_desembolso(xml["itens"], cab["valor_total"])
    analisaveis, nao_analisados = _resolver(chave, cab, itens, entrada)

    rodada, data_rodada = _rodada(i["pro_codigo"] for i in analisaveis)
    membros = sorted(set(rodada) | {str(i["pro_codigo"]) for i in analisaveis})
    try:
        saldos = get_realtime_stocks(membros)
    except Exception as e:
        print(f"Erro ao ler o saldo dos produtos da nota: {e}")
        raise HTTPException(status_code=502, detail="Não foi possível ler o saldo de estoque do ERP agora.")

    return {
        "nota": {
            "chave": chave, "numero": cab["numero"], "emitente": cab["emitente"], "cnpj_emitente": cab["cnpj"],
            "emissao": cab["emissao"], "valor_total": cab["valor_total"],
            "lancada": lancada, "data_entrada": entrada["dt_entrada"] if lancada else None,
            "rodada": data_rodada,
        },
        **an.montar_analise(cab["valor_total"], analisaveis, nao_analisados, rodada, saldos, lancada,
                            vencimentos, base, base_prazo, comprometido, hoje,
                            CUSTO_CAIXA_TAXA_MES, CUSTO_CAIXA_DIAS_TETO),
        "nao_comprados": _nao_comprados(_nomes_fornecedor(cab["cnpj"]) + [(cab["emitente"] or "").strip()],
                                        membros),
    }
