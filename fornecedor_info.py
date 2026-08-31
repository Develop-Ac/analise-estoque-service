# -*- coding: utf-8 -*-
"""Historico de compra (Mongo), parametros e grupos de fornecedor (caches TTL)."""
import os
import time as _time
from sqlalchemy import text
from infra_db import get_db_connection, get_sql_connection


def _sug_norm(s):
    return (str(s).strip() if s is not None else "")


def _sug_float(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    if f != f:  # NaN
        return 0.0
    return f

_HIST_CACHE = {"ts": 0.0, "data": None}
_HIST_TTL_S = int(os.getenv("COMPRAS_HIST_TTL_S") or 21600)  # 6h
SEM_HIST_COMPRA = "SEM HISTÓRICO DE COMPRA"


def get_compras_historico(force=False):
    """
    Lista PRODUTO -> fornecedores de quem JÁ COMPRAMOS, LIDA DO MONGO (coleção
    compras_fornecedor), empacotada pelo batch (main.atualizar_compras_fornecedor_mongo).
    NÃO toca no ERP no caminho do request — leitura rápida + cache por processo.
    Se o Mongo estiver vazio/indisponível, retorna {} (a tela carrega, sem
    agrupamento por fornecedor, até o batch popular).

    Retorna: {pro_codigo: [(for_nome, qtd_comprada), ...] ordenado por qtd desc}.
    """
    now = _time.time()
    cached = _HIST_CACHE.get("data")
    if not force and cached is not None and (now - _HIST_CACHE["ts"]) < _HIST_TTL_S:
        return cached
    hist = {}
    try:
        import empacotamento as emp
        hist = emp.carregar_compras_fornecedor()
    except Exception as e:
        print(f"AVISO: lista compras x fornecedor (Mongo) indisponível: {e}")
        hist = {}
    _HIST_CACHE["ts"] = now
    _HIST_CACHE["data"] = hist
    return hist


_FORN_PARAM_CACHE = {"ts": 0.0, "data": None}
_FORN_PARAM_TTL_S = int(os.getenv("FORN_PARAM_TTL_S") or 300)  # 5min


def get_fornecedor_parametros(force=False):
    """
    Parâmetros de compra por fornecedor (com_fornecedor_parametros, preenchidos na
    tela /compras/fornecedores). Casamento por NOME normalizado — o mesmo nome do
    histórico de compra (Mongo). Retorna:
      {FOR_NOME_UPPER: {"lead_time_dias", "tempo_revisao_dias", "pedido_minimo_valor",
                        "pedido_minimo_qtd"}}
    Vazio se a tabela não existir ainda (tolerante a schema antigo).
    """
    now = _time.time()
    cached = _FORN_PARAM_CACHE.get("data")
    if not force and cached is not None and (now - _FORN_PARAM_CACHE["ts"]) < _FORN_PARAM_TTL_S:
        return cached
    out = {}
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute(text(
                # SELECT * p/ tolerar a coluna pedido_minimo_qtd ausente
                # (SQL manual 2026-07-06 ainda não aplicado) — .get() devolve None.
                "SELECT * FROM com_fornecedor_parametros"
            )).mappings().all()
        finally:
            conn.close()
        for r in rows:
            nome = _sug_norm(r.get("for_nome")).upper()
            if not nome:
                continue
            def _pos(v):
                f = _sug_float(v)
                return f if f > 0 else None
            out[nome] = {
                "lead_time_dias": _pos(r.get("lead_time_dias")),
                "tempo_revisao_dias": _pos(r.get("tempo_revisao_dias")),
                "pedido_minimo_valor": _pos(r.get("pedido_minimo_valor")),
                "pedido_minimo_qtd": _pos(r.get("pedido_minimo_qtd")),
            }
    except Exception as e:
        print(f"AVISO: com_fornecedor_parametros indisponível ({e}).")
        out = {}
    _FORN_PARAM_CACHE["ts"] = now
    _FORN_PARAM_CACHE["data"] = out
    return out


_FORN_GRUPO_CACHE = {"ts": 0.0, "data": None}
_FORN_GRUPO_TTL_S = int(os.getenv("FORN_GRUPO_TTL_S") or 600)  # 10min


def _nomes_fornecedores_erp():
    """{for_codigo: NOME UPPER} do cadastro vivo do ERP — erp-firebird-api
    primeiro, OPENQUERY como plano B (mesmo padrão do estoque realtime)."""
    from erp_api import ERP_API_URL, fornecedores_nomes_via_api
    if ERP_API_URL:
        try:
            brutos = fornecedores_nomes_via_api()
            nomes = {c: n.strip().upper() for c, n in brutos.items() if n and n.strip()}
            print(f"[ERP-API] nomes de fornecedores: {len(nomes)} via api")
            return nomes
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível p/ fornecedores ({e}) — caindo para o OPENQUERY")

    nomes = {}
    sconn = get_sql_connection()
    try:
        try:
            sconn.timeout = int(os.getenv("STOCK_RT_TIMEOUT_S") or 15)
        except Exception:
            pass
        cur = sconn.cursor()
        cur.execute(
            "SELECT * FROM OPENQUERY(CONSULTA, "
            "'SELECT for_codigo, for_nome FROM fornecedores WHERE empresa = 3')"
        )
        for cod, nome in cur.fetchall():
            try:
                n = str(nome or "").strip().upper()
                if n:
                    nomes[int(cod)] = n
            except (TypeError, ValueError):
                pass
    finally:
        sconn.close()
    return nomes


def _carregar_grupos_fornecedor(force=False):
    """
    Fornecedores RELACIONADOS (matriz/filiais — com_fornecedor_relacionamento,
    módulo fornecedor-grupo do compras, tela /compras/fornecedores). A tabela é
    por for_codigo; os NOMES vêm do cadastro do ERP (empresa 3) via OPENQUERY.
    Cacheia DOIS mapas:
      grupos:    {NOME_UPPER: [nomes UPPER de TODO o grupo]}
      principal: {NOME_UPPER: NOME_UPPER do fornecedor PRINCIPAL do grupo
                  (flag `principal` da tabela; fallback = 1º em ordem alfabética)}
    """
    now = _time.time()
    cached = _FORN_GRUPO_CACHE.get("data")
    if not force and cached is not None and (now - _FORN_GRUPO_CACHE["ts"]) < _FORN_GRUPO_TTL_S:
        return cached
    out = {"grupos": {}, "principal": {}}
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute(text(
                "SELECT group_id, for_codigo, COALESCE(principal, false) "
                "FROM com_fornecedor_relacionamento"
            )).fetchall()
        finally:
            conn.close()

        grupos = {}
        for gid, cod, prin in rows:
            try:
                grupos.setdefault(str(gid), []).append((int(cod), bool(prin)))
            except (TypeError, ValueError):
                pass

        nomes = {}
        if grupos:
            nomes = _nomes_fornecedores_erp()

        for cods in grupos.values():
            ns = sorted({nomes[c] for c, _ in cods if c in nomes})
            if len(ns) < 2:
                continue
            prin_nome = next((nomes[c] for c, p in cods if p and c in nomes), None) or ns[0]
            for n in ns:
                out["grupos"].setdefault(n, set()).update(ns)
                out["principal"][n] = prin_nome
        out["grupos"] = {k: sorted(v) for k, v in out["grupos"].items()}
    except Exception as e:
        print(f"AVISO: fornecedores relacionados indisponíveis ({e}).")
        out = {"grupos": {}, "principal": {}}
    _FORN_GRUPO_CACHE["ts"] = now
    _FORN_GRUPO_CACHE["data"] = out
    return out


def get_grupos_fornecedor(force=False):
    """{NOME_UPPER: [nomes UPPER do grupo]} — ver _carregar_grupos_fornecedor."""
    return _carregar_grupos_fornecedor(force)["grupos"]


def get_fornecedor_principal_map(force=False):
    """{NOME_UPPER: NOME_UPPER do PRINCIPAL do grupo} — só p/ quem está em grupo."""
    return _carregar_grupos_fornecedor(force)["principal"]


def _expandir_fornecedores(nomes):
    """
    Lista de nomes (qualquer caixa) → lista UPPER SEM duplicatas incluindo os
    RELACIONADOS do grupo de cada um. Usada por /analise, /analise/export e
    pelo painel (via /fornecedores/expandir) — os dois lados filtram o MESMO conjunto.
    """
    grupos = get_grupos_fornecedor()
    out, vistos = [], set()
    for n in nomes or []:
        nu = _sug_norm(n).upper()
        if not nu:
            continue
        for m in [nu] + grupos.get(nu, []):
            if m not in vistos:
                vistos.add(m)
                out.append(m)
    return out


