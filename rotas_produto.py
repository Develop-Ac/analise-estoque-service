# -*- coding: utf-8 -*-
"""Rotas de produto: /produto/vendas-mensais."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from infra_db import get_sql_connection, get_db_connection
from erp_api import ERP_API_URL, vendas_diarias_via_api

router = APIRouter()


def _vendas_mapa_openquery(codes, ini):
    """Plano B: agregação ano/mês direto no OPENQUERY (com timeout)."""
    in_list = ", ".join(f"'{c}'" for c in codes)
    inner = ("SELECT EXTRACT(YEAR FROM LE.data) AS ano, EXTRACT(MONTH FROM LE.data) AS mes, "
             "SUM(LE.quantidade) AS qtd FROM lanctos_estoque LE "
             "WHERE LE.empresa = 3 AND LE.origem IN ('NFS','EVF','EFD') "
             f"AND LE.pro_codigo IN ({in_list}) AND LE.data >= '{ini.isoformat()}' "
             "GROUP BY EXTRACT(YEAR FROM LE.data), EXTRACT(MONTH FROM LE.data)")
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39) * 2)}')"
    mapa = {}
    conn = get_sql_connection()
    try:
        conn.timeout = int(os.getenv("VENDAS_MENSAIS_TIMEOUT_S") or 20)
    except Exception:
        pass
    try:
        cur = conn.cursor()
        cur.execute(query)
        for ano, mes, qtd in cur.fetchall():
            mapa[(int(ano), int(mes))] = float(qtd or 0)
    finally:
        conn.close()
    return mapa


def _vendas_mapa_api(codes, ini):
    """Σ por dia na erp-firebird-api (agregação no Firebird), colapsada em
    (ano, mes) aqui — a API não expõe EXTRACT, mas ≤1 linha/dia é barato."""
    import datetime as _dt
    mapa = {}
    for row in vendas_diarias_via_api(codes, ini.isoformat()):
        bruto = row.get('DATA')
        if bruto is None:
            continue
        d = _dt.datetime.fromisoformat(str(bruto).replace('Z', '+00:00'))
        chave = (d.year, d.month)
        mapa[chave] = mapa.get(chave, 0.0) + float(row.get('QTD') or 0)
    return mapa


def _venda_perdida_mapa(codes, ini):
    """Venda perdida por (ano, mes): VENDA_PERDIDA do ERP + ven_venda_perdida da
    intranet — as mesmas duas fontes que o motor soma à demanda. Quantidade BRUTA
    lançada (o motor ainda aplica o teto por evento antes de usar)."""
    in_list = ", ".join(f"'{c}'" for c in codes)
    mapa = {}
    inner = ("SELECT EXTRACT(YEAR FROM vp.data) AS ano, EXTRACT(MONTH FROM vp.data) AS mes, "
             "SUM(vp.quantidade) AS qtd FROM venda_perdida vp "
             f"WHERE vp.empresa = 3 AND vp.quantidade > 0 AND vp.pro_codigo IN ({in_list}) "
             f"AND vp.data >= '{ini.isoformat()}' "
             "GROUP BY EXTRACT(YEAR FROM vp.data), EXTRACT(MONTH FROM vp.data)")
    conn = get_sql_connection()
    try:
        conn.timeout = int(os.getenv("VENDAS_MENSAIS_TIMEOUT_S") or 20)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39) * 2)}')")
        for ano, mes, qtd in cur.fetchall():
            mapa[(int(ano), int(mes))] = float(qtd or 0)
    finally:
        conn.close()
    try:
        with get_db_connection() as pg:
            rows = pg.execute(text(
                "SELECT EXTRACT(YEAR FROM created_at)::int, EXTRACT(MONTH FROM created_at)::int, "
                "SUM(quantidade) FROM ven_venda_perdida "
                "WHERE quantidade > 0 AND pro_codigo::text = ANY(:cods) AND created_at >= :ini "
                "GROUP BY 1, 2"), {"cods": list(codes), "ini": ini}).fetchall()
        for ano, mes, qtd in rows:
            mapa[(ano, mes)] = mapa.get((ano, mes), 0.0) + float(qtd or 0)
    except Exception as e:
        print(f"AVISO: venda perdida da intranet indisponível p/ vendas mensais ({e})")
    return mapa


@router.get("/produto/vendas-mensais")
def produto_vendas_mensais(codigos: str, meses: int = 18):
    """
    Vendas (saídas) por mês de um ou mais produtos (SOMA — p/ o grupo consolidado),
    nos últimos `meses` meses. erp-firebird-api primeiro; OPENQUERY plano B.
    """
    import datetime as _dt
    codes = [c.strip().replace("'", "") for c in (codigos or "").split(",") if c.strip()]
    if not codes:
        return {"meses": []}
    codes = codes[:300]
    hoje = _dt.date.today()
    ini = (hoje.replace(day=1) - _dt.timedelta(days=int(meses) * 31)).replace(day=1)

    mapa = None
    if ERP_API_URL:
        try:
            mapa = _vendas_mapa_api(codes, ini)
            print(f"[ERP-API] vendas mensais: {len(codes)} codigos via api")
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível p/ vendas mensais ({e}) — caindo para o OPENQUERY")
            mapa = None
    if mapa is None:
        try:
            mapa = _vendas_mapa_openquery(codes, ini)
        except Exception as e:
            print(f"AVISO: vendas-mensais indisponível: {e}")
            return {"meses": [], "erro": True}

    # Venda perdida é complemento: se a leitura falhar, as vendas seguem e a
    # coluna fica nula (a tela distingue "sem dado" de "zero").
    try:
        mapa_vp = _venda_perdida_mapa(codes, ini)
    except Exception as e:
        print(f"AVISO: venda perdida indisponível p/ vendas mensais: {e}")
        mapa_vp = None

    # série contínua dos últimos `meses` meses (preenche zeros)
    out = []
    y, mth = ini.year, ini.month
    while (y, mth) <= (hoje.year, hoje.month):
        out.append({"mes": f"{y:04d}-{mth:02d}", "qtd": round(mapa.get((y, mth), 0.0), 2),
                    "venda_perdida": (None if mapa_vp is None
                                      else round(mapa_vp.get((y, mth), 0.0), 2))})
        mth += 1
        if mth > 12:
            mth = 1; y += 1
    return {"meses": out}
