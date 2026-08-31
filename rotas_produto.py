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
from infra_db import get_sql_connection
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

    # série contínua dos últimos `meses` meses (preenche zeros)
    out = []
    y, mth = ini.year, ini.month
    while (y, mth) <= (hoje.year, hoje.month):
        out.append({"mes": f"{y:04d}-{mth:02d}", "qtd": round(mapa.get((y, mth), 0.0), 2)})
        mth += 1
        if mth > 12:
            mth = 1; y += 1
    return {"meses": out}
