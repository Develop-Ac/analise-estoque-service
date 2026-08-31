# -*- coding: utf-8 -*-
"""Lotes de estoque (Postgres) e estoque realtime em volume (cache TTL)."""
import os
from sqlalchemy import text
from infra_db import get_db_connection, get_sql_connection
from erp_api import ERP_API_URL, get_realtime_stocks, todos_estoques_via_api
from modelos import LoteEstoque

def _get_stock_batches(pro_codes):
    """
    Busca os lotes de estoque na tabela com_data_saldo_produto
    """
    if not pro_codes: return {}
    
    conn = get_db_connection()
    try:
        # Sanitize
        fmt_codes = ["'" + str(c).replace("'", "") + "'" for c in pro_codes]
        in_clause = ",".join(fmt_codes)
        
        sql = text(f"""
            SELECT pro_codigo, data_compra, saldo_residual
            FROM com_data_saldo_produto
            WHERE pro_codigo IN ({in_clause})
            ORDER BY data_compra ASC
        """)
        
        rows = conn.execute(sql).fetchall()
        
        from datetime import date
        today = date.today()
        
        result = {}
        for r in rows:
            code = r[0] 
            dt = r[1]
            qty = float(r[2])
            
            if code not in result: result[code] = []
            
            days = (today - dt).days if dt else 0
            
            result[code].append(LoteEstoque(
                data_compra=str(dt),
                qtd=qty,
                dias_em_estoque=days
            ))
            
        return result
    except Exception as e:
        print(f"Erro ao buscar lotes: {e}")
        return {}
    finally:
        conn.close()


# ==========================================
# SUGESTÃO DE COMPRA (ponto de pedido)
# ==========================================
import time as _time_rt
import concurrent.futures as _futures
_STOCK_CACHE = {"ts": 0.0, "data": None}
_STOCK_TTL_S = int(os.getenv("STOCK_RT_TTL_S") or 120)  # 2 min
_RT_EXECUTOR = _futures.ThreadPoolExecutor(max_workers=2)  # p/ timeout rígido do realtime


def _todos_estoques_openquery():
    """Plano B: estoque de todos os produtos ativos numa OPENQUERY só."""
    inner = ("SELECT pro_codigo, estoque_disponivel FROM produtos "
             "WHERE empresa = 3 AND UPPER(inativo) = 'N' AND UPPER(comercializavel) = 'S'")
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39)*2)}')"
    conn = get_sql_connection()
    # timeout de consulta: se o ERP estiver lento, estoura e o endpoint cai no snapshot
    try:
        conn.timeout = int(os.getenv("STOCK_RT_TIMEOUT_S") or 15)
    except Exception:
        pass
    m = {}
    try:
        cur = conn.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            if row[0] is not None:
                m[str(row[0]).strip()] = float(row[1]) if row[1] is not None else 0.0
    finally:
        conn.close()
    return m


def get_all_realtime_stocks(force=False):
    """Estoque atual de TODOS os produtos ativos (empresa 3).
    Cacheado por processo (TTL curto) p/ toggles de filtro não baterem no ERP a
    cada request. Caminho preferido: erp-firebird-api paginada por watermark;
    plano B: OPENQUERY única (com timeout)."""
    now = _time_rt.time()
    if not force and _STOCK_CACHE["data"] is not None and (now - _STOCK_CACHE["ts"]) < _STOCK_TTL_S:
        return _STOCK_CACHE["data"]

    m = None
    if ERP_API_URL:
        try:
            ini = _time_rt.monotonic()
            m = todos_estoques_via_api()
            print(f"[ERP-API] estoque de todos os produtos: {len(m)} codigos via api "
                  f"em {int((_time_rt.monotonic() - ini) * 1000)}ms")
        except Exception as e:
            print(f"AVISO: erp-firebird-api indisponível p/ estoque total ({e}) — caindo para o OPENQUERY")
            m = None
    if m is None:
        m = _todos_estoques_openquery()

    _STOCK_CACHE["ts"] = now
    _STOCK_CACHE["data"] = m
    return m


def get_realtime_stocks_bulk(codes, chunk=400):
    """Estoque atual só de uma LISTA de produtos, em lotes (IN-list) — rápido quando
    o conjunto é pequeno (ex.: após filtrar por subgrupo/fornecedor)."""
    codes = list({str(x).strip() for x in codes if x is not None and str(x).strip()})
    m = {}
    for i in range(0, len(codes), chunk):
        try:
            m.update(get_realtime_stocks(codes[i:i + chunk]))
        except Exception as e:
            print(f"AVISO: falha no lote de estoque realtime ({i}): {e}")
    return m


import time as _time
