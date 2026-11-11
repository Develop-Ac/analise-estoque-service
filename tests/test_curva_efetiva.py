# -*- coding: utf-8 -*-
"""Curva efetiva do GET /analise: curva do grupo unificado só na visão agrupada."""
import os
import sqlite3
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))  # sem driver ODBC no teste

import rotas_analise as ra  # noqa: E402

G = ra._CHAVE_GRUPO


def test_visao_individual_inalterada():
    assert ra._curva_expr(False) == "curva_abc"
    assert ra._order_by(None, "asc", False) == f"""
                MIN(curva_abc) OVER (PARTITION BY {G}) ASC,
                MIN(pro_descricao) OVER (PARTITION BY {G}) ASC,
                pro_descricao ASC"""
    assert ra._order_by("curva", "asc", False) == "curva_abc ASC NULLS LAST, pro_descricao ASC"


def test_visao_agrupada_ordena_pela_curva_do_grupo():
    assert "grupo_curva" in ra._order_by(None, "asc", True)
    assert "grupo_curva" in ra._order_by("curva", "desc", True)


def _filtrar(curve, grouped):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE t (pro_codigo TEXT, curva_abc TEXT, grupo_chave TEXT, grupo_qtd_itens INT, grupo_curva TEXT)")
    db.executemany("INSERT INTO t VALUES (?,?,?,?,?)", [
        ("g1", "C", "GA", 3, "A"), ("g2", "C", "GA", 3, "A"), ("g3", "C", "GA", 3, "A"),
        ("gb", "A", "GB", 2, "B"),
        ("av", "A", None, None, None),
        ("um", "C", "GU", 1, "A"),
    ])
    sql, params = ra._filtro_curva(curve, grouped)
    return {r[0] for r in db.execute(f"SELECT pro_codigo FROM t WHERE {sql}", params)}


def test_filtro_curva_efetiva():
    assert _filtrar("A", True) == {"g1", "g2", "g3", "av"}
    assert _filtrar("A", False) == {"gb", "av"}
    assert ra._filtro_curva(" , ", True) is None


def test_select_abertura_grupo():
    assert ra._select_abertura_grupo(set()) == (
        "NULL AS grupo_curva_valor, NULL AS grupo_classe_giro, NULL AS grupo_meses_com_venda_12m")
    assert ra._select_abertura_grupo(set(ra._COLS_ABERTURA_GRUPO)) == (
        "grupo_curva_valor, grupo_classe_giro, grupo_meses_com_venda_12m")


def _exportar(monkeypatch, **kw):
    import asyncio
    import io
    import pandas as pd

    class _Conn:
        def close(self):
            pass

    capturado = {}

    def _read_sql(sql, conn, params=None):
        capturado["sql"], capturado["params"] = str(sql), params
        if kw.get("coverage_days"):
            return pd.DataFrame([{
                "pro_codigo": "1", "pro_descricao": "X", "estoque_disponivel": 0,
                "estoque_min_sugerido": 1, "estoque_max_sugerido": 2, "curva_abc": "C",
                "tipo_planejamento": "Normal", "sgr_codigo": 1, "alerta_tendencia_alta": "Não",
                "demanda_media_dia_ajustada": 0, "fornecedor1": "F", "qtd_vendida": 0,
                "group_id": "G", "grp_estoque_disponivel": 0, "grp_estoque_min_sugerido": 0,
                "grp_estoque_max_sugerido": 0, "curva_grupo": "A"}])
        return pd.DataFrame([{"Código": "1", "Curva": "C", "Curva do grupo": "A"}])

    monkeypatch.setattr(ra, "get_db_connection", lambda: _Conn())
    monkeypatch.setattr(pd, "read_sql", _read_sql)
    resp = ra.exportar_analise(**{"search": None, "pro_codigos": None, "marca": None, "trend": None,
                                  "status": None, "subgrupo": None, "saldo_categoria": None,
                                  "group_id": None, "kpi": None, "fornecedor": None, **kw})

    async def _corpo():
        return b"".join([c async for c in resp.body_iterator])

    df = pd.read_excel(io.BytesIO(asyncio.run(_corpo())))
    where = capturado["sql"].split("ORDER BY")[0].split("WHERE", 1)[1]
    return capturado["sql"], where, df


def test_export_curva_do_grupo(monkeypatch):
    sql, where, df = _exportar(monkeypatch, curve="A", grouped_view=True)
    assert '"Curva do grupo"' in sql and "grupo_curva" in where
    assert "Curva do grupo" in df.columns

    sql, where, _ = _exportar(monkeypatch, curve="A", grouped_view=False)
    assert "grupo_curva" not in where and "curva_abc IN (:curve_0)" in where

    _, _, df = _exportar(monkeypatch, curve="A", grouped_view=True, coverage_days=30)
    cols = list(df.columns)
    assert cols[cols.index("Curva") + 1] == "Curva do grupo"
