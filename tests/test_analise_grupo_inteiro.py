# -*- coding: utf-8 -*-
"""Visão agrupada do GET /analise: produto de grupo unificado que casa com o filtro traz o grupo inteiro."""
import os
import sqlite3
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))  # sem driver ODBC no teste

import rotas_analise as ra  # noqa: E402

FILTROS = [ra._SNAPSHOT, "pro_descricao LIKE :d"]


def _listar(filtros, grouped):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE com_fifo_completo (pro_codigo TEXT, pro_descricao TEXT, grupo_chave TEXT, "
               "grupo_qtd_itens INT, data_processamento TEXT)")
    db.executemany("INSERT INTO com_fifo_completo VALUES (?,?,?,?,?)", [
        ("g1", "PARA-BRISA TORO 21/", "GA", 3, "2026-11-28"),
        ("g2", "PARA-BRISA TORO 21/ X", "GA", 3, "2026-11-28"),
        ("g3", "PARA-BRISA TORO 23/", "GA", 3, "2026-11-28"),
        ("velho", "PARA-BRISA TORO 23/", "GA", 3, "2026-11-01"),
        ("av23", "PARA-BRISA TORO 23/", None, None, "2026-11-28"),
        ("um", "PARA-BRISA TORO 23/", "GU", 1, "2026-11-28"),
        ("av21", "PARA-BRISA TORO 21/", None, None, "2026-11-28"),
    ])
    sql = f"SELECT pro_codigo FROM com_fifo_completo WHERE {ra._where_listagem(filtros, grouped)}"
    return {r[0] for r in db.execute(sql, {"d": "%TORO 21/%"})}


def test_agrupado_traz_grupo_inteiro():
    assert _listar(FILTROS, True) == {"g1", "g2", "g3", "av21"}


def test_individual_inalterado():
    assert _listar(FILTROS, False) == {"g1", "g2", "av21"}
    assert ra._where_listagem(FILTROS, False) == " AND ".join(FILTROS)


def test_sem_filtro_proprio_texto_de_sempre():
    for f in ([ra._SNAPSHOT], ["1=1", ra._SNAPSHOT]):
        assert ra._where_listagem(f, True) == " AND ".join(f)
    assert ra._where_listagem([], True) == "1=1"


def test_order_by_agrupado_particiona_pelo_grupo_da_tela():
    for ob in (ra._order_by(None, "asc", True), ra._order_by("estoque", "desc", True)):
        assert f"PARTITION BY {ra._CHAVE_GRUPO_TELA}" in ob
        assert ra._CHAVE_GRUPO not in ob
        assert f"{ra._CHAVE_GRUPO_TELA} ASC" in ob and ob.rstrip().endswith("pro_codigo ASC")


def test_export_agrupado_traz_grupo_inteiro(monkeypatch):
    from test_curva_efetiva import _exportar
    sql, where, _ = _exportar(monkeypatch, search="X", grouped_view=True)
    assert "grupo_chave IN (SELECT grupo_chave FROM com_fifo_completo" in where
    assert where.count(":search") - where.count(":search_desc") == 2
    assert f"PARTITION BY {ra._CHAVE_GRUPO_TELA}" in sql


def test_export_individual_e_sem_filtro_inalterados(monkeypatch):
    from test_curva_efetiva import _exportar
    sql, where, _ = _exportar(monkeypatch, search="X", grouped_view=False)
    assert "grupo_chave IN" not in where and f"PARTITION BY {ra._CHAVE_GRUPO}" in sql
    _, where, _ = _exportar(monkeypatch, grouped_view=True)
    assert "grupo_chave IN" not in where


def _sql_contagem(monkeypatch, grouped):
    executados = []

    class _Conn:
        def close(self):
            pass

        def execute(self, sql, params=None):
            executados.append(str(sql))
            raise RuntimeError("sem banco")

    monkeypatch.setattr(ra, "get_db_connection", lambda: _Conn())
    resp = ra.listar_analise(
        page=1, limit=50, search=None, pro_codigos=None, marca="X", subgrupo=None,
        saldo_categoria=None, only_changes=False, critical=False, curve=None, trend=None,
        status=None, group_id=None, match_type="contains", coverage_days=None,
        grouped_view=grouped, kpi=None, fornecedor=None, sort=None, sort_dir="asc")
    assert resp["data"] == [] and resp["total"] == 0
    return executados[0]


def test_listagem_contagem_agrupada_traz_grupo_inteiro(monkeypatch):
    assert "grupo_chave IN (SELECT grupo_chave" in _sql_contagem(monkeypatch, True)
    assert "grupo_chave IN" not in _sql_contagem(monkeypatch, False)
