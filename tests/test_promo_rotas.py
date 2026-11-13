# -*- coding: utf-8 -*-
"""Lista de promoção (rotas_promo.py): base do parado há 240 d no SQL, nos totais e no grupo."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rotas_promo as rp  # noqa: E402
from modelos import PromoPlanRequest  # noqa: E402

_LINHA = dict(id=1, pro_codigo="10", pro_descricao="X", sgr_codigo=1, curva_abc="C", estoque_disponivel=8,
              estoque_max_sugerido=10, custo_unitario=100.0, preco_venda_1=270.0, preco_venda_2=160.0,
              demanda_real_dia=1 / 30.0, dias_sem_venda=20, qtd_varejo_12m=10, qtd_atacado_12m=0,
              fora_regua=False, tempo_medio_saldo_atual=300, acao=None, base_qtd=5, base_custo=90.0,
              em_grupo=False, grupo_demanda_dia=None)


class _Res:
    def __init__(self, valor=None, linhas=()):
        self.valor, self.linhas = valor, list(linhas)

    def scalar(self):
        return self.valor

    def __iter__(self):
        return iter(self.linhas)

    def fetchall(self):
        return self.linhas

    def mappings(self):
        return self.linhas


class _Conn:
    """Conexão falsa: responde to_regclass conforme `tabelas`, guarda o SELECT principal."""

    def __init__(self, tabelas):
        self.tabelas, self.sql = tabelas, None

    def execute(self, sql, params=None):
        s = str(sql)
        if "to_regclass" in s:
            return _Res("x" if params["n"] in self.tabelas else None)
        if "information_schema" in s:
            return _Res(linhas=[("qtd_varejo_12m",)])
        if "ven_regua_atacado" in s:
            return _Res(linhas=[])
        self.sql = s
        return _Res(linhas=[dict(_LINHA), dict(_LINHA, id=2, pro_codigo="11", base_qtd=None, base_custo=None)])


def test_sql_com_e_sem_base():
    com = _Conn({"fin_plano_parado_base"})
    linhas = rp._linhas(com, PromoPlanRequest())
    assert "LEFT JOIN fin_plano_parado_base b ON b.pro_codigo::text = f.pro_codigo" in com.sql
    assert "OR b.pro_codigo IS NOT NULL" in com.sql
    assert [x["na_base"] for x in linhas] == [True, False]

    sem = _Conn(set())
    rp._linhas(sem, PromoPlanRequest(grouped_view=True))
    assert "fin_plano_parado_base" not in sem.sql
    assert "NULL AS base_qtd" in sem.sql


def test_totais_parado_240():
    linhas = rp._linhas(_Conn({"fin_plano_parado_base"}), PromoPlanRequest())
    t = rp._totais(linhas)
    assert t["parado_240"] == {"itens": 1, "valor": 450.0}   # 5 un × R$ 90 da base
    assert t["valor_vender"] == round(sum(x["valor_vender"] for x in linhas), 2) == 500.0


def test_agrupar_soma_vender_e_marca_base():
    m1 = dict(id=1, grupo_chave="G", valor_parado=10.0, valor_excesso=0.0, caixa_potencial=5.0, acao="promocao",
              valor_vender=30.0, valor_parado_240=20.0, vender_qtd=3, na_base=True, grupo_excesso_qtd=0)
    m2 = dict(m1, id=2, valor_vender=12.0, valor_parado_240=0.0, vender_qtd=1, na_base=False)
    (g,) = rp._agrupar([m1, m2])
    assert (g["valor_vender"], g["valor_parado_240"], g["vender_qtd"], g["na_base"]) == (42.0, 20.0, 4, True)
    assert g["excesso_qtd"] == 0


class _ConnP(_Conn):
    """Igual à _Conn, guardando também os parâmetros do SELECT principal."""

    def execute(self, sql, params=None):
        res = super().execute(sql, params)
        if self.sql == str(sql):
            self.params = params
        return res


def test_filtros_busca_fornecedor_e_parado(monkeypatch):
    c = _ConnP({"fin_plano_parado_base"})
    rp._linhas(c, PromoPlanRequest(search="PARA"))
    assert "f.pro_codigo LIKE :search" in c.sql and c.params["search"] == "%PARA%"

    c = _ConnP(set())
    rp._linhas(c, PromoPlanRequest(pro_codigos="123, 456"))
    assert "f.pro_codigo IN (:cod_0, :cod_1)" in c.sql

    monkeypatch.setattr(rp, "_expandir_fornecedores", lambda nomes: ["X", "X FILIAL"])
    c = _ConnP(set())
    rp._linhas(c, PromoPlanRequest(fornecedores=["X"]))
    assert (c.params["forn_0"], c.params["forn_1"]) == ("X", "X FILIAL")
    assert "f.fornecedor_principal" in c.sql and ":forn_0, :forn_1" in c.sql

    linhas = rp._linhas(_Conn({"fin_plano_parado_base"}), PromoPlanRequest(so_parado=True))
    assert [x["pro_codigo"] for x in linhas] == ["10"]
