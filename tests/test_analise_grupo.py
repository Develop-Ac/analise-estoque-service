# -*- coding: utf-8 -*-
"""Análise de um grupo só (analise_grupo.py) e a curva contra a base (main.calcular_grupos_descricao)."""
import os
import sys
import types

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))  # sem driver ODBC no teste

import analise_grupo as ag  # noqa: E402
import main  # noqa: E402

HOJE = pd.Timestamp("2026-10-19")


def _cadastro(linhas):
    return pd.DataFrame(linhas, columns=["PRO_CODIGO", "PRO_DESCRICAO"])


def test_fechar_membros_por_descricao_e_vinculo():
    cad = _cadastro([("1", "PASTILHA X"), ("2", "PASTILHA X"), ("3", "PASTILHA Y"),
                     ("4", "PASTILHA Y"), ("9", "OUTRA")])
    chamadas = []

    def ler(cods, descs):
        chamadas.append((set(cods), set(descs)))
        return cad[cad["PRO_CODIGO"].isin(cods) | cad["PRO_DESCRICAO"].isin(descs)]

    sp, vinc = ag.fechar_membros({"1"}, [["1", "3"], ["7", "8"]], ler)
    assert set(sp["PRO_CODIGO"]) == {"1", "2", "3", "4"}
    assert vinc == [["1", "3"]]


def test_fechar_membros_trava_tamanho(monkeypatch):
    monkeypatch.setattr(ag, "MAX_PRODUTOS", 2)
    cad = _cadastro([(str(i), f"D{i}") for i in range(10)])
    vinculos = [[str(i), str(i + 1)] for i in range(9)]
    try:
        ag.fechar_membros({"0"}, vinculos, lambda c, d: cad[cad["PRO_CODIGO"].isin(c)])
        assert False, "deveria travar"
    except ValueError:
        pass


def _vendas(codigo, qtd_mes, valor_un):
    datas = pd.date_range(HOJE - pd.DateOffset(months=11), HOJE, freq="7D")
    return pd.DataFrame({"PRO_CODIGO": codigo, "DATA": datas, "QUANTIDADE_AJUSTADA": qtd_mes / 4,
                         "TOTAL_LIQUIDO": qtd_mes / 4 * valor_un, "PRECO_CUSTO": valor_un / 2})


def _grupo(valores_base=None):
    sp = pd.DataFrame({"PRO_CODIGO": ["1", "2"], "PRO_DESCRICAO": ["PASTILHA X", "PASTILHA Y"],
                       "MAR_DESCRICAO": ["COBREQ", "FRASLE"], "SGR_CODIGO": [10, 10],
                       "ESTOQUE_DISPONIVEL": [5, 3], "FORNECEDOR1": ["F", "F"]})
    sai = pd.concat([_vendas("1", 8, 50.0), _vendas("2", 4, 50.0)], ignore_index=True)
    return main.calcular_grupos_descricao(
        sai, pd.DataFrame(), sp, HOJE, {}, remap_chaves={"PASTILHA Y": "PASTILHA X"},
        valores_base=valores_base)


def test_vinculo_vira_um_grupo_so():
    g = _grupo()
    assert list(g["GRUPO"]) == ["PASTILHA X"]
    assert int(g["GRUPO_QTD_ITENS"].iloc[0]) == 2
    assert float(g["GRUPO_ESTOQUE_DISPONIVEL"].iloc[0]) == 8.0


def test_curva_posicionada_contra_a_base():
    sozinho = _grupo()["GRUPO_CURVA"].iloc[0]
    assert sozinho == "D"  # sem base, o único grupo fecha 100% do valor
    # grupo é o maior e tem metade do valor total -> A
    valor = float(main.calcular_demanda_recente_e_variabilidade(
        pd.concat([_vendas("1", 8, 50.0), _vendas("2", 4, 50.0)]).assign(PRO_CODIGO="G"),
        pd.DataFrame(), HOJE)["VALOR_VENDIDO_12M"].iloc[0])
    assert _grupo(valores_base=[valor / 4] * 4)["GRUPO_CURVA"].iloc[0] == "A"
    # grupo é miúdo perto da base -> cauda
    assert _grupo(valores_base=[1e7] * 50)["GRUPO_CURVA"].iloc[0] in ("C", "D")
