"""Curva ABC combinada (valor × giro) — rodar: python -m pytest tests/test_curva_abc.py"""
import pandas as pd

import main as m


def test_curva_por_valor_cortes():
    assert [m.curva_por_valor(p) for p in (10, 70, 70.1, 90, 95, 97, 99)] == list("AABBCCD")


def test_classe_giro():
    assert [m.classe_giro(x) for x in (13, 6, 5, 3, 2, 1, 0, None)] == list("AABBCCCC")


def test_matriz():
    # peça cara que vende 1x/ano sai de A
    assert m.curva_combinada("A", "C") == "B"
    # item barato que vende todo mês sobe
    assert m.curva_combinada("D", "A") == "B"
    assert m.curva_combinada("B", "A") == "A"
    assert m.curva_combinada("A", "B") == "A"
    assert m.curva_combinada("B", "B") == "B"
    assert m.curva_combinada("C", "B") == "C"
    assert m.curva_combinada("D", "C") == "D"


def test_meses_com_venda_no_motor():
    hoje = pd.Timestamp("2026-11-01")
    datas = pd.to_datetime(["2026-01-10", "2026-01-20", "2026-03-05", "2026-10-02"])
    sai = pd.DataFrame({"PRO_CODIGO": ["1"] * 4 + ["2"], "DATA": list(datas) + [pd.Timestamp("2026-05-01")],
                        "QUANTIDADE_AJUSTADA": [200, 1, 1, 1, 1], "TOTAL_LIQUIDO": [10, 10, 10, 10, 5000]})
    vp = pd.DataFrame({"PRO_CODIGO": ["2"], "QUANTIDADE": [3], "DATA": [pd.Timestamp("2026-07-01")]})
    res = m.calcular_demanda_recente_e_variabilidade(sai, vp, hoje).set_index("PRO_CODIGO")
    assert res.loc["1", "MESES_COM_VENDA_12M"] == 3   # 200 un num mês conta 1 mês
    assert res.loc["2", "MESES_COM_VENDA_12M"] == 1   # venda perdida não conta como giro
