# -*- coding: utf-8 -*-
"""Análise de compra por cotação (analise_cotacao.py) — valor estimado e a caminho."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analise_cotacao as ac  # noqa: E402
import analise_pedido as ap  # noqa: E402


def _rod(pro, **kw):
    base = {"descricao": f"PROD {pro}", "grupo_chave": None, "grupo_qtd_itens": 1,
            "grupo_estoque_max": None, "grupo_demanda_dia": None,
            "estoque_max_sugerido": None, "demanda_real_dia": None, "sob_encomenda": False,
            "custo_unitario": 100.0}
    base.update(kw)
    return base


def _linha(pro="1", qtd=8):
    return {"pro_codigo": pro, "pro_descricao": f"ITEM {pro}", "quantidade": qtd}


_ROD = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
_CAD = {"1": {"COMERCIALIZAVEL": "S", "PRO_DESCRICAO": "PROD 1"}}


def _analise(linhas=None, rod=_ROD, a_caminho=None):
    itens = ac.valorar_itens_cotacao(linhas or [_linha()], rod)
    return ap.montar_analise_pedido(itens, _CAD, rod, {"1": 10}, a_caminho or {}, 0.0217, 365)


def test_criterio_1_estoque_cobertura_excesso():
    r = _analise()
    [u] = r["unidades"]
    assert (u["estoque_antes"], u["estoque_depois"], u["cobertura_depois"]) == (10, 18, 36.0)
    assert (u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (6, 600.0)
    assert r["valor_total"] == 800.0


def test_criterio_2_pedidos_da_mesma_cotacao_fora_do_a_caminho():
    mesma = [{"pedido_id": "p1", "pedido_cotacao": 77, "pro_codigo": 1, "qtd": 4}]
    outra = [{"pedido_id": "p2", "pedido_cotacao": 88, "pro_codigo": 1, "qtd": 4}]
    assert ap.somar_a_caminho(mesma, cotacao=77) == {}
    assert ap.somar_a_caminho(outra, cotacao=77) == {"1": 4.0}
    [u] = _analise(a_caminho=ap.somar_a_caminho(outra, cotacao=77))["unidades"]
    assert u["a_caminho"] == 4


def test_criterio_3_sem_custo_valor_zero_e_aviso():
    rod = {**_ROD, "2": _rod("2", custo_unitario=None)}
    a, b = ac.valorar_itens_cotacao([_linha(), _linha("2", qtd=3)], rod)
    assert a["desembolso"] == 800.0 and "aviso" not in a
    assert b["desembolso"] == 0 and b["aviso"] == ac.AVISO_SEM_CUSTO
    [c] = ac.valorar_itens_cotacao([_linha("9")], {})
    assert c["desembolso"] == 0 and c["aviso"] == ac.AVISO_SEM_CUSTO
    assert _analise([_linha(), _linha("2", qtd=3)], rod=rod)["valor_total"] == 800.0
