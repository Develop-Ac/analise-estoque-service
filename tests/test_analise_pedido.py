# -*- coding: utf-8 -*-
"""Análise de compra por pedido (analise_pedido.py) — valor dos itens, a caminho e contas."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analise_pedido as ap  # noqa: E402


def _rod(pro, **kw):
    base = {"descricao": f"PROD {pro}", "grupo_chave": None, "grupo_qtd_itens": 1,
            "grupo_estoque_max": None, "grupo_demanda_dia": None,
            "estoque_max_sugerido": None, "demanda_real_dia": None, "sob_encomenda": False}
    base.update(kw)
    return base


def _linha(pro="1", qtd=8, vu=100, ipi=None, status=None):
    return {"pro_codigo": pro, "pro_descricao": f"ITEM {pro}", "quantidade": qtd,
            "valor_unitario": vu, "ipi": ipi, "status_item": status}


_ROD = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
_CAD = {"1": {"COMERCIALIZAVEL": "S", "PRO_DESCRICAO": "PROD 1"}}


def _analise(a_caminho=None, itens=None, rod=_ROD, saldos=None):
    valorados = ap.valorar_itens(itens or [_linha()], None, False)
    return ap.montar_analise_pedido(valorados, _CAD, rod, saldos or {"1": 10}, a_caminho or {}, 0.0217, 365)


def test_criterio_1_estoque_cobertura_excesso():
    r = _analise()
    [u] = r["unidades"]
    assert (u["estoque_antes"], u["estoque_depois"], u["cobertura_depois"]) == (10, 18, 36.0)
    assert (u["sai_de"], u["sai_ate"]) == (20.0, 36.0)
    assert (u["excesso_existente"], u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (0, 6, 600.0)
    assert u["a_caminho"] == 0 and r["valor_total"] == 800.0


def test_criterio_2_custo_sem_prazo():
    r = _analise()
    assert (r["custo_caixa"]["custo_compra"], r["custo_caixa"]["custo_excesso"]) == (16.20, 13.02)
    assert (r["resumo"]["custo_caixa_compra"], r["resumo"]["custo_caixa_excesso"]) == (16.20, 13.02)
    assert r["custo_caixa"]["base_prazo"] == "sem_prazo"
    assert "prazo_medio_dias" not in r["custo_caixa"] and "fluxo" not in r


def test_criterio_3_a_caminho_entra_no_estoque():
    [u] = _analise(a_caminho={"1": 4})["unidades"]
    assert (u["a_caminho"], u["estoque_antes"], u["estoque_depois"]) == (4, 14, 22)
    assert (u["excesso_existente"], u["excesso_gerado_qtd"]) == (2, 8)


def test_criterio_4_proprio_pedido_fora_do_a_caminho():
    linhas = [{"pedido_id": "p1", "pro_codigo": 1, "qtd": 5}, {"pedido_id": "p2", "pro_codigo": 1, "qtd": 4}]
    assert ap.somar_a_caminho(linhas, "p1") == {"1": 4.0}


def test_criterio_5_nao_atendido_fora():
    itens = ap.valorar_itens([_linha(), _linha("2", qtd=3, vu=50, status="nao_atendido")], None, False)
    assert [i["pro_codigo"] for i in itens] == ["1"]
    assert _analise(itens=[_linha(), _linha("2", qtd=3, vu=50, status="nao_atendido")])["valor_total"] == 800.0


def test_criterio_6_frete_rateado_e_ipi():
    a, b = ap.valorar_itens([_linha("1", qtd=3, vu=100), _linha("2", qtd=1, vu=100)], 40, False)
    assert (a["desembolso"], b["desembolso"]) == (330.0, 110.0)
    [c] = ap.valorar_itens([_linha("1", qtd=1, vu=100, ipi=10)], None, False)
    [d] = ap.valorar_itens([_linha("1", qtd=1, vu=100, ipi=10)], None, True)
    assert (c["desembolso"], d["desembolso"]) == (110.0, 100.0)
    [e] = ap.valorar_itens([_linha("1", vu=None)], None, False)
    assert e["desembolso"] == 0 and e["aviso"] == ap.AVISO_SEM_VALOR
    # sem valor em nenhum item, o frete não vira valor de mercadoria
    [f] = ap.valorar_itens([_linha("1", qtd=10, vu=None)], 40, False)
    assert f["desembolso"] == 0


def test_criterio_7_duas_marcas_do_grupo_viram_uma_unidade():
    g = {"grupo_chave": "G", "grupo_qtd_itens": 2, "grupo_estoque_max": 12, "grupo_demanda_dia": 0.5}
    rod = {"1": _rod("1", **g), "2": _rod("2", **g)}
    r = _analise(itens=[_linha("1", qtd=5), _linha("2", qtd=3)], rod=rod, saldos={"1": 6, "2": 4})
    [u] = r["unidades"]
    assert (u["tipo"], u["qtd_nota"], u["valor_nota"]) == ("grupo", 8, 800.0)
    assert (u["estoque_antes"], u["estoque_depois"]) == (10, 18)
    assert {p["pro_codigo"] for p in u["produtos"]} == {"1", "2"}


def test_nao_comercializavel_vira_nao_analisado():
    cad = {"1": {"COMERCIALIZAVEL": "N", "PRO_DESCRICAO": "CADEIRA"}}
    ok, fora = ap.separar_itens(ap.valorar_itens([_linha()], None, False), cad)
    assert ok == [] and fora == [{"pro_codigo": "1", "descricao": "CADEIRA", "valor": 800.0,
                                  "motivo": "imobilizado ou uso e consumo"}]
