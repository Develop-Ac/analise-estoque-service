# -*- coding: utf-8 -*-
"""Fotos da análise de compra (analise_foto.py) — assinatura, motivo, tardia e recorte."""
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analise_foto as af  # noqa: E402


def _it(pro="1", qtd=8, vu=100, ipi=None, status=None):
    return {"pro_codigo": pro, "quantidade": qtd, "valor_unitario": vu, "ipi": ipi, "status_item": status}


_CAB = {"status": "Aguardando analise", "frete": 50, "ipi_no_valor": False}


def test_assinatura_ignora_ordem_e_tipo_numerico():
    a = af.assinatura_cotacao([_it("1", 8), _it("2", 3)])
    assert a == af.assinatura_cotacao([_it("2", Decimal("3")), _it("1", 8.0)])
    assert a.endswith(":0")


def test_cotacao_muda_com_quantidade_e_fornecedor_nao_entra():
    a = af.assinatura_cotacao([_it("1", 8)])
    assert a != af.assinatura_cotacao([_it("1", 9)])
    assert a == af.assinatura_cotacao([{**_it("1", 8), "for_codigo": 77}])


def test_pedido_muda_com_cada_campo():
    base = af.assinatura_pedido(_CAB, [_it()])
    assert base.endswith(":0")
    assert base != af.assinatura_pedido(_CAB, [_it(vu=101)])
    assert base != af.assinatura_pedido(_CAB, [_it(ipi=5)])
    assert base != af.assinatura_pedido({**_CAB, "frete": 60}, [_it()])
    assert base != af.assinatura_pedido(_CAB, [_it(status="nao_atendido")])
    liberado = af.assinatura_pedido({**_CAB, "status": "Liberado"}, [_it()])
    assert liberado.endswith(":1") and liberado.split(":")[0] == base.split(":")[0]
    assert af.assinatura_pedido({**_CAB, "status": "Em analise"}, [_it()]) == base


def test_nota_muda_com_produto_e_lancamento():
    base = af.assinatura_nota([_it("1")], False)
    assert base != af.assinatura_nota([_it("1"), _it("2")], False)
    assert af.assinatura_nota([_it("1")], True) == base[:-1] + "1"


def test_motivo():
    a0 = af.assinatura_pedido(_CAB, [_it()])
    assert af.motivo("pedido", None, a0) == "criacao"
    assert af.motivo("pedido", a0, af.assinatura_pedido(_CAB, [_it(qtd=9)])) == "alteracao"
    liberado_alterado = af.assinatura_pedido({**_CAB, "status": "Liberado"}, [_it(qtd=9)])
    assert af.motivo("pedido", a0, liberado_alterado) == "liberacao"
    n0, n1 = af.assinatura_nota([_it()], False), af.assinatura_nota([_it(), _it("2")], True)
    assert af.motivo("nota", n0, n1) == "lancamento"
    assert af.motivo("nota", n0, af.assinatura_nota([_it("2")], False)) == "identificacao"
    assert af.motivo("cotacao", "x:0", "y:0") == "alteracao"


def test_tardia_so_na_versao_1():
    criado = datetime(2026, 11, 1, 8, 0)
    assert af.tardia(1, criado, criado + timedelta(days=2))
    assert not af.tardia(1, criado, criado + timedelta(hours=20))
    assert not af.tardia(2, criado, criado + timedelta(days=2))
    assert not af.tardia(1, None, criado)


def _un(chave, pros, valor, excesso, cc, ce):
    return {"chave": chave, "produtos": [{"pro_codigo": p} for p in pros], "valor_nota": valor,
            "excesso_gerado_valor": excesso, "custo_caixa_compra": cc, "custo_caixa_excesso": ce,
            "selos": []}


def test_criterio_13_recorte_soma_so_as_linhas_do_documento():
    conteudo = {
        "cotacao": {"numero": 123},
        "unidades": [_un("G1", ["1", "2"], 800, 600, 10, 5), _un("3", ["3"], 500, 0, 4, 0),
                     _un("4", ["4"], 300, 100, 2, 1)],
        "nao_analisados": [{"pro_codigo": "9", "valor": 50}, {"pro_codigo": "8", "valor": 70}],
        "resumo": {"valor_nota": 1720},
        "custo_caixa": {"taxa_mes": 0.0217, "custo_compra": 16, "custo_excesso": 6},
    }
    assert af.produtos_do_conteudo(conteudo) == {"1", "2", "3", "4", "8", "9"}
    rec, recortada = af.recortar(conteudo, {"2", "4", "9"})
    assert recortada
    assert [u["chave"] for u in rec["unidades"]] == ["G1", "4"]
    assert len(rec["unidades"][0]["produtos"]) == 2  # unidade inteira
    assert [n["pro_codigo"] for n in rec["nao_analisados"]] == ["9"]
    assert rec["resumo"]["valor_nota"] == 1150
    assert rec["resumo"]["valor_analisado"] == 1100
    assert rec["resumo"]["excesso_gerado_valor"] == 700
    assert rec["custo_caixa"] == {"taxa_mes": 0.0217, "custo_compra": 12, "custo_excesso": 6}
    assert rec["cotacao"] == {"numero": 123}
    assert conteudo["resumo"] == {"valor_nota": 1720}  # original intacto


def test_recorte_sem_corte_e_foto_sem_chaves():
    conteudo = {"unidades": [_un("1", ["1"], 100, 0, 0, 0)], "resumo": {"x": 1}}
    assert af.recortar(conteudo, {"1"}) == (conteudo, False)
    rec, recortada = af.recortar({"unidades": [_un("1", ["1"], 100, 0, 0, 0)]}, {"2"})
    assert recortada and rec["unidades"] == [] and rec["resumo"]["valor_nota"] == 0
    assert "custo_caixa" not in rec


def _uh(chave="1", antes=4, maximo=12, qtd=8, vu=100, venda=0.5):
    """Unidade no formato de montar_unidades com a conta de excesso de verdade."""
    gerado = af.an.excesso(antes, antes + qtd, qtd, maximo)[1] if maximo is not None else 0
    depois = antes + qtd
    return {"chave": chave, "maximo": maximo, "venda_dia": venda, "estoque_antes": antes,
            "estoque_depois": depois, "qtd_nota": qtd, "valor_nota": qtd * vu,
            "cobertura_depois": depois / venda, "excesso_gerado_qtd": gerado,
            "excesso_gerado_valor": gerado * vu}


def test_criterio_10_maximo_caiu():
    r = af.confrontar([_uh(antes=4, maximo=12)], [_uh(antes=4, maximo=8)])
    u = r["unidades"][0]
    assert u["epoca"]["excesso_gerado_qtd"] == 0 and u["hoje"]["excesso_gerado_qtd"] == 4
    assert u["parcela_maximo_qtd"] == 4 and u["parcela_maximo_valor"] == 400
    assert u["virou"] and u["causa"] == "maximo"
    assert u["epoca"]["qtd"] == 8
    assert r["resumo"] == {"excesso_epoca": 0, "excesso_hoje": 400, "parcela_maximo": 400}


def test_criterio_11_estoque_subiu():
    u = af.confrontar([_uh(antes=4, maximo=12)], [_uh(antes=9, maximo=12)])["unidades"][0]
    assert u["hoje"]["excesso_gerado_qtd"] == 5 and u["parcela_maximo_qtd"] == 0
    assert u["virou"] and u["causa"] == "estoque"


def test_criterio_12_parcela_sem_virar():
    r = af.confrontar([_uh(antes=10, maximo=12)], [_uh(antes=10, maximo=9)])
    u = r["unidades"][0]
    assert u["epoca"]["excesso_gerado_qtd"] == 6 and u["hoje"]["excesso_gerado_qtd"] == 8
    assert u["parcela_maximo_qtd"] == 2 and u["parcela_maximo_valor"] == 200
    assert not u["virou"] and u["causa"] is None


def test_unidade_nova_e_maximo_nulo():
    r = af.confrontar([_uh("1", maximo=None)], [_uh("1", maximo=8), _uh("2", maximo=8)])
    a, b = r["unidades"]
    assert a["epoca"]["maximo"] is None and a["parcela_maximo_qtd"] == 0 and not a["virou"] and a["causa"] is None
    assert b["epoca"] is None and not b["virou"]


def test_evolucao():
    fases = [{"fase": "cotacao", "documento": "123", "numero": 123,
              "conteudo_primeira": {"resumo": {"excesso_gerado_valor": 0}},
              "conteudo": {"resumo": {"excesso_gerado_valor": 600}}},
             {"fase": "pedido", "documento": "abc", "numero": 123,
              "conteudo": {"resumo": {"excesso_gerado_valor": 700}}}]
    assert af.evolucao(fases, {"resumo": {"excesso_gerado_valor": 800}}) == [
        {"fase": "cotacao", "documento": "123", "numero": 123, "primeira": 0, "ultima": 600},
        {"fase": "pedido", "documento": "abc", "numero": 123, "primeira": 700, "ultima": 700},
        {"fase": "hoje", "primeira": 800, "ultima": 800},
    ]
    assert af.evolucao([], None) == []


def test_nao_analisado_da_nota_sem_produto():
    conteudo = {"unidades": [_un("1", ["1"], 100, 0, 0, 0)],
                "nao_analisados": [{"n_item": 2, "descricao": "x", "valor": 10, "motivo": "sem de-para"}]}
    assert af.produtos_do_conteudo(conteudo) == {"1"}
    rec, recortada = af.recortar(conteudo, {"1"})
    assert recortada and rec["nao_analisados"] == []
