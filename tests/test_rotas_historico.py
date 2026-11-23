# -*- coding: utf-8 -*-
"""Rotas da cotação e do pedido ligadas ao histórico: sem 'hoje' depois da entrega, sem ERP."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import foto_compra as fc  # noqa: E402
import rotas_cotacao as rc  # noqa: E402
import rotas_pedido as rp  # noqa: E402

_ITENS = [{"pro_codigo": "1", "pro_descricao": "ITEM 1", "quantidade": 8, "valor_unitario": 100,
           "ipi": None, "status_item": None}]


def _sem_erp(modulo, monkeypatch):
    def erp(_):
        raise AssertionError("não deveria ler o ERP")
    monkeypatch.setattr(modulo, "get_realtime_stocks", erp)
    monkeypatch.setattr(fc, "responder", lambda fase, doc, calc, usr: (fase, doc, calc, usr))


def test_pedido_entregue_sem_hoje_e_sem_erp(monkeypatch):  # critério 15
    cab = {"id": 7, "pedido_cotacao": 123, "for_codigo": 1, "status": "Entregue", "origem": "cotacao",
           "frete": 0, "ipi_no_valor": False, "created_at": None}
    monkeypatch.setattr(rp, "_pedido", lambda id: (cab, _ITENS))
    _sem_erp(rp, monkeypatch)
    fase, doc, calc, usr = rp.analise_pedido(" 7 ", None, "u1")
    assert (fase, doc, usr) == ("pedido", "7", "u1")
    assert calc["conteudo"] is None and calc["sem_hoje"] == fc.SEM_HOJE
    assert calc["produtos"] == {"1"} and calc["numero"] == 123 and calc["assinatura"].endswith(":1")


def test_pedido_encomenda_409(monkeypatch):
    class _Pg:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            s = str(sql)
            linhas = ([{"id": 7, "pedido_cotacao": None, "for_codigo": 1, "status": "Liberado",
                        "origem": "encomenda", "frete": 0, "ipi_no_valor": False, "created_at": None}]
                      if "FROM com_pedido WHERE" in s else _ITENS)
            return type("R", (), {"mappings": lambda self: self, "first": lambda self: linhas[0],
                                  "__iter__": lambda self: iter(linhas)})()
    monkeypatch.setattr(rp, "get_db_connection", _Pg)
    with pytest.raises(HTTPException) as e:
        rp.calcular("7")
    assert e.value.status_code == 409


def test_cotacao_com_pedido_entregue_sem_hoje(monkeypatch):  # critério 16
    cab = {"pedido_cotacao": 123, "created_at": None, "entregue": True}
    monkeypatch.setattr(rc, "_cotacao", lambda n: (cab, [{"pro_codigo": " 1", "quantidade": 8}]))
    _sem_erp(rc, monkeypatch)
    fase, doc, calc, _ = rc.analise_cotacao(123, None, None)
    assert (fase, doc) == ("cotacao", "123")
    assert calc["conteudo"] is None and calc["sem_hoje"] == fc.SEM_HOJE and calc["produtos"] == {"1"}


def _nota_falsa(rn, monkeypatch, entrada):
    cab = {"numero": 55, "emitente": "FORN", "cnpj": "1", "emissao": None, "valor_total": 0.0}
    monkeypatch.setattr(rn, "_nota_pg", lambda c: {"cabecalho": cab, "itens": [], "duplicatas": [],
                                                    "criado_em": "2026-11-20"})
    monkeypatch.setattr(rn, "_entrada_emp3", lambda c: entrada)
    monkeypatch.setattr(rn, "_titulos_da_nota", lambda nfe: [])
    monkeypatch.setattr(rn, "_comprometido", lambda i, f, n: {})
    monkeypatch.setattr(rn, "_resolver", lambda *a: ([{"pro_codigo": "1", "quantidade": 8, "desembolso": 800.0}], []))
    monkeypatch.setattr(rn, "_rodada", lambda pros: ({}, None))
    monkeypatch.setattr(rn, "get_realtime_stocks", lambda m: {})
    monkeypatch.setattr(rn, "_nomes_fornecedor", lambda c: [])
    monkeypatch.setattr(rn, "_nao_comprados", lambda n, m: {"itens": []})


def test_nota_calcular_conteudo_e_assinatura(monkeypatch):  # critério 21
    import rotas_nota as rn
    _nota_falsa(rn, monkeypatch, None)
    calc = rn.calcular("1" * 44)
    assert set(calc["conteudo"]) == {"nota", "resumo", "unidades", "nao_analisados", "custo_caixa",
                                     "fluxo", "nao_comprados"}
    assert calc["sem_hoje"] is None and calc["numero"] == 55 and calc["criado_em"] == "2026-11-20"
    _nota_falsa(rn, monkeypatch, {"nfe": 9, "dt_entrada": None, "for_codigo": 1})
    assert rn.calcular("1" * 44)["assinatura"] != calc["assinatura"]


def test_sinais_notas_api_falhou(monkeypatch):
    import rotas_nota as rn
    monkeypatch.setattr(rn, "ERP_API_URL", "http://erp")

    def falha(*a):
        raise RuntimeError("fora")
    monkeypatch.setattr(rn, "_erp_api_post", falha)
    assert rn._sinais_notas(["1" * 44]) == {}
    monkeypatch.setattr(rn, "ERP_API_URL", "")
    assert rn._sinais_notas(["1" * 44]) == {}
