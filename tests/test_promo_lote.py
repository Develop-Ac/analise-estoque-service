# -*- coding: utf-8 -*-
"""Lote semanal (rotas_promo.py): monta na primeira abertura da semana, não remonta, recusa."""
import os
import sys
from datetime import date

import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rotas_promo as rp  # noqa: E402
from modelos import PromoLoteRecusaRequest  # noqa: E402
from test_promo_rotas import _Res  # noqa: E402

_LOTE = [dict(pro_codigo="20", pro_descricao="B", posicao=2, acao="promocao", sgr_descricao="F", mar_descricao="M",
              vender_qtd=3, valor_vender=400.0, preco_atacado=150.0),
         dict(pro_codigo="10", pro_descricao="A", posicao=1, acao="giro_caixa", sgr_descricao="F", mar_descricao="M",
              vender_qtd=5, valor_vender=900.0, preco_atacado=120.0)]


class _Res2(_Res):
    rowcount = 2


class _ConnL:
    """Conexão falsa do lote: `tabelas` para to_regclass, `ja_tem` = lote da semana já gravado."""

    def __init__(self, tabelas=("com_promo_lote",), ja_tem=False):
        self.tabelas, self.ja_tem, self.chamadas, self.fechada = set(tabelas), ja_tem, [], False

    def execute(self, sql, params=None):
        s = str(sql)
        self.chamadas.append((s, params))
        if "to_regclass" in s:
            return _Res("x" if params["n"] in self.tabelas else None)
        if "SELECT 1 FROM com_promo_lote" in s:
            return _Res(1 if self.ja_tem else None)
        if "INSERT INTO com_promo_lote" in s:
            self.ja_tem = True
        if "FROM com_promo_lote WHERE semana = :s ORDER BY" in s:
            return _Res(linhas=[dict(r, recusado_em=None, recusado_por=None, recusa_motivo=None)
                                 for r in sorted(_LOTE, key=lambda r: r["posicao"])])
        if "UPDATE com_promo_lote" in s:
            return _Res2()
        return _Res(linhas=[])

    def commit(self):
        pass

    def close(self):
        self.fechada = True

    def sqls(self, trecho):
        return [(s, p) for s, p in self.chamadas if trecho in s]


@pytest.fixture
def conn(monkeypatch):
    c = _ConnL()
    monkeypatch.setattr(rp, "get_db_connection", lambda: c)
    monkeypatch.setattr(rp, "hoje_cuiaba", lambda: date(2026, 11, 18))
    monkeypatch.setattr(rp, "_linhas", lambda conn, req: [{"grouped": req.grouped_view}])
    monkeypatch.setattr(rp, "montar_lote", lambda linhas, fora: list(_LOTE))
    return c


def test_sem_tabela_503(conn):
    conn.tabelas = set()
    with pytest.raises(HTTPException) as e:
        rp.lote_semanal()
    assert e.value.status_code == 503 and "2026-11-17_promo_lote_semanal.sql" in e.value.detail


def test_primeira_chamada_grava_e_segunda_nao(conn):
    r = rp.lote_semanal()
    ins = conn.sqls("INSERT INTO com_promo_lote")
    assert len(ins) == 2 and all(p["semana"] == date(2026, 11, 16) for _, p in ins)
    assert r["semana"] == "2026-11-16" and r["corrente"] is True
    assert [x["pro_codigo"] for x in r["itens"]] == ["10", "20"]
    assert r["itens"][0]["situacao"] == "sugerido" and r["itens"][0]["vendido_qtd"] == 0
    assert conn.sqls("pg_advisory_xact_lock")

    conn.chamadas.clear()
    rp.lote_semanal()
    assert not conn.sqls("INSERT INTO com_promo_lote")


def test_semana_passada_nao_monta(conn):
    r = rp.lote_semanal("2026-11-09")
    assert not conn.sqls("INSERT INTO com_promo_lote") and not conn.sqls("pg_advisory_xact_lock")
    assert r["corrente"] is False and r["semana"] == "2026-11-09"
    with pytest.raises(HTTPException) as e:
        rp.lote_semanal("xx")
    assert e.value.status_code == 400


def test_recusar_e_desfazer(conn):
    r = rp.recusar_lote(PromoLoteRecusaRequest(itens=["10", "20"], motivo="sem margem"), x_user_id=None)
    (s, p), = conn.sqls("UPDATE com_promo_lote")
    assert r == {"ok": True, "itens": 2}
    assert "recusado_em = NOW()" in s
    assert p == {"usr": None, "motivo": "sem margem", "s": date(2026, 11, 16), "itens": ["10", "20"]}

    conn.chamadas.clear()
    with pytest.raises(HTTPException) as e:   # tela aberta no lote da semana passada
        rp.recusar_lote(PromoLoteRecusaRequest(itens=["10"], semana="2026-11-09"), x_user_id=None)
    assert e.value.status_code == 409 and not conn.sqls("UPDATE com_promo_lote")

    rp.desfazer_recusa_lote(PromoLoteRecusaRequest(itens=["10"], motivo="x", semana="2026-11-16"))
    (s, p), = conn.sqls("UPDATE com_promo_lote")
    assert "recusado_em = NULL" in s
    assert p == {"usr": None, "motivo": None, "s": date(2026, 11, 16), "itens": ["10"]}


def test_fora_da_sugestao_corte_e_oferta():
    c = _ConnL()
    rp._fora_da_sugestao(c, date(2026, 11, 16))
    assert not c.sqls("ven_oferta_item")
    (_, p), = c.sqls("recusado_em IS NOT NULL")
    assert p == {"corte": date(2026, 10, 19)}

    c = _ConnL(tabelas=("ven_oferta_item",))
    rp._fora_da_sugestao(c, date(2026, 11, 16))
    assert c.sqls("FROM ven_oferta_item WHERE status <> 'ENCERRADO'")


class _ConnO(_ConnL):
    """_ConnL com as ofertas: `ofertas` = linhas (semana, pro_codigo, item_id, oferta_id, nome, qtd, valor
    [, status]; sem status = APROVADO) e `lote_semanas` = linhas (semana, pro_codigo, recusado_em,
    valor_vender) do acompanhamento."""

    def __init__(self, tabelas=("com_promo_lote", "ven_oferta_item", "ven_oferta"), ofertas=(), lote_semanas=()):
        super().__init__(tabelas, ja_tem=True)
        self.ofertas, self.lote_semanas = list(ofertas), list(lote_semanas)

    def execute(self, sql, params=None):
        s = str(sql)
        if "information_schema.columns" in s and "ven_orcamento_item" in s:
            self.chamadas.append((s, params))
            return _Res(1)
        if "JOIN ven_oferta_item i" in s:
            self.chamadas.append((s, params))
            return _Res(linhas=sorted((r if len(r) == 8 else (*r, "APROVADO") for r in self.ofertas), key=lambda r: r[2]))
        if "SELECT semana, pro_codigo, recusado_em, valor_vender" in s:
            self.chamadas.append((s, params))
            return _Res(linhas=self.lote_semanas)
        return super().execute(sql, params)


def _usar(monkeypatch, c):
    monkeypatch.setattr(rp, "get_db_connection", lambda: c)
    monkeypatch.setattr(rp, "hoje_cuiaba", lambda: date(2026, 11, 18))
    return c


def test_item_com_vinculo_sai_em_oferta(monkeypatch):
    s = date(2026, 11, 16)
    c = _usar(monkeypatch, _ConnO(ofertas=[(s, "20", 5, 7, "Black Friday", 2, 300.0),
                                          (s, "20", 9, 8, "Liquida para-brisa", 1, 100.0)]))
    r = rp.lote_semanal()
    it = {x["pro_codigo"]: x for x in r["itens"]}
    assert it["20"]["situacao"] == "em_oferta"
    assert (it["20"]["oferta_id"], it["20"]["oferta_nome"]) == (8, "Liquida para-brisa")   # item de maior id
    assert (it["20"]["vendido_qtd"], it["20"]["vendido_valor"]) == (3, 400)
    assert it["10"]["situacao"] == "sugerido" and it["10"]["oferta_id"] is None and it["10"]["vendido_qtd"] == 0
    (sql, p), = c.sqls("JOIN ven_oferta_item i")
    assert p == {"ini": s, "fim": s}
    assert "i.inicio >= l.semana AND i.inicio <= l.semana + 7" in sql   # segunda seguinte: inicio em UTC
    assert "ven_orcamento_item" in sql and "oc.status = 'FECHADO'" in sql and "fora_promocao = FALSE" in sql


def test_oferta_encerrada_libera_o_item(monkeypatch):
    s = date(2026, 11, 16)
    # 20: única oferta encerrada -> volta a sugerido, com o grupo e o vendido à mostra
    # 10: uma encerrada (id maior) e outra no ar -> em oferta, pelo grupo que ainda vale
    c = _usar(monkeypatch, _ConnO(ofertas=[(s, "20", 5, 7, "Black Friday", 2, 300.0, "ENCERRADO"),
                                          (s, "10", 6, 8, "No ar", 1, 100.0, "APROVADO"),
                                          (s, "10", 9, 7, "Black Friday", 4, 400.0, "ENCERRADO")]))
    it = {x["pro_codigo"]: x for x in rp.lote_semanal()["itens"]}
    assert it["20"]["situacao"] == "sugerido" and "ativa" not in it["20"]
    assert (it["20"]["oferta_nome"], it["20"]["vendido_qtd"], it["20"]["vendido_valor"]) == ("Black Friday", 2, 300)
    assert (it["10"]["situacao"], it["10"]["oferta_id"], it["10"]["oferta_nome"], it["10"]["vendido_qtd"]) == ("em_oferta", 8, "No ar", 5)


def test_acompanhamento_numeros_exatos(monkeypatch):
    s, ant = date(2026, 11, 16), date(2026, 11, 9)
    lote = [(s, str(i), "2026-11-17 10:00" if 45 <= i < 50 else None, 100.0) for i in range(50)]
    lote += [(ant, "1", None, 50.0)]
    vend = [(4, 1000.0), (3, 900.0), (5, 1500.0)] + [(0, 0.0)] * 17   # 12 unidades, R$ 3.400
    ofertas = [(s, str(i), 100 + i, 1, "BF", q, v) for i, (q, v) in enumerate(vend)]
    c = _usar(monkeypatch, _ConnO(ofertas=ofertas, lote_semanas=lote))
    r = rp.acompanhamento_lote(semanas=99)
    assert [x["semana"] for x in r] == ["2026-11-16", "2026-11-09"]
    assert r[0] == {"semana": "2026-11-16", "sugeridos": 50, "recusados": 5, "em_oferta": 20,
                    "valor_vender": 5000.0, "vendido_qtd": 12, "vendido_valor": 3400}
    assert r[1]["sugeridos"] == 1 and r[1]["em_oferta"] == 0
    (_, p), = c.sqls("SELECT semana, pro_codigo, recusado_em")
    assert p == {"n": 52}
    (_, p), = c.sqls("JOIN ven_oferta_item i")
    assert p == {"ini": ant, "fim": s}


def test_acompanhamento_sem_tabela_lista_vazia(monkeypatch):
    _usar(monkeypatch, _ConnO(tabelas=()))
    assert rp.acompanhamento_lote() == []


def test_sem_ven_oferta_item_vendido_zero(monkeypatch):
    s = date(2026, 11, 16)
    c = _usar(monkeypatch, _ConnO(tabelas=("com_promo_lote",), lote_semanas=[(s, "10", None, 900.0)]))
    r = rp.lote_semanal()
    assert all(x["vendido_qtd"] == 0 and x["vendido_valor"] == 0 and x["situacao"] == "sugerido" for x in r["itens"])
    a = rp.acompanhamento_lote()
    assert a[0]["vendido_qtd"] == 0 and a[0]["em_oferta"] == 0
    assert not c.sqls("ven_oferta") and not c.sqls("ven_orcamento")


def _peq(codigo, valor, marca="M1", familia="F1", **kw):
    """Linha avaliada mínima para pequenos/_totais."""
    return dict(id=int(codigo), pro_codigo=codigo, acao="promocao", preco_atacado=10.0, vender_qtd=1,
                valor_vender=valor, publico="atacado", sgr_descricao=familia, mar_descricao=marca,
                valor_parado=valor, valor_excesso=0.0, caixa_potencial=0.0, na_base=False, valor_parado_240=0.0,
                grupo_chave="G", **kw)


@pytest.fixture
def peq(monkeypatch):
    """Rotas dos itens pequenos sobre linhas fixas; _linhas filtra por marca como o SQL faz."""
    linhas = [_peq("1", 299.0), _peq("2", 300.0), _peq("3", 100.0, marca="M2"),
              _peq("4", 50.0, marca=None), _peq("5", 120.0, marca="M2")]
    c = _usar(monkeypatch, _ConnL(tabelas=("com_promo_lote", "ven_oferta_item")))
    vistos = []

    def _linhas(conn, req):
        vistos.append(req.grouped_view)
        return [dict(x) for x in linhas if not req.brands or x["mar_descricao"] in req.brands]

    def _execute(sql, params=None, _orig=c.execute):   # produto 5 numa oferta aberta
        if "FROM ven_oferta_item WHERE status" in str(sql):
            return _Res(linhas=[("5",)])
        return _orig(sql, params)

    c.execute = _execute
    monkeypatch.setattr(rp, "_linhas", _linhas)
    monkeypatch.setattr(rp, "_get_stock_batches", lambda codigos: {})
    return vistos


def test_ranking_pequenos_por_marca_soma_total(peq):
    r = rp.ranking_pequenos("marca")
    assert r["por"] == "marca" and peq == [True]
    assert r["linhas"] == [{"nome": "M1", "itens": 1, "valor_vender": 299.0},      # 300 não é pequeno
                           {"nome": "M2", "itens": 1, "valor_vender": 100.0},      # 5 está em oferta
                           {"nome": None, "itens": 1, "valor_vender": 50.0}]
    assert r["total"] == {"itens": 3, "valor_vender": 449.0}
    assert sum(x["valor_vender"] for x in r["linhas"]) == r["total"]["valor_vender"]
    assert sum(x["itens"] for x in r["linhas"]) == r["total"]["itens"]
    assert rp.ranking_pequenos()["linhas"][0] == {"nome": "F1", "itens": 3, "valor_vender": 449.0}
    with pytest.raises(HTTPException) as e:
        rp.ranking_pequenos("grupo")
    assert e.value.status_code == 400


def test_plan_pequeno_valor_lista_plana_da_marca(peq):
    from modelos import PromoPlanRequest
    r = rp.planejar_promocao(PromoPlanRequest(pequeno_valor=True, brands=["M2"], sort="valor_vender"))
    assert peq == [True]
    assert [x["pro_codigo"] for x in r["items"]] == ["3"] and not any(x.get("is_grupo") for x in r["items"])
    assert r["total"] == 1 and r["totais"]["itens"] == 1 and r["totais"]["valor_vender"] == 100.0

    r = rp.planejar_promocao(PromoPlanRequest(pequeno_valor=True, sort="valor_vender"))
    assert [x["pro_codigo"] for x in r["items"]] == ["1", "3", "4"]   # 299 entra, 300 e o em oferta não


def test_plan_sem_pequeno_valor_agrupa_como_antes(peq):
    from modelos import PromoPlanRequest
    r = rp.planejar_promocao(PromoPlanRequest(grouped_view=True))
    assert r["total"] == 1 and r["items"][0]["is_grupo"] and len(r["items"][0]["membros"]) == 5
