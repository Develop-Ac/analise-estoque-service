# -*- coding: utf-8 -*-
"""Fotos no banco (foto_compra.py): tabela desligada e gravação só quando a assinatura muda."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import foto_compra as fc  # noqa: E402


class _Res:
    def __init__(self, valor=None, linhas=()):
        self.valor, self.linhas = valor, list(linhas)

    def scalar(self):
        return self.valor

    def mappings(self):
        return self

    def first(self):
        return self.linhas[0] if self.linhas else None

    def all(self):
        return self.linhas


class _Conn:
    """Conexão falsa: `ultima` é a última foto do documento (ou None); guarda os INSERTs."""

    def __init__(self, ultima=None, tabela=True):
        self.ultima, self.tabela, self.inserts, self.updates, self.commits = ultima, tabela, [], [], 0

    def execute(self, sql, params=None):
        s = str(sql)
        if "to_regclass" in s:
            return _Res("x" if self.tabela else None)
        if "ORDER BY versao DESC" in s:
            return _Res(linhas=[self.ultima] if self.ultima else [])
        if s.strip().startswith("INSERT"):
            self.inserts.append(params)
            return _Res(99)
        if s.strip().startswith("UPDATE"):
            self.updates.append(params)
        return _Res()

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


_ULT = {"id": 7, "versao": 2, "assinatura": "abc:0", "sinais": "3:3:0"}


def test_sem_tabela():
    assert fc.tabela_ligada(_Conn(tabela=False)) is False
    assert fc.tabela_ligada(_Conn()) is True


def test_mesma_assinatura_nao_insere():
    c = _Conn(dict(_ULT))
    assert fc.gravar_foto(c, "nota", "CHAVE", {"x": 1}, "abc:0", "rotina") == 7
    assert c.inserts == [] and c.updates == [] and c.commits == 1
    # sinais diferentes atualizam a foto vigente, sem versão nova
    fc.gravar_foto(c, "nota", "CHAVE", {"x": 1}, "abc:0", "rotina", sinais="4:3:0")
    assert c.inserts == [] and c.updates == [{"s": "4:3:0", "id": 7}]


def test_assinatura_diferente_insere_versao_seguinte():
    c = _Conn(dict(_ULT))
    assert fc.gravar_foto(c, "nota", "CHAVE", {"x": 1}, "def:1", "abertura", usuario_id=5) == 99
    (p,) = c.inserts
    assert (p["versao"], p["motivo"], p["origem"], p["usuario"], p["tardia"]) == (3, "lancamento", "abertura", "5", False)
    assert p["c"] == '{"x": 1}'

    c = _Conn(None)
    fc.gravar_foto(c, "cotacao", 123, {}, "abc:0", "rotina")
    assert (c.inserts[0]["versao"], c.inserts[0]["motivo"], c.inserts[0]["doc"]) == (1, "criacao", "123")


_CALC = {"conteudo": {"unidades": [], "resumo": {}}, "sem_hoje": None, "assinatura": "abc:0",
         "criado_em": None, "rodada": None, "produtos": {"1"}, "numero": 10}


def test_criterio_19_sem_tabela_devolve_hoje(monkeypatch):
    monkeypatch.setattr(fc, "get_db_connection", lambda: _Conn(tabela=False))
    r = fc.responder("cotacao", 10, _CALC, "5")
    assert r["hoje"] == _CALC["conteudo"] and r["fases"] == [] and r["comparacoes"] == []
    assert r["historico"] == {"ligado": False, "aviso": fc.AVISO_DESLIGADO}
    assert r["documento"] == {"fase": "cotacao", "id": "10", "numero": 10}


def test_falha_na_gravacao_nao_propaga(monkeypatch):
    class _Quebra(_Conn):
        def execute(self, sql, params=None):
            if "pg_advisory" in str(sql):
                raise RuntimeError("banco fora")
            return super().execute(sql, params)

    monkeypatch.setattr(fc, "get_db_connection", lambda: _Quebra())
    r = fc.responder("pedido", 3, _CALC, "5")
    assert r["hoje"] == _CALC["conteudo"] and r["historico"]["ligado"] is False and r["fases"] == []


class _Cadeia(_Conn):
    """Pedido 3 da cotação 10; a foto 50 é da nota CHAVE."""

    def execute(self, sql, params=None):
        s = str(sql)
        if "SELECT pedido_cotacao FROM com_pedido" in s:
            return _Res(10)
        if "SELECT fase, documento, conteudo" in s:
            return _Res(linhas=[("nota", "CHAVE", {})])
        return super().execute(sql, params)


def test_criterio_14_cadeia_do_pedido_sem_nota():
    assert fc.cadeia(_Cadeia(), "pedido", "3") == [("cotacao", "10", 10), ("pedido", "3", 10)]


def test_foto_fora_da_cadeia_404(monkeypatch):
    monkeypatch.setattr(fc, "get_db_connection", lambda: _Cadeia())
    with pytest.raises(HTTPException) as e:
        fc.responder_foto("pedido", "3", 50)
    assert e.value.status_code == 404


def test_responder_com_historico_recorta_fase_anterior(monkeypatch):
    un = lambda pro, ger: {"chave": pro, "produtos": [{"pro_codigo": pro}], "qtd_nota": 8, "valor_nota": 800,
                           "maximo": 12, "estoque_antes": 4, "estoque_depois": 12, "venda_dia": 0.5,
                           "excesso_gerado_qtd": ger, "excesso_gerado_valor": ger * 100,
                           "custo_caixa_compra": 0, "custo_caixa_excesso": 0, "selos": []}
    cot = {"unidades": [un("1", 0), un("2", 0)], "nao_analisados": [], "resumo": {"excesso_gerado_valor": 0}}
    hoje = {"unidades": [un("1", 0)], "resumo": {"excesso_gerado_valor": 0}}
    fotos = {9: ("cotacao", "10", cot), 11: ("pedido", "3", hoje)}
    vistas = []
    monkeypatch.setattr(fc, "get_db_connection", lambda: _Conn())
    monkeypatch.setattr(fc, "gravar_foto", lambda *a, **k: 11)
    monkeypatch.setattr(fc, "ultima_foto", lambda c, f, d: {"id": 11} if f == "pedido" else None)
    monkeypatch.setattr(fc, "registrar_vista", lambda c, i, u: vistas.append((i, u)))
    monkeypatch.setattr(fc, "cadeia", lambda c, f, d: [("cotacao", "10", 10), ("pedido", "3", 10)])
    monkeypatch.setattr(fc, "versoes", lambda c, f, d: [{"id": 9 if f == "cotacao" else 11}])
    monkeypatch.setattr(fc, "conteudo_da_foto", lambda c, i: fotos[i])
    r = fc.responder("pedido", 3, {**_CALC, "conteudo": hoje}, "5")
    assert r["historico"]["ligado"] is True and vistas == [(11, "5")]
    assert [(f["fase"], f["recortada"], len(f["conteudo"]["unidades"])) for f in r["fases"]] == \
        [("cotacao", True, 1), ("pedido", False, 1)]
    assert [c["foto_id"] for c in r["comparacoes"]] == [9, 11]
    assert [e["fase"] for e in r["evolucao"]] == ["cotacao", "pedido", "hoje"]
