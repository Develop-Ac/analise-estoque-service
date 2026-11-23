# -*- coding: utf-8 -*-
"""Rotina de fotos: só refaz a análise de quem mudou, falha não grava nem derruba, lote limitado."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import HTTPException  # noqa: E402

import analise_foto as af  # noqa: E402
import foto_compra as fc  # noqa: E402
import rotina_fotos as rf  # noqa: E402


class _Res:
    def __init__(self, valor=None, linhas=()):
        self.valor, self.linhas = valor, list(linhas)

    def scalar(self):
        return self.valor

    def mappings(self):
        return self

    def all(self):
        return self.linhas


class _Banco:
    """Postgres falso: cotações (itens por número) e as fotos gravadas."""

    def __init__(self, cotacoes):
        self.cotacoes, self.fotos, self.destravas = cotacoes, [], 0

    def __call__(self):
        return self   # get_db_connection()

    def execute(self, sql, params=None):
        s = str(sql)
        if "to_regclass" in s:
            return _Res("x")
        if "pg_try_advisory_lock" in s:
            return _Res(True)
        if "pg_advisory_unlock" in s:
            self.destravas += 1
            return _Res(True)
        if "DISTINCT ON" in s:
            ult = {}
            for f in self.fotos:
                ult[(f["fase"], f["documento"])] = f
            return _Res(linhas=list(ult.values()))
        if "FROM com_cotacao c" in s:
            return _Res(linhas=[{"pedido_cotacao": n, "pro_codigo": "1", "quantidade": q}
                                for n, q in self.cotacoes.items()])
        return _Res(linhas=[])

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _montar(monkeypatch, cotacoes, calcular=None):
    rf._FALHAS.clear()
    banco = _Banco(cotacoes)
    chamadas = []

    def calc(numero):
        chamadas.append(numero)
        if calcular:
            calcular(numero)
        linhas = [{"pro_codigo": "1", "quantidade": banco.cotacoes[numero]}]
        return {"conteudo": {"unidades": []}, "assinatura": af.assinatura_cotacao(linhas),
                "criado_em": None, "rodada": None}

    def gravar(conn, fase, doc, conteudo, assinatura, origem, **kw):
        if not any(f["documento"] == doc and f["assinatura"] == assinatura for f in conn.fotos):
            conn.fotos.append({"fase": fase, "documento": doc, "assinatura": assinatura, "sinais": None})

    monkeypatch.setattr(rf, "get_db_connection", banco)
    monkeypatch.setattr(rf.estado_job, "em_execucao", lambda: False)
    monkeypatch.setattr(rf.rotas_cotacao, "calcular", calc)
    monkeypatch.setattr(fc, "gravar_foto", gravar)
    return banco, chamadas


def test_duas_passadas_sem_alteracao_uma_versao(monkeypatch):  # critério 8
    banco, chamadas = _montar(monkeypatch, {123: 8})
    rf.passada()
    rf.passada()
    assert len(banco.fotos) == 1 and chamadas == [123]
    assert banco.destravas == 2
    banco.cotacoes[123] = 9   # quantidade mudou: versão nova
    rf.passada()
    assert len(banco.fotos) == 2


def test_falha_no_calculo_nao_grava_nem_derruba(monkeypatch):  # critério 9
    def erp(numero):
        if numero == 1:
            raise HTTPException(status_code=502, detail="ERP fora")
    banco, chamadas = _montar(monkeypatch, {1: 8, 2: 5}, erp)
    assert rf.passada() == 2
    assert [f["documento"] for f in banco.fotos] == ["2"] and banco.destravas == 1
    rf.passada()   # tenta de novo a que falhou
    assert chamadas.count(1) == 2


def test_analise_semanal_rodando_nao_faz_nada(monkeypatch):
    banco, chamadas = _montar(monkeypatch, {123: 8})
    monkeypatch.setattr(rf.estado_job, "em_execucao", lambda: True)
    assert rf.passada() == 0 and chamadas == [] and banco.fotos == []


def test_lote_para_no_maximo(monkeypatch):
    banco, chamadas = _montar(monkeypatch, {n: 1 for n in range(1, 8)})
    monkeypatch.setattr(rf, "FOTO_LOTE_MAX", 3)
    assert rf.passada() == 3 and len(chamadas) == 3 and len(banco.fotos) == 3


def test_falha_vai_para_o_fim_da_fila(monkeypatch):
    def erp(numero):
        if numero == 1:
            raise HTTPException(status_code=404, detail="ruim")
    banco, chamadas = _montar(monkeypatch, {1: 8, 2: 5, 3: 4}, erp)
    monkeypatch.setattr(rf, "FOTO_LOTE_MAX", 1)
    rf.passada()   # a 1 falha
    rf.passada()   # a 2 passa na frente da que falhou
    assert chamadas == [1, 2]
