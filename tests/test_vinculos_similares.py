# -*- coding: utf-8 -*-
"""Vínculo manual de similares unindo grupo_chave (vinculos_similares.py)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import vinculos_similares as vs  # noqa: E402


def test_sem_vinculo_nao_muda():
    assert vs.unificar_chaves({"1": "PASTILHA", "2": "DISCO"}, []) == {}


def test_vinculo_une_chaves_na_menor():
    pro2chave = {"1": "PASTILHA X", "2": "PASTILHA Y", "3": "PASTILHA Y"}
    # 3 não foi vinculado, mas tem a chave de 2: vai junto (mapa é por chave)
    assert vs.unificar_chaves(pro2chave, [["1", "2"]]) == {"PASTILHA Y": "PASTILHA X"}


def test_vinculo_cruza_linha_da_marca():
    pro2chave = {"1": "AMORTECEDOR", "2": "AMORTECEDOR • 1ª LINHA"}
    assert vs.unificar_chaves(pro2chave, [["1", "2"]]) == {"AMORTECEDOR • 1ª LINHA": "AMORTECEDOR"}


def test_vinculos_encadeados_sao_transitivos():
    pro2chave = {"1": "C", "2": "B", "3": "A"}
    out = vs.unificar_chaves(pro2chave, [["1", "2"], ["2", "3"]])
    assert out == {"B": "A", "C": "A"}


def test_membro_sem_chave_original_fica_fora():
    pro2chave = {"1": "FILTRO"}  # "2" é original: sem chave
    assert vs.unificar_chaves(pro2chave, [["1", "2"]]) == {}


def test_codigo_com_espaco():
    pro2chave = {"1": "B", "2": "A"}
    assert vs.unificar_chaves(pro2chave, [[" 1 ", "2 "]]) == {"B": "A"}


def test_produto_sem_descricao_nao_vira_ponte_entre_vinculos():
    # Produto sem descrição chega do pandas como NaN. Antes virava a chave "nan", e dois
    # vínculos manuais sem relação entre si, cada um com um produto assim, viravam um grupo só.
    import main as m
    descricoes = {"1": "CAPA COMPASS", "2": float("nan"), "3": "MAQUINA FOX", "4": float("nan"), "5": None}
    pro2chave = {p: k for p, d in descricoes.items() if (k := m.grupo_chave_com_linha(d, "X", {}))}
    assert pro2chave == {"1": "CAPA COMPASS", "3": "MAQUINA FOX"}
    assert vs.unificar_chaves(pro2chave, [["1", "2"], ["3", "4"]]) == {}


def test_descricao_provisoria_nao_forma_grupo_nem_ponte():
    # Cadastro provisório do ERP ("AAA", "AAAA") não é o mesmo produto: sem chave, não
    # agrupa entre si nem liga dois vínculos manuais (caso real: grupo "AAA" com 17 itens).
    import main as m
    assert [m.descricao_provisoria(d) for d in ("AAA", "AAAA", " A A ", "MOUSE", "", None)] == \
        [True, True, True, False, False, False]
    descricoes = {"1": "CAPA CORSA", "2": "AAAA", "3": "FAROL S-10", "4": "AAAA", "5": "AAA"}
    pro2chave = {p: k for p, d in descricoes.items() if (k := m.grupo_chave_com_linha(d, "X", {}))}
    assert pro2chave == {"1": "CAPA CORSA", "3": "FAROL S-10"}
    assert vs.unificar_chaves(pro2chave, [["1", "2"], ["3", "4", "5"]]) == {}
