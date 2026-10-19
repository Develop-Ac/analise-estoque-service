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
