# -*- coding: utf-8 -*-
"""Canal 12 m (main._anexar_canal_12m): lido antes de fechar a conexão; falha vira ERRO com colunas nulas."""
import inspect
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main  # noqa: E402


class _Conn:
    timeout = 0


def _saldo():
    return pd.DataFrame({"PRO_CODIGO": [" 10", "20"], "SALDO": [5, 3]})


def test_canal_lido_antes_de_fechar_conexao():
    src = inspect.getsource(main.carregar_dados_do_banco)
    assert src.index("_anexar_canal_12m(") < src.rindex("conn.close()")


def test_canal_preenche_quantidades(monkeypatch):
    canal = pd.DataFrame({"PRO_CODIGO": ["10"], "QTD_ATACADO_12M": [4], "QTD_VAREJO_12M": [7]})
    monkeypatch.setattr(main.pd, "read_sql", lambda sql, conn: canal.copy())
    df = main._anexar_canal_12m(_saldo(), _Conn()).set_index("PRO_CODIGO")
    assert df.loc["10", "QTD_VAREJO_12M"] == 7
    assert df.loc["10", "QTD_ATACADO_12M"] == 4
    assert pd.isna(df.loc["20", "QTD_VAREJO_12M"])


def test_canal_falha_loga_erro_e_deixa_nulo(monkeypatch, capsys):
    def _falha(sql, conn):
        raise RuntimeError("conexão fechada")

    monkeypatch.setattr(main.pd, "read_sql", _falha)
    df = main._anexar_canal_12m(_saldo(), _Conn())
    assert len(df) == 2
    assert df["QTD_VAREJO_12M"].isna().all() and df["QTD_ATACADO_12M"].isna().all()
    assert "ERRO" in capsys.readouterr().out
