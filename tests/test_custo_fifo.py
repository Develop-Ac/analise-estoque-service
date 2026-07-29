# -*- coding: utf-8 -*-
"""
Custo FIFO das camadas vivas — a base de `custo_unitario` em com_fifo_completo.

O bug que estes testes travam: `custo_unitario` trazia a média ponderada do
preco_custo das LINHAS DE VENDA de 12 meses (custo do que foi vendido), e não o
custo do estoque. Quando o estoque zera e volta com custo diferente, o valor
ficava preso no custo antigo.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import empacotamento as emp  # noqa: E402

HOJE = pd.Timestamp("2026-07-26")


_COLS_ENT = ["PRO_CODIGO", "DATA", "QUANTIDADE", "PRECO_CUSTO", "LANCTO"]
_COLS_SAI = ["PRO_CODIGO", "DATA", "QUANTIDADE_AJUSTADA", "LANCTO"]


def _ent(rows):
    """rows: (data, qtd, custo) -> DataFrame de entradas no formato do ETL."""
    return pd.DataFrame([{"PRO_CODIGO": "P", "DATA": pd.Timestamp(d), "QUANTIDADE": q,
                          "PRECO_CUSTO": c, "LANCTO": i + 1}
                         for i, (d, q, c) in enumerate(rows)], columns=_COLS_ENT)


def _sai(rows):
    """rows: (data, qtd) -> DataFrame de saídas no formato do ETL."""
    return pd.DataFrame([{"PRO_CODIGO": "P", "DATA": pd.Timestamp(d),
                          "QUANTIDADE_AJUSTADA": q, "LANCTO": 1000 + i}
                         for i, (d, q) in enumerate(rows)], columns=_COLS_SAI)


def _saldo(qtd):
    return pd.DataFrame([{"PRO_CODIGO": "P", "ESTOQUE_DISPONIVEL": qtd}])


def _custo(ent, sai, saldo, packs=None):
    """Roda o motor e devolve (custo_fifo, linha completa do agregado)."""
    _, df_long, _ = emp.fifo_por_camadas(ent, sai, _saldo(saldo), packs=packs, hoje=HOJE)
    res = emp.custo_fifo_por_produto(df_long)
    if res.empty:
        return None, None
    linha = res[res["PRO_CODIGO"] == "P"].iloc[0]
    return linha["CUSTO_FIFO"], linha


# ---------------------------------------------------------------------------
# 1. O caso que motivou a correção: estoque zerou e voltou com custo diferente
# ---------------------------------------------------------------------------
def test_47709_estoque_zerou_e_reentrou_com_custo_muito_diferente():
    """
    Produto 47709 (CABO CAPO HB20), movimento real do ERP:
      04/08/2025 entrada NFE 38354 ... 1 un @ 950,34
      06/08/2025 saída  NFS       ... 1 un
      25/09/2025 entrada NFE 38758 ... 6 un @  10,37
      20/02/2026 saída  NFS       ... 1 un
    (a entrada de 872,09 da NFE 37772 é cancelada pela CNE 37772 e o ETL a
    descarta antes do FIFO — ver main.py `nfe_com_cne`.)

    Saldo hoje = 5 un, TODAS da camada de 10,37 → custo FIFO = 10,37.
    O valor gravado antes da correção era 441,23 = (872,09 + 10,37) / 2, a média
    das duas vendas da janela.
    """
    ent = _ent([("2025-08-04", 1, 950.34), ("2025-09-25", 6, 10.37)])
    sai = _sai([("2025-08-06", 1), ("2026-02-20", 1)])

    custo, linha = _custo(ent, sai, saldo=5)

    assert custo == pytest.approx(10.37)
    assert linha["N_CAMADAS_VIVAS"] == 1
    # guarda explícita contra a regressão para a média das vendas
    assert custo != pytest.approx(441.23, abs=0.01)
    # e contra a média simples das entradas (hipótese refutada no diagnóstico)
    assert custo != pytest.approx((950.34 + 10.37) / 2, abs=0.01)


# ---------------------------------------------------------------------------
# 2. Múltiplas camadas vivas -> média PONDERADA por quantidade
# ---------------------------------------------------------------------------
def test_multiplas_camadas_vivas_media_ponderada():
    """3 camadas vivas com custos diferentes: pondera por quantidade, não por camada."""
    ent = _ent([("2024-01-10", 2, 10.00),
                ("2025-03-05", 8, 20.00),
                ("2026-02-20", 10, 30.00)])
    sai = _sai([])

    custo, linha = _custo(ent, sai, saldo=20)

    esperado = (2 * 10.0 + 8 * 20.0 + 10 * 30.0) / 20  # 480 / 20 = 24,00
    assert custo == pytest.approx(esperado)
    assert custo == pytest.approx(24.00)
    assert linha["N_CAMADAS_VIVAS"] == 3
    assert linha["N_CUSTOS_DISTINTOS"] == 3
    # a média SIMPLES entre camadas (20,00) seria diferente: a ponderação importa
    assert custo != pytest.approx(20.00)
    # e a camada mais antiga (FIFO puro) também difere — fica registrada à parte
    assert linha["CUSTO_FIFO_CAMADA_ANTIGA"] == pytest.approx(10.00)


def test_camadas_parcialmente_consumidas_pondera_pelo_que_sobrou():
    """Consumo parcial: a camada antiga entra na média só com o saldo restante."""
    ent = _ent([("2024-01-10", 10, 10.00), ("2026-01-10", 10, 30.00)])
    sai = _sai([("2025-06-01", 8)])          # come 8 da camada de 10,00

    custo, linha = _custo(ent, sai, saldo=12)

    assert custo == pytest.approx((2 * 10.0 + 10 * 30.0) / 12)   # 26,666...
    assert linha["N_CAMADAS_VIVAS"] == 2


# ---------------------------------------------------------------------------
# 3. Entrada única
# ---------------------------------------------------------------------------
def test_entrada_unica():
    ent = _ent([("2025-08-20", 4, 277.33)])
    sai = _sai([("2025-09-04", 3)])

    custo, linha = _custo(ent, sai, saldo=1)

    assert custo == pytest.approx(277.33)
    assert linha["N_CAMADAS_VIVAS"] == 1
    assert linha["N_CUSTOS_DISTINTOS"] == 1
    # com uma só camada, FIFO puro e ponderado coincidem
    assert linha["CUSTO_FIFO_CAMADA_ANTIGA"] == pytest.approx(custo)


def test_entrada_unica_sem_custo_no_erp_nao_vira_zero():
    """preco_custo ausente/zero não pode virar custo 0,00 — tem de ficar sem custo,
    para o item cair no fallback de custo do main.py em vez de valer nada."""
    ent = _ent([("2025-08-20", 4, None)])
    custo, linha = _custo(ent, _sai([]), saldo=4)

    assert custo is None or pd.isna(custo)
    assert linha["N_CAMADAS_VIVAS"] == 1

    ent0 = _ent([("2025-08-20", 4, 0.0)])
    custo0, _ = _custo(ent0, _sai([]), saldo=4)
    assert custo0 is None or pd.isna(custo0)


# ---------------------------------------------------------------------------
# 4. Ordem FIFO: quem sai primeiro é a camada mais antiga
# ---------------------------------------------------------------------------
def test_consome_camada_mais_antiga_primeiro():
    """A camada cara e antiga é consumida antes; sobra a barata e nova."""
    ent = _ent([("2024-01-01", 5, 100.00), ("2025-01-01", 5, 10.00)])
    sai = _sai([("2025-06-01", 5)])

    custo, linha = _custo(ent, sai, saldo=5)

    assert custo == pytest.approx(10.00)
    assert linha["N_CAMADAS_VIVAS"] == 1


def test_estoque_zerado_nao_tem_camada_viva():
    """Sem saldo não existe custo FIFO — o main.py cai para a última entrada."""
    ent = _ent([("2025-01-01", 3, 50.00)])
    sai = _sai([("2025-02-01", 3)])

    _, df_long, _ = emp.fifo_por_camadas(ent, sai, _saldo(0), hoje=HOJE)
    res = emp.custo_fifo_por_produto(df_long)

    assert res.empty or res[res["PRO_CODIGO"] == "P"].empty


# ---------------------------------------------------------------------------
# 5. Camada de ajuste de inventário (saldo do ERP > movimento) não tem custo
# ---------------------------------------------------------------------------
def test_camada_de_ajuste_de_inventario_fica_fora_da_media():
    """
    Saldo do ERP (10) maior que o movimento (4): a reconciliação cria uma camada
    de ajuste sem nota. Ela não tem custo e não pode entrar na média — senão
    entraria como 0,00 e derrubaria o custo do item.
    """
    ent = _ent([("2025-01-01", 4, 80.00)])
    custo, linha = _custo(ent, _sai([]), saldo=10)

    assert custo == pytest.approx(80.00)          # e não (4*80 + 6*0)/10 = 32,00
    assert linha["N_CAMADAS_VIVAS"] == 2          # a de ajuste existe...
    assert linha["QTD_COM_CUSTO"] == pytest.approx(4.0)   # ...mas fica fora da média


def test_reconciliacao_para_menos_consome_as_camadas_antigas():
    """Saldo do ERP menor que o movimento: consome FIFO, sobra a camada nova."""
    ent = _ent([("2024-01-01", 5, 100.00), ("2025-01-01", 5, 10.00)])
    custo, _ = _custo(ent, _sai([]), saldo=5)
    assert custo == pytest.approx(10.00)


# ---------------------------------------------------------------------------
# 6. Pacote (zona congelada do Mongo) — custo persiste e pacote antigo não quebra
# ---------------------------------------------------------------------------
def test_pack_roundtrip_preserva_custo():
    cam = [{"data_compra": pd.Timestamp("2025-01-01"), "qtd": 3.0, "custo": 12.34}]
    doc = emp._cam_to_doc(cam)
    assert doc[0]["custo"] == pytest.approx(12.34)
    volta = emp._cam_from_doc(doc)
    assert volta[0]["custo"] == pytest.approx(12.34)
    assert volta[0]["qtd"] == pytest.approx(3.0)


def test_pack_antigo_sem_campo_custo_continua_legivel():
    """Pacotes gravados antes da correção não têm `custo`: viram None, não erro."""
    doc_v1 = [{"data_compra": "2025-01-01", "qtd": 3.0}]
    volta = emp._cam_from_doc(doc_v1)
    assert volta[0]["custo"] is None

    _, df_long, _ = emp.fifo_por_camadas(_ent([]), _sai([]), _saldo(3),
                                         packs={"P": volta}, hoje=HOJE)
    res = emp.custo_fifo_por_produto(df_long)
    assert res.empty or pd.isna(res.iloc[0]["CUSTO_FIFO"])


def test_pack_semeia_camada_com_custo():
    """Camada vinda do pacote entra no FIFO com seu custo e é consumida primeiro."""
    packs = {"P": [{"data_compra": pd.Timestamp("2024-01-01"), "qtd": 2.0, "custo": 99.0}]}
    ent = _ent([("2025-06-01", 3, 11.0)])

    custo, linha = _custo(ent, _sai([]), saldo=5, packs=packs)
    assert custo == pytest.approx((2 * 99.0 + 3 * 11.0) / 5)

    # consumindo 2 unidades, a camada do pacote some e sobra só a de 11,00
    custo2, _ = _custo(ent, _sai([("2025-07-01", 2)]), saldo=3, packs=packs)
    assert custo2 == pytest.approx(11.0)


def test_entradas_sinteticas_carregam_preco_custo():
    docs = {"P": {"camadas": [{"data_compra": "2025-01-01", "qtd": 2.0, "custo": 7.5}]}}
    df = emp.to_entradas_sinteticas(docs)
    assert "PRECO_CUSTO" in df.columns
    assert df.iloc[0]["PRECO_CUSTO"] == pytest.approx(7.5)


def test_processar_fifo_devolve_custo_na_camada():
    eventos = [{"tipo": "E", "data": pd.Timestamp("2025-01-01"), "qtd": 5.0, "custo": 3.0},
               {"tipo": "S", "data": pd.Timestamp("2025-02-01"), "qtd": 2.0}]
    _, cam = emp.processar_fifo([], eventos)
    assert len(cam) == 1
    assert cam[0]["custo"] == pytest.approx(3.0)
    assert cam[0]["qtd"] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# 7. Regressão: a correção não pode mexer no que o motor já produzia
# ---------------------------------------------------------------------------
def test_data_compra_por_venda_permanece_a_media_ponderada():
    """DATA_COMPRA (base do tempo médio em estoque) não muda com a adição do custo."""
    ent = _ent([("2025-01-01", 2, 10.0), ("2025-01-11", 2, 20.0)])
    sai = _sai([("2025-02-01", 4)])

    data_compra, _, _ = emp.fifo_por_camadas(ent, sai, _saldo(0), hoje=HOJE)
    # 2 un de 01/01 + 2 un de 11/01 -> média ponderada = 06/01
    assert pd.Timestamp(data_compra.iloc[0]).normalize() == pd.Timestamp("2025-01-06")


def test_entradas_sem_coluna_preco_custo_nao_quebram():
    """Chamador legado que não passa PRECO_CUSTO continua funcionando."""
    ent = pd.DataFrame([{"PRO_CODIGO": "P", "DATA": pd.Timestamp("2025-01-01"),
                         "QUANTIDADE": 5.0, "LANCTO": 1}])
    _, df_long, _ = emp.fifo_por_camadas(ent, _sai([]), _saldo(5), hoje=HOJE)
    assert len(df_long) == 1
    assert pd.isna(df_long.iloc[0]["CUSTO_CAMADA"])


# ---------------------------------------------------------------------------
# 8. Cadeia de fallback do custo_unitario (main.montar_custo_e_margem)
# ---------------------------------------------------------------------------
def _met(rows):
    return pd.DataFrame(rows)


def _fifo(rows):
    return pd.DataFrame(rows, columns=["PRO_CODIGO", "CUSTO_FIFO"])


def test_cadeia_de_fallback_escolhe_o_degrau_certo():
    """
    A: tem camada viva            -> camada_viva
    B: sem camada, tem entrada    -> ultima_entrada
    C: sem camada e sem entrada   -> cadastro
    D: nada                       -> NULL (cai no nível de serviço da curva)
    """
    import main

    met = _met([
        {"PRO_CODIGO": "A", "CUSTO_CADASTRO": 999.0, "PRECO_UNIT_12M": 100.0, "CUSTO_COGS_12M": 80.0},
        {"PRO_CODIGO": "B", "CUSTO_CADASTRO": 999.0, "PRECO_UNIT_12M": 100.0, "CUSTO_COGS_12M": 80.0},
        {"PRO_CODIGO": "C", "CUSTO_CADASTRO": 55.0, "PRECO_UNIT_12M": 100.0, "CUSTO_COGS_12M": 80.0},
        {"PRO_CODIGO": "D", "CUSTO_CADASTRO": None, "PRECO_UNIT_12M": 100.0, "CUSTO_COGS_12M": 80.0},
    ])
    fifo = _fifo([{"PRO_CODIGO": "A", "CUSTO_FIFO": 10.0}])
    ent = pd.DataFrame([
        {"PRO_CODIGO": "B", "DATA": pd.Timestamp("2024-01-01"), "PRECO_CUSTO": 30.0},
        {"PRO_CODIGO": "B", "DATA": pd.Timestamp("2026-01-01"), "PRECO_CUSTO": 40.0},
    ])

    out = main.montar_custo_e_margem(met, fifo, ent).set_index("PRO_CODIGO")

    assert out.loc["A", "CUSTO_UNIT"] == pytest.approx(10.0)
    assert out.loc["A", "CUSTO_FONTE"] == "camada_viva"
    # a camada viva ganha do cadastro, mesmo com cadastro preenchido
    assert out.loc["B", "CUSTO_UNIT"] == pytest.approx(40.0)   # a entrada MAIS RECENTE
    assert out.loc["B", "CUSTO_FONTE"] == "ultima_entrada"
    assert out.loc["C", "CUSTO_UNIT"] == pytest.approx(55.0)
    assert out.loc["C", "CUSTO_FONTE"] == "cadastro"
    assert pd.isna(out.loc["D", "CUSTO_UNIT"])
    assert out.loc["D", "CUSTO_FONTE"] is None


def test_margem_fica_prospectiva_e_preserva_a_realizada():
    """Margem passa a ser contra o custo do estoque; a base antiga fica separada."""
    import main

    met = _met([{"PRO_CODIGO": "47709", "CUSTO_CADASTRO": None,
                 "PRECO_UNIT_12M": 16.90, "CUSTO_COGS_12M": 441.23}])
    out = main.montar_custo_e_margem(met, _fifo([{"PRO_CODIGO": "47709", "CUSTO_FIFO": 10.37}]),
                                     None).iloc[0]

    assert out["CUSTO_UNIT"] == pytest.approx(10.37)
    assert out["MARGEM_UNIT"] == pytest.approx(16.90 - 10.37)     # prospectiva: POSITIVA
    assert out["MARGEM_PCT"] == pytest.approx((16.90 - 10.37) / 16.90)
    assert out["MARGEM_UNIT_REALIZADA"] == pytest.approx(16.90 - 441.23)  # antiga: negativa


def test_sem_venda_na_janela_margem_fica_zero_nao_menos_custo():
    """Contrato preservado: item sem venda tinha margem 0 e continua com 0."""
    import main

    met = _met([{"PRO_CODIGO": "X", "CUSTO_CADASTRO": 50.0,
                 "PRECO_UNIT_12M": 0.0, "CUSTO_COGS_12M": None}])
    out = main.montar_custo_e_margem(met, None, None).iloc[0]

    assert out["CUSTO_UNIT"] == pytest.approx(50.0)
    assert out["MARGEM_UNIT"] == 0.0
    assert out["MARGEM_PCT"] == 0.0


def test_custo_fifo_zero_ou_negativo_nao_e_aceito():
    """Custo <= 0 não vale como custo: desce um degrau na cadeia."""
    import main

    met = _met([{"PRO_CODIGO": "A", "CUSTO_CADASTRO": 33.0,
                 "PRECO_UNIT_12M": 100.0, "CUSTO_COGS_12M": 80.0}])
    out = main.montar_custo_e_margem(met, _fifo([{"PRO_CODIGO": "A", "CUSTO_FIFO": 0.0}]),
                                     None).iloc[0]
    assert out["CUSTO_UNIT"] == pytest.approx(33.0)
    assert out["CUSTO_FONTE"] == "cadastro"
