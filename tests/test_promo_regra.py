# -*- coding: utf-8 -*-
"""Regra da lista de promoção (promo_regra.py) — ação por precedência, público e pisos."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import promo_regra as pr  # noqa: E402


def _item(**kw):
    base = dict(estoque_disponivel=30, estoque_max_sugerido=10, custo_unitario=100.0, preco_venda_1=270.0,
                preco_venda_2=160.0, demanda_real_dia=1 / 30.0, dias_sem_venda=20, curva_abc="C",
                sgr_codigo=1, qtd_varejo_12m=10, qtd_atacado_12m=0, fora_regua=False, tempo_medio_saldo_atual=300)
    base.update(kw)
    return base


def test_acao_precedencia():
    V, N = 300, 90  # idade média do saldo: velho (> 240 d) / novo
    assert pr.acao_sugerida(None, 10, 3, "A", V)[0] == "revisar_cadastro"
    assert pr.acao_sugerida(100, None, None, "A", V) == ("giro_caixa", "sem_venda_12m")
    assert pr.acao_sugerida(100, 400, 3, "A", V) == ("giro_caixa", "sem_venda_12m")
    assert pr.acao_sugerida(100, 10, 30, "A", V) == ("giro_caixa", "cobertura_alta")
    assert pr.acao_sugerida(100, 10, 18, "B", V) == ("vender_sem_repor", None)
    assert pr.acao_sugerida(100, 10, 18, "C", V) == ("promocao", "promocao_24")
    assert pr.acao_sugerida(100, 10, 8, "A", V) == ("promocao", "promocao_12")
    assert pr.acao_sugerida(100, 10, 2, "D", V) == ("promocao", "promocao_6")
    # sem idade informada = trata como velho (conservador)
    assert pr.acao_sugerida(100, 10, 30, "A") == ("giro_caixa", "cobertura_alta")


def test_saldo_recente_nao_liquida():
    """Compra grande recente (36 un há 65 d) com cobertura de 25 meses: não é liquidação —
    a promoção do ERP é por item e pegaria as unidades novas. Vira vender sem repor."""
    N = 94
    assert pr.acao_sugerida(482, 49, 24.9, "A", N) == ("vender_sem_repor", None)
    assert pr.acao_sugerida(482, 49, 24.9, "D", N) == ("vender_sem_repor", None)
    # nunca vendeu, mas saldo recente: começa em promoção 30 %; vira liquidação quando envelhecer
    assert pr.acao_sugerida(100, None, None, "D", N) == ("promocao", "promocao_24")
    assert pr.acao_sugerida(100, None, None, "D", 241) == ("giro_caixa", "sem_venda_12m")


def test_publico():
    assert pr.publico(0, 0) == "ambos"
    assert pr.publico(1, 9) == "atacado"
    assert pr.publico(9, 1) == "varejo"
    assert pr.publico(5, 5) == "ambos"


def test_promocao_precos_e_escada():
    r = pr.avaliar(_item(demanda_real_dia=1.0))  # cobertura 1 mês → 15 %
    assert r["acao"] == "promocao" and r["publico"] == "varejo"
    assert r["preco_varejo"] == 229.5 and r["desc_varejo_pct"] == 15.0
    # atacado: faixa 1D (custo 100) GERAL markup 1,85 desc 3 % → piso 179,45 > tabela 160 → sem promoção
    assert r["preco_atacado"] is None and r["motivo_atacado"] == "tabela_abaixo_regua"
    assert r["valor_parado"] == 3000.0 and r["excesso_qtd"] == 20 and r["valor_excesso"] == 2000.0


def test_liquidacao_pisos_e_bonus():
    r = pr.avaliar(_item(dias_sem_venda=None, demanda_real_dia=0, qtd_varejo_12m=0, preco_venda_1=200.0))
    assert r["acao"] == "giro_caixa" and r["acao_subtipo"] == "sem_venda_12m" and r["publico"] == "ambos"
    # atacado: 25 % abaixo de 160 = 120 < piso 130 → piso manda; bônus 5 % do custo
    assert r["preco_atacado"] == 130.0 and r["bonus_liquidacao_unit"] == 5.0
    # varejo: 50 % de 200 = 100 < piso 130 e < atacado 130 → 130 (nunca abaixo do atacado)
    assert r["preco_varejo"] == 130.0 and r["desc_varejo_pct"] == 35.0
    assert r["caixa_potencial"] == 20 * 130.0


def test_varejo_nunca_abaixo_do_atacado_e_piso_acima_da_tabela():
    r = pr.avaliar(_item(dias_sem_venda=None, demanda_real_dia=0, preco_venda_1=125.0, preco_venda_2=140.0))
    assert r["preco_atacado"] == 130.0                      # 140 × 0,75 = 105 < piso 130
    assert r["preco_varejo"] is None and r["motivo_varejo"] == "tabela_abaixo_piso"  # piso 130 ≥ tabela 125


def test_vender_sem_repor_sem_desconto_e_fora_regua():
    r = pr.avaliar(_item(curva_abc="A", demanda_real_dia=1 / 30.0, estoque_disponivel=20))  # cobertura 20 m
    assert r["acao"] == "vender_sem_repor" and r["preco_varejo"] == 270.0 and r["preco_atacado"] == 160.0
    r2 = pr.avaliar(_item(fora_regua=True, demanda_real_dia=1.0))
    assert r2["acao"] == "promocao" and r2["motivo_atacado"] == "fora_regua"
    r3 = pr.avaliar(_item(fora_regua=True, dias_sem_venda=None, demanda_real_dia=0))
    assert r3["preco_atacado"] == 130.0                     # fora da régua entra só na liquidação sem venda


def test_troca_manual_prevalece():
    r = pr.avaliar(_item(demanda_real_dia=1.0, acao_manual="giro_caixa", preco_varejo_manual=150.0))
    assert r["acao_sugerida"] == "promocao" and r["acao"] == "giro_caixa" and r["acao_manual"] is True
    assert r["preco_varejo"] == 150.0 and r["motivo_varejo"] == "manual"


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("ok", n)
