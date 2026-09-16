# -*- coding: utf-8 -*-
"""Regra da lista de promoção por valor parado: ação sugerida, público e preço por tabela.

Função pura (sem banco) — a rota só alimenta com as linhas do com_fifo_completo e a
régua v3 do atacado (ven_regua_atacado). Regras (spec docs/promocao-valor-parado.md):

  ação (precedência): revisar_cadastro (sem custo) → giro_caixa (saldo VELHO — idade média
  FIFO > 240 d — e sem venda em 12 m ou cobertura > 24 m) → vender_sem_repor (curva A/B com
  cobertura > 12 m, ou saldo recente com cobertura > 24 m: compra grande que ainda vai
  girar) → promocao (degrau pela cobertura).

  A idade do saldo manda porque a promoção do ERP é POR ITEM, não por quantidade: um
  desconto de liquidação atinge também as unidades que entraram semana passada e
  venderiam a preço cheio. Item que nunca vendeu mas tem saldo recente começa em
  promoção (30 %) e só vira liquidação quando o saldo envelhece — a escada no tempo
  acontece sozinha, run a run.

  varejo   (tabela 1): desconto por degrau — promoção 15/20/30 %, liquidação 40/50 % —
                       piso custo × 1,30; nunca abaixo do promocional do atacado.
  atacado  (tabela 2): promoção = tabela 2 − desconto máximo da faixa, piso custo ×
                       markup × (1 − desc. máx.) — o piso da régua; liquidação = 15/25 %
                       abaixo da tabela 2, piso custo × 1,30 (1,25 da bolsa + 0,05 de
                       bônus do vendedor). Piso acima da tabela = sem promoção nessa tabela.

Nenhum rótulo aqui cita plano futuro: o operacional vê só ações operacionais.
"""
import os

CORTES = [10.01, 38.50, 69.61, 124.37, 249.86, 299.87, 395.27, 496.36, 696.37, 996.38]
FAIXAS = ["1A", "1B", "1C", "1D", "2A", "2B", "2C", "3A", "3B", "3C", "3D"]
# Seed da régua v3 (ago/2026) — usado só se ven_regua_atacado não estiver acessível.
REGUA_PADRAO = {}
for _f, _g, _p, _d in zip(FAIXAS,
                          [2.85, 2.30, 1.95, 1.85, 1.70, 1.62, 1.56, 1.51, 1.47, 1.44, 1.42],
                          [2.30, 2.10, 1.90, 1.75, 1.60, 1.50, 1.44, 1.42, 1.41, 1.39, 1.38],
                          [.03, .03, .03, .03, .05, .06, .07, .08, .08, .09, .10]):
    REGUA_PADRAO[("GERAL", _f)] = (_g, _d)
    REGUA_PADRAO[("PB", _f)] = (_p, _d)

PISO_VAREJO = float(os.getenv("PROMO_PISO_VAREJO", "1.30"))
PISO_ATACADO_LIQ = float(os.getenv("PROMO_PISO_ATACADO_LIQ", "1.30"))
BONUS_LIQ_PCT = float(os.getenv("PROMO_BONUS_LIQ_PCT", "0.05"))     # do custo, por unidade
VALIDADE_DIAS = int(os.getenv("PROMO_VALIDADE_DIAS", "30"))
ESCADA_VAREJO = {"promocao_6": .15, "promocao_12": .20, "promocao_24": .30, "cobertura_alta": .40, "sem_venda_12m": .50}
ESCADA_ATACADO_LIQ = {"cobertura_alta": .15, "sem_venda_12m": .25}
SGR_PARABRISA = 154
IDADE_LIQUIDACAO_DIAS = int(os.getenv("PROMO_IDADE_LIQUIDACAO_DIAS", "240"))  # = faixa "Obsoleto" da análise

ACOES = ("revisar_cadastro", "giro_caixa", "vender_sem_repor", "promocao")
PUBLICOS = ("atacado", "varejo", "ambos")


def _num(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v == v else None  # NaN → None


def faixa(custo):
    c = _num(custo) or 0.0
    for i, corte in enumerate(CORTES):
        if c <= corte:
            return FAIXAS[i]
    return FAIXAS[-1]


def classe(sgr_codigo):
    return "PB" if sgr_codigo == SGR_PARABRISA else "GERAL"


def acao_sugerida(custo, dias_sem_venda, cob_meses, curva, idade_saldo_dias=None):
    """Devolve (acao, subtipo). subtipo: sem_venda_12m | cobertura_alta | promocao_6/12/24 | None.

    idade_saldo_dias = idade média FIFO do saldo atual (tempo_medio_saldo_atual). Sem o dado,
    assume saldo velho (comportamento conservador só na ausência da informação)."""
    c = _num(custo)
    if c is None or c <= 0:
        return "revisar_cadastro", None
    sem_venda = dias_sem_venda is None or dias_sem_venda > 365
    cob = _num(cob_meses)
    idade = _num(idade_saldo_dias)
    saldo_velho = idade is None or idade > IDADE_LIQUIDACAO_DIAS
    ab = (curva or "").upper() in ("A", "B")
    if sem_venda:
        return ("giro_caixa", "sem_venda_12m") if saldo_velho else ("promocao", "promocao_24")
    if cob is not None and cob > 24:
        if saldo_velho:
            return "giro_caixa", "cobertura_alta"
        # saldo recente com cobertura alta = compra grande recente: não desconta, não repõe
        return "vender_sem_repor", None
    if ab and cob is not None and cob > 12:
        return "vender_sem_repor", None
    if cob is None or cob <= 6:
        return "promocao", "promocao_6"
    if cob <= 12:
        return "promocao", "promocao_12"
    return "promocao", "promocao_24"


def publico(qtd_varejo_12m, qtd_atacado_12m):
    v = _num(qtd_varejo_12m) or 0.0
    a = _num(qtd_atacado_12m) or 0.0
    if v + a <= 0:
        return "ambos"
    share = a / (v + a)
    if share >= 0.8:
        return "atacado"
    if share <= 0.2:
        return "varejo"
    return "ambos"


def precos(acao, subtipo, custo, preco1, preco2, sgr_codigo, fora_regua=False, regua=None):
    """Preço promocional nas duas tabelas. None = sem promoção naquela tabela (ver motivo)."""
    regua = regua or REGUA_PADRAO
    c = _num(custo)
    p1 = _num(preco1)
    p2 = _num(preco2)
    out = {"preco_varejo": None, "preco_atacado": None, "desc_varejo_pct": None, "desc_atacado_pct": None,
           "motivo_varejo": None, "motivo_atacado": None, "bonus_liquidacao_unit": None,
           "piso_varejo": None, "piso_atacado": None}   # pisos em R$ (a tela mostra "no piso" quando travou)

    if acao == "revisar_cadastro":
        out["motivo_varejo"] = out["motivo_atacado"] = "sem_custo"
        return out
    if acao == "vender_sem_repor":
        out.update(preco_varejo=p1, preco_atacado=p2, desc_varejo_pct=0.0, desc_atacado_pct=0.0)
        if not p1: out["motivo_varejo"] = "sem_tabela"
        if not p2: out["motivo_atacado"] = "sem_tabela"
        return out

    # ---- atacado (tabela 2) ----
    if not p2 or p2 <= 0:
        out["motivo_atacado"] = "sem_tabela"
    elif fora_regua and not (acao == "giro_caixa" and subtipo == "sem_venda_12m"):
        out["motivo_atacado"] = "fora_regua"
    else:
        if acao == "giro_caixa":
            d = ESCADA_ATACADO_LIQ[subtipo]
            piso = c * PISO_ATACADO_LIQ
        else:
            mk, dmax = regua.get((classe(sgr_codigo), faixa(c)), REGUA_PADRAO[(classe(sgr_codigo), faixa(c))])
            d = float(dmax)
            piso = c * float(mk) * (1 - d)
        out["piso_atacado"] = round(piso, 2)
        if piso >= p2:
            out["motivo_atacado"] = "tabela_abaixo_regua"
        else:
            pa = round(max(p2 * (1 - d), piso), 2)
            out.update(preco_atacado=pa, desc_atacado_pct=round((1 - pa / p2) * 100, 1))
            if acao == "giro_caixa":
                out["bonus_liquidacao_unit"] = round(c * BONUS_LIQ_PCT, 2)

    # ---- varejo (tabela 1) ----
    if not p1 or p1 <= 0:
        out["motivo_varejo"] = "sem_tabela"
    else:
        d = ESCADA_VAREJO[subtipo]
        out["piso_varejo"] = round(max(c * PISO_VAREJO, out["preco_atacado"] or 0.0), 2)  # balcão nunca abaixo do atacado
        pv = max(p1 * (1 - d), out["piso_varejo"])
        if pv >= p1:
            out["motivo_varejo"] = "tabela_abaixo_piso"
        else:
            pv = round(pv, 2)
            out.update(preco_varejo=pv, desc_varejo_pct=round((1 - pv / p1) * 100, 1))
    return out


def avaliar(item, regua=None):
    """item: dict com estoque_disponivel, estoque_max_sugerido, custo_unitario, preco_venda_1/2,
    demanda_real_dia, dias_sem_venda, curva_abc, sgr_codigo, qtd_varejo_12m, qtd_atacado_12m,
    fora_regua. Devolve o item enriquecido (novo dict)."""
    r = dict(item)
    saldo = _num(r.get("estoque_disponivel")) or 0.0
    mx = _num(r.get("estoque_max_sugerido")) or 0.0
    custo = _num(r.get("custo_unitario"))
    dem = _num(r.get("demanda_real_dia")) or 0.0
    r["excesso_qtd"] = max(saldo - mx, 0.0)
    r["valor_parado"] = round(saldo * (custo or 0.0), 2)
    r["valor_excesso"] = round(r["excesso_qtd"] * (custo or 0.0), 2)
    r["cobertura_meses"] = round(saldo / (dem * 30.0), 1) if dem > 0 else None
    r["idade_saldo_dias"] = _num(r.get("tempo_medio_saldo_atual"))
    acao, sub = acao_sugerida(custo, r.get("dias_sem_venda"), r["cobertura_meses"], r.get("curva_abc"), r["idade_saldo_dias"])
    r["acao_sugerida"], r["acao_subtipo"] = acao, sub
    r["publico"] = publico(r.get("qtd_varejo_12m"), r.get("qtd_atacado_12m"))
    r.update(precos(acao, sub, custo, r.get("preco_venda_1"), r.get("preco_venda_2"),
                    r.get("sgr_codigo"), bool(r.get("fora_regua")), regua))
    # troca manual do comprador (com_promo_acao) prevalece sobre a sugestão
    manual = r.get("acao_manual")
    r["acao_manual"] = bool(manual)
    r["acao"] = manual if manual in ACOES else acao
    if manual and _num(r.get("preco_varejo_manual")):
        r["preco_varejo"] = _num(r["preco_varejo_manual"]); r["motivo_varejo"] = "manual"
    if manual and _num(r.get("preco_atacado_manual")):
        r["preco_atacado"] = _num(r["preco_atacado_manual"]); r["motivo_atacado"] = "manual"
    # caixa potencial = excesso vendido ao preço do público do item
    pv, pa = r.get("preco_varejo"), r.get("preco_atacado")
    if r["publico"] == "atacado":
        ref = pa
    elif r["publico"] == "varejo":
        ref = pv
    else:
        ref = (pv + pa) / 2 if pv and pa else (pv or pa)
    r["caixa_potencial"] = round(r["excesso_qtd"] * ref, 2) if ref else 0.0
    return r
