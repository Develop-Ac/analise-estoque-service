# -*- coding: utf-8 -*-
"""Análise de compra por pedido — cálculo puro, sem banco.

Valora os itens do pedido de compra (quantidade × valor unitário, IPI e frete rateado),
separa o que não é revenda e reaproveita as contas da análise da nota (unidades, custo de
caixa e resumo). Pedido não tem vencimento: custo de caixa sem prazo e sem fluxo.
"""
import analise_nota as an

# Status do pedido que podem ser analisados (Entregue, Cancelado e afins ficam de fora).
STATUS_ANALISAVEIS = ('Aguardando analise', 'Em analise', 'Liberado', 'Vínculo sugerido', 'Faturado',
                      'Faturado parcialmente', 'Em Trânsito', 'Em Trânsito parcialmente')
AVISO_SEM_ST = "valores sem ST"
AVISO_SEM_VALOR = "sem valor unitário no pedido"


def _n(v):
    return float(v) if v is not None else 0.0


def valorar_itens(itens, frete, ipi_no_valor):
    """Desembolso de cada item do pedido, fechando com Σ itens + frete.

    `itens` = linhas de com_pedido_itens; item `nao_atendido` fica fora. Base = qtd × valor
    unitário, mais o IPI (percentual) quando o valor unitário não o inclui (`ipi_no_valor`).
    O frete do pedido é rateado pela base de cada item. Sem valor unitário: valor 0 e aviso.
    Devolve [{pro_codigo, descricao, quantidade, desembolso, aviso?}].
    """
    saida = []
    for it in itens:
        if it.get("status_item") == "nao_atendido":
            continue
        qtd = _n(it.get("quantidade"))
        item = {"pro_codigo": str(it["pro_codigo"]).strip(), "descricao": it.get("pro_descricao"),
                "quantidade": qtd, "desembolso": 0.0}
        if it.get("valor_unitario") is None:
            item["aviso"] = AVISO_SEM_VALOR
        else:
            base = qtd * _n(it["valor_unitario"])
            ipi = _n(it.get("ipi"))
            if ipi > 0 and not ipi_no_valor:
                base *= 1 + ipi / 100
            item["desembolso"] = base
        saida.append(item)
    return an.ratear_desembolso(saida, sum(i["desembolso"] for i in saida) + _n(frete))


def separar_itens(itens, cadastro):
    """(analisáveis, não analisados). Produto no cadastro e não comercializável vira não analisado
    (imobilizado/uso e consumo); produto fora do cadastro não é barrado."""
    ok, fora = [], []
    for it in itens:
        cad = cadastro.get(it["pro_codigo"]) or {}
        descricao = cad.get("PRO_DESCRICAO") or it.get("descricao")
        if cad and str(cad.get("COMERCIALIZAVEL") or "").strip() != "S":
            fora.append({"pro_codigo": it["pro_codigo"], "descricao": descricao,
                         "valor": it["desembolso"], "motivo": an.MOTIVO_IMOBILIZADO})
        else:
            ok.append({**it, "descricao": descricao})
    return ok, fora


def somar_a_caminho(linhas, pedido_id):
    """{pro_codigo: qtd} dos pedidos em aberto, sem as linhas do próprio pedido."""
    soma = {}
    for ln in linhas:
        if ln["pedido_id"] == pedido_id:
            continue
        pro = str(ln["pro_codigo"]).strip()
        soma[pro] = soma.get(pro, 0.0) + _n(ln["qtd"])
    return soma


def montar_analise_pedido(itens, cadastro, rodada, saldos, a_caminho, taxa_mes, dias_teto):
    """Encadeia separação → unidades (com a caminho) → custo de caixa sem prazo → resumo.
    `itens` = saída de valorar_itens."""
    ok, nao_analisados = separar_itens(itens, cadastro)
    unidades = an.montar_unidades(ok, rodada, saldos, False, a_caminho)
    custo = an.aplicar_custo_caixa(unidades, 0, taxa_mes, dias_teto)
    custo.pop("prazo_medio_dias")
    custo["base_prazo"] = "sem_prazo"
    valor_pedido = round(sum(i["desembolso"] for i in itens), 2)
    return {
        "valor_total": valor_pedido,
        "resumo": an.montar_resumo(valor_pedido, unidades, nao_analisados),
        "unidades": unidades,
        "nao_analisados": nao_analisados,
        "custo_caixa": custo,
    }
