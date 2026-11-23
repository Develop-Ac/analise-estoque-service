# -*- coding: utf-8 -*-
"""Análise de compra por cotação — cálculo puro, sem banco.

A cotação ainda não tem preço: cada item vale quantidade × custo unitário da última rodada
(com_fifo_completo). As demais contas (separação, a caminho, custo de caixa sem prazo e
resumo) são as do pedido, em analise_pedido.montar_analise_pedido.
"""
from analise_pedido import _n

AVISO_ESTIMADO = "valores estimados pelo custo médio: a cotação ainda não tem preço"
AVISO_SEM_CUSTO = "sem custo na rodada"


def valorar_itens_cotacao(itens, rodada):
    """[{pro_codigo, descricao, quantidade, desembolso, aviso?}] dos itens da cotação.
    `itens` = linhas de com_cotacao_itens; produto sem custo na rodada: valor 0 e aviso."""
    saida = []
    for it in itens:
        pro = str(it["pro_codigo"]).strip()
        qtd = _n(it.get("quantidade"))
        custo = (rodada.get(pro) or {}).get("custo_unitario")
        item = {"pro_codigo": pro, "descricao": it.get("pro_descricao"), "quantidade": qtd,
                "desembolso": round(qtd * _n(custo), 2)}
        if not custo:
            item["aviso"] = AVISO_SEM_CUSTO
        saida.append(item)
    return saida
