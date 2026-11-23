# -*- coding: utf-8 -*-
"""Análise de compra de uma cotação: GET /analise/cotacao/{numero}.

Lê a cotação e os itens da intranet (com_cotacao / com_cotacao_itens), o que já está a
caminho nos pedidos em aberto de outras cotações e o saldo de agora. Sem preço ainda: o
valor de cada item é estimado pelo custo unitário da última rodada (analise_cotacao.py).
Sem fluxo a pagar e sem não comprados do fornecedor. Cotação com pedido já entregue não tem
situação de hoje (está na análise da nota): só o histórico. Cada abertura grava a foto da análise
(se mudou) e quem viu (foto_compra.responder); `?foto=` devolve uma versão guardada.
"""
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text

import analise_cotacao as ac
import analise_foto as af
import analise_pedido as ap
import foto_compra as fc
from erp_api import get_realtime_stocks
from infra_db import get_db_connection
from rotas_nota import CUSTO_CAIXA_DIAS_TETO, CUSTO_CAIXA_TAXA_MES, _cadastro, _exigir_tela, _rodada
from rotas_pedido import _a_caminho

router = APIRouter()


def _cotacao(numero):
    """(cabeçalho, itens) da cotação pelo número (pedido_cotacao). Inexistente -> 404.
    O cabeçalho leva `entregue`: algum pedido da cotação com status 'Entregue...'."""
    with get_db_connection() as pg:
        cab = pg.execute(text("SELECT pedido_cotacao, created_at FROM com_cotacao WHERE pedido_cotacao = :n"),
                         {"n": numero}).mappings().first()
        if not cab:
            raise HTTPException(status_code=404, detail="Cotação não encontrada.")
        itens = [dict(r) for r in pg.execute(text(
            "SELECT pro_codigo, pro_descricao, quantidade FROM com_cotacao_itens WHERE pedido_cotacao = :n"),
            {"n": numero}).mappings()]
        entregue = pg.execute(text("SELECT EXISTS (SELECT 1 FROM com_pedido WHERE pedido_cotacao = :n "
                                   "AND status LIKE 'Entregue%')"), {"n": numero}).scalar()
    return {**cab, "entregue": bool(entregue)}, itens


def _produtos(linhas):
    return {str(i["pro_codigo"]).strip() for i in linhas}


def calcular(numero):
    """Cálculo da cotação no formato comum das análises (conteudo, sem_hoje, assinatura, criado_em,
    rodada, produtos, numero). Pedido da cotação entregue: sem conteúdo e sem leitura do ERP."""
    cab, linhas = _cotacao(numero)
    pros = _produtos(linhas)
    calc = {"conteudo": None, "sem_hoje": None, "assinatura": af.assinatura_cotacao(linhas),
            "criado_em": cab["created_at"], "rodada": None, "produtos": pros, "numero": cab["pedido_cotacao"]}
    if cab["entregue"]:
        return {**calc, "sem_hoje": fc.SEM_HOJE}
    cadastro = _cadastro(pros)
    rodada, data_rodada = _rodada(pros)
    itens = ac.valorar_itens_cotacao(linhas, rodada)
    membros = sorted(set(rodada) | pros)
    a_caminho = ap.somar_a_caminho(_a_caminho(membros), cotacao=numero)
    try:
        saldos = get_realtime_stocks(membros)
    except Exception as e:
        print(f"Erro ao ler o saldo dos produtos da cotação: {e}")
        raise HTTPException(status_code=502, detail="Não foi possível ler o saldo de estoque do ERP agora.")

    analise = ap.montar_analise_pedido(itens, cadastro, rodada, saldos, a_caminho,
                                       CUSTO_CAIXA_TAXA_MES, CUSTO_CAIXA_DIAS_TETO)
    conteudo = {
        "cotacao": {
            "numero": cab["pedido_cotacao"], "criada_em": cab["created_at"],
            "valor_total": analise.pop("valor_total"), "rodada": data_rodada, "avisos": [ac.AVISO_ESTIMADO],
        },
        **analise,
    }
    return {**calc, "conteudo": conteudo, "rodada": data_rodada}


@router.get("/analise/cotacao/{numero}",
            dependencies=[Depends(_exigir_tela("/compras/cotacoes", "Sem permissão para ver cotações."))])
def analise_cotacao(numero: int, foto: int | None = None, x_user_id: str | None = Header(None)):
    """Análise de compra da cotação (unidades com estoque, a caminho, cobertura e excesso, custo de
    caixa sem prazo e não analisados, com valores estimados) mais o histórico das fotos. Com `foto`,
    devolve só aquela versão guardada."""
    if foto is not None:
        return fc.responder_foto("cotacao", str(numero), foto, _produtos(_cotacao(numero)[1]))
    return fc.responder("cotacao", str(numero), calcular(numero), x_user_id)
