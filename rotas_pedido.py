# -*- coding: utf-8 -*-
"""Análise de compra de um pedido de compra: GET /analise/pedido/{id}.

Lê o pedido e os itens da intranet (com_pedido / com_pedido_itens), o nome do fornecedor
no cadastro da EMPRESA 3 (erp-firebird-api, OPENQUERY como plano B), o que já está a
caminho nos outros pedidos em aberto e o saldo de agora. As contas moram em
analise_pedido.py; aqui só se lê e se monta a resposta. Pedido não tem vencimento:
sem fluxo a pagar. Nada é gravado.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

import analise_nota as an
import analise_pedido as ap
from erp_api import _erp_api_consulta, get_realtime_stocks
from infra_db import get_db_connection
from rotas_nota import (CUSTO_CAIXA_DIAS_TETO, CUSTO_CAIXA_TAXA_MES, _api_ou_oq, _cadastro, _exigir_tela,
                        _nao_comprados, _oq, _rodada)
from sugestao_compra import PEDIDO_STATUS_EM_ABERTO, _sql_in_list

router = APIRouter()


def _pedido(id):
    """(cabeçalho, itens) do pedido. Inexistente -> 404; encomenda ou status fora da análise -> 409."""
    with get_db_connection() as pg:
        cab = pg.execute(text("SELECT id, pedido_cotacao, for_codigo, status, COALESCE(origem, 'cotacao') AS origem, "
                              "frete, ipi_no_valor FROM com_pedido WHERE id = :id"), {"id": id}).mappings().first()
        if not cab:
            raise HTTPException(status_code=404, detail="Pedido não encontrado.")
        itens = [dict(r) for r in pg.execute(text(
            "SELECT pro_codigo, pro_descricao, quantidade, valor_unitario, ipi, status_item "
            "FROM com_pedido_itens WHERE pedido_id = :id"), {"id": id}).mappings()]
    cab = dict(cab)
    if cab["origem"] == "encomenda":
        raise HTTPException(status_code=409, detail="Pedido de encomenda não passa pela análise de compra.")
    if cab["status"] not in ap.STATUS_ANALISAVEIS:
        raise HTTPException(status_code=409, detail=f"Pedido com status '{cab['status']}' não pode ser analisado.")
    return cab, itens


def _nome_fornecedor(for_codigo):
    """FOR_NOME do fornecedor no cadastro da EMPRESA 3 (o nome do histórico de compra) ou None."""
    if for_codigo is None:
        return None
    linhas = _api_ou_oq(
        "nome do fornecedor",
        lambda: _erp_api_consulta("fornecedores", {
            "empresa": 3, "campos": ["FOR_NOME"], "limite": 2,
            "filtros": [{"campo": "FOR_CODIGO", "op": "igual", "valor": int(for_codigo)}]}),
        lambda: _oq(f"SELECT for_nome FROM fornecedores WHERE empresa = 3 AND for_codigo = {int(for_codigo)}"))
    return next((str(r["FOR_NOME"]).strip() for r in linhas if r.get("FOR_NOME")), None)


def _a_caminho(membros):
    """Linhas {pedido_id, pro_codigo, qtd} dos pedidos em aberto com os membros, na mesma regra
    da Sugestão de Compra (status em aberto, sem itens 'nao_atendido')."""
    if not membros:
        return []
    with get_db_connection() as pg:
        return [dict(r) for r in pg.execute(text(f"""
            SELECT p.id AS pedido_id, i.pro_codigo::text AS pro_codigo, SUM(i.quantidade) AS qtd
            FROM com_pedido p
            JOIN com_pedido_itens i ON i.pedido_id = p.id
            WHERE p.status IN ({_sql_in_list(PEDIDO_STATUS_EM_ABERTO)})
              AND COALESCE(i.status_item, '') <> 'nao_atendido'
              AND i.pro_codigo::text = ANY(:membros)
            GROUP BY p.id, i.pro_codigo::text
        """), {"membros": membros}).mappings()]


@router.get("/analise/pedido/{id}",
            dependencies=[Depends(_exigir_tela("/compras/cotacao/pedido", "Sem permissão para ver pedidos de compra."))])
def analise_pedido(id: str):
    """Análise de compra do pedido: unidades (grupo/produto) com estoque, a caminho, cobertura e
    excesso, custo de caixa sem prazo, não analisados e não comprados do fornecedor. Só leitura."""
    cab, linhas = _pedido(id.strip())
    itens = ap.valorar_itens(linhas, cab["frete"], cab["ipi_no_valor"])
    pros = {i["pro_codigo"] for i in itens}
    cadastro = _cadastro(pros)
    rodada, data_rodada = _rodada(pros)
    membros = sorted(set(rodada) | pros)
    a_caminho = ap.somar_a_caminho(_a_caminho(membros), cab["id"])
    try:
        saldos = get_realtime_stocks(membros)
    except Exception as e:
        print(f"Erro ao ler o saldo dos produtos do pedido: {e}")
        raise HTTPException(status_code=502, detail="Não foi possível ler o saldo de estoque do ERP agora.")

    analise = ap.montar_analise_pedido(itens, cadastro, rodada, saldos, a_caminho,
                                       CUSTO_CAIXA_TAXA_MES, CUSTO_CAIXA_DIAS_TETO)
    nome = _nome_fornecedor(cab["for_codigo"])
    # Fornecedor informado sem nome no cadastro = ERP fora: a lista não pôde ser montada.
    nao_comprados = (_nao_comprados([nome] if nome else [], membros) if nome or cab["for_codigo"] is None else
                     an.montar_nao_comprados({}, membros, None, "Não foi possível montar esta lista agora."))
    return {
        "pedido": {
            "id": cab["id"], "numero": cab["pedido_cotacao"], "for_codigo": cab["for_codigo"],
            "fornecedor": nome, "status": cab["status"], "valor_total": analise.pop("valor_total"),
            "rodada": data_rodada, "avisos": [ap.AVISO_SEM_ST],
        },
        **analise,
        "nao_comprados": nao_comprados,
    }
