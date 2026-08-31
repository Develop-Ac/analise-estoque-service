# -*- coding: utf-8 -*-
"""Rotas de catalogo: subgrupos, marcas, fornecedores, categorias."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from infra_db import get_db_connection
from fornecedor_info import _expandir_fornecedores, get_compras_historico, get_fornecedor_principal_map, _sug_norm
from sugestao_compra import _carregar_itens_sugestao, _codigos_candidatos
from config import FORN_EXPR

router = APIRouter()

@router.get("/subgroups")
def listar_subgrupos(
    curva: Optional[str] = None,
    fornecedor: Optional[str] = None,
):
    """
    Retorna lista de subgrupos disponiveis na analise atual.

    Se `curva` e/ou `fornecedor` forem informados, devolve APENAS os subgrupos
    dos produtos que passam nesses filtros — usa a mesma lógica de candidatos da
    sugestão de compra (`_codigos_candidatos`), para o filtro dependente na tela
    (compras/sugestao) ficar coerente com o resultado.
    """
    try:
        # Sem filtros dependentes → lista completa, direto do banco (rápido).
        if not (curva or fornecedor):
            conn = get_db_connection()
            try:
                sql = text("SELECT DISTINCT sgr_descricao FROM com_fifo_completo WHERE sgr_descricao IS NOT NULL ORDER BY sgr_descricao")
                rows = conn.execute(sql).fetchall()
                return [row[0] for row in rows if row[0]]
            finally:
                conn.close()

        # Com filtros → mesma base/lógica da sugestão, sem filtrar por subgrupo.
        conn = get_db_connection()
        try:
            items = _carregar_itens_sugestao(conn)
        finally:
            conn.close()

        historico = {}
        try:
            historico = get_compras_historico()
        except Exception as e:
            print(f"AVISO: histórico indisponível em /subgroups (usando vazio). {e}")

        codes = _codigos_candidatos(items, historico, curva, None, fornecedor, consolidar_grupo=True)
        subs = set()
        for it in items:
            if _sug_norm(it.get("pro_codigo")) in codes:
                sgr = it.get("sgr_descricao")
                if sgr:
                    subs.add(sgr)
        return sorted(subs)
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

@router.get("/brands")
def listar_marcas():
    """
    Retorna lista de marcas disponiveis na analise atual.
    """
    try:
        conn = get_db_connection()
        sql = text("SELECT DISTINCT mar_descricao FROM com_fifo_completo WHERE mar_descricao IS NOT NULL ORDER BY mar_descricao")
        rows = conn.execute(sql).fetchall()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals(): conn.close()

@router.get("/fornecedores")
def listar_fornecedores():
    """
    Fornecedores da análise atual, para o filtro do Painel de Estoque e do
    drill-down: principal do HISTÓRICO de compra, fallback fornecedor 1 do
    cadastro (mesma expressão FORN_EXPR usada nos filtros e nos cards Metabase).
    """
    try:
        conn = get_db_connection()
        sql = text(
            f"SELECT DISTINCT {FORN_EXPR} AS forn FROM com_fifo_completo "
            f"WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo) "
            f"AND {FORN_EXPR} IS NOT NULL ORDER BY 1"
        )
        rows = conn.execute(sql).fetchall()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals(): conn.close()

@router.get("/fornecedores/expandir")
def expandir_fornecedores_endpoint(nomes: str = ""):
    """
    Expande nomes de fornecedores ('||'-separados) incluindo os RELACIONADOS
    (grupo matriz/filiais do compras). Retorna nomes em UPPER — o painel usa
    o resultado para filtrar os cards do Metabase com o mesmo conjunto do /analise.
    """
    lista = [n for n in (nomes or "").split("||") if n.strip()]
    return _expandir_fornecedores(lista)

@router.get("/categories")
def listar_categorias_estocagem():
    """
    Retorna lista de categorias de estocagem disponiveis na analise atual.
    """
    try:
        conn = get_db_connection()
        sql = text("SELECT DISTINCT categoria_saldo_atual FROM com_fifo_completo WHERE categoria_saldo_atual IS NOT NULL ORDER BY categoria_saldo_atual")
        rows = conn.execute(sql).fetchall()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals(): conn.close()

