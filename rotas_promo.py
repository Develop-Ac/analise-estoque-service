# -*- coding: utf-8 -*-
"""Rotas de promocao: /promo/plan (x2, primeira vence) e /promo/export."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from infra_db import get_db_connection
from modelos import AnaliseItem, PromoPlanRequest
from estoque_rt import _get_stock_batches
from config import FORN_EXPR

router = APIRouter()

@router.post("/promo/plan", response_model=List[AnaliseItem])
def planejar_promocao(req: PromoPlanRequest):
    """
    Retorna lista de produtos com estoque excedente para o periodo informado (dias).
    Prioriza itens Obsoletos.
    Considera logica de Grupo se houver.
    Se grouped_view=True, retorna todo o grupo caso algum membro atenda aos filtros e seja excedente.
    """
    try:
        return _get_promotion_data(req)
    except Exception as e:
        print(f"Erro promo plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _desconto_promocao(custo, preco, categoria):
    """% de desconto sugerido para a promoção, sobre o preço 1 (varejo).

    Regra: consome uma fração da margem sobre o preço conforme a urgência de
    desova do saldo (Obsoleto 80% da margem, Lento 60%, demais 40%), arredonda
    para baixo em múltiplos de 5% (comercial) e trava em 60%. Por construção o
    preço final nunca fica abaixo do custo (fração < 100% da margem)."""
    try:
        c = float(custo or 0)
        p = float(preco or 0)
    except (TypeError, ValueError):
        return None
    if c <= 0 or p <= 0 or p <= c:
        return None
    margem_sobre_preco = (p - c) / p
    fator = {"OBSOLETO": 0.8, "LENTO": 0.6}.get((categoria or "").strip().upper(), 0.4)
    pct = margem_sobre_preco * fator * 100.0
    if pct >= 5.0:
        pct = math.floor(pct / 5.0) * 5.0
    else:
        pct = round(pct, 1)
    return min(pct, 60.0)

def _get_promotion_data(req: PromoPlanRequest):
    conn = get_db_connection()
    try:
        # 1. Filtros Basicos
        filters = ["data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)"]
        params = {}
        
        if req.subgroups:
            sgs = [s.strip() for s in req.subgroups if s.strip()]
            if sgs:
                sg_keys = [f"sg_{i}" for i in range(len(sgs))]
                for k, v in zip(sg_keys, sgs): params[k] = v
                filters.append(f"sgr_descricao IN ({','.join([':'+k for k in sg_keys])})")
        
        if req.brands:
            brs = [b.strip() for b in req.brands if b.strip()]
            if brs:
                br_keys = [f"br_{i}" for i in range(len(brs))]
                for k, v in zip(br_keys, brs): params[k] = v
                filters.append(f"mar_descricao IN ({','.join([':'+k for k in br_keys])})")

        if req.categories:
            cats = [c.strip() for c in req.categories if c.strip()]
            if cats:
                cat_keys = [f"cat_{i}" for i in range(len(cats))]
                for k, v in zip(cat_keys, cats): params[k] = v
                filters.append(f"categoria_saldo_atual IN ({','.join([':'+k for k in cat_keys])})")

        # 2. Logica de Calculo de Excesso
        #    Sem days (padrão): excesso = estoque acima do máximo sugerido OFICIAL
        #    (tempo padrão do cálculo). Com days: escala o máximo pela cobertura.

        # Ref Dias Expression (Same as listar_analise)
        indiv_ref_expr = """
            (CASE
                WHEN sgr_codigo = 154 THEN
                    (CASE WHEN curva_abc = 'A' THEN 120.0 WHEN curva_abc = 'B' THEN 180.0 ELSE 240.0 END)
                ELSE
                    (CASE WHEN curva_abc = 'A' THEN 60.0 WHEN curva_abc = 'B' THEN 90.0 ELSE 120.0 END)
            END)
        """

        if req.days and req.days > 0:
            params["days"] = float(req.days)
            factor_expr = f"(:days / {indiv_ref_expr})"
            sim_max_expr = f"CEIL(estoque_max_sugerido * (:days / {indiv_ref_expr}))"
        else:
            factor_expr = "1.0"
            sim_max_expr = "CEIL(estoque_max_sugerido)"

        where_basic = " AND ".join(filters)

        sql = text(f"""
            WITH base_items AS (
                 SELECT
                    *,
                    ({indiv_ref_expr}) as ref_days,
                    {factor_expr} as factor,
                    {sim_max_expr} as sim_max_individual,
                    (estoque_disponivel - {sim_max_expr}) as excess_qty
                 FROM com_fifo_completo
                 WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo) -- Base consistency filter
            ),
            
            grp_agg AS (
                SELECT 
                    group_id, 
                    SUM(sim_max_individual) as grp_sim_max_total
                FROM base_items
                WHERE group_id IS NOT NULL AND group_id <> ''
                GROUP BY group_id
            ),
            
            target_candidates AS (
                SELECT 
                    b.*,
                    g.grp_sim_max_total
                FROM base_items b
                LEFT JOIN grp_agg g ON b.group_id = g.group_id
                WHERE 
                    -- Apply User Filters Here
                    ({where_basic}) 
                    
                    -- APPLY EXCESS LOGIC
                    AND (
                        (b.group_id IS NOT NULL AND b.group_id <> '' AND b.grp_estoque_disponivel > g.grp_sim_max_total)
                        OR
                        ((b.group_id IS NULL OR b.group_id = '') AND b.estoque_disponivel > b.sim_max_individual)
                    )
            )
            
            SELECT DISTINCT
               final.*
            FROM base_items final
            JOIN target_candidates tc ON 
            (
                (:grouped_view = 1 AND final.group_id = tc.group_id AND final.group_id IS NOT NULL AND final.group_id <> '')
                OR
                (final.id = tc.id)
            )
            
            ORDER BY 
                final.curva_abc ASC,
                final.excess_qty DESC,
                final.pro_descricao ASC
        """)
        
        params['grouped_view'] = 1 if req.grouped_view else 0
        
        rows = conn.execute(sql, params).mappings().all()
        
        results = []
        for r in rows:
            d = dict(r)
            
            # Cast complex types
            if d.get('data_min_venda'): d['data_min_venda'] = str(d['data_min_venda'])
            if d.get('data_max_venda'): d['data_max_venda'] = str(d['data_max_venda'])
            if d.get('data_processamento'): d['data_processamento'] = str(d['data_processamento'])
            if d.get('dados_alteracao_json'): d['dados_alteracao_json'] = str(d['dados_alteracao_json'])

            results.append(d)
            
        # Enriquecer com detalhes de lotes/obsolescencia
        # Buscar lotes para todos os itens retornados (Batch fetch)
        final_codes = [i['pro_codigo'] for i in results]
        if final_codes:
            batch_map = _get_stock_batches(final_codes)
            
            for item in results:
                code = item['pro_codigo']
                batches = batch_map.get(code, [])
                
                # Calcular obsoleto (> 240 dias, conforme regra geral)
                obs_qty = sum(b.qtd for b in batches if b.dias_em_estoque > 240)
                
                item['lotes_estoque'] = batches
                item['estoque_obsoleto'] = obs_qty

        # Desconto sugerido para a promoção (sobre o preço 1 — varejo)
        for item in results:
            item['desconto_sugerido_pct'] = _desconto_promocao(
                item.get('custo_unitario'), item.get('preco_venda_1'),
                item.get('categoria_saldo_atual'))

        return results
    finally:
        conn.close()

@router.post("/promo/plan", response_model=List[AnaliseItem])
def planejar_promocao(req: PromoPlanRequest):
    """
    Retorna lista de produtos com estoque excedente para o periodo informado (dias).
    """
    try:
        return _get_promotion_data(req)
    except Exception as e:
        print(f"Erro promo plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/promo/export")
def exportar_promocao(req: PromoPlanRequest):
    """
    Gera um arquivo Excel com o plano de promoção.
    """
    try:
        data = _get_promotion_data(req)
        if not data:
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado para exportação")
            
        df = pd.DataFrame(data)
        
        # Selecionar e Renomear colunas para o Excel
        cols_map = {
            "pro_codigo": "Código",
            "pro_descricao": "Descrição",
            "curva_abc": "Curva ABC",
            "categoria_saldo_atual": "Categoria",
            "estoque_disponivel": "Estoque Atual",
            "sim_max_individual": "Máximo Sugerido",
            "excess_qty": "Quantidade Excedente",
            "custo_unitario": "Custo (R$)",
            "preco_venda_1": "Preço 1 Varejo (R$)",
            "preco_venda_2": "Preço 2 Atacado Esp. (R$)",
            "desconto_sugerido_pct": "Desconto Sugerido (%)",
            "sgr_descricao": "Subgrupo",
            "mar_descricao": "Marca",
            "fornecedor1": "Fornecedor"
        }
        
        # Ensure sim_max_individual exist in result (it came from raw sql dict)
        # Note: In _get_promotion_data we return dicts matching column names from SQL.
        # SQL aliases match mostly.
        
        available_cols = [c for c in cols_map.keys() if c in df.columns]
        df_export = df[available_cols].rename(columns=cols_map)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_export.to_excel(writer, index=False, sheet_name="Promocao")
            
        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="plano_promocao.xlsx"'
        }
        return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
    except Exception as e:
        print(f"Erro export promo: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ROTAS
# ==========================================
