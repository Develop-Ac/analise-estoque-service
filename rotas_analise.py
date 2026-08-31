# -*- coding: utf-8 -*-
"""Rotas de analise: /analise, /analise/export, /analise/simular, /analise/comparacao-custo."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from infra_db import get_db_connection
from erp_api import get_realtime_stocks
from modelos import AnaliseItem, PaginatedResponse, SimulationRequest
from fornecedor_info import _expandir_fornecedores, get_fornecedor_parametros, get_fornecedor_principal_map, get_grupos_fornecedor, get_compras_historico, _sug_norm, _sug_float
from sugestao_compra import montar_memoria_calculo, _dias_ciclo, _Z_POR_CURVA
from estoque_rt import _get_stock_batches
from config import FORN_EXPR

router = APIRouter()

@router.post("/analise/simular", response_model=List[AnaliseItem])
def simular_analise_grupo(req: SimulationRequest):
    """
    Realiza uma análise unificada (como se fosse um grupo) para os itens selecionados.
    Retorna os itens com valores recalculados baseados na soma do conjunto.
    """
    if not req.pro_codigos:
        return []
        
    try:
        conn = get_db_connection()
        
        # Buscar dados dos produtos
        fmt_codes = [f"'{c.strip()}'" for c in req.pro_codigos]
        clause_in = ",".join(fmt_codes)
        
        # Query Dados Básicos
        # Reutilizando colunas padrão
        sql = text(f"""
            SELECT * FROM com_fifo_completo WHERE pro_codigo IN ({clause_in})
        """)
        
        rows = conn.execute(sql).mappings().all()
        items = [dict(r) for r in rows]
        
        if not items:
            return []
            
        # Calcular Totais do "Grupo Ad-Hoc"
        total_estoque = sum(float(i['estoque_disponivel'] or 0) for i in items)
        total_demanda = sum(float(i['demanda_media_dia_ajustada'] or 0) for i in items)
        sum_min = sum(float(i['estoque_min_sugerido'] or 0) for i in items)
        sum_max = sum(float(i['estoque_max_sugerido'] or 0) for i in items)
        
        # Simulação com Coverage Days (se fornecido)
        # Se coverage_days > 0, recalculamos o Min/Max Alvo baseado na demanda total
        if req.coverage_days > 0:
            # Regra simples unificada: DemandaTotal * Dias
            # Ignorando curvas individuais, usando regra geral ou media?
            # Vamos usar a lógica de "Maior Curva" do grupo.
            # Se tiver algum A, trata como A.
            curves = [i['curva_abc'] for i in items]
            best_curve = 'C'
            if 'A' in curves: best_curve = 'A'
            elif 'B' in curves: best_curve = 'B'
            
            # Definir ref dias (Simplificado, idealmente pega do config do main)
            ref_dias_base = 120 # C
            if best_curve == 'A': ref_dias_base = 60
            if best_curve == 'B': ref_dias_base = 90
            
            # Fator
            factor = req.coverage_days / ref_dias_base
            
            # Recalcular Unificado
            target_min = sum_min * factor
            target_max = sum_max * factor
            
            # Atualizar sums
            sum_min = target_min
            sum_max = target_max
            
        # Distribuir o resultado para resposta
        resp_items = []
        for i in items:
            # Converter para AnaliseItem (fields match dict keys largely)
            # Mas vamos atualizar os valores com o TOTAL DO GRUPO
            # para indicar que a analise é conjunta.
            
            i['original_stock'] = i['estoque_disponivel']
            i['estoque_disponivel'] = total_estoque
            # i['demanda_media_dia_ajustada'] = total_demanda # Opcional
            
            i['estoque_min_sugerido'] = sum_min
            i['estoque_max_sugerido'] = sum_max
            
            i['is_grouped_view'] = True
            i['group_count'] = len(items)
            i['group_id'] = "SIMULATION"
            
            # Cast date fields to string if necessary/None
            if i.get('data_min_venda'): i['data_min_venda'] = str(i['data_min_venda'])
            if i.get('data_max_venda'): i['data_max_venda'] = str(i['data_max_venda'])
            if i.get('data_processamento'): i['data_processamento'] = str(i['data_processamento'])
            if i.get('dados_alteracao_json'): i['dados_alteracao_json'] = str(i['dados_alteracao_json'])
            
            resp_items.append(i)
            
        return resp_items
        
    except Exception as e:
        print(f"Erro simulacao: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/analise/comparacao-custo")
def comparacao_nivel_servico_custo():
    """
    Comparação AGREGADA entre o nível de serviço da CURVA e o de CUSTO (razão crítica /
    newsvendor). ORFÃO: a tela de comparação foi removida e o nível de custo virou o
    OFICIAL (NS_MODO=custo → estoque_min_base == estoque_min_custo, deltas ≈ 0). Endpoint
    mantido como relatório de auditoria read-only; pode ser removido se ninguém consumir.
    Considera itens com nivel_servico_custo preenchido e fora de 'sob encomenda'.
    """
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar no banco: {e}")
    try:
        base_where = ("nivel_servico_custo IS NOT NULL "
                      "AND COALESCE(sob_encomenda, FALSE) = FALSE")
        rows = conn.execute(text(f"""
            SELECT curva_abc AS curva,
                   COUNT(*)                                                       AS itens,
                   COALESCE(SUM(estoque_min_base), 0)                             AS min_curva,
                   COALESCE(SUM(estoque_min_custo), 0)                            AS min_custo,
                   COALESCE(SUM(estoque_min_base  * COALESCE(custo_unitario,0)),0) AS capital_curva,
                   COALESCE(SUM(estoque_min_custo * COALESCE(custo_unitario,0)),0) AS capital_custo,
                   SUM(CASE WHEN estoque_min_custo > estoque_min_base THEN 1 ELSE 0 END) AS subiram,
                   SUM(CASE WHEN estoque_min_custo < estoque_min_base THEN 1 ELSE 0 END) AS desceram,
                   SUM(CASE WHEN estoque_min_custo = estoque_min_base THEN 1 ELSE 0 END) AS iguais,
                   AVG(nivel_servico_custo)                                       AS ns_custo_medio,
                   AVG(margem_pct)                                                AS margem_media
            FROM com_fifo_completo
            WHERE {base_where}
            GROUP BY curva_abc
            ORDER BY curva_abc
        """)).mappings().all()

        def _row(r):
            d = dict(r)
            for k in ("itens", "min_curva", "min_custo", "subiram", "desceram", "iguais"):
                d[k] = int(_sug_float(d.get(k)))
            for k in ("capital_curva", "capital_custo", "ns_custo_medio", "margem_media"):
                d[k] = round(_sug_float(d.get(k)), 4)
            d["delta_capital"] = round(d["capital_custo"] - d["capital_curva"], 2)
            return d
        por_curva = [_row(r) for r in rows]

        keys_i = ("itens", "min_curva", "min_custo", "subiram", "desceram", "iguais")
        keys_f = ("capital_curva", "capital_custo")
        totais = {k: sum(r[k] for r in por_curva) for k in keys_i}
        for k in keys_f:
            totais[k] = round(sum(r[k] for r in por_curva), 2)
        totais["delta_capital"] = round(totais["capital_custo"] - totais["capital_curva"], 2)

        def _movimentos(direcao):
            return [dict(r) for r in conn.execute(text(f"""
                SELECT pro_codigo, pro_descricao, mar_descricao, curva_abc AS curva,
                       margem_pct, nivel_servico_custo, custo_unitario,
                       estoque_min_base  AS min_curva,
                       estoque_min_custo AS min_custo,
                       (estoque_min_custo - estoque_min_base) * COALESCE(custo_unitario,0) AS delta_capital
                FROM com_fifo_completo
                WHERE {base_where} AND custo_unitario IS NOT NULL
                ORDER BY (estoque_min_custo - estoque_min_base) * COALESCE(custo_unitario,0) {direcao}
                LIMIT 15
            """)).mappings().all()]

        return {
            "por_curva": por_curva,
            "totais": totais,
            "top_reducao": _movimentos("ASC"),   # maior liberação de capital
            "top_aumento": _movimentos("DESC"),  # maior aumento de proteção
            "params": {
                "holding_rate_anual": float(os.getenv("HOLDING_RATE_ANUAL") or 0.25),
                "faixa_curva": {"A": [0.90, 0.99], "B": [0.85, 0.98], "C": [0.80, 0.96], "D": [0.75, 0.94]},
                "modo": os.getenv("NS_MODO") or "sombra",
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals(): conn.close()


@router.get("/analise", response_model=PaginatedResponse)
def listar_analise(
    page: int = 1,
    limit: int = 50,
    search: Optional[str] = None,
    pro_codigos: Optional[str] = Query(None, description="Lista de códigos separados por virgula"),
    marca: Optional[str] = None,
    subgrupo: Optional[str] = None,
    saldo_categoria: Optional[str] = Query(None, description="Faixas de idade FIFO do saldo (categoria_saldo_atual), separadas por vírgula: Rápido/Médio/Lento/Obsoleto"),
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None,
    group_id: Optional[str] = None,
    match_type: str = "contains",
    coverage_days: Optional[int] = None,
    grouped_view: bool = False,
    kpi: Optional[str] = None,
    fornecedor: Optional[str] = None,
):
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"ERRO DE CONEXAO BANCO: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao conectar no banco de dados: {str(e)}")

    try:
        offset = (page - 1) * limit
        
        # Filtros Cláusula WHERE
        filters = []
        params = {}

        if only_changes:
            filters.append("teve_alteracao_analise = TRUE")

        # FILTER: Ensure we only fetch the latest analysis snapshot
        filters.append("data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)")

        # DRILL-DOWN dos KPIs do Painel de Estoque: mesma condição SQL dos cards
        # do dashboard Metabase — a lista soma EXATAMENTE o valor exibido no card.
        KPI_FILTERS = {
            "excesso": ("(estoque_max_sugerido > 0 AND estoque_disponivel > estoque_max_sugerido "
                        "AND COALESCE(custo_unitario,0) > 0)"),
            "capital_parado": ("(estoque_disponivel > 0 "
                               "AND categoria_saldo_atual IN ('Lento','Obsoleto'))"),
            "ruptura": ("(COALESCE(estoque_disponivel,0) <= 0 AND COALESCE(demanda_real_dia,0) > 0 "
                        "AND COALESCE(sob_encomenda,false) = false)"),
            "venda_perdida": "(COALESCE(venda_perdida_12m,0) > 0)",
        }
        if kpi:
            cond = KPI_FILTERS.get(kpi.strip().lower())
            if cond:
                filters.append(cond)

        # Filtro de fornecedor (mesma expressão do {{fornecedor}} dos cards):
        # aceita vários nomes separados por '||' e EXPANDE para os fornecedores
        # RELACIONADOS (grupo matriz/filiais do compras).
        if fornecedor:
            nomes_f = _expandir_fornecedores([n for n in fornecedor.split("||") if n.strip()])
            if nomes_f:
                fkeys = {f"forn_{i}": n for i, n in enumerate(nomes_f)}
                params.update(fkeys)
                keys = ", ".join(f":{k}" for k in fkeys)
                filters.append(f"UPPER(TRIM({FORN_EXPR})) IN ({keys})")

        if group_id:
            filters.append("group_id = :group_id")
            params["group_id"] = group_id
            
        # 1. Simulation Factor Prep
        sim_active = coverage_days is not None and coverage_days > 0
        
        # SQL Expressions for References and scaling
        indiv_ref_expr = """
            (CASE 
                WHEN sgr_codigo = 154 THEN 
                    (CASE WHEN curva_abc = 'A' THEN 120.0 WHEN curva_abc = 'B' THEN 180.0 ELSE 240.0 END)
                ELSE 
                    (CASE WHEN curva_abc = 'A' THEN 60.0 WHEN curva_abc = 'B' THEN 90.0 ELSE 120.0 END)
            END)
        """
        
        grp_ref_dias_expr = """
            (CASE 
                WHEN MAX(sgr_codigo) = 154 THEN 
                    (CASE WHEN MIN(curva_abc) = 'A' THEN 120.0 WHEN MIN(curva_abc) = 'B' THEN 180.0 ELSE 240.0 END)
                ELSE 
                    (CASE WHEN MIN(curva_abc) = 'A' THEN 60.0 WHEN MIN(curva_abc) = 'B' THEN 90.0 ELSE 120.0 END)
            END)
        """

        if sim_active:
             params["cv_days"] = float(coverage_days)
             i_min_expr = f"CEIL(estoque_min_sugerido * (:cv_days / {indiv_ref_expr}))"
             i_max_expr = f"CEIL(estoque_max_sugerido * (:cv_days / {indiv_ref_expr}))"
             g_min_expr = f"CEIL(MAX(grp_estoque_min_sugerido) * (:cv_days / {grp_ref_dias_expr}))"
             g_max_expr = f"CEIL(MAX(grp_estoque_max_sugerido) * (:cv_days / {grp_ref_dias_expr}))"
        else:
             i_min_expr = "CEIL(estoque_min_sugerido)"
             i_max_expr = "CEIL(estoque_max_sugerido)"
             g_min_expr = "CEIL(MAX(grp_estoque_min_sugerido))"
             g_max_expr = "CEIL(MAX(grp_estoque_max_sugerido))"

        # 2. STATUS FILTER LOGIC (Includes 'critical' checkbox)
        # We combine them if present, or handle separately if they overlap.
        # Here we treat 'critical=true' as adding 'critico' to the status filter list.
        filter_status_list = []
        if critical:
            filter_status_list.append("critico")
        if status:
            filter_status_list.extend([s.strip().lower() for s in status.split(",") if s.strip()])
        
        if filter_status_list:
            def build_status_sql_cond(stock_col, min_col, max_col, s_list):
                conds = []
                if "critico" in s_list or "critical" in s_list:
                    conds.append(f"({stock_col} < {min_col})")
                if "excesso" in s_list or "excess" in s_list:
                    conds.append(f"({stock_col} > {max_col})")
                if "normal" in s_list:
                    conds.append(f"({stock_col} >= {min_col} AND {stock_col} <= {max_col})")
                return f"({' OR '.join(conds)})" if conds else "1=1"

            if grouped_view:
                # Group-Aware Filter: Filter at Group level first
                grp_status_cond = build_status_sql_cond("MAX(grp_estoque_disponivel)", g_min_expr, g_max_expr, filter_status_list)
                
                sql_matching_groups = f"""
                    SELECT group_id 
                    FROM com_fifo_completo 
                    WHERE group_id IS NOT NULL AND group_id <> ''
                    AND data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
                    GROUP BY group_id
                    HAVING {grp_status_cond}
                """
                
                indiv_status_cond = build_status_sql_cond("estoque_disponivel", i_min_expr, i_max_expr, filter_status_list)
                
                filters.append(f"""(
                    (group_id IN ({sql_matching_groups})) 
                    OR 
                    ((group_id IS NULL OR group_id = '') AND {indiv_status_cond})
                )""")
            else:
                # Regular Individual Filter
                filters.append(build_status_sql_cond("estoque_disponivel", i_min_expr, i_max_expr, filter_status_list))

        if curve:
            curves = [c.strip().upper() for c in curve.split(",") if c.strip()]
            if curves:
                curve_params = {f"curve_{i}": c for i, c in enumerate(curves)}
                params.update(curve_params)
                keys = ", ".join([f":{k}" for k in curve_params.keys()])
                filters.append(f"curva_abc IN ({keys})")

        if trend:
            # Trend -> tendencia_label
            trends = [t.strip() for t in trend.split(",") if t.strip()]
            if trends:
                 trend_params = {f"trend_{i}": t for i, t in enumerate(trends)}
                 params.update(trend_params)
                 keys = ", ".join([f":{k}" for k in trend_params.keys()])
                 filters.append(f"tendencia_label IN ({keys})")

        if search:
            if match_type == "exact":
                filters.append("(pro_codigo = :search OR pro_descricao = :search_desc)")
                params["search"] = search
                params["search_desc"] = search
            elif match_type == "starts_with":
                filters.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
                params["search"] = f"{search}%"
                params["search_desc"] = f"{search}%"
            else: # contains (default)
                filters.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
                params["search"] = f"%{search}%"
                params["search_desc"] = f"%{search}%"

        if pro_codigos:
             # Separa os códigos por vírgula
             codigos_list = [c.strip() for c in pro_codigos.split(",") if c.strip()]
             if codigos_list:
                 # Cria parametros dinamicos para o IN (:cod0, :cod1, ...)
                 cod_params = {f"cod_{i}": c for i, c in enumerate(codigos_list)}
                 params.update(cod_params)
                 keys = ", ".join([f":{k}" for k in cod_params.keys()])
                 filters.append(f"pro_codigo IN ({keys})")

        if marca:
            filters.append("mar_descricao ILIKE :marca")
            params["marca"] = f"%{marca}%"
            
        if subgrupo:
            filters.append("sgr_descricao ILIKE :subgrupo")
            params["subgrupo"] = f"%{subgrupo}%"

        # Filtro por TEMPO EM ESTOQUE (idade FIFO do saldo atual): faixas
        # Rápido/Médio/Lento/Obsoleto derivadas de tempo_medio_saldo_atual.
        if saldo_categoria:
            cats = [c.strip() for c in saldo_categoria.split(",") if c.strip()]
            if cats:
                cat_params = {f"saldocat_{i}": c for i, c in enumerate(cats)}
                params.update(cat_params)
                keys = ", ".join(f":{k}" for k in cat_params)
                filters.append(f"categoria_saldo_atual IN ({keys})")

        where_clause = " AND ".join(filters) if filters else "1=1"
        
        # Query Total
        count_sql = text(f"SELECT COUNT(*) FROM com_fifo_completo WHERE {where_clause}")
        try:
            total = conn.execute(count_sql, params).scalar()
        except Exception as e:
             print(f"ERRO QUERY: {e}")
             return {
                "data": [],
                "total": 0,
                "page": page,
                "limit": limit,
                "total_pages": 0
            }

        # Somas do FILTRO ATUAL (todas as linhas do where, não só a página):
        # capital = Σ estoque×custo (estoque>0) e CME acumulado — MESMAS fórmulas
        # dos cards do painel, para os números conversarem entre as abas.
        capital_total = cme_total = None
        try:
            _hold_tot = float(os.getenv("HOLDING_RATE_ANUAL") or 0.25)
            tot_row = conn.execute(text(f"""
                SELECT
                  COALESCE(SUM(CASE WHEN estoque_disponivel > 0
                      THEN COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0) ELSE 0 END), 0),
                  COALESCE(SUM(CASE WHEN estoque_disponivel > 0
                      THEN COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0)
                           * :hold_tot * GREATEST(COALESCE(tempo_medio_saldo_atual,0),0) / 365.0 ELSE 0 END), 0)
                FROM com_fifo_completo WHERE {where_clause}
            """), {**params, "hold_tot": _hold_tot}).fetchone()
            capital_total = round(float(tot_row[0] or 0), 2)
            cme_total = round(float(tot_row[1] or 0), 2)
        except Exception as e:
            print(f"AVISO: totais capital/CME do filtro indisponíveis: {e}")
        
        # Query Dados
        data_sql = text(f"""
            SELECT 
                id, pro_codigo, pro_descricao, pro_referencia, sgr_codigo, sgr_descricao, mar_descricao, fornecedor1,
                estoque_disponivel, demanda_media_dia, demanda_media_dia_ajustada,
                tempo_medio_estoque, CAST(data_min_venda AS TEXT) as data_min_venda, 
                CAST(data_max_venda AS TEXT) as data_max_venda, qtd_vendida,
                curva_abc, categoria_estocagem, estoque_min_sugerido, estoque_max_sugerido,
                tipo_planejamento, teve_alteracao_analise, 
                CAST(data_processamento AS TEXT) as data_processamento,
                dias_ruptura, fator_tendencia, tendencia_label, alerta_tendencia_alta,
                CAST(dados_alteracao_json AS TEXT) as dados_alteracao_json,
                group_id,
                grp_estoque_disponivel, grp_qtd_vendida, grp_valor_vendido, grp_num_vendas,
                grp_vendas_ult_12m, grp_vendas_12m_ant, grp_estoque_min_base, grp_estoque_max_base,
                grp_estoque_min_ajustado, grp_estoque_max_ajustado, grp_estoque_min_sugerido,
                grp_estoque_max_sugerido, grp_demanda_media_dia, rateio_prop_grupo,
                tempo_medio_saldo_atual, categoria_saldo_atual,
                demanda_real_dia, sigma_demanda_dia, cv_demanda, classe_xyz,
                estoque_seguranca, nivel_servico_z, lead_time_dias,
                venda_perdida_12m, valor_vendido_12m,
                padrao_demanda, metodo_reposicao, fator_sazonal, demanda_planejamento_dia,
                mean_size_mes, cv2_tamanho,
                grupo_chave, grupo_estoque_min, grupo_estoque_max, grupo_qtd_itens, grupo_estoque_disponivel, grupo_demanda_dia,
                grupo_estoque_seguranca, grupo_curva, grupo_metodo, grupo_fator_sazonal,
                grupo_mean_size, grupo_cv2,
                custo_unitario, margem_unitaria, margem_pct,
                nivel_servico_custo, z_custo, estoque_min_custo, estoque_max_custo, estoque_seg_custo,
                grupo_nivel_servico_custo, grupo_estoque_min_custo, grupo_estoque_max_custo, grupo_margem_pct,
                eh_original, teve_outlier_aparado, outlier_qtd_aparada, outlier_motivo
            FROM com_fifo_completo
            WHERE {where_clause}
            ORDER BY
                -- 1. Melhor Curva do Grupo (Prioridade A)
                MIN(curva_abc) OVER (PARTITION BY CASE WHEN group_id IS NOT NULL AND group_id <> '' THEN group_id ELSE pro_codigo END) ASC,
                
                -- 2. Ordem Alfabética do Grupo (Mantém consistência)
                MIN(pro_descricao) OVER (PARTITION BY CASE WHEN group_id IS NOT NULL AND group_id <> '' THEN group_id ELSE pro_codigo END) ASC,
                
                -- 3. Ordenação dentro do grupo ou itens soltos
                pro_descricao ASC
            LIMIT :limit OFFSET :offset
        """)
        
        params["limit"] = limit
        params["offset"] = offset
        
        result = conn.execute(data_sql, params).mappings().all()
        data_list = [dict(row) for row in result]

        # ---------------------------------------------------------------------
        # MEMÓRIA DE CÁLCULO (mín/máx): individual e do GRUPO consolidado (pooled)
        # ---------------------------------------------------------------------
        _grp_membros = {}
        for _it in data_list:
            gk = _it.get("grupo_chave")
            if gk:
                _grp_membros.setdefault(gk, []).append({
                    "marca": _it.get("mar_descricao"),
                    "pro_codigo": _it.get("pro_codigo"),
                    "demanda_dia": round(_sug_float(_it.get("demanda_real_dia")), 4),
                    "min_ind": int(_sug_float(_it.get("estoque_min_sugerido"))),
                    "max_ind": int(_sug_float(_it.get("estoque_max_sugerido"))),
                })
        for _it in data_list:
            _cv = _it.get("curva_abc")
            _it["memoria"] = montar_memoria_calculo(
                escopo="item",
                minimo=_it.get("estoque_min_sugerido"), maximo=_it.get("estoque_max_sugerido"),
                curva=_cv, classe=_it.get("classe_xyz"), metodo=_it.get("metodo_reposicao"),
                demanda_dia=(_it.get("demanda_planejamento_dia") if _it.get("demanda_planejamento_dia") is not None
                             else _it.get("demanda_media_dia_ajustada")),
                sigma_dia=_it.get("sigma_demanda_dia"),
                z=(_it.get("nivel_servico_z") if _it.get("nivel_servico_z") is not None
                   else _Z_POR_CURVA.get(_sug_norm(_cv).upper())),
                lead_time=(_it.get("lead_time_dias") or 17),
                ss=_it.get("estoque_seguranca"), fator_sazonal=_it.get("fator_sazonal"),
                sgr_codigo=_it.get("sgr_codigo"),
                msize=_it.get("mean_size_mes"), cv2=_it.get("cv2_tamanho"),
                ns_custo=_it.get("nivel_servico_custo"), z_custo=_it.get("z_custo"),
                min_custo=_it.get("estoque_min_custo"), max_custo=_it.get("estoque_max_custo"),
                ss_custo=_it.get("estoque_seg_custo"), margem_pct=_it.get("margem_pct"),
                custo_unit=_it.get("custo_unitario"),
                outlier_aparado=_it.get("teve_outlier_aparado"),
                outlier_qtd=_it.get("outlier_qtd_aparada"), outlier_motivo=_it.get("outlier_motivo"),
            )
            gk = _it.get("grupo_chave")
            if gk and _sug_float(_it.get("grupo_estoque_max")) > 0:
                _gcv = _it.get("grupo_curva") or _cv
                _it["memoria_grupo"] = montar_memoria_calculo(
                    escopo="grupo",
                    minimo=_it.get("grupo_estoque_min"), maximo=_it.get("grupo_estoque_max"),
                    curva=_gcv, classe=_it.get("classe_xyz"), metodo=_it.get("grupo_metodo"),
                    demanda_dia=_it.get("grupo_demanda_dia"), sigma_dia=None,
                    z=_Z_POR_CURVA.get(_sug_norm(_gcv).upper()),
                    lead_time=(_it.get("lead_time_dias") or 17),
                    ss=_it.get("grupo_estoque_seguranca"), fator_sazonal=_it.get("grupo_fator_sazonal"),
                    sgr_codigo=_it.get("sgr_codigo"),
                    msize=_it.get("grupo_mean_size"), cv2=_it.get("grupo_cv2"),
                    ns_custo=_it.get("grupo_nivel_servico_custo"),
                    min_custo=_it.get("grupo_estoque_min_custo"), max_custo=_it.get("grupo_estoque_max_custo"),
                    margem_pct=_it.get("grupo_margem_pct"),
                    membros=_grp_membros.get(gk),
                )

        # ---------------------------------------------------------------------
        # OBS: Realtime Stock fetch moved down to include group members
        # ---------------------------------------------------------------------
        
        # ---------------------------------------------------------------------
        # LÓGICA DE GRUPOS (SIMILARES)
        # ---------------------------------------------------------------------
        # 1. Buscar Group IDs para os itens da pagina
        page_start_codes = [item['pro_codigo'] for item in data_list]
        
        if page_start_codes:
            try:
                # Prepara IN clause
                fmt_codes = [f"'{c}'" for c in page_start_codes]
                sql_groups = text(f"SELECT pro_codigo, group_id FROM com_relacionamento_itens WHERE pro_codigo IN ({','.join(fmt_codes)})")
                group_map = {row[0]: row[1] for row in conn.execute(sql_groups).fetchall()}
                
                # Identificar grupos unicos envolvidos
                unique_groups = {g for g in group_map.values() if g}
                
                if unique_groups:
                    try:
                        # 2. Get ALL members of these groups to fetch their realtime stock too
                        groups_fmt = [f"'{g}'" for g in unique_groups]
                    
                        sql_members = text(f"SELECT group_id, pro_codigo FROM com_relacionamento_itens WHERE group_id IN ({','.join(groups_fmt)})")
                        rows_members = conn.execute(sql_members).fetchall()
                        
                        group_members = {}
                        extra_codes = set()
                        
                        for rid, rcode in rows_members:
                             if rid not in group_members: group_members[rid] = []
                             group_members[rid].append(rcode)
                             extra_codes.add(rcode)

                        # 3. Combine Page Codes + Group Member Codes for Bulk Realtime Fetch
                        page_codes = set(item['pro_codigo'] for item in data_list)
                        all_codes_to_fetch = list(page_codes.union(extra_codes))

                        # 4. Fetch Realtime Stock for ALL
                        stock_map = {}
                        try:
                            if all_codes_to_fetch:
                                 stock_map = get_realtime_stocks(all_codes_to_fetch)
                        except Exception as e:
                            print(f"AVISO: Falha ao buscar estoque realtime (Grupo): {e}")

                        # 5. Calculate Realtime Group Totals
                        grp_realtime_sums = {}
                        for gid, members in group_members.items():
                            group_stock_sum = 0.0
                            for mcode in members:
                                group_stock_sum += float(stock_map.get(mcode, 0)) # Uses realtime if avail, else 0 (safest assumption for realtime)
                            grp_realtime_sums[gid] = group_stock_sum

                        # Update Individual Page Items with their realtime stock
                        for item in data_list:
                             code = item['pro_codigo']
                             if code in stock_map:
                                 item['estoque_disponivel'] = stock_map[code]

                        # 6. Aggregations using PERSISTED fields + Realtime Stock Sum
                        
                        # Note: We still fetch realtime stock above and sum it into grp_realtime_sums[gid]
                        # But for other static analysis fields (Min/Max/Avg/Sold), we now assume they are
                        # already present in 'item' (grp_*) from the main query.
                        
                        # However, we still need 'qtd_itens' (group_count) which might not be in the main item row unless we join or count.
                        # Actually, we can count it from sql_members result.
                        group_counts_map = {}
                        for gid, mems in group_members.items():
                             group_counts_map[gid] = len(mems)

                        for item in data_list:
                            code = item['pro_codigo']
                            gid = group_map.get(code)
                            
                            if gid:
                                # Update fields
                                item['original_stock'] = item['estoque_disponivel']
                                item['group_id'] = gid
                                item['group_count'] = group_counts_map.get(gid, 1)
                                item['is_grouped_view'] = True
                                
                                # Use Realtime Stock Sum for "Current Stock"
                                item['grp_estoque_disponivel'] = grp_realtime_sums.get(gid, 0.0)
                                
                                # Use Persisted Static Analysis Data
                                # (If data is missing/null, fallback to 0)
                                item['grp_qtd_vendida'] = float(item.get('grp_qtd_vendida') or 0)
                                item['grp_demanda_media_dia'] = float(item.get('grp_demanda_media_dia') or 0)
                                item['grp_estoque_min_sugerido'] = float(item.get('grp_estoque_min_sugerido') or 0)
                                item['grp_estoque_max_sugerido'] = float(item.get('grp_estoque_max_sugerido') or 0)
                                item['grp_estoque_max_ajustado'] = float(item.get('grp_estoque_max_ajustado') or 0)

                    except Exception as e:
                        print(f"ERRO CRITICO EM GRUPOS: {e}")

                else:
                     # Case: No groups involved
                     pro_codigos = [item['pro_codigo'] for item in data_list]
                     if pro_codigos:
                        try:
                            stock_map = get_realtime_stocks(pro_codigos)
                            for item in data_list:
                                if item['pro_codigo'] in stock_map:
                                    item['estoque_disponivel'] = stock_map[item['pro_codigo']]
                        except Exception as e:
                            print(f"AVISO: Falha ao buscar estoque realtime (Sem Grupo): {e}")

            except Exception as e:
                print(f"Erro ao processar grupos/estoque: {e}")

        
        # -----------------------------------------------------------------
        # CAPITAL PARADO + CUSTO DE MANTER ACUMULADO (após o estoque realtime)
        #   valor_estoque = estoque × custo unitário (médio 12m)
        #   custo_manter_acumulado = valor × HOLDING_RATE_ANUAL × idade/365,
        #   com idade = tempo_medio_saldo_atual (idade média PONDERADA do saldo
        #   pelos lotes FIFO). Como a idade média é ponderada pela quantidade,
        #   Σ(lote×idade) == estoque×idade_média — o acumulado é exato dado o
        #   custo médio. É quanto esse saldo JÁ custou parado até hoje.
        # -----------------------------------------------------------------
        _hold = float(os.getenv("HOLDING_RATE_ANUAL") or 0.25)
        for item in data_list:
            try:
                cu = float(item.get("custo_unitario") or 0)
                est = float(item.get("estoque_disponivel") or 0)
                idade = float(item.get("tempo_medio_saldo_atual") or 0)
            except (TypeError, ValueError):
                continue
            if cu > 0:
                va = est * cu
                item["valor_estoque"] = round(va, 2)
                item["custo_manter_acumulado"] = (round(va * _hold * max(idade, 0.0) / 365.0, 2)
                                                  if idade > 0 else 0.0)

        total_pages = (total + limit - 1) // limit if limit > 0 else 0

        return {
            "data": data_list,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "capital_total": capital_total,
            "cme_total": cme_total
        }
    except Exception as e:
        print(f"ERRO GERAL API: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        conn.close()


@router.get("/analise/export")
def exportar_analise(
    search: Optional[str] = None,
    pro_codigos: Optional[str] = Query(None),
    marca: Optional[str] = None,
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None,
    subgrupo: Optional[str] = None,
    saldo_categoria: Optional[str] = None,
    coverage_days: int = 0,  # Novo Parametro
    group_id: Optional[str] = None,
    match_type: str = "contains",
    kpi: Optional[str] = None,
    fornecedor: Optional[str] = None,
):
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro banco: {e}")

    try:
        where_clauses = ["1=1"]
        params = {}

        # Drill-down dos KPIs do painel (mesmas condições do listar_analise)
        _KPI_FILTERS = {
            "excesso": ("(estoque_max_sugerido > 0 AND estoque_disponivel > estoque_max_sugerido "
                        "AND COALESCE(custo_unitario,0) > 0)"),
            "capital_parado": ("(estoque_disponivel > 0 "
                               "AND categoria_saldo_atual IN ('Lento','Obsoleto'))"),
            "ruptura": ("(COALESCE(estoque_disponivel,0) <= 0 AND COALESCE(demanda_real_dia,0) > 0 "
                        "AND COALESCE(sob_encomenda,false) = false)"),
            "venda_perdida": "(COALESCE(venda_perdida_12m,0) > 0)",
        }
        if kpi and _KPI_FILTERS.get(kpi.strip().lower()):
            where_clauses.append(_KPI_FILTERS[kpi.strip().lower()])

        if fornecedor:
            nomes_f = _expandir_fornecedores([n for n in fornecedor.split("||") if n.strip()])
            if nomes_f:
                fkeys = {f"forn_{i}": n for i, n in enumerate(nomes_f)}
                params.update(fkeys)
                keys = ", ".join(f":{k}" for k in fkeys)
                where_clauses.append(f"UPPER(TRIM({FORN_EXPR})) IN ({keys})")

        if search:
            if match_type == "exact":
                where_clauses.append("(pro_codigo = :search OR pro_descricao = :search_desc)")
                params["search"] = search
                params["search_desc"] = search
            elif match_type == "starts_with":
                where_clauses.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
                params["search"] = f"{search}%"
                params["search_desc"] = f"{search}%"
            else: # contains
                where_clauses.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
                params["search"] = f"%{search}%"
                params["search_desc"] = f"%{search}%"

        if pro_codigos:
             codigos_list = [c.strip() for c in pro_codigos.split(",") if c.strip()]
             if codigos_list:
                 cod_params = {f"cod_{i}": c for i, c in enumerate(codigos_list)}
                 params.update(cod_params)
                 keys = ", ".join([f":{k}" for k in cod_params.keys()])
                 where_clauses.append(f"pro_codigo IN ({keys})")

        if marca:
            where_clauses.append("mar_descricao ILIKE :marca")
            params["marca"] = f"%{marca}%"
            
        if only_changes: # Filter should apply regardless of simulation mode
            where_clauses.append("teve_alteracao_analise = TRUE")

        # FILTER: Ensure we only fetch the latest analysis snapshot
        where_clauses.append("data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)")
        
        if group_id:
            where_clauses.append("group_id = :group_id")
            params["group_id"] = group_id
            
        if critical:
             where_clauses.append("estoque_disponivel < estoque_min_sugerido")

        if curve:
            curves = [c.strip().upper() for c in curve.split(",") if c.strip()]
            if curves:
                curve_params = {f"curve_{i}": c for i, c in enumerate(curves)}
                params.update(curve_params)
                keys = ", ".join([f":{k}" for k in curve_params.keys()])
                where_clauses.append(f"curva_abc IN ({keys})")
        
        if trend:
            # Trend -> tendencia_label (match logic from listar_analise)
            trends = [t.strip() for t in trend.split(",") if t.strip()]
            if trends:
                 trend_params = {f"trend_{i}": t for i, t in enumerate(trends)}
                 params.update(trend_params)
                 keys = ", ".join([f":{k}" for k in trend_params.keys()])
                 where_clauses.append(f"tendencia_label IN ({keys})")

        if status:
            status_list = [s.strip().lower() for s in status.split(",") if s.strip()]
            status_conditions = []
            if "critico" in status_list or "critical" in status_list:
                status_conditions.append("estoque_disponivel < estoque_min_sugerido")
            if "excesso" in status_list or "excess" in status_list:
                status_conditions.append("estoque_disponivel > estoque_max_sugerido")
            if "normal" in status_list:
                status_conditions.append("estoque_disponivel >= estoque_min_sugerido AND estoque_disponivel <= estoque_max_sugerido")
            
            if status_conditions:
                where_clauses.append(f"({' OR '.join(status_conditions)})")

        if subgrupo:
            where_clauses.append("sgr_descricao ILIKE :subgrupo")
            params["subgrupo"] = f"%{subgrupo}%"

        # Tempo em estoque (idade FIFO): mesmas faixas do listar_analise
        if saldo_categoria:
            cats = [c.strip() for c in saldo_categoria.split(",") if c.strip()]
            if cats:
                cat_params = {f"saldocat_{i}": c for i, c in enumerate(cats)}
                params.update(cat_params)
                keys = ", ".join(f":{k}" for k in cat_params)
                where_clauses.append(f"categoria_saldo_atual IN ({keys})")

        where_clause = " AND ".join(where_clauses)
        
        # Seleção de colunas muda baseada na cobertura
        if coverage_days > 0:
             # Modo Simulação: Traz dados brutos para calcular no Python
             query_columns = """
                pro_codigo, pro_descricao, estoque_disponivel, 
                estoque_min_sugerido, estoque_max_sugerido, 
                curva_abc, tipo_planejamento, sgr_codigo, alerta_tendencia_alta,
                demanda_media_dia_ajustada, fornecedor1, qtd_vendida,
                group_id, grp_estoque_disponivel, grp_estoque_min_sugerido, grp_estoque_max_sugerido
             """
        else:
             # Modo Padrão: Traz colunas formatadas para relatorio de analise
             query_columns = """
                pro_codigo as "Código",
                pro_descricao as "Descrição",
                mar_descricao as "Marca",
                curva_abc as "Curva",
                estoque_disponivel as "Estoque",
                demanda_media_dia_ajustada as "Média/Dia",
                tendencia_label as "Tendência",
                estoque_min_sugerido as "Min Sugerido",
                estoque_max_sugerido as "Max Sugerido",
                CASE 
                    WHEN estoque_disponivel < estoque_min_sugerido THEN 'Crítico'
                    WHEN estoque_disponivel > estoque_max_sugerido THEN 'Excesso'
                    ELSE 'Normal'
                END as "Status",
                qtd_vendida as "Qtd Vendida",
                valor_vendido as "Valor Vendido",
                periodo_dias as "Dias Período",
                tempo_medio_estoque as "Tempo Médio Est.",
                fornecedor1 as "Fornecedor",
                tipo_planejamento as "Tipo Planejamento",
                dados_alteracao_json as "Detalhes Mudança",
                group_id as "ID Grupo",
                grp_estoque_disponivel as "Estoque Grupo",
                grp_estoque_min_sugerido as "Min Grupo",
                grp_estoque_max_sugerido as "Max Grupo",
                tempo_medio_saldo_atual as "Idade Saldo Atual",
                categoria_saldo_atual as "Cat. Saldo Atual",
                custo_unitario as "Custo Unit. (R$)",
                ROUND(COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0), 2) as "Valor em Estoque (R$)",
                ROUND(COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0)
                      * {hold_rate} * GREATEST(COALESCE(tempo_medio_saldo_atual,0),0) / 365.0, 2) as "CME Acum. (R$)"
             """.format(hold_rate=float(os.getenv("HOLDING_RATE_ANUAL") or 0.25))

        export_sql = text(f"""
            SELECT {query_columns}
            FROM com_fifo_completo 
            WHERE {where_clause}
            ORDER BY 
                MIN(curva_abc) OVER (PARTITION BY CASE WHEN group_id IS NOT NULL AND group_id <> '' THEN group_id ELSE pro_codigo END) ASC,
                MIN(pro_descricao) OVER (PARTITION BY CASE WHEN group_id IS NOT NULL AND group_id <> '' THEN group_id ELSE pro_codigo END) ASC,
                pro_descricao ASC
        """)
        
        import pandas as pd
        import io
        from fastapi.responses import StreamingResponse
        import numpy as np # Para calculos vetoriais se quisermos, mas loop simples resolve

        df = pd.read_sql(export_sql, conn, params=params)
        
        # Sanitize numeric columns to avoid "cannot convert float NaN to integer"
        numeric_cols = ['sgr_codigo', 'estoque_disponivel', 'estoque_min_sugerido', 'estoque_max_sugerido', 'grp_estoque_disponivel', 'grp_estoque_min_sugerido', 'grp_estoque_max_sugerido']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        if coverage_days > 0:
            # LÓGICA DE SIMULAÇÃO (Portada do TypeScript/Go)
            
            def calculate_row(row):
                estoque = float(row['estoque_disponivel'] or 0)
                dbMin = float(row['estoque_min_sugerido'] or 0)
                dbMax = float(row['estoque_max_sugerido'] or 0)
                tipo = (row['tipo_planejamento'] or "Normal").strip()
                curva = (row['curva_abc'] or "C").upper()
                sgr = int(row['sgr_codigo'] or 0)
                alerta = row['alerta_tendencia_alta'] or "Não"

                # 1. Sob Demanda
                if tipo == "Sob_Demanda":
                    return 0, 0, 0, 0, 0

                # 2. Sem politica
                if dbMin == 0 and dbMax == 0:
                    return 0, 0, 0, 0, 0

                # 3. Escalar
                if sgr == 154:
                    ref_dias_map = {"A": 120, "B": 180, "C": 240, "D": 120}
                else:
                    ref_dias_map = {"A": 60, "B": 90, "C": 120, "D": 45}
                
                ref_dias = ref_dias_map.get(curva, 240 if sgr == 154 else 120)
                factor = coverage_days / ref_dias
                
                targetMin = np.ceil(dbMin * factor)
                targetMax = np.ceil(dbMax * factor)
                
                if tipo == "Pouco_Historico":
                    targetMin = np.ceil(targetMin / 2.0)
                    targetMax = np.ceil(targetMax / 2.0)
                
                if targetMax < targetMin: targetMax = targetMin

                if targetMax <= 0 or estoque >= targetMax:
                    sugestao_final = 0
                    sugestao_min = 0
                else:
                    baseNeededMax = targetMax - estoque
                    baseNeededMin = max(0, targetMin - estoque) # Minimo para chegar no Min

                    boost = 1.2 if (alerta == "Sim" and curva in ["A", "B"]) else 1.0
                    
                    valMax = baseNeededMax * boost
                    valMin = baseNeededMin * boost
                    
                    if curva in ["A", "B"]:
                        sugestao_final = np.ceil(valMax)
                        sugestao_min = np.ceil(valMin)
                    else:
                        sugestao_final = round(valMax)
                        sugestao_min = round(valMin)

                return factor, targetMin, targetMax, max(0, sugestao_min), max(0, sugestao_final)

            # Aplicar calculo
            results = df.apply(calculate_row, axis=1, result_type='expand')
            df[['Fator Escala', 'Min Ajustado', 'Max Ajustado', 'Sugestão Min', 'Sugestão Max']] = results
            
            # Formatar e renomear para output final
            df['Dias Cobertura'] = coverage_days
            
            # Selecionar e reordenar colunas finais
            final_columns = {
                'pro_codigo': 'Código',
                'pro_descricao': 'Descrição',
                'curva_abc': 'Curva',
                'tipo_planejamento': 'Planejamento',
                'estoque_disponivel': 'Estoque Atual',
                'estoque_min_sugerido': 'Min Original',
                'estoque_max_sugerido': 'Max Original',
                'Dias Cobertura': 'Dias Cobertura',
                'Fator Escala': 'Fator Escala',
                'Min Ajustado': 'Min Ajustado',
                'Max Ajustado': 'Max Ajustado',
                'Sugestão Min': 'Sugestão Min (Repor Seg.)',
                'Sugestão Max': 'Sugestão Max (Ideal)',
                'fornecedor1': 'Fornecedor',
                'group_id': 'ID Grupo',
                'grp_estoque_disponivel': 'Estoque Grupo',
                'grp_estoque_min_sugerido': 'Min Grupo',
                'grp_estoque_max_sugerido': 'Max Grupo'
            }
            df = df.rename(columns=final_columns)
            # Manter apenas as colunas desejadas na ordem
            desired_order = list(final_columns.values())
            df = df[desired_order]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            sheet_name = 'Simulacao Compra' if coverage_days > 0 else 'Analise Estoque'
            df.to_excel(writer, index=False, sheet_name=sheet_name)
                
        output.seek(0)
        
        filename_prefix = "simulacao_compra" if coverage_days > 0 else "analise_estoque"
        headers = {
            'Content-Disposition': f'attachment; filename="{filename_prefix}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.xlsx"'
        }
        
        return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

    except Exception as e:
        print(f"ERRO EXPORT: {e}")
        # Importante: Logar stacktrace completo em produção
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

