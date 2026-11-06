# -*- coding: utf-8 -*-
"""Rotas de similares: agrupamento de produtos por descricao."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from infra_db import get_db_connection
from modelos import GroupRequest

router = APIRouter()

@router.post("/similar/group")
def criar_grupo_similares(req: GroupRequest):
    """
    Cria um grupo ou adiciona produtos a um grupo existente.
    Se um dos produtos já tiver grupo, usa esse grupo.
    Se múltiplos grupos encontrados, mescla (unifica no primeiro).
    """
    if not req.pro_codigos or len(req.pro_codigos) < 1:
         raise HTTPException(status_code=400, detail="Lista de produtos vazia")
         
    import uuid
    from sqlalchemy import text
    
    try:
        conn = get_db_connection()
        trans = conn.begin()
        
        # 1. Identificar se algum já tem grupo
        current_groups = set()
        for pc in req.pro_codigos:
             res = conn.execute(text("SELECT group_id FROM com_relacionamento_itens WHERE pro_codigo = :c AND origem <> 'avulso'"), {"c": pc.strip()}).scalar()
             if res:
                 current_groups.add(res)
        
        # Definir Group ID Final
        if len(current_groups) == 0:
            final_group_id = str(uuid.uuid4())
        elif len(current_groups) == 1:
            final_group_id = list(current_groups)[0]
        else:
            # Merge: Pick first, update others
            final_group_id = list(current_groups)[0]
            # Atualizar os grupos antigos para o novo (Merge)
            placeholders = ",".join([f"'{g}'" for g in current_groups])
            conn.execute(text(f"UPDATE com_relacionamento_itens SET group_id = :new_g WHERE group_id IN ({placeholders})"), {"new_g": final_group_id})
        
        # Upsert produtos (origem 'manual': a análise une o grupo_chave deles)
        for pc in req.pro_codigos:
            code = pc.strip()
            # Check exist
            exists = conn.execute(text("SELECT 1 FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": code}).scalar()
            if exists:
                conn.execute(text("UPDATE com_relacionamento_itens SET group_id = :g, origem = 'manual' WHERE pro_codigo = :c"), {"g": final_group_id, "c": code})
            else:
                conn.execute(text("INSERT INTO com_relacionamento_itens (group_id, pro_codigo, origem) VALUES (:g, :c, 'manual')"), {"g": final_group_id, "c": code})
        
        trans.commit()
        return {"status": "success", "group_id": final_group_id, "message": "Produtos agrupados com sucesso"}
        
    except Exception as e:
        trans.rollback()
        print(f"Erro agrupamento: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.delete("/similar/ungroup")
def desvincular_produtos(req: GroupRequest):
    """
    Tira os produtos de qualquer grupo, inclusive o da própria descrição: ficam
    'avulso' em com_relacionamento_itens (a análise os planeja sozinhos e o
    auto-agrupamento não os junta de novo) até serem vinculados outra vez.
    No snapshot atual saem do grupo na hora; `restantes` = quem ficou no(s)
    grupo(s), para a tela oferecer a análise do grupo (POST /similar/analise-grupo).
    """
    from sqlalchemy import text
    from vinculos_similares import AVULSO, group_id_avulso
    if not req.pro_codigos:
        raise HTTPException(status_code=400, detail="Lista de produtos vazia")
    codes = sorted({pc.strip() for pc in req.pro_codigos if pc.strip()})
    snapshot = "data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)"
    try:
        conn = get_db_connection()
        trans = conn.begin()

        # Grupos de onde saem (antes de limpar), para devolver quem fica
        chaves = [r[0] for r in conn.execute(text(
            f"SELECT DISTINCT grupo_chave FROM com_fifo_completo "
            f"WHERE {snapshot} AND pro_codigo = ANY(:c) AND grupo_chave IS NOT NULL"), {"c": codes}).fetchall()]

        for code in codes:
            # 1. Marca como avulso (substitui qualquer vínculo, manual ou automático)
            conn.execute(text("""
                INSERT INTO com_relacionamento_itens (group_id, pro_codigo, origem) VALUES (:g, :c, :o)
                ON CONFLICT (pro_codigo) DO UPDATE SET group_id = EXCLUDED.group_id, origem = EXCLUDED.origem
            """), {"g": group_id_avulso(code), "c": code, "o": AVULSO})

        # 2. Sai do grupo consolidado no snapshot atual (planejado pelo mín/máx do próprio item)
        conn.execute(text(f"""
            UPDATE com_fifo_completo
            SET grupo_chave = NULL, grupo_qtd_itens = NULL, grupo_estoque_disponivel = NULL,
                grupo_demanda_dia = NULL, grupo_fator_sazonal = NULL, grupo_curva = NULL,
                grupo_padrao = NULL, grupo_metodo = NULL, grupo_estoque_min = NULL,
                grupo_estoque_max = NULL, grupo_estoque_seguranca = NULL, grupo_mean_size = NULL,
                grupo_cv2 = NULL, grupo_lead_time_dias = NULL, grupo_nivel_servico_custo = NULL,
                grupo_estoque_min_custo = NULL, grupo_estoque_max_custo = NULL, grupo_margem_pct = NULL
            WHERE {snapshot} AND pro_codigo = ANY(:c)
        """), {"c": codes})

        restantes = [r[0] for r in conn.execute(text(
            f"SELECT pro_codigo FROM com_fifo_completo "
            f"WHERE {snapshot} AND grupo_chave = ANY(:k) ORDER BY pro_codigo"), {"k": chaves}).fetchall()] if chaves else []

        # Cabeçalho do grupo que ficou: nº de itens e estoque já sem quem saiu
        # (mín/máx consolidados só mudam com a análise do grupo).
        if chaves:
            conn.execute(text(f"""
                UPDATE com_fifo_completo f
                SET grupo_qtd_itens = t.n, grupo_estoque_disponivel = t.est
                FROM (SELECT grupo_chave, COUNT(*) AS n, SUM(COALESCE(estoque_disponivel, 0)) AS est
                      FROM com_fifo_completo WHERE {snapshot} AND grupo_chave = ANY(:k)
                      GROUP BY grupo_chave) t
                WHERE f.{snapshot} AND f.grupo_chave = t.grupo_chave
            """), {"k": chaves})

        for pc in codes:
            # 3. Limpa dados do agrupamento antigo (group_id / grp_*) na tabela principal
            conn.execute(text("""
                UPDATE com_fifo_completo
                SET
                    group_id = NULL,
                    grp_estoque_disponivel = NULL,
                    grp_qtd_vendida = NULL,
                    grp_valor_vendido = NULL,
                    grp_num_vendas = NULL,
                    grp_vendas_ult_12m = NULL,
                    grp_vendas_12m_ant = NULL,
                    grp_estoque_min_base = NULL,
                    grp_estoque_max_base = NULL,
                    grp_estoque_min_ajustado = NULL,
                    grp_estoque_max_ajustado = NULL,
                    grp_estoque_min_sugerido = NULL,
                    grp_estoque_max_sugerido = NULL,
                    grp_demanda_media_dia = NULL,
                    rateio_prop_grupo = NULL
                WHERE pro_codigo = :c
            """), {"c": pc})

        trans.commit()
        return {"status": "success", "message": "Produtos desvinculados", "restantes": restantes}
    except Exception as e:
        trans.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/similar/recalc")
def recalcular_grupo_especifico(req: GroupRequest):
    """
    Recalcula estatísticas de grupo APENAS para os itens solicitados (e seus irmãos de grupo).
    Não roda a análise completa, apenas agregações de grupo.
    """
    if not req.pro_codigos:
        raise HTTPException(status_code=400, detail="Lista vazia")

    from sqlalchemy import text
    try:
        conn = get_db_connection()
        trans = conn.begin()

        # 1. Descobrir Grupos Envolvidos
        target_groups = set()
        fmt_codes = [f"'{c.strip()}'" for c in req.pro_codigos]
        if fmt_codes:
            sql_find_groups = text(f"SELECT DISTINCT group_id FROM com_relacionamento_itens WHERE pro_codigo IN ({','.join(fmt_codes)}) AND origem <> 'avulso'")
            rows = conn.execute(sql_find_groups).fetchall()
            for r in rows:
                if r[0]: target_groups.add(str(r[0]))
        
        if not target_groups:
             return {"status": "success", "message": "Nenhum grupo encontrado para recalcular (talvez itens avulsos)"}

        # 2. Para cada grupo, recalcular
        for gid in target_groups:
            # Buscar todos os membros
            sql_mems = text("SELECT pro_codigo FROM com_relacionamento_itens WHERE group_id = :gid")
            mems = [r[0] for r in conn.execute(sql_mems, {"gid": gid}).fetchall()]
            
            if not mems: continue
            
            # Buscar dados base de TODOS os membros na com_fifo_completo
            mems_fmt = [f"'{m}'" for m in mems]
            sql_data = text(f"""
                SELECT 
                    pro_codigo, 
                    COALESCE(estoque_disponivel, 0) as est,
                    COALESCE(qtd_vendida, 0) as qtd_vendida,
                    COALESCE(valor_vendido, 0) as valor_vendido,
                    COALESCE(num_vendas, 0) as num_vendas,
                    COALESCE(vendas_ult_12m, 0) as v12,
                    COALESCE(vendas_12m_ant, 0) as v12_ant,
                    COALESCE(estoque_min_base, 0) as min_base,
                    COALESCE(estoque_max_base, 0) as max_base,
                    COALESCE(estoque_min_ajustado, 0) as min_aj,
                    COALESCE(estoque_max_ajustado, 0) as max_aj,
                    COALESCE(estoque_min_sugerido, 0) as min_sug,
                    COALESCE(estoque_max_sugerido, 0) as max_sug,
                    COALESCE(demanda_media_dia, 0) as dem_raw,
                    COALESCE(demanda_media_dia_ajustada, 0) as dem_aj
                FROM com_fifo_completo
                WHERE pro_codigo IN ({','.join(mems_fmt)})
                AND data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
            """)
            
            data_rows = conn.execute(sql_data).mappings().all()
            
            # Calcular Somas
            sum_est = sum(float(x['est']) for x in data_rows)
            sum_qtd = sum(float(x['qtd_vendida']) for x in data_rows)
            sum_val = sum(float(x['valor_vendido']) for x in data_rows)
            sum_num = sum(int(x['num_vendas']) for x in data_rows)
            sum_v12 = sum(float(x['v12']) for x in data_rows)
            sum_v12_ant = sum(float(x['v12_ant']) for x in data_rows)
            
            sum_min_base = sum(int(x['min_base']) for x in data_rows)
            sum_max_base = sum(int(x['max_base']) for x in data_rows)
            sum_min_aj = sum(int(x['min_aj']) for x in data_rows)
            sum_max_aj = sum(int(x['max_aj']) for x in data_rows)
            sum_min_sug = sum(int(x['min_sug']) for x in data_rows)
            sum_max_sug = sum(int(x['max_sug']) for x in data_rows)
            
            # Demanda Média do Grupo (PONDERADA pela Qtd Vendida de cada item)
            # Lógica conforme main.py: GRP_DEMANDA = SUM( ItemDemand * (ItemSales / GroupSales) )
            grp_dem_weighted = 0.0
            if sum_qtd > 0:
                for x in data_rows:
                     # Usando demanda_media_dia (dem_raw) como base, conforme main.py
                     d_raw = float(x['dem_raw'])
                     q = float(x['qtd_vendida'])
                     share = q / sum_qtd
                     grp_dem_weighted += (d_raw * share)
            
            # Atualizar Cada Membro com os Totais + Rateio
            for row in data_rows:
                code = row['pro_codigo']
                indiv_qtd = float(row['qtd_vendida'])
                
                # Rateio
                rateio = 0.0
                if sum_qtd > 0:
                    rateio = indiv_qtd / sum_qtd
                    
                # Update SQL
                sql_upd = text("""
                    UPDATE com_fifo_completo
                    SET
                        group_id = :gid,
                        grp_estoque_disponivel = :g_est,
                        grp_qtd_vendida = :g_qtd,
                        grp_valor_vendido = :g_val,
                        grp_num_vendas = :g_num,
                        grp_vendas_ult_12m = :g_v12,
                        grp_vendas_12m_ant = :g_v12ant,
                        grp_estoque_min_base = :g_minb,
                        grp_estoque_max_base = :g_maxb,
                        grp_estoque_min_ajustado = :g_mina,
                        grp_estoque_max_ajustado = :g_maxa,
                        grp_estoque_min_sugerido = :g_mins,
                        grp_estoque_max_sugerido = :g_maxs,
                        grp_demanda_media_dia = :g_dem,
                        rateio_prop_grupo = :rat
                    WHERE pro_codigo = :code
                      AND data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo) 
                """)
                
                conn.execute(sql_upd, {
                    "gid": gid,
                    "g_est": sum_est,
                    "g_qtd": sum_qtd,
                    "g_val": sum_val,
                    "g_num": sum_num,
                    "g_v12": sum_v12,
                    "g_v12ant": sum_v12_ant,
                    "g_minb": sum_min_base,
                    "g_maxb": sum_max_base,
                    "g_mina": sum_min_aj,
                    "g_maxa": sum_max_aj,
                    "g_mins": sum_min_sug,
                    "g_maxs": sum_max_sug,
                    "g_dem": grp_dem_weighted, # Weighted Average
                    "rat": rateio,
                    "code": code
                })

        trans.commit()
        return {"status": "success", "message": f"Grupos recalculados: {len(target_groups)}"}

    except Exception as e:
        trans.rollback()
        print(f"Erro recalc: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/similar/analise-grupo")
def analisar_grupo_vinculado(req: GroupRequest):
    """
    Análise unificada só do(s) grupo(s) dos produtos informados (ver analise_grupo.py):
    lê do ERP apenas os membros, roda o motor do consolidado e grava no snapshot atual.
    """
    import estado_job
    if estado_job.em_execucao():
        raise HTTPException(status_code=409, detail="Análise completa em execução; tente quando terminar.")
    from analise_grupo import analisar_grupo
    try:
        return analisar_grupo(req.pro_codigos)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Erro análise de grupo: {e}")
        raise HTTPException(status_code=500, detail=f"Erro na análise do grupo: {e}")

@router.post("/similar/auto-group")
def trigger_auto_group():
    """
    Dispara a rotina de auto-agrupamento manualmente.
    """
    try:
        # Import dynamic to reuse logic from main if possible, or reimplement
        # Since main.py has the logic, let's try to import or copy. 
        # Copying logic to avoid circular imports or heavy deps on API request
        # Actually, let's just implement the logic here cleanly or import.
        # Calling main.agrupar_similares_automaticamente()
        from main import agrupar_similares_automaticamente
        agrupar_similares_automaticamente()
        return {"status": "success", "message": "Auto-agrupamento finalizado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro auto-agrupamento: {str(e)}")

