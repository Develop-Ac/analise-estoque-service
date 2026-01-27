from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
import io
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional
import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()
# ==========================================
# CONFIGURAÇÕES
# ==========================================
# CONFIGURAÇÕES
# ==========================================
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://usuario:senha@host:5432/database")

SQL_HOST = os.getenv('SQL_HOST', '127.0.0.1')
SQL_PORT = os.getenv('SQL_PORT', '1433')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'master')
SQL_USER = os.getenv('SQL_USER', 'sa')
SQL_PASSWORD = os.getenv('SQL_PASSWORD', 'senha_secreta')
TDS_VERSION = os.getenv('TDS_VERSION', '7.4')

# ==========================================
# SETUP
# ==========================================
app = FastAPI(title="API Analise Estoque", description="API Read-only para dados de analise de estoque")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    engine = create_engine(POSTGRES_URL)
    return engine.connect()

    return engine.connect()

def get_sql_connection():
    """Conexão com SQL Server (ERP) via FreeTDS/PyODBC"""
    sql_driver = os.getenv('SQL_DRIVER', '{FreeTDS}')
    if "FreeTDS" in sql_driver:
        conn_str = (
            f"DRIVER={sql_driver};"
            f"SERVER={SQL_HOST};"
            f"PORT={SQL_PORT};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={{{SQL_USER}}};"
            f"PWD={{{SQL_PASSWORD}}};"
        )
    else:
        conn_str = (
            f"DRIVER={sql_driver};"
            f"SERVER={SQL_HOST},{SQL_PORT};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={{{SQL_USER}}};"
            f"PWD={{{SQL_PASSWORD}}};"
        )
    return pyodbc.connect(conn_str)

def get_realtime_stocks(pro_codes):
    """
    Busca o estoque atual (saldo) para uma lista de códigos de produto
    usando OPENQUERY no SQL Server (Linked Server CONSULTA)
    """
    if not pro_codes:
        return {}
        
    # Formata lista para SQL: 'COD1', 'COD2'
    # Sanitize inputs (simple replace of ' to '')
    safe_codes = [c.replace("'", "") for c in pro_codes]
    codes_str = "','" .join(safe_codes)
    
    # Query interna a ser executada no Linked Server
    # Buscando da tabela PRODUTOS (saldo atual)
    # Supondo coluna PRO_CODIGO e ESTOQUE_DISPONIVEL (conforme visto no main.py)
    # main.py usa: SELECT ... pro.estoque_disponivel FROM produtos pro ...
    
    # Preparing the IN clause for SQL Server OPENQUERY
    # We need to escape single quotes by doubling them '' because we are inside an OPENQUERY string
    
    # 1. Start with safe codes (already stripped of single quotes)
    # 2. Join them with "','" to create the list for the IN clause: COD1','COD2
    # 3. For OPENQUERY, we need to double the single quotes: COD1'',''COD2
    
    # Let's rebuild properly:
    # We want final customized SQL: ... IN (''COD1'', ''COD2'') ...
    
    formatted_codes_list = [f"''{c}''" for c in safe_codes]
    in_clause_inner = ", ".join(formatted_codes_list)

    inner_query = f"SELECT pro_codigo, estoque_disponivel FROM produtos WHERE pro_codigo IN ({in_clause_inner}) AND empresa = 3"
    
    # Query final
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner_query}')"
    
    conn = get_sql_connection()
    stock_map = {}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        for row in rows:
            # row[0] = pro_codigo, row[1] = estoque_disponivel
            # Normalizar codigo (strip)
            if row[0]:
                code = str(row[0]).strip()
                qty = float(row[1]) if row[1] is not None else 0.0
                stock_map[code] = qty
                
    except Exception as e:
        print(f"Erro no SQL Server: {e}")
        raise e
    finally:
        conn.close()
        
    return stock_map

# ==========================================
# MODELOS
# ==========================================
class LoteEstoque(BaseModel):
    data_compra: str # YYYY-MM-DD
    qtd: float
    dias_em_estoque: int
class AnaliseItem(BaseModel):
    id: int
    pro_codigo: str
    pro_descricao: str
    sgr_codigo: Optional[int]
    sgr_descricao: Optional[str]
    mar_descricao: Optional[str]
    fornecedor1: Optional[str]
    estoque_disponivel: Optional[float]
    demanda_media_dia: Optional[float]
    demanda_media_dia_ajustada: Optional[float]
    tempo_medio_estoque: Optional[float]
    data_min_venda: Optional[str]
    data_max_venda: Optional[str]
    qtd_vendida: Optional[float]
    curva_abc: Optional[str]
    categoria_estocagem: Optional[str]
    estoque_min_sugerido: Optional[float]
    estoque_max_sugerido: Optional[float]
    tipo_planejamento: Optional[str]
    teve_alteracao_analise: Optional[bool]
    data_processamento: Optional[str]
    dias_ruptura: Optional[float]
    fator_tendencia: Optional[float]
    tendencia_label: Optional[str]
    tendencia_label: Optional[str]
    alerta_tendencia_alta: Optional[str] # Sim/Não
    dados_alteracao_json: Optional[str] # JSON string
    # Group fields
    group_id: Optional[str] = None
    group_count: Optional[int] = 0
    is_grouped_view: Optional[bool] = False
    original_stock: Optional[float] = None
    
    # Persisted Group Fields
    grp_estoque_disponivel: Optional[float] = None
    grp_qtd_vendida: Optional[float] = None
    grp_valor_vendido: Optional[float] = None
    grp_num_vendas: Optional[int] = None
    grp_vendas_ult_12m: Optional[float] = None
    grp_vendas_12m_ant: Optional[float] = None
    grp_estoque_min_base: Optional[int] = None
    grp_estoque_max_base: Optional[int] = None
    grp_estoque_min_ajustado: Optional[int] = None
    grp_estoque_max_ajustado: Optional[int] = None
    grp_estoque_min_sugerido: Optional[int] = None
    grp_estoque_max_sugerido: Optional[int] = None
    grp_demanda_media_dia: Optional[float] = None
    rateio_prop_grupo: Optional[float] = None
    rateio_prop_grupo: Optional[float] = None
    tempo_medio_saldo_atual: Optional[float] = None
    categoria_saldo_atual: Optional[str] = None
    
    # Detailed Stock Info
    estoque_obsoleto: Optional[float] = 0
    lotes_estoque: Optional[List[LoteEstoque]] = []

class PaginatedResponse(BaseModel):
    data: List[AnaliseItem]
    total: int
    page: int
    limit: int
    total_pages: int

class GroupRequest(BaseModel):
    pro_codigos: List[str]

class SimulationRequest(BaseModel):
    pro_codigos: List[str]
    coverage_days: int

class PromoPlanRequest(BaseModel):
    days: int = 60
    subgroups: Optional[List[str]] = None
    brands: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    grouped_view: bool = False

@app.post("/analise/simular", response_model=List[AnaliseItem])
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

@app.post("/promo/plan", response_model=List[AnaliseItem])
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

def _get_promotion_data(req: PromoPlanRequest):
    conn = get_db_connection()
    try:
        # 1. Filtros Basicos
        filters = ["data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)"]
        params = {"days": float(req.days)}
        
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

        # 2. Logica de Calculo de Excesso (com base nos dias)
        
        # Ref Dias Expression (Same as listar_analise)
        indiv_ref_expr = """
            (CASE 
                WHEN sgr_codigo = 154 THEN 
                    (CASE WHEN curva_abc = 'A' THEN 120.0 WHEN curva_abc = 'B' THEN 180.0 ELSE 240.0 END)
                ELSE 
                    (CASE WHEN curva_abc = 'A' THEN 60.0 WHEN curva_abc = 'B' THEN 90.0 ELSE 120.0 END)
            END)
        """
        
        where_basic = " AND ".join(filters)
        
        sql = text(f"""
            WITH base_items AS (
                 SELECT 
                    *,
                    ({indiv_ref_expr}) as ref_days,
                    (:days / {indiv_ref_expr}) as factor,
                    CEIL(estoque_max_sugerido * (:days / {indiv_ref_expr})) as sim_max_individual,
                    (estoque_disponivel - CEIL(estoque_max_sugerido * (:days / {indiv_ref_expr}))) as excess_qty
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

        return results
    finally:
        conn.close()

@app.post("/promo/plan", response_model=List[AnaliseItem])
def planejar_promocao(req: PromoPlanRequest):
    """
    Retorna lista de produtos com estoque excedente para o periodo informado (dias).
    """
    try:
        return _get_promotion_data(req)
    except Exception as e:
        print(f"Erro promo plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/promo/export")
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
            "sim_max_individual": "Máximo Simulado",
            "excess_qty": "Quantidade Excedente",
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
@app.post("/similar/group")
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
             res = conn.execute(text("SELECT group_id FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": pc.strip()}).scalar()
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
        
        # Upsert produtos
        for pc in req.pro_codigos:
            code = pc.strip()
            # Check exist
            exists = conn.execute(text("SELECT 1 FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": code}).scalar()
            if exists:
                conn.execute(text("UPDATE com_relacionamento_itens SET group_id = :g WHERE pro_codigo = :c"), {"g": final_group_id, "c": code})
            else:
                conn.execute(text("INSERT INTO com_relacionamento_itens (group_id, pro_codigo) VALUES (:g, :c)"), {"g": final_group_id, "c": code})
        
        trans.commit()
        return {"status": "success", "group_id": final_group_id, "message": "Produtos agrupados com sucesso"}
        
    except Exception as e:
        trans.rollback()
        print(f"Erro agrupamento: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/similar/ungroup")
def desvincular_produtos(req: GroupRequest):
    """
    Remove produtos de qualquer grupo que participem.
    Também limpa os dados de grupo na tabela principal.
    """
    from sqlalchemy import text
    try:
        conn = get_db_connection()
        trans = conn.begin()
        
        for pc in req.pro_codigos:
            # 1. Remove do relacionamento
            conn.execute(text("DELETE FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": pc.strip()})
            
            # 2. Limpa dados de grupo na tabela principal
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
            """), {"c": pc.strip()})
            
        trans.commit()
        return {"status": "success", "message": "Produtos desvinculados"}
    except Exception as e:
        trans.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/similar/recalc")
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
            sql_find_groups = text(f"SELECT DISTINCT group_id FROM com_relacionamento_itens WHERE pro_codigo IN ({','.join(fmt_codes)})")
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

@app.post("/similar/auto-group")
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

@app.get("/subgroups")
def listar_subgrupos():
    """
    Retorna lista de subgrupos disponiveis na analise atual.
    """
    try:
        conn = get_db_connection()
        # Buscar distinct sgr_descricao que não seja nulo, ordenado
        sql = text("SELECT DISTINCT sgr_descricao FROM com_fifo_completo WHERE sgr_descricao IS NOT NULL ORDER BY sgr_descricao")
        rows = conn.execute(sql).fetchall()
        
        # Retorna lista simples de strings
        return [row[0] for row in rows if row[0]]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals(): conn.close()

@app.get("/brands")
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

@app.get("/categories")
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

@app.get("/analise", response_model=PaginatedResponse)
def listar_analise(
    page: int = 1,
    limit: int = 50,
    search: Optional[str] = None,
    pro_codigos: Optional[str] = Query(None, description="Lista de códigos separados por virgula"),
    marca: Optional[str] = None,
    subgrupo: Optional[str] = None,
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None,
    group_id: Optional[str] = None,
    match_type: str = "contains",
    coverage_days: Optional[int] = None,
    grouped_view: bool = False
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
        
        # Query Dados
        data_sql = text(f"""
            SELECT 
                id, pro_codigo, pro_descricao, sgr_codigo, sgr_descricao, mar_descricao, fornecedor1,
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
                tempo_medio_saldo_atual, categoria_saldo_atual
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

        
        total_pages = (total + limit - 1) // limit if limit > 0 else 0
        
        return {
            "data": data_list,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages
        }
    except Exception as e:
        print(f"ERRO GERAL API: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        conn.close()

@app.get("/analise/export")
def exportar_analise(
    search: Optional[str] = None,
    pro_codigos: Optional[str] = Query(None),
    marca: Optional[str] = None,
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None,
    coverage_days: int = 0,  # Novo Parametro
    group_id: Optional[str] = None,
    match_type: str = "contains"
):
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro banco: {e}")

    try:
        where_clauses = ["1=1"]
        params = {}
        
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
                categoria_saldo_atual as "Cat. Saldo Atual"
             """

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

@app.get("/", response_class=HTMLResponse)
def root():
    state = load_state()
    last_run = state.get("last_run", "Nunca executado")
    
    html_content = f"""
    <html>
        <head>
            <title>Analise Estoque Service</title>
            <style>
                body {{ font-family: Arial, sans-serif; padding: 40px; text-align: center; }}
                .status {{ padding: 20px; background-color: #dff0d8; color: #3c763d; border-radius: 5px; margin: 20px 0; }}
                .info {{ color: #666; }}
            </style>
        </head>
        <body>
            <h1>Analise Estoque Service</h1>
            <div class="status">
                <h2>✓ Serviço Operante</h2>
                <p>Status: Online e Aguardando Requisições</p>
            </div>
            <div class="info">
                <p><strong>Última Análise:</strong> {last_run}</p>
                <p><strong>Próxima Verificação Automática:</strong> A cada 7 dias</p>
            </div>
            <p><a href="/docs">Ver Documentação da API</a></p>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
def health_check():
    return {"status": "ok"}

# ==========================================
# BACKGROUND TASK
# ==========================================
# ==========================================
# BACKGROUND TASK
# ==========================================
import threading
import time
import datetime
import json
from pathlib import Path

BACKGROUND_Running = False
# CORREÇÃO: Usar caminho absoluto relativo ao arquivo para consistência
BASE_DIR = Path(__file__).resolve().parent
ARQUIVO_ESTADO = BASE_DIR / "data" / "fifo_service_state.json"
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS', 7))

def load_state():
    if not ARQUIVO_ESTADO.exists():
        print(f"DEBUG: Arquivo de estado não encontrado em {ARQUIVO_ESTADO}")
        return {}
    try:
        with open(ARQUIVO_ESTADO, "r") as f:
            state = json.load(f)
            print(f"DEBUG: Estado carregado de {ARQUIVO_ESTADO}: {state}")
            return state
    except Exception as e:
        print(f"DEBUG: Erro ao ler estado: {e}")
        return {}

def save_state(state):
    try:
        with open(ARQUIVO_ESTADO, "w") as f:
            json.dump(state, f)
            print(f"DEBUG: Estado salvo em {ARQUIVO_ESTADO}")
    except Exception as e:
        print(f"Erro ao salvar estado: {e}")

def background_scheduler():
    global BACKGROUND_Running
    print("Iniciando scheduler de background (Regra: Domingo >= 14:00)...")
    print(f"Monitorando arquivo de estado: {ARQUIVO_ESTADO}")
    
    # Garantir que pasta data existe
    if ARQUIVO_ESTADO.parent.name == "data":
        try:
            os.makedirs(ARQUIVO_ESTADO.parent, exist_ok=True)
        except Exception:
            pass

    # Lazy import para evitar falha de startup da API se houver erro de dependencia no main.py
    try:
        from main import run_job
    except ImportError as e:
        print(f"ERRO CRITICO: Nao foi possivel importar main.py. O job nao rodara. Erro: {e}")
        return
    except Exception as e:
        print(f"ERRO CRITICO: Erro ao carregar main.py: {e}")
        return

    while True:
        try:
            now = datetime.datetime.now()
            
            # Regra: Domingo (6) e Hora >= 14
            is_sunday = (now.weekday() == 6)
            is_time = (now.hour >= 14)

            # Para teste/debug pode-se forçar com variavel de ambiente ou checar aqui
            
            if is_sunday and is_time:
                state = load_state()
                last_run_str = state.get("last_run")
                should_run = False
                
                if not last_run_str:
                    print(f"Agendamento: Nenhuma execução registrada. Domingo detectado. Rodando...")
                    should_run = True
                else:
                    try:
                        last_run = datetime.datetime.fromisoformat(last_run_str)
                        # Se a última execução não foi HOJE, então roda.
                        if last_run.date() != now.date():
                            print(f"Agendamento: Última execução foi {last_run}. Rodando job de Domingo agora...")
                            should_run = True
                        else:
                            # Já rodou hoje
                            pass
                    except ValueError:
                        print("Agendamento: Erro ao parsear data anterior. Forçando execução de Domingo...")
                        should_run = True
                
                if should_run:
                    if not BACKGROUND_Running:
                        BACKGROUND_Running = True
                        try:
                            print(f">>> Iniciando execução do Job FIFO: {now}")
                            run_job()
                            state = load_state()
                            state["last_run"] = datetime.datetime.now().isoformat()
                            save_state(state)
                            print(">>> Job FIFO finalizado com sucesso.")
                        except Exception as e:
                            print(f"Erro ao rodar job: {e}")
                        finally:
                            BACKGROUND_Running = False
                    else:
                        print("Job já está rodando. Ignorando trigger.")
            else:
                # Opcional: Log apenas 1 vez por hora se nao for domingo
                # if now.minute == 0:
                #    print(f"Aguardando Domingo 14hs. Agora: {now}")
                pass
                
        except Exception as e:
            print(f"Erro no background scheduler: {e}")
            BACKGROUND_Running = False
            
        # Verifica a cada 10 minutos (600s) para não perder a janela, mas sem busy-wait e sem flood
        time.sleep(600) 

@app.on_event("startup")
def startup_event():
    print("API Analise Estoque iniciada na porta 8000")
    
    # -----------------------------------------------------------
    # MIGRATION: Ensure 'dados_alteracao_json' column exists
    # -----------------------------------------------------------
    try:
        conn = get_db_connection()
        # Check if column exists
        check_sql = text("SELECT column_name FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='dados_alteracao_json'")
        exists = conn.execute(check_sql).scalar()
        
        if not exists:
            print("MIGRATION: Adding 'dados_alteracao_json' column to 'com_fifo_completo'...")
            conn.execute(text("ALTER TABLE com_fifo_completo ADD COLUMN dados_alteracao_json TEXT"))
            conn.commit()
            print("MIGRATION: Column added successfully.")
        
        conn.close()
    except Exception as e:
        print(f"WARNING: Database migration failed: {e}")

    # Inicia a thread de background
    thread = threading.Thread(target=background_scheduler, daemon=True)
    thread.start()

def _get_stock_batches(pro_codes):
    """
    Busca os lotes de estoque na tabela com_data_saldo_produto
    """
    if not pro_codes: return {}
    
    conn = get_db_connection()
    try:
        # Sanitize
        fmt_codes = [f"'{str(c).replace('\'','')}'" for c in pro_codes]
        in_clause = ",".join(fmt_codes)
        
        sql = text(f"""
            SELECT pro_codigo, data_compra, saldo_residual
            FROM com_data_saldo_produto
            WHERE pro_codigo IN ({in_clause})
            ORDER BY data_compra ASC
        """)
        
        rows = conn.execute(sql).fetchall()
        
        from datetime import date
        today = date.today()
        
        result = {}
        for r in rows:
            code = r[0] 
            dt = r[1]
            qty = float(r[2])
            
            if code not in result: result[code] = []
            
            days = (today - dt).days if dt else 0
            
            result[code].append(LoteEstoque(
                data_compra=str(dt),
                qtd=qty,
                dias_em_estoque=days
            ))
            
        return result
    except Exception as e:
        print(f"Erro ao buscar lotes: {e}")
        return {}
    finally:
        conn.close()
