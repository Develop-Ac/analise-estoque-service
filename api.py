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
    # SQLAlchemy 2.x não aceita o alias 'postgres://' — normaliza para 'postgresql://'
    url = POSTGRES_URL.replace("postgres://", "postgresql://")
    engine = create_engine(url)
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
    id: Optional[int] = None
    pro_codigo: Optional[str] = None
    pro_descricao: Optional[str] = None
    pro_referencia: Optional[str] = None
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

    # Estoque de segurança estatístico / ABC-XYZ / venda perdida
    demanda_real_dia: Optional[float] = None
    sigma_demanda_dia: Optional[float] = None
    cv_demanda: Optional[float] = None
    classe_xyz: Optional[str] = None
    estoque_seguranca: Optional[int] = None
    nivel_servico_z: Optional[float] = None
    lead_time_dias: Optional[int] = None
    venda_perdida_12m: Optional[float] = None
    valor_vendido_12m: Optional[float] = None
    padrao_demanda: Optional[str] = None
    metodo_reposicao: Optional[str] = None
    fator_sazonal: Optional[float] = None
    demanda_planejamento_dia: Optional[float] = None

    # Consolidação por grupo (descrição) + memória de cálculo
    grupo_chave: Optional[str] = None
    grupo_estoque_min: Optional[int] = None
    grupo_estoque_max: Optional[int] = None
    grupo_demanda_dia: Optional[float] = None
    grupo_estoque_seguranca: Optional[int] = None
    grupo_curva: Optional[str] = None
    grupo_metodo: Optional[str] = None
    grupo_fator_sazonal: Optional[float] = None
    # Nível de serviço econômico (razão crítica / newsvendor) — modo sombra
    custo_unitario: Optional[float] = None
    margem_unitaria: Optional[float] = None
    margem_pct: Optional[float] = None
    nivel_servico_custo: Optional[float] = None
    z_custo: Optional[float] = None
    estoque_min_custo: Optional[int] = None
    estoque_max_custo: Optional[int] = None
    estoque_seg_custo: Optional[int] = None
    grupo_nivel_servico_custo: Optional[float] = None
    grupo_estoque_min_custo: Optional[int] = None
    grupo_estoque_max_custo: Optional[int] = None
    grupo_margem_pct: Optional[float] = None
    memoria: Optional[dict] = None
    memoria_grupo: Optional[dict] = None

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

@app.get("/analise/comparacao-custo")
def comparacao_nivel_servico_custo():
    """
    Comparação AGREGADA entre o nível de serviço da CURVA (oficial) e o de CUSTO
    (razão crítica / newsvendor — modo sombra). Só leitura; alimenta a tela de
    comparação. Considera itens com nivel_servico_custo preenchido (têm margem/custo
    confiáveis) e fora de 'sob encomenda'. Capital = ponto de pedido × custo unitário.
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
                grupo_chave, grupo_estoque_min, grupo_estoque_max, grupo_demanda_dia,
                grupo_estoque_seguranca, grupo_curva, grupo_metodo, grupo_fator_sazonal,
                grupo_mean_size, grupo_cv2,
                custo_unitario, margem_unitaria, margem_pct,
                nivel_servico_custo, z_custo, estoque_min_custo, estoque_max_custo, estoque_seg_custo,
                grupo_nivel_servico_custo, grupo_estoque_min_custo, grupo_estoque_max_custo, grupo_margem_pct
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
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS') or 7)

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
        fmt_codes = ["'" + str(c).replace("'", "") + "'" for c in pro_codes]
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


# ==========================================
# SUGESTÃO DE COMPRA (ponto de pedido)
# ==========================================
import time as _time_rt
import concurrent.futures as _futures
_STOCK_CACHE = {"ts": 0.0, "data": None}
_STOCK_TTL_S = int(os.getenv("STOCK_RT_TTL_S") or 120)  # 2 min
_RT_EXECUTOR = _futures.ThreadPoolExecutor(max_workers=2)  # p/ timeout rígido do realtime


def get_all_realtime_stocks(force=False):
    """Estoque atual de TODOS os produtos ativos (empresa 3) numa query só.
    Cacheado por processo (TTL curto) p/ toggles de filtro não baterem no ERP a cada request."""
    now = _time_rt.time()
    if not force and _STOCK_CACHE["data"] is not None and (now - _STOCK_CACHE["ts"]) < _STOCK_TTL_S:
        return _STOCK_CACHE["data"]
    inner = ("SELECT pro_codigo, estoque_disponivel FROM produtos "
             "WHERE empresa = 3 AND UPPER(inativo) = 'N' AND UPPER(comercializavel) = 'S'")
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39)*2)}')"
    conn = get_sql_connection()
    # timeout de consulta: se o ERP estiver lento, estoura e o endpoint cai no snapshot
    try:
        conn.timeout = int(os.getenv("STOCK_RT_TIMEOUT_S") or 15)
    except Exception:
        pass
    m = {}
    try:
        cur = conn.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            if row[0] is not None:
                m[str(row[0]).strip()] = float(row[1]) if row[1] is not None else 0.0
    finally:
        conn.close()
    _STOCK_CACHE["ts"] = now
    _STOCK_CACHE["data"] = m
    return m


def get_realtime_stocks_bulk(codes, chunk=400):
    """Estoque atual só de uma LISTA de produtos, em lotes (IN-list) — rápido quando
    o conjunto é pequeno (ex.: após filtrar por subgrupo/fornecedor)."""
    codes = list({str(x).strip() for x in codes if x is not None and str(x).strip()})
    m = {}
    for i in range(0, len(codes), chunk):
        try:
            m.update(get_realtime_stocks(codes[i:i + chunk]))
        except Exception as e:
            print(f"AVISO: falha no lote de estoque realtime ({i}): {e}")
    return m


import time as _time
_HIST_CACHE = {"ts": 0.0, "data": None}
_HIST_TTL_S = int(os.getenv("COMPRAS_HIST_TTL_S") or 21600)  # 6h
SEM_HIST_COMPRA = "SEM HISTÓRICO DE COMPRA"


def get_compras_historico(force=False):
    """
    Lista PRODUTO -> fornecedores de quem JÁ COMPRAMOS, LIDA DO MONGO (coleção
    compras_fornecedor), empacotada pelo batch (main.atualizar_compras_fornecedor_mongo).
    NÃO toca no ERP no caminho do request — leitura rápida + cache por processo.
    Se o Mongo estiver vazio/indisponível, retorna {} (a tela carrega, sem
    agrupamento por fornecedor, até o batch popular).

    Retorna: {pro_codigo: [(for_nome, qtd_comprada), ...] ordenado por qtd desc}.
    """
    now = _time.time()
    cached = _HIST_CACHE.get("data")
    if not force and cached is not None and (now - _HIST_CACHE["ts"]) < _HIST_TTL_S:
        return cached
    hist = {}
    try:
        import empacotamento as emp
        hist = emp.carregar_compras_fornecedor()
    except Exception as e:
        print(f"AVISO: lista compras x fornecedor (Mongo) indisponível: {e}")
        hist = {}
    _HIST_CACHE["ts"] = now
    _HIST_CACHE["data"] = hist
    return hist


def _sug_norm(s):
    return (str(s).strip() if s is not None else "")


def _sug_float(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    if f != f:  # NaN
        return 0.0
    return f


def _sug_posicao(it, stock_map, usar_rt):
    """Estoque (realtime se disponível, senão snapshot) + em trânsito de UM item."""
    cod = _sug_norm(it.get("pro_codigo"))
    est = stock_map.get(cod) if usar_rt else None
    estoque = float(est) if est is not None else _sug_float(it.get("estoque_snapshot"))
    transito = _sug_float(it.get("em_transito"))
    return cod, estoque, transito


# Z (nível de serviço) por curva — espelha Z_POR_CURVA do main.py.
_Z_POR_CURVA = {"A": 2.054, "B": 1.645, "C": 1.282, "D": 1.036}
# Nível de serviço (probabilidade) por curva — espelha NS_POR_CURVA do main.py.
_NS_POR_CURVA = {"A": 0.98, "B": 0.95, "C": 0.90, "D": 0.85}
_ALPHA_CROSTON = 0.1

# Cobertura por classe (dias de ciclo) — espelha REGRAS_DIAS do main.py.
_REGRAS_DIAS = {
    "default": {"A": (20, 60), "B": (30, 90), "C": (45, 120), "D": (0, 45)},
    154:       {"A": (45, 120), "B": (60, 180), "C": (90, 240), "D": (0, 120)},
}


def _dias_ciclo(curva, sgr_codigo):
    try:
        sgr = int(sgr_codigo)
    except (TypeError, ValueError):
        sgr = None
    regra = _REGRAS_DIAS.get(sgr, _REGRAS_DIAS["default"])
    dmin, dmax = regra.get(_sug_norm(curva).upper(), regra.get("C", (45, 120)))
    return max(dmax - dmin, 1), dmin, dmax


def montar_memoria_calculo(*, escopo, minimo, maximo, curva, classe, metodo,
                           demanda_dia, sigma_dia, z, lead_time, ss, fator_sazonal,
                           sgr_codigo, msize=None, cv2=None, membros=None,
                           ns_custo=None, z_custo=None, min_custo=None, max_custo=None,
                           ss_custo=None, margem_pct=None, custo_unit=None):
    """
    Memória de cálculo do mín/máx: fórmula + valores REAIS que compuseram a
    quantidade. escopo='grupo'|'item'. `membros` (grupo) = contribuição por marca.
    """
    ciclo, _dmin, _dmax = _dias_ciclo(curva, sgr_codigo)
    comp = [
        {"rotulo": "Demanda planejada", "valor": round(_sug_float(demanda_dia), 4), "unid": "un/dia"},
        {"rotulo": "Lead time (reposição)", "valor": int(_sug_float(lead_time)), "unid": "dias"},
        {"rotulo": "Estoque de segurança", "valor": int(_sug_float(ss)), "unid": "un"},
        {"rotulo": "Dias de ciclo (cobertura do máximo)", "valor": ciclo, "unid": "dias"},
    ]
    if _sug_float(z):
        comp.append({"rotulo": "Nível de serviço (Z)", "valor": round(_sug_float(z), 3)})
    if _sug_float(sigma_dia):
        comp.append({"rotulo": "σ da demanda/dia", "valor": round(_sug_float(sigma_dia), 4)})
    fs = _sug_float(fator_sazonal)
    if fs and abs(fs - 1.0) >= 0.01:
        comp.append({"rotulo": "Fator sazonal (próximo período)", "valor": round(fs, 3), "unid": "x"})
    mem = {
        "escopo": escopo,
        "minimo": int(_sug_float(minimo)),
        "maximo": int(_sug_float(maximo)),
        "curva": curva,
        "classe": classe,
        "metodo": metodo or "Normal (Z·σ·√LT)",
        "formula": ("Mínimo (ponto de pedido) = demanda × lead time + estoque de segurança.  "
                    "Máximo = Mínimo + demanda × dias de ciclo."),
        "componentes": comp,
    }

    # ----- Derivação do MÉTODO DE DEMANDA (passo a passo, com números reais) -----
    lt = _sug_float(lead_time)
    dem = _sug_float(demanda_dia)
    ss_v = _sug_float(ss)
    mmin = int(_sug_float(minimo))
    mmax = int(_sug_float(maximo))
    met = metodo or "Normal (Z·σ·√LT)"
    intermit = any(k in met for k in ("Croston", "Poisson", "Binomial"))
    passos = []
    if intermit:
        ns = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        dem_c = dem * (1 - _ALPHA_CROSTON / 2.0)
        lam = dem_c * lt
        dist = "Binomial Negativa" if "Binomial" in met else "Poisson"
        passos = [
            f"Demanda intermitente/grumosa → Croston + {dist} composta (não usa a Normal).",
            f"Demanda corrigida (Croston, 1−α/2) = {round(dem, 4)} × {round(1 - _ALPHA_CROSTON / 2.0, 3)} = {round(dem_c, 4)} un/dia.",
            f"λ (esperado no lead time) = demanda corrigida × lead time = {round(dem_c, 4)} × {int(lt)} = {round(lam, 2)} un.",
            f"Ponto de pedido (mín) = quantil {int(ns * 100)}% da {dist} da demanda no lead time = {mmin} un.",
            f"Estoque de segurança = ponto de pedido − λ = {mmin} − {round(lam, 2)} = {int(ss_v)} un.",
            f"Máximo = quantil {int(ns * 100)}% no horizonte (lead time + dias de ciclo) = {mmax} un.",
        ]
    else:
        z_v = _sug_float(z)
        sig = _sug_float(sigma_dia)
        if sig > 0 and z_v > 0:
            passos.append(f"Estoque de segurança = Z × σ × √(lead time) = {round(z_v, 3)} × {round(sig, 4)} × √{int(lt)} = {int(ss_v)} un.")
        else:
            passos.append(f"Estoque de segurança (Z·σ·√lead time) = {int(ss_v)} un.")
        passos.append(f"Ponto de pedido (mín) = demanda × lead time + SS = {round(dem, 4)} × {int(lt)} + {int(ss_v)} = {mmin} un.")
        passos.append(f"Máximo = mín + demanda × dias de ciclo = {mmin} + {round(dem, 4)} × {ciclo} = {mmax} un.")
    mem["metodo_calculo"] = {"tipo": met, "passos": passos}

    # ----- Dados para o GRÁFICO (igual ao manual) -----
    import math as _math
    if intermit:
        ns = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        lam = _sug_float(demanda_dia) * (1 - _ALPHA_CROSTON / 2.0) * lt
        kmax = max(mmin + 5, 8)

        def _cdf_arr(mean, var):
            """CDF[0..kmax] de Poisson (var≈mean) ou Binomial Negativa (var>mean).
            Espelha _quantil_demanda do main.py (var≥média; limiar Poisson 1.10)."""
            out = []; acc = 0.0
            if mean <= 0:
                return [1.0] * (kmax + 1)
            var = max(var, mean)
            if var <= mean * 1.10:
                pmf = _math.exp(-mean)
                for k in range(kmax + 1):
                    acc += pmf; out.append(min(acc, 1.0)); pmf = pmf * mean / (k + 1)
            else:
                r = mean * mean / (var - mean); pr = r / (r + mean); pmf = pr ** r
                for k in range(kmax + 1):
                    acc += pmf; out.append(min(acc, 1.0)); pmf = pmf * (k + r) / (k + 1) * (1 - pr)
            return out

        msize_v = _sug_float(msize)
        cv2_v = _sug_float(cv2)
        if msize_v > 0:
            # EXATO: mesma dispersão do modelo (disp = tam médio × (1+CV²); var = λ·disp)
            disp = max(msize_v * (1.0 + cv2_v), 1.0)
            var = lam * disp
            exato = True
        else:
            # Fallback (dados sem mean_size/cv2): ajusta var p/ o quantil NS = ponto de pedido
            lo, hi, var = lam, max(lam * 40, lam + 1.0), lam
            if mmin > 0 and lam > 0:
                for _ in range(40):
                    mid = (lo + hi) / 2.0
                    cdf = _cdf_arr(lam, mid)
                    q = next((k for k in range(len(cdf)) if cdf[k] >= ns), kmax)
                    if q >= mmin:
                        hi = mid; var = mid
                    else:
                        lo = mid
            exato = False
        cdf = _cdf_arr(lam, var)
        barras = []; prev = 0.0
        for k in range(kmax + 1):
            pmf = max(cdf[k] - prev, 0.0); prev = cdf[k]
            barras.append({"k": k, "pmf": round(pmf, 4), "cdf": round(cdf[k], 4)})
        mem["graf"] = {"tipo": "distribuicao", "dist": ("Binomial Negativa" if var > lam * 1.05 else "Poisson"),
                       "nivel_servico": ns, "ponto_pedido": mmin, "lambda": round(lam, 2),
                       "exato": exato, "barras": barras}
    else:
        mem["graf"] = {"tipo": "serra", "maximo": mmax, "minimo": mmin, "seguranca": int(ss_v),
                       "demanda_dia": round(dem, 4), "lead_time": int(lt), "ciclo": ciclo}

    # ----- Comparação com o NÍVEL DE SERVIÇO POR CUSTO (razão crítica, modo sombra) -----
    if ns_custo is not None:
        nsc = _sug_float(ns_custo)
        cmin = int(_sug_float(min_custo)); cmax = int(_sug_float(max_custo))
        ns_curva = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        mem["custo"] = {
            "nivel_servico": round(nsc, 4),
            "nivel_servico_curva": round(ns_curva, 4),
            "z": (round(_sug_float(z_custo), 3) if z_custo is not None else None),
            "minimo": cmin,
            "maximo": cmax,
            "seguranca": int(_sug_float(ss_custo)),
            "delta_min": cmin - mmin,
            "delta_max": cmax - mmax,
            "margem_pct": (round(_sug_float(margem_pct), 4) if margem_pct is not None else None),
            "custo_unit": (round(_sug_float(custo_unit), 2) if custo_unit is not None else None),
            "delta_capital": round((cmin - mmin) * _sug_float(custo_unit), 2) if custo_unit is not None else None,
            "formula": "p* = margem ÷ (margem + custo de manter);  limitado pela faixa da curva ABC.",
        }

    if membros:
        mem["membros"] = membros
    return mem


def montar_sugestao_compra(items, stock_map, *, historico=None, consolidar_grupo=True,
                           usar_estoque_realtime=True, fornecedor=None, curva=None,
                           subgrupo=None, apenas_zerados=False, incluir_sem_historico=False):
    """
    Transforma as linhas de com_fifo_completo (1 por produto/marca) na sugestão de
    compra agrupada por fornecedor. FUNÇÃO PURA — usada pelo endpoint e pela validação.

    Cada item (dict) deve ter as chaves: pro_codigo, pro_descricao, mar_descricao,
    sgr_descricao, fornecedor1, curva_abc, classe_xyz, padrao_demanda, metodo_reposicao,
    ponto_pedido (mín individual), maximo (máx individual), estoque_snapshot, em_transito,
    demanda_media_dia_ajustada, valor_vendido_12m, sob_encomenda, grupo_chave,
    grupo_estoque_min, grupo_estoque_max, grupo_curva, grupo_padrao, grupo_metodo.

    Modo grupo (consolidar_grupo=True):
      - omite produtos "Sob Encomenda";
      - produtos com grupo_chave usam grupo_estoque_min/max e a POSIÇÃO CONSOLIDADA
        (soma do estoque+trânsito de todas as marcas do grupo) -> 1 linha por GRUPO;
      - produtos sem grupo_chave (avulsos não-originais) caem no cálculo individual.
    """
    import math
    from collections import defaultdict

    usar_rt = usar_estoque_realtime and bool(stock_map)
    historico = historico or {}
    curvas_filtro = [c.strip().upper() for c in curva.split(",")] if curva else None
    subs_filtro = [s.strip().lower() for s in subgrupo.split(",")] if subgrupo else None
    ordem_curva = {"A": 0, "B": 1, "C": 2, "D": 3}
    grupos = {}  # fornecedor -> [rec]

    def _forns_hist(cod):
        """[(for_nome, qtd)] desc — de quem JÁ COMPRAMOS este produto."""
        return historico.get(_sug_norm(cod), [])

    def _sub_ok(sgr):
        return (not subs_filtro) or (_sug_norm(sgr).lower() in subs_filtro)

    def _passa_filtros_comuns(curva_item, estoque_total):
        if curvas_filtro and (curva_item or "").upper() not in curvas_filtro:
            return False
        if apenas_zerados and estoque_total > 0:
            return False
        return True

    def _registrar(bucket, rec):
        grupos.setdefault(bucket, []).append(rec)

    def _tratar_individual(it):
        ponto = _sug_float(it.get("ponto_pedido"))
        maximo = _sug_float(it.get("maximo"))
        if maximo <= 0 or ponto <= 0:
            return
        cod, estoque, transito = _sug_posicao(it, stock_map, usar_rt)
        posicao = estoque + transito
        if posicao > ponto:
            return
        qtd = int(math.ceil(maximo - posicao))
        if qtd <= 0:
            return
        curva_item = _sug_norm(it.get("curva_abc"))
        if not _passa_filtros_comuns(curva_item, estoque):
            return
        if not _sub_ok(it.get("sgr_descricao")):
            return
        # Fornecedor vem do HISTÓRICO de compra (não do cadastro fornecedor1/2/3).
        h = _forns_hist(cod)
        all_forns = [n for n, _ in h]
        top = all_forns[0] if all_forns else SEM_HIST_COMPRA
        if fornecedor:
            # filtro casa se o termo está em QUALQUER fornecedor já comprado do item
            casado = next((n for n in all_forns if fornecedor.lower() in n.lower()), None)
            if not casado:
                return
            bucket = casado
        else:
            bucket = top
        if bucket == SEM_HIST_COMPRA and not incluir_sem_historico:
            return  # produto nunca comprado -> fora da lista
        memoria_item = montar_memoria_calculo(
            escopo="item", minimo=ponto, maximo=maximo,
            curva=curva_item, classe=it.get("classe_xyz"), metodo=it.get("metodo_reposicao"),
            demanda_dia=(it.get("demanda_planejamento_dia")
                         if it.get("demanda_planejamento_dia") is not None
                         else it.get("demanda_media_dia_ajustada")),
            sigma_dia=it.get("sigma_demanda_dia"),
            z=(it.get("nivel_servico_z") if it.get("nivel_servico_z") is not None
               else _Z_POR_CURVA.get(_sug_norm(curva_item).upper())),
            lead_time=(it.get("lead_time_dias") or 17),
            ss=it.get("estoque_seguranca"),
            fator_sazonal=it.get("fator_sazonal"),
            sgr_codigo=it.get("sgr_codigo"),
            msize=it.get("mean_size_mes"), cv2=it.get("cv2_tamanho"),
        )
        _registrar(bucket, {
            "tipo": "individual",
            "memoria": memoria_item,
            "grupo_chave": None,
            "pro_codigo": cod,
            "pro_descricao": it.get("pro_descricao"),
            "marca": it.get("mar_descricao"),
            "subgrupo": it.get("sgr_descricao"),
            "curva_abc": curva_item,
            "classe_xyz": it.get("classe_xyz"),
            "padrao_demanda": it.get("padrao_demanda"),
            "metodo_reposicao": it.get("metodo_reposicao"),
            "qtd_itens_grupo": 1,
            "marcas": [it.get("mar_descricao")] if it.get("mar_descricao") else [],
            "fornecedores": all_forns or [SEM_HIST_COMPRA],
            "estoque_atual": round(estoque, 2),
            "em_transito": round(transito, 2),
            "posicao": round(posicao, 2),
            "ponto_pedido": int(ponto),
            "maximo": int(maximo),
            "qtd_sugerida": qtd,
            "criticidade": "Zerado" if estoque <= 0 else "Abaixo do mínimo",
            "deficit": round(ponto - posicao, 2),
            "membros": [],
        })

    if not consolidar_grupo:
        for it in items:
            if it.get("sob_encomenda"):
                continue
            _tratar_individual(it)
    else:
        membros = defaultdict(list)
        avulsos = []
        for it in items:
            if it.get("sob_encomenda"):
                continue
            gk = it.get("grupo_chave")
            if gk is not None and _sug_norm(gk) != "":
                membros[_sug_norm(gk)].append(it)
            else:
                avulsos.append(it)

        for gk, mem in membros.items():
            # grupo_estoque_min/max são iguais para todos os membros (vêm do merge); usa o maior por segurança
            maximo = max((_sug_float(m.get("grupo_estoque_max")) for m in mem), default=0.0)
            ponto = max((_sug_float(m.get("grupo_estoque_min")) for m in mem), default=0.0)
            if maximo <= 0:
                continue
            gsgr = next((m.get("sgr_descricao") for m in mem if m.get("sgr_descricao")), None)
            if not _sub_ok(gsgr):
                continue

            estoque_total = transito_total = 0.0
            membros_det = []
            sup_qty = defaultdict(float)   # fornecedor -> qtd comprada (grupo todo)
            for m in mem:
                cod, est, tr = _sug_posicao(m, stock_map, usar_rt)
                estoque_total += est
                transito_total += tr
                h = _forns_hist(cod)
                for n, qq in h:
                    sup_qty[n] += qq
                membros_det.append({
                    "pro_codigo": cod,
                    "marca": m.get("mar_descricao"),
                    "classe_xyz": m.get("classe_xyz"),
                    # de quem MAIS COMPRAMOS essa marca (histórico); todos abaixo
                    "fornecedor": (h[0][0] if h else SEM_HIST_COMPRA),
                    "fornecedores_hist": [n for n, _ in h],
                    "estoque_atual": round(est, 2),
                    "em_transito": round(tr, 2),
                    "valor_vendido_12m": _sug_float(m.get("valor_vendido_12m")),
                    "demanda_media_dia_ajustada": _sug_float(m.get("demanda_media_dia_ajustada")),
                    # composição do cálculo (memória)
                    "demanda_real_dia": _sug_float(m.get("demanda_real_dia")),
                    "min_ind": int(_sug_float(m.get("ponto_pedido"))),
                    "max_ind": int(_sug_float(m.get("maximo"))),
                })
            posicao = estoque_total + transito_total
            if posicao > ponto:
                continue
            qtd = int(math.ceil(maximo - posicao))
            if qtd <= 0:
                continue

            # atributos do grupo (compartilhados entre membros)
            curva_item = next((_sug_norm(m.get("grupo_curva")) for m in mem if m.get("grupo_curva")), "")
            padrao = next((m.get("grupo_padrao") for m in mem if m.get("grupo_padrao")), None)
            metodo = next((m.get("grupo_metodo") for m in mem if m.get("grupo_metodo")), None)
            if not _passa_filtros_comuns(curva_item, estoque_total):
                continue

            # ordena membros por relevância (vendas) p/ marca primária
            membros_det.sort(key=lambda x: (-x["valor_vendido_12m"], -x["demanda_media_dia_ajustada"],
                                            -x["estoque_atual"]))
            # fornecedores exibidos = quem mais nos vendeu de CADA marca (distinto)
            forns_ord, seen_f = [], set()
            marcas_ord, seen_m = [], set()
            for d in membros_det:
                f = d["fornecedor"]
                if f and f not in seen_f:
                    seen_f.add(f); forns_ord.append(f)
                mk = d.get("marca")
                if mk and mk not in seen_m:
                    seen_m.add(mk); marcas_ord.append(mk)
            # classe do grupo = a da marca principal (não há XYZ consolidado na análise)
            classe_grp = next((d["classe_xyz"] for d in membros_det if d.get("classe_xyz")), None)
            # bucket padrão = fornecedor de quem MAIS COMPRAMOS no grupo inteiro
            primario = max(sup_qty.items(), key=lambda kv: kv[1])[0] if sup_qty else SEM_HIST_COMPRA
            todos_forns_grupo = set(sup_qty.keys())  # p/ filtro: tudo já comprado no grupo

            # filtro/bucket: se filtrado, casa contra TUDO que já compramos do grupo
            if fornecedor:
                casado = next((n for n in sorted(todos_forns_grupo) if fornecedor.lower() in n.lower()), None)
                if not casado:
                    continue
                bucket = casado
            else:
                bucket = primario
            if bucket == SEM_HIST_COMPRA and not incluir_sem_historico:
                continue  # grupo sem histórico de compra -> fora da lista

            sgr = gsgr
            memoria_grp = montar_memoria_calculo(
                escopo="grupo", minimo=ponto, maximo=maximo,
                curva=curva_item, classe=classe_grp, metodo=metodo,
                demanda_dia=next((m.get("grupo_demanda_dia") for m in mem if m.get("grupo_demanda_dia") is not None), None),
                sigma_dia=None,
                z=_Z_POR_CURVA.get(_sug_norm(curva_item).upper()),
                lead_time=next((m.get("lead_time_dias") for m in mem if m.get("lead_time_dias")), 17),
                ss=next((m.get("grupo_estoque_seguranca") for m in mem if m.get("grupo_estoque_seguranca") is not None), None),
                fator_sazonal=next((m.get("grupo_fator_sazonal") for m in mem if m.get("grupo_fator_sazonal") is not None), None),
                sgr_codigo=next((m.get("sgr_codigo") for m in mem if m.get("sgr_codigo") is not None), None),
                msize=next((m.get("grupo_mean_size") for m in mem if m.get("grupo_mean_size") is not None), None),
                cv2=next((m.get("grupo_cv2") for m in mem if m.get("grupo_cv2") is not None), None),
                membros=[{"marca": d["marca"], "pro_codigo": d["pro_codigo"],
                          "demanda_dia": round(d["demanda_real_dia"], 4),
                          "min_ind": d["min_ind"], "max_ind": d["max_ind"]}
                         for d in membros_det],
            )
            _registrar(bucket, {
                "tipo": "grupo",
                "memoria": memoria_grp,
                "grupo_chave": gk,
                "pro_codigo": membros_det[0]["pro_codigo"],  # representativo (maior venda)
                "pro_descricao": gk,
                "marca": marcas_ord[0] if marcas_ord else None,
                "subgrupo": sgr,
                "curva_abc": curva_item,
                "classe_xyz": classe_grp,
                "padrao_demanda": padrao,
                "metodo_reposicao": metodo,
                "qtd_itens_grupo": len(mem),
                "marcas": marcas_ord,
                "fornecedores": forns_ord,
                "estoque_atual": round(estoque_total, 2),
                "em_transito": round(transito_total, 2),
                "posicao": round(posicao, 2),
                "ponto_pedido": int(ponto),
                "maximo": int(maximo),
                "qtd_sugerida": qtd,
                "criticidade": "Zerado" if estoque_total <= 0 else "Abaixo do mínimo",
                "deficit": round(ponto - posicao, 2),
                "membros": membros_det,
            })

        for it in avulsos:
            _tratar_individual(it)

    fornecedores = []
    for f, its in grupos.items():
        its.sort(key=lambda x: (ordem_curva.get(x["curva_abc"], 9), -x["deficit"]))
        fornecedores.append({
            "fornecedor": f,
            "qtd_itens": len(its),
            "qtd_total_sugerida": sum(x["qtd_sugerida"] for x in its),
            "itens": its,
        })
    fornecedores.sort(key=lambda x: -x["qtd_itens"])

    return {
        "modo": "grupo" if consolidar_grupo else "individual",
        "total_itens": sum(g["qtd_itens"] for g in fornecedores),
        "total_fornecedores": len(fornecedores),
        "estoque_realtime": usar_rt,
        "fornecedores": fornecedores,
    }


def _carregar_itens_sugestao(conn):
    """Lê as linhas base da última análise + em trânsito (tolerante a colunas novas ausentes)."""
    existentes = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name='com_fifo_completo'"
    ))}

    def opt(c):
        return c if c in existentes else f"NULL AS {c}"

    sql = text(f"""
        WITH base AS (
            SELECT pro_codigo, pro_descricao, mar_descricao, sgr_descricao, sgr_codigo, fornecedor1,
                   curva_abc, {opt('classe_xyz')}, {opt('padrao_demanda')}, {opt('metodo_reposicao')},
                   COALESCE(estoque_min_sugerido,0) AS ponto_pedido,
                   COALESCE(estoque_max_sugerido,0) AS maximo,
                   COALESCE(estoque_disponivel,0)   AS estoque_snapshot,
                   demanda_media_dia_ajustada,
                   {opt('valor_vendido_12m')},
                   {opt('demanda_planejamento_dia')}, {opt('demanda_real_dia')},
                   {opt('sigma_demanda_dia')}, {opt('nivel_servico_z')}, {opt('lead_time_dias')},
                   {opt('estoque_seguranca')}, {opt('fator_sazonal')},
                   {opt('mean_size_mes')}, {opt('cv2_tamanho')},
                   {opt('sob_encomenda')}, {opt('grupo_chave')},
                   {opt('grupo_estoque_min')}, {opt('grupo_estoque_max')},
                   {opt('grupo_curva')}, {opt('grupo_padrao')}, {opt('grupo_metodo')},
                   {opt('grupo_demanda_dia')}, {opt('grupo_estoque_seguranca')}, {opt('grupo_fator_sazonal')},
                   {opt('grupo_mean_size')}, {opt('grupo_cv2')}
            FROM com_fifo_completo
            WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
        ),
        pedido AS (
            SELECT i.pro_codigo::text AS pro_codigo, SUM(i.quantidade) AS qtd_ped
            FROM com_pedido p
            JOIN com_pedido_itens i ON i.pedido_id = p.id
            WHERE p.status IN ('Liberado', 'Em Trânsito parcialmente')
            GROUP BY i.pro_codigo::text
        ),
        recebido AS (
            SELECT vi.pro_codigo::text AS pro_codigo, SUM(vi.quantidade_alocada) AS qtd_rec
            FROM com_pedido p
            JOIN com_pedido_nfe_vinculo v ON v.pedido_id = p.id
            JOIN com_pedido_nfe_vinculo_item vi ON vi.vinculo_id = v.id
            WHERE p.status IN ('Liberado', 'Em Trânsito parcialmente')
              AND COALESCE(v.confirmado, false) = true
              AND COALESCE(v.rejeitado, false) = false
            GROUP BY vi.pro_codigo::text
        )
        SELECT b.*,
               GREATEST(COALESCE(ped.qtd_ped,0) - COALESCE(rec.qtd_rec,0), 0) AS em_transito
        FROM base b
        LEFT JOIN pedido   ped ON ped.pro_codigo = b.pro_codigo::text
        LEFT JOIN recebido rec ON rec.pro_codigo = b.pro_codigo::text
    """)
    return [dict(r) for r in conn.execute(sql).mappings().all()]


def _codigos_candidatos(items, historico, curva, subgrupo, fornecedor, consolidar_grupo):
    """
    Códigos de produto que passam nos filtros que NÃO dependem de estoque
    (curva, subgrupo, fornecedor-histórico), respeitando os grupos. Usado para
    buscar o estoque realtime SÓ desses itens (em vez dos ~45k).
    """
    curvas = [c.strip().upper() for c in curva.split(",")] if curva else None
    subs = [s.strip().lower() for s in subgrupo.split(",")] if subgrupo else None
    forn = fornecedor.lower() if fornecedor else None
    historico = historico or {}

    def forn_ok(cod):
        return (not forn) or any(forn in n.lower() for n, _ in historico.get(_sug_norm(cod), []))

    def sub_ok(sgr):
        return (not subs) or (_sug_norm(sgr).lower() in subs)

    codes = set()
    if not consolidar_grupo:
        for it in items:
            if it.get("sob_encomenda"):
                continue
            if curvas and _sug_norm(it.get("curva_abc")).upper() not in curvas:
                continue
            if not sub_ok(it.get("sgr_descricao")):
                continue
            if not forn_ok(it.get("pro_codigo")):
                continue
            codes.add(_sug_norm(it.get("pro_codigo")))
        return codes

    from collections import defaultdict
    membros = defaultdict(list)
    avulsos = []
    for it in items:
        if it.get("sob_encomenda"):
            continue
        gk = it.get("grupo_chave")
        if gk is not None and _sug_norm(gk) != "":
            membros[_sug_norm(gk)].append(it)
        else:
            avulsos.append(it)
    for gk, mem in membros.items():
        gcurva = next((_sug_norm(m.get("grupo_curva")).upper() for m in mem if m.get("grupo_curva")), "")
        gsgr = next((m.get("sgr_descricao") for m in mem if m.get("sgr_descricao")), None)
        if curvas and gcurva not in curvas:
            continue
        if not sub_ok(gsgr):
            continue
        if forn and not any(forn_ok(m.get("pro_codigo")) for m in mem):
            continue
        for m in mem:
            codes.add(_sug_norm(m.get("pro_codigo")))
    for it in avulsos:
        if curvas and _sug_norm(it.get("curva_abc")).upper() not in curvas:
            continue
        if not sub_ok(it.get("sgr_descricao")):
            continue
        if not forn_ok(it.get("pro_codigo")):
            continue
        codes.add(_sug_norm(it.get("pro_codigo")))
    return codes


@app.get("/compras/sugestao")
def sugestao_compra(
    fornecedor: Optional[str] = None,
    curva: Optional[str] = None,
    subgrupo: Optional[str] = None,
    apenas_zerados: bool = False,
    usar_estoque_realtime: bool = True,
    consolidar_grupo: bool = True,
    incluir_sem_historico: bool = False,
):
    """
    Lista o que COMPRAR, agrupado por fornecedor, usando o ponto de pedido.

      Posição   = estoque atual (ERP) + em trânsito (pedidos Liberado / Em Trânsito parcialmente,
                  já descontado o que foi recebido por NF)
      Comprar?  = Posição <= ponto de pedido
      Quanto?   = Máximo - Posição

    Com consolidar_grupo=True (padrão): usa o mín/máx CONSOLIDADO do grupo (mesma descrição,
    várias marcas), soma a posição de todas as marcas e devolve 1 linha por grupo; produtos
    "Sob Encomenda" (originais) são omitidos. Com False: usa o mín/máx individual por marca.
    """
    conn = get_db_connection()
    try:
        items = _carregar_itens_sugestao(conn)
    finally:
        conn.close()

    historico = {}
    try:
        historico = get_compras_historico()
    except Exception as e:
        print(f"AVISO: histórico de compra indisponível (usando vazio). {e}")

    # ESTRATÉGIA: aplica os filtros que NÃO dependem de estoque (curva/subgrupo/
    # fornecedor) e busca o estoque realtime SÓ dos itens filtrados. Sem filtro,
    # cai no "todos" (com timeout rígido + fallback pro snapshot).
    tem_filtro = bool(curva or subgrupo or fornecedor)
    stock_map = {}
    if usar_estoque_realtime:
        if tem_filtro:
            codes = _codigos_candidatos(items, historico, curva, subgrupo, fornecedor, consolidar_grupo)
            try:
                fut = _RT_EXECUTOR.submit(get_realtime_stocks_bulk, codes)
                stock_map = fut.result(timeout=int(os.getenv("STOCK_RT_HARD_TIMEOUT_S") or 20))
            except Exception as e:
                print(f"AVISO: estoque realtime (lote) lento/indisponível ({type(e).__name__}), usando snapshot.")
                stock_map = {}
        else:
            try:
                fut = _RT_EXECUTOR.submit(get_all_realtime_stocks)
                stock_map = fut.result(timeout=int(os.getenv("STOCK_RT_HARD_TIMEOUT_S") or 20))
            except Exception as e:
                print(f"AVISO: estoque realtime lento/indisponível ({type(e).__name__}), usando snapshot.")
                stock_map = {}

    return montar_sugestao_compra(
        items, stock_map,
        historico=historico,
        consolidar_grupo=consolidar_grupo,
        usar_estoque_realtime=usar_estoque_realtime,
        fornecedor=fornecedor,
        curva=curva,
        subgrupo=subgrupo,
        apenas_zerados=apenas_zerados,
        incluir_sem_historico=incluir_sem_historico,
    )


@app.get("/produto/vendas-mensais")
def produto_vendas_mensais(codigos: str, meses: int = 18):
    """
    Vendas (saídas) por mês de um ou mais produtos (SOMA — p/ o grupo consolidado),
    nos últimos `meses` meses. Consulta leve no ERP, usada ao abrir a memória.
    """
    import datetime as _dt
    codes = [c.strip().replace("'", "") for c in (codigos or "").split(",") if c.strip()]
    if not codes:
        return {"meses": []}
    codes = codes[:300]
    hoje = _dt.date.today()
    ini = (hoje.replace(day=1) - _dt.timedelta(days=int(meses) * 31)).replace(day=1)
    in_list = ", ".join(f"'{c}'" for c in codes)
    inner = ("SELECT EXTRACT(YEAR FROM LE.data) AS ano, EXTRACT(MONTH FROM LE.data) AS mes, "
             "SUM(LE.quantidade) AS qtd FROM lanctos_estoque LE "
             "WHERE LE.empresa = 3 AND LE.origem IN ('NFS','EVF','EFD') "
             f"AND LE.pro_codigo IN ({in_list}) AND LE.data >= '{ini.isoformat()}' "
             "GROUP BY EXTRACT(YEAR FROM LE.data), EXTRACT(MONTH FROM LE.data)")
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39) * 2)}')"
    mapa = {}
    try:
        conn = get_sql_connection()
        try:
            conn.timeout = int(os.getenv("VENDAS_MENSAIS_TIMEOUT_S") or 20)
        except Exception:
            pass
        try:
            cur = conn.cursor()
            cur.execute(query)
            for ano, mes, qtd in cur.fetchall():
                mapa[(int(ano), int(mes))] = float(qtd or 0)
        finally:
            conn.close()
    except Exception as e:
        print(f"AVISO: vendas-mensais indisponível: {e}")
        return {"meses": [], "erro": True}

    # série contínua dos últimos `meses` meses (preenche zeros)
    out = []
    y, mth = ini.year, ini.month
    while (y, mth) <= (hoje.year, hoje.month):
        out.append({"mes": f"{y:04d}-{mth:02d}", "qtd": round(mapa.get((y, mth), 0.0), 2)})
        mth += 1
        if mth > 12:
            mth = 1; y += 1
    return {"meses": out}
