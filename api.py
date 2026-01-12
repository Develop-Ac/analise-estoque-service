from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional
import os
import pyodbc

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
    conn_str = (
        "DRIVER={FreeTDS};"
        f"SERVER={SQL_HOST};"
        f"PORT={SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={{{SQL_USER}}};"
        f"PWD={{{SQL_PASSWORD}}};"
        f"TDS_Version={TDS_VERSION};"
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
class AnaliseItem(BaseModel):
    id: int
    pro_codigo: str
    pro_descricao: str
    sgr_codigo: Optional[int]
    mar_descricao: Optional[str]
    fornecedor1: Optional[str]
    estoque_disponivel: Optional[float]
    demanda_media_dia: Optional[float]
    demanda_media_dia_ajustada: Optional[float]
    tempo_medio_estoque: Optional[float]
    data_min_venda: Optional[str]
    data_max_venda: Optional[str]
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
    dados_alteracao_json: Optional[str] # JSON string

class PaginatedResponse(BaseModel):
    data: List[AnaliseItem]
    total: int
    page: int
    limit: int
    total_pages: int

# ==========================================
# ROTAS
# ==========================================
@app.get("/analise", response_model=PaginatedResponse)
def listar_analise(
    page: int = 1,
    limit: int = 50,
    search: Optional[str] = None,
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None
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
            
        if critical:
            # Mantendo compatibilidade: Crítico
            filters.append("(estoque_disponivel < estoque_min_sugerido)")
            
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
                 
        if status:
            # Status: Critical, Excess, Normal
            status_list = [s.strip().lower() for s in status.split(",") if s.strip()]
            
            status_conditions = []
            if "critico" in status_list or "critical" in status_list:
                status_conditions.append("(estoque_disponivel < estoque_min_sugerido)")
            
            if "excesso" in status_list or "excess" in status_list:
                status_conditions.append("(estoque_disponivel > estoque_max_sugerido)")
                
            if "normal" in status_list:
                status_conditions.append("(estoque_disponivel >= estoque_min_sugerido AND estoque_disponivel <= estoque_max_sugerido)")
            
            if status_conditions:
                filters.append(f"({' OR '.join(status_conditions)})")

        if search:
            filters.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
            params["search"] = f"{search}%"
            params["search_desc"] = f"%{search}%"
            
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
                id, pro_codigo, pro_descricao, sgr_codigo, mar_descricao, fornecedor1,
                estoque_disponivel, demanda_media_dia, demanda_media_dia_ajustada,
                tempo_medio_estoque, CAST(data_min_venda AS TEXT) as data_min_venda, 
                CAST(data_max_venda AS TEXT) as data_max_venda,
                curva_abc, categoria_estocagem, estoque_min_sugerido, estoque_max_sugerido,
                tipo_planejamento, teve_alteracao_analise, 
                CAST(data_processamento AS TEXT) as data_processamento,
                dias_ruptura, fator_tendencia, tendencia_label,
                CAST(dados_alteracao_json AS TEXT) as dados_alteracao_json
            FROM com_fifo_completo 
            WHERE {where_clause}
            ORDER BY 
                CASE WHEN teve_alteracao_analise = TRUE THEN 0 ELSE 1 END,
                curva_abc ASC, 
                pro_descricao ASC
            LIMIT :limit OFFSET :offset
        """)
        
        params["limit"] = limit
        params["offset"] = offset
        
        result = conn.execute(data_sql, params).mappings().all()
        data_list = [dict(row) for row in result]
        
        # ---------------------------------------------------------------------
        # ATUALIZAÇÃO STOCK EM TEMPO REAL (OPENQUERY)
        # ---------------------------------------------------------------------
        # Extrai códigos para buscar no ERP
        pro_codigos = [item['pro_codigo'] for item in data_list]
        
        if pro_codigos:
            try:
                # Busca estoque atualizado via SQL Server OPENQUERY
                stock_map = get_realtime_stocks(pro_codigos)
                
                # Atualiza a lista com o dado novo
                for item in data_list:
                    code = item['pro_codigo']
                    if code in stock_map:
                        item['estoque_disponivel'] = stock_map[code]
                        # Nota: o status (critico/excesso) sera recalculado no frontend com base neste novo valor
            except Exception as e:
                 print(f"AVISO: Falha ao buscar estoque realtime: {e}")
                 # Nao crashamos a request, apenas seguimos com o dado "cacheado" do postgres
        
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
    only_changes: bool = False,
    critical: bool = False,
    curve: Optional[str] = None,
    trend: Optional[str] = None,
    status: Optional[str] = None
):
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro banco: {e}")

    try:
        # Reutilizando logica de filtro (Copy-Paste para garantir isolamento)
        filters = []
        params = {}
        
        if only_changes:
            filters.append("teve_alteracao_analise = TRUE")

        filters.append("data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)")
            
        if critical:
            filters.append("(estoque_disponivel < estoque_min_sugerido)")
            
        if curve:
            curves = [c.strip().upper() for c in curve.split(",") if c.strip()]
            if curves:
                curve_params = {f"curve_{i}": c for i, c in enumerate(curves)}
                params.update(curve_params)
                keys = ", ".join([f":{k}" for k in curve_params.keys()])
                filters.append(f"curva_abc IN ({keys})")

        if trend:
            trends = [t.strip() for t in trend.split(",") if t.strip()]
            if trends:
                 trend_params = {f"trend_{i}": t for i, t in enumerate(trends)}
                 params.update(trend_params)
                 keys = ", ".join([f":{k}" for k in trend_params.keys()])
                 filters.append(f"tendencia_label IN ({keys})")
                 
        if status:
            status_list = [s.strip().lower() for s in status.split(",") if s.strip()]
            status_conditions = []
            if "critico" in status_list or "critical" in status_list:
                status_conditions.append("(estoque_disponivel < estoque_min_sugerido)")
            if "excesso" in status_list or "excess" in status_list:
                status_conditions.append("(estoque_disponivel > estoque_max_sugerido)")
            if "normal" in status_list:
                status_conditions.append("(estoque_disponivel >= estoque_min_sugerido AND estoque_disponivel <= estoque_max_sugerido)")
            if status_conditions:
                filters.append(f"({' OR '.join(status_conditions)})")

        if search:
            filters.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
            params["search"] = f"{search}%"
            params["search_desc"] = f"%{search}%"
            
        where_clause = " AND ".join(filters) if filters else "1=1"

        # Query p/ Export - Trazendo mais campos e calculando Status
        export_sql = text(f"""
            SELECT 
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
                dados_alteracao_json as "Detalhes Mudança"
            FROM com_fifo_completo 
            WHERE {where_clause}
            ORDER BY 
                CASE WHEN teve_alteracao_analise = TRUE THEN 0 ELSE 1 END,
                curva_abc ASC, 
                pro_descricao ASC
        """)
        
        # Leitura com Pandas
        import pandas as pd
        import io
        from fastapi.responses import StreamingResponse

        df = pd.read_sql(export_sql, conn, params=params)
        
        # Converter colunas numericas se precisar (Pandas ja deve fazer)
        
        output = io.BytesIO()
        # MUDANÇA: engine openpyxl para compatibilidade
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Analise')
                
        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="analise_estoque_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.xlsx"'
        }
        
        return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

    except Exception as e:
        print(f"ERRO EXPORT: {e}")
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
    print("Iniciando scheduler de background...")
    print(f"Monitorando arquivo de estado: {ARQUIVO_ESTADO}")
    
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
            state = load_state()
            last_run_str = state.get("last_run")
            should_run = False
            
            if not last_run_str:
                print("Primeira execução detectada (Sem data anterior). Rodando job...")
                should_run = True
            else:
                try:
                    last_run = datetime.datetime.fromisoformat(last_run_str)
                    now = datetime.datetime.now()
                    dias_passados = (now - last_run).days
                    
                    if dias_passados >= INTERVALO_DIAS:
                        print(f"Intervalo de {INTERVALO_DIAS} dias atingido. (Última: {last_run_str}, Passados: {dias_passados}). Rodando job...")
                        should_run = True
                    else:
                        # Log apenas ocasional ou na startup para não floodar
                        pass
                        # print(f"Skipping analysis. Última execução: {last_run_str}. ({dias_passados} dias atrás)")
                except ValueError:
                    print("Erro ao parsear data anterior. Rodando job...")
                    should_run = True
            
            if should_run:
                if not BACKGROUND_Running:
                   BACKGROUND_Running = True
                   print(">>> Iniciando execução do Job FIFO...")
                   run_job()
                   state["last_run"] = datetime.datetime.now().isoformat()
                   save_state(state)
                   BACKGROUND_Running = False
                   print(">>> Job FIFO finalizado.")
                else:
                    print("Job já está rodando. Ignorando trigger.")
                
        except Exception as e:
            print(f"Erro no background scheduler: {e}")
            BACKGROUND_Running = False
            
        # Verifica a cada 1 hora
        time.sleep(3600) 

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
