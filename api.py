from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional
import os

# ==========================================
# CONFIGURAÇÕES
# ==========================================
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://usuario:senha@host:5432/database")

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

# ==========================================
# MODELOS
# ==========================================
class AnaliseItem(BaseModel):
    id: int
    pro_codigo: str
    pro_descricao: Optional[str]
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
    estoque_min_sugerido: Optional[int]
    estoque_max_sugerido: Optional[int]
    tipo_planejamento: Optional[str]
    teve_alteracao_analise: Optional[bool]
    data_processamento: Optional[str]
    dias_ruptura: Optional[int]
    fator_tendencia: Optional[float]
    tendencia_label: Optional[str]

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
    critical: bool = False
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
            
        if critical:
            # Crítico: Estoque Atual < Min Sugerido
            filters.append("(estoque_disponivel < estoque_min_sugerido)")
            
        if search:
            # Busca por Código ou Descrição
            filters.append("(pro_codigo LIKE :search OR pro_descricao ILIKE :search_desc)")
            params["search"] = f"{search}%"
            params["search_desc"] = f"%{search}%"
            
        where_clause = " AND ".join(filters) if filters else "1=1"
        
        # Query Total
        count_sql = text(f"SELECT COUNT(*) FROM com_fifo_completo WHERE {where_clause}")
        try:
            total = conn.execute(count_sql, params).scalar()
        except Exception as e:
             # Tabela pode nao existir ainda
             print(f"ERRO QUERY: {e}")
             # Retorna vazio se a tabela nao existe (primeira inicializacao)
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
                dias_ruptura, fator_tendencia, tendencia_label
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
ARQUIVO_ESTADO = Path("fifo_service_state.json")
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS', 7))

def load_state():
    if not ARQUIVO_ESTADO.exists():
        return {}
    try:
        with open(ARQUIVO_ESTADO, "r") as f:
            return json.load(f)
    except:
        return {}

def save_state(state):
    with open(ARQUIVO_ESTADO, "w") as f:
        json.dump(state, f)

def background_scheduler():
    global BACKGROUND_Running
    print("Iniciando scheduler de background...")
    
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
                print("Primeira execução detectada. Rodando job...")
                should_run = True
            else:
                last_run = datetime.datetime.fromisoformat(last_run_str)
                dias_passados = (datetime.datetime.now() - last_run).days
                
                if dias_passados >= INTERVALO_DIAS:
                    print(f"Intervalo de {INTERVALO_DIAS} dias atingido. Rodando job...")
                    should_run = True
                else:
                    print(f"Skipping analysis. Última execução: {last_run_str}. ({dias_passados} dias atrás)")
            
            if should_run:
                BACKGROUND_Running = True
                run_job()
                state["last_run"] = datetime.datetime.now().isoformat()
                save_state(state)
                BACKGROUND_Running = False
                
        except Exception as e:
            print(f"Erro no background scheduler: {e}")
            BACKGROUND_Running = False
            
        # Verifica a cada 1 hora
        time.sleep(3600) 

@app.on_event("startup")
def startup_event():
    print("API Analise Estoque iniciada na porta 8000")
    # Inicia a thread de background
    thread = threading.Thread(target=background_scheduler, daemon=True)
    thread.start()
