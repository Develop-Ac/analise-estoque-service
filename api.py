from fastapi import FastAPI, HTTPException, Query
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
    conn = get_db_connection()
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
        total = conn.execute(count_sql, params).scalar()
        
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
        
    finally:
        conn.close()

@app.get("/health")
def health_check():
    return {"status": "ok"}
