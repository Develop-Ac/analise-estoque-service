# -*- coding: utf-8 -*-
"""Modelos pydantic da API."""
from pydantic import BaseModel
from typing import List, Optional

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
    grupo_qtd_itens: Optional[int] = None
    grupo_estoque_disponivel: Optional[float] = None
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
    eh_original: Optional[bool] = None
    teve_outlier_aparado: Optional[bool] = None
    outlier_qtd_aparada: Optional[float] = None
    outlier_motivo: Optional[str] = None
    memoria: Optional[dict] = None
    memoria_grupo: Optional[dict] = None

    # Detailed Stock Info
    estoque_obsoleto: Optional[float] = 0
    lotes_estoque: Optional[List[LoteEstoque]] = []

    # Capital parado e custo de carregamento acumulado do saldo atual
    valor_estoque: Optional[float] = None            # estoque × custo unitário (R$)
    custo_manter_acumulado: Optional[float] = None   # valor × HOLDING_RATE × idade média/365 (R$)

    # Preços do cadastro (ERP) + desconto sugerido para promoção
    preco_venda_1: Optional[float] = None            # tabela 1 — varejo
    preco_venda_2: Optional[float] = None            # tabela 2 — atacado especial
    desconto_sugerido_pct: Optional[float] = None    # % sugerido sobre o preço 1 (varejo)

class PaginatedResponse(BaseModel):
    data: List[AnaliseItem]
    total: int
    page: int
    limit: int
    total_pages: int
    # Somas do FILTRO ATUAL (where completo, não só a página)
    capital_total: Optional[float] = None   # Σ estoque × custo (itens com estoque > 0)
    cme_total: Optional[float] = None       # Σ CME acumulado (mesma fórmula do painel)

class GroupRequest(BaseModel):
    pro_codigos: List[str]

class SimulationRequest(BaseModel):
    pro_codigos: List[str]
    coverage_days: int

class PromoPlanRequest(BaseModel):
    # days=None (padrão): excesso medido contra o MÁXIMO SUGERIDO oficial do
    # cálculo (tempo padrão por curva). Informar days só para simular outra cobertura.
    days: Optional[int] = None
    subgroups: Optional[List[str]] = None
    brands: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    grouped_view: bool = False

