import pyodbc
import pandas as pd
import numpy as np
from pathlib import Path
import time
import datetime
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os
import uuid
import sys
import math
# Imports para PostgreSQL
import psycopg2
from sqlalchemy import create_engine, text
import warnings
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONFIGURAÇÕES GERAIS
# ==========================================

# Intervalo de execução em dias
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS', 7))

# Caminhos dos arquivos
BASE_DIR = Path(__file__).resolve().parent
ARQUIVO_SAIDA = BASE_DIR / "resultado_fifo_completo.xlsx"
ARQUIVO_ESTADO = BASE_DIR / "data/fifo_service_state.json"
ARQUIVO_ANTERIOR = BASE_DIR / "historico_analise_anterior.pkl"

# Configurações de E-mail (PREENCHER AQUI)
# Configurações de E-mail
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER", "email-ssl.com.br")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", 587))
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "seu_email@exemplo.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "sua_senha")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "email_destino@exemplo.com")

# Configurações PostgreSQL
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://usuario:senha@host:5432/database")
TABELA_FIFO = "com_fifo_completo"

# ==========================================
# PARÂMETROS DE REPOSIÇÃO / ESTOQUE DE SEGURANÇA
# ==========================================
# Janela recente usada como base da demanda e da variabilidade (substitui a
# média desde 2005). ABC também passa a ser calculada sobre esta janela.
JANELA_DEMANDA_MESES = int(os.getenv("JANELA_DEMANDA_MESES", 12))

# Lead time (prazo de reposição) em dias. Não há prazo por fornecedor no ERP,
# então usamos um valor global configurável (a regra do subgrupo 154 segue
# valendo no ciclo, em regras_dias).
LEAD_TIME_DIAS = int(os.getenv("LEAD_TIME_DIAS", 17))

# Z (fator de nível de serviço) por curva — A=98%, B=95%, C=90%, D=85%.
# Estoque de segurança = Z * sigma_demanda_dia * sqrt(lead_time)  (Silver-Pyke).
Z_POR_CURVA = {
    "A": float(os.getenv("Z_CURVA_A", 2.054)),  # 98%
    "B": float(os.getenv("Z_CURVA_B", 1.645)),  # 95%
    "C": float(os.getenv("Z_CURVA_C", 1.282)),  # 90%
    "D": float(os.getenv("Z_CURVA_D", 1.036)),  # 85%
}

# Limiares XYZ pelo coeficiente de variação (CV) da demanda mensal.
#   X = previsível (CV baixo) | Y = média | Z = errática (CV alto)
XYZ_LIMIAR_X = float(os.getenv("XYZ_LIMIAR_X", 0.5))
XYZ_LIMIAR_Y = float(os.getenv("XYZ_LIMIAR_Y", 1.0))

# Tratamento de outliers da VENDA_PERDIDA (cap por evento). Poucos lançamentos
# atípicos (cotações grandes / erro de digitação) concentram a maior parte da
# quantidade, então limitamos cada apontamento.
VP_CAP_MULT = float(os.getenv("VP_CAP_MULT", 3.0))    # x venda média por evento do item
VP_CAP_PISO = float(os.getenv("VP_CAP_PISO", 5.0))    # teto mínimo por evento (un)
VP_CAP_TETO = float(os.getenv("VP_CAP_TETO", 200.0))  # teto absoluto por evento (un)

# Dias médios por mês (conversão sigma mensal -> diário)
DIAS_POR_MES = 30.44

# Teto do estoque de segurança, em nº de ciclos de demanda da classe.
# Evita que itens A/Z (demanda intermitente) peçam segurança exagerada, onde a
# hipótese de normalidade do SS clássico superestima. 0 ou negativo = sem teto.
SS_CAP_CICLOS = float(os.getenv("SS_CAP_CICLOS", 1.0))

# ==========================================
# AGRUPAMENTO POR PRODUTO (consolida marcas) E PRODUTOS ORIGINAIS
# ==========================================
# O "mesmo produto" é agrupado pela DESCRIÇÃO (a marca fica no mar_codigo), e a
# demanda é consolidada entre as marcas (substitutos). Produtos ORIGINAIS
# (montadora/genuíno) saem do cálculo estocado — são por encomenda.
# Regra do original: marca contém ORIGINAL/GENUINO, MENOS as exceções abaixo
# (marcas aftermarket que só usam "original" no nome).
MARCAS_ORIGINAL_EXCECOES = [s.strip().upper() for s in os.getenv(
    "MARCAS_ORIGINAL_EXCECOES", "DECAL LINE,DRIFT,INDUSTRIA ORIGINAL,ORIGINAL PARTS"
).split(",") if s.strip()]

def marca_eh_original(mar):
    """True se a marca é OEM/encomenda (montadora-original ou genuíno)."""
    m = str(mar or "").upper()
    if not ("ORIGINAL" in m or "ORIGINAIS" in m or "GENUINO" in m):
        return False
    return not any(ex in m for ex in MARCAS_ORIGINAL_EXCECOES)

# Nível de serviço (PROBABILIDADE) por curva — usado no ponto de pedido via
# Poisson/Binomial Negativa para itens intermitentes (espelha os Z acima).
NS_POR_CURVA = {
    "A": float(os.getenv("NS_CURVA_A", 0.98)),
    "B": float(os.getenv("NS_CURVA_B", 0.95)),
    "C": float(os.getenv("NS_CURVA_C", 0.90)),
    "D": float(os.getenv("NS_CURVA_D", 0.85)),
}

# Croston/SBA: constante de suavização (a correção de viés usa 1 - alpha/2).
ALPHA_CROSTON = float(os.getenv("ALPHA_CROSTON", 0.1))

# Limiares Syntetos-Boylan-Croston (classificação do padrão de demanda):
#   ADI  = intervalo médio entre vendas (esparsidade)
#   CV²  = variabilidade do tamanho das vendas
SBC_ADI = float(os.getenv("SBC_ADI", 1.32))
SBC_CV2 = float(os.getenv("SBC_CV2", 0.49))

# Sazonalidade (índice por subgrupo, anos completos)
SAZ_ANOS = int(os.getenv("SAZ_ANOS", 5))
SAZ_AMPLITUDE_MIN = float(os.getenv("SAZ_AMPLITUDE_MIN", 1.5))  # pico/vale mínimo p/ ser "sazonal"
SAZ_VOL_MIN = float(os.getenv("SAZ_VOL_MIN", 300))             # volume anual mínimo do subgrupo
SAZ_CONSIST_MIN = float(os.getenv("SAZ_CONSIST_MIN", 0.6))     # consistência do trimestre de pico
SAZ_FATOR_MIN = float(os.getenv("SAZ_FATOR_MIN", 0.5))         # trava do fator sazonal
SAZ_FATOR_MAX = float(os.getenv("SAZ_FATOR_MAX", 2.0))

# ==========================================
# CONEXÃO ODBC / CARGA DE DADOS
# ==========================================

def get_connection():
    # Configurar warning do pandas para silenciar o aviso de DBAPI2 vs SQLAlchemy
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

    server = os.getenv('SQL_HOST', '127.0.0.1')
    port = os.getenv('SQL_PORT', '1433')
    # O padrão 'master' geralmente não contém as tabelas de negócio.
    # O usuário DEVE configurar SQL_DATABASE com o nome correto (ex: Tretor_...)
    database = os.getenv('SQL_DATABASE', 'master')
    user = os.getenv('SQL_USER', 'sa')
    password = os.getenv('SQL_PASSWORD', 'senha_secreta')
    
    tds_version = os.getenv('TDS_VERSION', '7.4')
    sql_driver = os.getenv('SQL_DRIVER', '{FreeTDS}') # Default to FreeTDS if not set, but .env will set it

    # É importante envolver senha em chaves {} se houver caracteres especiais
    if "FreeTDS" in sql_driver:
        conn_str = (
            f"DRIVER={sql_driver};"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UID={{{user}}};"
            f"PWD={{{password}}};"
        )
    else:
        # Microsoft Driver (não aceita PORT como parametro separado, usa virgula no server)
        conn_str = (
            f"DRIVER={sql_driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={{{user}}};"
            f"PWD={{{password}}};"
        )
    
    # DEBUG: Imprimir string de conexão (ocultando senha se possível, mas aqui é debug local)
    print(f"DEBUG: Conectando com DRIVER={sql_driver}, SERVER={server}...")
    # print(f"DEBUG: conn_str completo: {conn_str}") 

    return pyodbc.connect(conn_str)


def get_postgres_engine():
    """Cria engine do SQLAlchemy para PostgreSQL"""
    print(f"DEBUG: POSTGRES_URL={POSTGRES_URL}")
    # Replace postgres:// with postgresql:// just in case it is still wrong in memory for some reason
    final_url = POSTGRES_URL.replace("postgres://", "postgresql://")
    return create_engine(final_url)


def criar_tabela_postgres():
    """Cria a tabela com_fifo_completo no PostgreSQL se ela não existir"""
    engine = get_postgres_engine()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS com_fifo_completo (
        id SERIAL PRIMARY KEY,
        data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        pro_codigo VARCHAR(50),
        tempo_medio_estoque DECIMAL(15,4),
        qtd_vendida DECIMAL(15,4),
        valor_vendido DECIMAL(15,4),
        data_min_venda DATE,
        data_max_venda DATE,
        periodo_dias INTEGER,
        demanda_media_dia DECIMAL(15,6),
        num_vendas INTEGER,
        vendas_ult_12m DECIMAL(15,4),
        vendas_12m_ant DECIMAL(15,4),
        fator_tendencia DECIMAL(10,6),
        tendencia_label VARCHAR(20),
        dias_ruptura INTEGER,
        demanda_media_dia_ajustada DECIMAL(15,6),
        pro_descricao TEXT,
        estoque_disponivel DECIMAL(15,4),
        mar_descricao VARCHAR(100),
        sgr_codigo INTEGER,
        fornecedor1 VARCHAR(200),
        fornecedor2 VARCHAR(200),
        fornecedor3 VARCHAR(200),
        pct_acum_valor DECIMAL(10,4),
        curva_abc VARCHAR(10),
        categoria_estocagem VARCHAR(20),
        estoque_min_base INTEGER,
        estoque_max_base INTEGER,
        fator_ajuste_tendencia DECIMAL(10,6),
        estoque_min_ajustado INTEGER,
        estoque_max_ajustado INTEGER,
        estoque_min_sugerido INTEGER,
        estoque_max_sugerido INTEGER,
        tipo_planejamento VARCHAR(30),
        alerta_tendencia_alta VARCHAR(10),
        descricao_calculo_estoque TEXT,
        teve_alteracao_analise BOOLEAN DEFAULT FALSE,
        group_id VARCHAR(100),
        grp_estoque_disponivel DECIMAL(15,4),
        grp_qtd_vendida DECIMAL(15,4),
        grp_valor_vendido DECIMAL(15,4),
        grp_num_vendas INTEGER,
        grp_vendas_ult_12m DECIMAL(15,4),
        grp_vendas_12m_ant DECIMAL(15,4),
        grp_estoque_min_base INTEGER,
        grp_estoque_max_base INTEGER,
        grp_estoque_min_ajustado INTEGER,
        grp_estoque_max_ajustado INTEGER,
        grp_estoque_min_sugerido INTEGER,
        grp_estoque_max_sugerido INTEGER,
        grp_demanda_media_dia DECIMAL(15,6),
        rateio_prop_grupo DECIMAL(10,6),
        tempo_medio_saldo_atual DECIMAL(15,2),
        categoria_saldo_atual VARCHAR(50),
        pro_referencia VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_com_fifo_data_processamento ON com_fifo_completo (data_processamento);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_curva_abc ON com_fifo_completo (curva_abc);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_categoria_estocagem ON com_fifo_completo (categoria_estocagem);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_categoria_saldo_atual ON com_fifo_completo (categoria_saldo_atual);
    
    -- Tabela de Relacionamento de Itens (Similares)
    CREATE TABLE IF NOT EXISTS com_relacionamento_itens (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(100) NOT NULL,
        pro_codigo VARCHAR(50) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Migração automática: Adicionar colunas novas se não existirem
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='com_fifo_completo' AND column_name='sgr_codigo') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN sgr_codigo INTEGER;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='com_fifo_completo' AND column_name='sgr_descricao') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN sgr_descricao VARCHAR(200);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='com_fifo_completo' AND column_name='group_id') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN group_id VARCHAR(100);
        END IF;

        -- Migração colunas de grupo (GRP_*)
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_disponivel') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_disponivel DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_qtd_vendida') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_qtd_vendida DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_valor_vendido') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_valor_vendido DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_num_vendas') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_num_vendas INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_vendas_ult_12m') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_vendas_ult_12m DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_vendas_12m_ant') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_vendas_12m_ant DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_min_base') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_min_base INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_max_base') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_max_base INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_min_ajustado') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_min_ajustado INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_max_ajustado') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_max_ajustado INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_min_sugerido') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_min_sugerido INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_estoque_max_sugerido') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_estoque_max_sugerido INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grp_demanda_media_dia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grp_demanda_media_dia DECIMAL(15,6);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='rateio_prop_grupo') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN rateio_prop_grupo DECIMAL(10,6);
        END IF;

        -- NOVAS COLUNAS: Idade e Categoria do Saldo Atual (FIFO)
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='tempo_medio_saldo_atual') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN tempo_medio_saldo_atual DECIMAL(15,2);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='categoria_saldo_atual') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN categoria_saldo_atual VARCHAR(50);
        END IF;

        -- Referência
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='pro_referencia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN pro_referencia VARCHAR(50);
        END IF;

        -- ===== Estoque de segurança estatístico / ABC-XYZ / venda perdida =====
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='demanda_real_dia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN demanda_real_dia DECIMAL(15,6);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='sigma_demanda_dia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN sigma_demanda_dia DECIMAL(15,6);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='cv_demanda') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN cv_demanda DECIMAL(10,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='classe_xyz') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN classe_xyz VARCHAR(2);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='estoque_seguranca') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN estoque_seguranca INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='nivel_servico_z') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN nivel_servico_z DECIMAL(6,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='lead_time_dias') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN lead_time_dias INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='venda_perdida_12m') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN venda_perdida_12m DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='valor_vendido_12m') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN valor_vendido_12m DECIMAL(15,4);
        END IF;

        -- ===== Padrão de demanda (Croston/Poisson) + sazonalidade =====
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='padrao_demanda') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN padrao_demanda VARCHAR(20);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='metodo_reposicao') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN metodo_reposicao VARCHAR(30);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='fator_sazonal') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN fator_sazonal DECIMAL(8,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='demanda_planejamento_dia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN demanda_planejamento_dia DECIMAL(15,6);
        END IF;

        -- ===== Originais (encomenda) + cálculo consolidado por grupo (descrição) =====
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='sob_encomenda') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN sob_encomenda BOOLEAN DEFAULT FALSE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_chave') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_chave VARCHAR(255);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_qtd_itens') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_qtd_itens INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_estoque_disponivel') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_estoque_disponivel DECIMAL(15,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_demanda_dia') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_demanda_dia DECIMAL(15,6);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_fator_sazonal') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_fator_sazonal DECIMAL(8,4);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_curva') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_curva VARCHAR(10);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_padrao') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_padrao VARCHAR(20);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_metodo') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_metodo VARCHAR(30);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_estoque_min') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_estoque_min INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_estoque_max') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_estoque_max INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='grupo_estoque_seguranca') THEN
            ALTER TABLE com_fifo_completo ADD COLUMN grupo_estoque_seguranca INTEGER;
        END IF;
    END
    $$;

    CREATE INDEX IF NOT EXISTS idx_com_fifo_classe_xyz ON com_fifo_completo (classe_xyz);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_padrao_demanda ON com_fifo_completo (padrao_demanda);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_grupo_chave ON com_fifo_completo (grupo_chave);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_sob_encomenda ON com_fifo_completo (sob_encomenda);

    CREATE INDEX IF NOT EXISTS idx_com_fifo_group_id ON com_fifo_completo (group_id);
    CREATE INDEX IF NOT EXISTS idx_rel_group_id ON com_relacionamento_itens (group_id);
    CREATE INDEX IF NOT EXISTS idx_rel_pro_codigo ON com_relacionamento_itens (pro_codigo);

    -- Tabela de Saldo Residual (FIFO detalhado)
    CREATE TABLE IF NOT EXISTS com_data_saldo_produto (
        id SERIAL PRIMARY KEY,
        pro_codigo VARCHAR(50),
        data_compra DATE,
        saldo_residual DECIMAL(15,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_saldo_prod_codigo ON com_data_saldo_produto(pro_codigo);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("Tabela com_fifo_completo criada/verificada com sucesso no PostgreSQL")
    except Exception as e:
        print(f"Erro ao criar tabela PostgreSQL: {e}")
        raise


def carregar_dados_do_banco(corte=None):
    """
    Lê do banco as "abas" lógicas:
      - SAIDAS_GERAL  (saídas detalhadas a partir de `corte`, default 2005)
      - ENTRADAS
      - DEVOLUCOES
      - SALDO_PRODUTO (estoque atual)

    `corte` (YYYY-MM-DD ou None): no modo incremental (empacotamento), só lê
    movimento de saída/entrada a partir do corte (janela). As camadas anteriores
    vêm do pacote no Mongo. None = carga completa (desde 2005), usada no backfill.
    """
    conn = get_connection()

    data_ini = str(corte) if corte else "2005-01-01"

    # >>>>>>>>>>>>> SQLs <<<<<<<<<<<<

    # Saídas a partir de `data_ini` (OPENQUERY no Linked Server CONSULTA).
    sql_saidas_geral = f"""
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT
            LE.pro_codigo,
            LE.nfe,
            LE.nfs,
            LE.lancto,
            LE.preco_custo,
            LE.total_liquido,
            LE.data,
            LE.origem,
            LE.quantidade
        FROM lanctos_estoque LE
        WHERE LE.data    >= ''{data_ini}''
          AND LE.empresa = 3
          AND LE.origem IN (''NFS'',''EVF'', ''EFD'')
          AND NOT EXISTS (
                SELECT 1
                FROM lanctos_estoque C
                WHERE C.empresa = LE.empresa
                  AND C.nfs     = LE.nfs
                  AND C.origem  = ''CNS''
                  AND C.data    >= ''{data_ini}''
            )
        ORDER BY
            LE.pro_codigo ASC,
            LE.data ASC,
            LE.lancto ASC
    ')
    """

    sql_entradas = f"""
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT
            LE.pro_codigo,
            LE.nfe,
            LE.nfs,
            LE.lancto,
            LE.preco_custo,
            LE.total_liquido,
            LE.data,
            LE.origem,
            LE.quantidade,
            ROW_NUMBER() OVER (
                PARTITION BY LE.pro_codigo
                ORDER BY LE.data ASC, LE.lancto ASC
            ) AS indice,
            SUM(LE.quantidade) OVER (
                PARTITION BY LE.pro_codigo
                ORDER BY LE.data ASC, LE.lancto ASC
                ROWS UNBOUNDED PRECEDING
            ) AS qtd_acumulada
        FROM lanctos_estoque LE
        WHERE LE.data >= ''{data_ini}''
          AND LE.origem IN (''NFE'',''CNE'',''LIA'',''CAD'',''CDE'')
          AND LE.empresa = 3
        ORDER BY
            LE.pro_codigo ASC,
            LE.data ASC,
            LE.lancto ASC
    ')
    """

    sql_devolucoes = """
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT
            nfsi.nfs, 
            nfsi.pro_codigo,
            nfsi.qtde_devolvida
        FROM nfs_itens nfsi
        WHERE nfsi.qtde_devolvida > 0
          AND nfsi.empresa = 3
    ')
    """

    sql_saldo_produto = """
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT 
            pro.pro_codigo,
            pro.pro_descricao,
            pro.subgrp_codigo AS SGR_CODIGO,
            subp.subgrp_descricao AS SGR_DESCRICAO,
            pro.referencia AS PRO_REFERENCIA,
            pro.estoque_disponivel,
            mar.mar_descricao,
            f1.for_nome AS fornecedor1,
            f2.for_nome AS fornecedor2,
            f3.for_nome AS fornecedor3
        FROM produtos pro
        LEFT JOIN marcas mar
            ON mar.empresa    = pro.empresa
           AND mar.mar_codigo = pro.mar_codigo
        LEFT JOIN produtos_subgrupos SUBP
            ON SUBP.subgrp_codigo = PRO.subgrp_codigo
            AND SUBP.empresa = PRO.empresa
        LEFT JOIN fornecedores f1
            ON f1.empresa     = pro.empresa
           AND f1.for_codigo  = pro.for_codigo      -- fornecedor principal
        LEFT JOIN fornecedores f2
            ON f2.empresa     = pro.empresa
           AND f2.for_codigo  = pro.for_codigo2     -- fornecedor 2
        LEFT JOIN fornecedores f3
            ON f3.empresa     = pro.empresa
           AND f3.for_codigo  = pro.for_codigo3     -- fornecedor 3
        WHERE pro.empresa = 3
          AND UPPER(pro.inativo) = ''N''
          AND UPPER(pro.comercializavel) = ''S''
    ')
    """

    # Venda perdida (demanda observada perdida) — base para correção de demanda
    # censurada. Tabela pequena (empresa 3); filtramos a janela no pandas.
    sql_venda_perdida = """
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT
            vp.pro_codigo,
            vp.quantidade,
            vp.data
        FROM venda_perdida vp
        WHERE vp.empresa = 3
          AND vp.quantidade > 0
    ')
    """

    print("\nLendo dados do banco via ODBC...")

    df_saidas      = pd.read_sql(sql_saidas_geral,  conn)
    df_ent         = pd.read_sql(sql_entradas,      conn)
    df_dev         = pd.read_sql(sql_devolucoes,    conn)
    df_saldo_produto = pd.read_sql(sql_saldo_produto, conn)
    try:
        df_vp      = pd.read_sql(sql_venda_perdida,  conn)
    except Exception as e:
        print(f"  - AVISO: falha ao ler VENDA_PERDIDA ({e}). Seguindo sem ela.")
        df_vp = pd.DataFrame(columns=["PRO_CODIGO", "QUANTIDADE", "DATA"])

    print(f"  - Saídas carregadas: {len(df_saidas)} registros")
    print(f"  - Entradas carregadas: {len(df_ent)} registros")
    print(f"  - Devoluções carregadas: {len(df_dev)} registros")
    print(f"  - Produtos (Saldo) carregados: {len(df_saldo_produto)} registros")
    print(f"  - Venda perdida carregada: {len(df_vp)} registros")

    # Normalizar nomes das colunas de saldo_produto
    df_saldo_produto = df_saldo_produto.rename(columns={
        "pro_codigo":          "PRO_CODIGO",
        "PRO_CODIGO":          "PRO_CODIGO",
        "pro_descricao":       "PRO_DESCRICAO",
        "PRO_DESCRICAO":       "PRO_DESCRICAO",
        "subgrp_codigo":       "SGR_CODIGO",
        "SGR_CODIGO":          "SGR_CODIGO",
        "estoque_disponivel":  "ESTOQUE_DISPONIVEL",
        "ESTOQUE_DISPONIVEL":  "ESTOQUE_DISPONIVEL",
        "mar_descricao":       "MAR_DESCRICAO",
        "MAR_DESCRICAO":       "MAR_DESCRICAO",
        "subgrp_descricao":    "SGR_DESCRICAO",
        "SUBGRP_DESCRICAO":    "SGR_DESCRICAO",
        "fornecedor1":         "FORNECEDOR1",
        "FORNECEDOR1":         "FORNECEDOR1",
        "fornecedor2":         "FORNECEDOR2",
        "FORNECEDOR2":         "FORNECEDOR2",
        "FORNECEDOR3":         "FORNECEDOR3",
        "referencia":          "PRO_REFERENCIA", # Caso venha lowercase do SQL
        "pro_referencia":      "PRO_REFERENCIA"
    })
    
    # DEBUG: Verificar colunas carregadas
    print("DEBUG: Colunas do df_saldo_produto:", df_saldo_produto.columns.tolist())
    if "SGR_DESCRICAO" in df_saldo_produto.columns:
        print("DEBUG: Amostra SGR_DESCRICAO:", df_saldo_produto["SGR_DESCRICAO"].head().tolist())
    else:
        print("DEBUG: SGR_DESCRICAO não encontrada no DataFrame!")

    conn.close()
    return df_saidas, df_ent, df_dev, df_saldo_produto, df_vp

def aplicar_analise_agrupada(df_met: pd.DataFrame) -> pd.DataFrame:
    """
    Análise agrupada por group_id (APENAS PARA EXIBIÇÃO):
      - adiciona group_id (com_relacionamento_itens)
      - cria colunas GRP_* com somatórios do grupo
        * em especial, GRP_ESTOQUE_MIN_AJUSTADO / GRP_ESTOQUE_MAX_AJUSTADO = SOMA dos itens
      - calcula RATEIO_PROP_GRUPO = QTD_VENDIDA / GRP_QTD_VENDIDA
      - calcula GRP_DEMANDA_MEDIA_DIA ponderada por RATEIO_PROP_GRUPO (apenas exibição)
    
    IMPORTANTE:
      - NÃO sobrescreve ESTOQUE_MIN_SUGERIDO / ESTOQUE_MAX_SUGERIDO
      - NÃO altera min/max individual (nem ajustado nem sugerido)
    """
    print("Aplicando análise agrupada para produtos similares (apenas exibição, sem sobrescrever cálculos)...")

    try:
        engine_pg = get_postgres_engine()

        df_groups = pd.read_sql(
            "SELECT pro_codigo, group_id FROM com_relacionamento_itens",
            engine_pg
        )

        if df_groups.empty:
            return df_met

        df_groups = df_groups.rename(columns={"pro_codigo": "PRO_CODIGO"})

        # Tipos compatíveis
        df_groups["PRO_CODIGO"] = df_groups["PRO_CODIGO"].astype(str).str.strip()
        df_met["PRO_CODIGO"] = df_met["PRO_CODIGO"].astype(str).str.strip()

        # Merge group_id
        if "group_id" not in df_met.columns:
            df_met = df_met.merge(df_groups, on="PRO_CODIGO", how="left")
        else:
            df_met = df_met.merge(df_groups, on="PRO_CODIGO", how="left", suffixes=("", "_new"))
            df_met["group_id"] = df_met["group_id"].fillna(df_met["group_id_new"])
            df_met = df_met.drop(columns=["group_id_new"])

        mask_grouped = df_met["group_id"].notna()
        if not mask_grouped.any():
            return df_met

        # Somar colunas relevantes para o grupo (sem alterar as individuais)
        cols_sum_base = [
            "ESTOQUE_DISPONIVEL",
            "QTD_VENDIDA",
            "NUM_VENDAS",
            "VENDAS_ULT_12M",
            "ESTOQUE_MIN_BASE",
            "ESTOQUE_MAX_BASE",
            # soma do sugerido para consulta/exibição, sem mexer no individual
            "ESTOQUE_MIN_SUGERIDO",
            "ESTOQUE_MAX_SUGERIDO",
        ]
        cols_sum = [c for c in cols_sum_base if c in df_met.columns]

        for c in cols_sum:
            df_met[c] = pd.to_numeric(df_met[c], errors="coerce").fillna(0)

        grp_agg = (
            df_met.loc[mask_grouped]
            .groupby("group_id")[cols_sum]
            .sum()
            .reset_index()
            .rename(columns={c: f"GRP_{c}" for c in cols_sum})
        )

        df_met = df_met.merge(grp_agg, on="group_id", how="left")

        # ===== RATEIO_PROP_GRUPO (baseado em vendas) =====
        if "GRP_QTD_VENDIDA" in df_met.columns and "QTD_VENDIDA" in df_met.columns:
            def calc_prop(row):
                if pd.isna(row["group_id"]):
                    return np.nan
                grp_v = float(row.get("GRP_QTD_VENDIDA", 0) or 0)
                if grp_v <= 0:
                    return 0.0
                qtd = float(row.get("QTD_VENDIDA", 0) or 0)
                return qtd / grp_v

            df_met["RATEIO_PROP_GRUPO"] = df_met.apply(calc_prop, axis=1)
        else:
            df_met["RATEIO_PROP_GRUPO"] = np.nan

        # ===== GRP_DEMANDA_MEDIA_DIA (ponderada por rateio, apenas exibição) =====
        # Observação: isso vira uma "média ponderada" da demanda do grupo, útil pra leitura.
        if "DEMANDA_MEDIA_DIA" in df_met.columns:
            df_met["DEMANDA_MEDIA_DIA"] = pd.to_numeric(df_met["DEMANDA_MEDIA_DIA"], errors="coerce").fillna(0)
            df_met["_DEMANDA_PONDERADA"] = df_met["DEMANDA_MEDIA_DIA"] * df_met["RATEIO_PROP_GRUPO"].fillna(0)

            grp_demanda = (
                df_met.loc[mask_grouped]
                .groupby("group_id")["_DEMANDA_PONDERADA"]
                .sum()
                .reset_index()
                .rename(columns={"_DEMANDA_PONDERADA": "GRP_DEMANDA_MEDIA_DIA"})
            )

            df_met = df_met.merge(grp_demanda, on="group_id", how="left")
            df_met = df_met.drop(columns=["_DEMANDA_PONDERADA"])

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # IMPORTANTE: NÃO ratear e NÃO sobrescrever ESTOQUE_MIN/MAX_SUGERIDO
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        return df_met

    except Exception as e:
        print(f"Erro ao aplicar análise agrupada: {e}")
        return df_met


# ==========================================
# MÉTRICAS, ABC, TENDÊNCIA, ESTOQUE MIN/MÁX
# ==========================================

def _quantil_demanda(media, var, p, z):
    """
    Ponto de pedido = quantil (nível p) da demanda no horizonte, escolhendo a
    distribuição certa para o padrão:
      - media<=0            -> 0
      - media grande (>=60) -> aproximação Normal: media + z*sqrt(var)
      - var ~ media         -> Poisson exata (demanda intermitente)
      - var > media         -> Binomial Negativa exata (demanda grumosa/sobredispersa)
    Retorna inteiro.
    """
    try:
        media = float(media); var = float(var); p = float(p); z = float(z)
    except (TypeError, ValueError):
        return 0
    if media <= 0:
        return 0
    var = max(var, media)  # nunca abaixo da Poisson (var=media)

    if media >= 60:
        return int(math.ceil(media + z * math.sqrt(var)))

    # Poisson exata
    if var <= media * 1.10:
        cdf = 0.0; pmf = math.exp(-media); k = 0
        while k < 100000:
            cdf += pmf
            if cdf >= p:
                return k
            k += 1
            pmf *= media / k
        return k

    # Binomial Negativa (mean=media, var=var): r>0, 0<pr<1
    r = media * media / (var - media)
    pr = r / (r + media)
    cdf = 0.0
    pmf = pr ** r            # P(X=0)
    k = 0
    while k < 100000:
        cdf += pmf
        if cdf >= p:
            return k
        k += 1
        pmf *= (k + r - 1) / k * (1 - pr)
    return k


def calcular_indices_sazonais(df_sai_fifo: pd.DataFrame,
                              df_saldo_produto: pd.DataFrame,
                              hoje: pd.Timestamp,
                              anos: int = SAZ_ANOS,
                              vendas_mensais_extra: pd.DataFrame = None) -> dict:
    """
    Índice sazonal multiplicativo por SUBGRUPO, em anos completos.
    Normaliza cada ano pela sua média mensal (remove crescimento) e tira a média
    entre anos. Marca como confiável só quando há amplitude, volume, histórico e
    CONSISTÊNCIA (pico no mesmo trimestre na maioria dos anos) — evita rotular
    ruído de item de baixo giro como sazonal.

    Retorna: { sgr_codigo(int): {"idx": [12 índices jan..dez], "confiavel": bool} }
    """
    ano_fim = hoje.year - 1            # último ano completo
    ano_ini = ano_fim - anos + 1
    if df_saldo_produto.empty:
        return {}

    sp = df_saldo_produto.copy()
    sp["PRO_CODIGO"] = sp["PRO_CODIGO"].astype(str).str.strip()
    pro2sgr = sp.dropna(subset=["SGR_CODIGO"]).set_index("PRO_CODIGO")["SGR_CODIGO"].to_dict()

    s = df_sai_fifo[["PRO_CODIGO", "DATA", "QUANTIDADE_AJUSTADA"]].copy()
    s["DATA"] = pd.to_datetime(s["DATA"], errors="coerce")
    s["PRO_CODIGO"] = s["PRO_CODIGO"].astype(str).str.strip()
    s = s[(s["DATA"].dt.year >= ano_ini) & (s["DATA"].dt.year <= ano_fim)]
    s["SGR"] = s["PRO_CODIGO"].map(pro2sgr)
    s = s.dropna(subset=["SGR"])
    s["ANO"] = s["DATA"].dt.year
    s["MES"] = s["DATA"].dt.month
    partes_agg = [s[["SGR", "ANO", "MES", "QUANTIDADE_AJUSTADA"]]]

    # Meses CONGELADOS no pacote (modo incremental): SGR_CODIGO, ANO, MES, QTD
    if vendas_mensais_extra is not None and not vendas_mensais_extra.empty:
        ex = vendas_mensais_extra.copy()
        ex = ex[(ex["ANO"] >= ano_ini) & (ex["ANO"] <= ano_fim)]
        if not ex.empty:
            ex = ex.rename(columns={"SGR_CODIGO": "SGR", "QTD": "QUANTIDADE_AJUSTADA"})
            partes_agg.append(ex[["SGR", "ANO", "MES", "QUANTIDADE_AJUSTADA"]])

    base = pd.concat(partes_agg, ignore_index=True)
    if base.empty:
        return {}
    agg = base.groupby(["SGR", "ANO", "MES"])["QUANTIDADE_AJUSTADA"].sum().reset_index()

    indices = {}
    for sgr, g in agg.groupby("SGR"):
        piv = g.pivot_table(index="ANO", columns="MES",
                            values="QUANTIDADE_AJUSTADA", aggfunc="sum").reindex(columns=range(1, 13))
        piv = piv.dropna(how="all")
        if piv.shape[0] < 3:
            continue
        media_ano = piv.mean(axis=1)
        norm = piv.div(media_ano, axis=0)
        idx = norm.mean(axis=0)
        if idx.isna().any() or idx.sum() == 0:
            continue
        idx = idx / idx.mean()
        vol = float(piv.sum(axis=1).mean())
        amp = float(idx.max() / idx.min()) if idx.min() > 0 else np.inf
        tri_geral = (int(idx.idxmax()) - 1) // 3
        tri_ano = ((piv.idxmax(axis=1) - 1) // 3)
        consist = float((tri_ano == tri_geral).mean())
        confiavel = (vol >= SAZ_VOL_MIN) and (amp >= SAZ_AMPLITUDE_MIN) and (consist >= SAZ_CONSIST_MIN)
        try:
            key = int(sgr)
        except (TypeError, ValueError):
            continue
        indices[key] = {"idx": [float(idx.get(m, 1.0)) for m in range(1, 13)],
                        "confiavel": bool(confiavel)}
    return indices


def _fator_sazonal_forward(indices_sgr, hoje, horizonte_dias):
    """Fator sazonal médio dos meses cobertos por [hoje, hoje+horizonte]."""
    if not indices_sgr or not indices_sgr.get("confiavel"):
        return 1.0
    idx = indices_sgr["idx"]
    fim = hoje + pd.Timedelta(days=int(horizonte_dias))
    meses = pd.period_range(hoje.to_period("M"), fim.to_period("M"), freq="M")
    vals = [idx[m.month - 1] for m in meses]
    if not vals:
        return 1.0
    return float(min(max(float(np.mean(vals)), SAZ_FATOR_MIN), SAZ_FATOR_MAX))


def calcular_demanda_recente_e_variabilidade(df_sai_fifo: pd.DataFrame,
                                             df_vp: pd.DataFrame,
                                             hoje: pd.Timestamp,
                                             janela_meses: int = JANELA_DEMANDA_MESES) -> pd.DataFrame:
    """
    Demanda real recente por produto na janela de `janela_meses`, somando
    vendas + venda perdida observada (com cap por evento). Base para:
      - demanda média diária (substitui a média desde 2005 e o fator de ruptura);
      - desvio-padrão diário da demanda -> estoque de segurança estatístico;
      - coeficiente de variação (CV) -> classe XYZ;
      - valor vendido na janela -> curva ABC recente.

    Retorna DataFrame por PRO_CODIGO com:
      DEM_DIA_VENDAS, DEM_DIA_REAL, SIGMA_DEMANDA_DIA, CV_DEMANDA,
      VENDA_PERDIDA_12M, VALOR_VENDIDO_12M
    """
    data_ini = (hoje - pd.DateOffset(months=janela_meses)).normalize()
    dias_janela = max((hoje - data_ini).days, 1)
    meses = pd.period_range(data_ini.to_period("M"), hoje.to_period("M"), freq="M")
    n_meses = max(len(meses), 1)

    # ---------- Vendas na janela ----------
    s = df_sai_fifo[["PRO_CODIGO", "DATA", "QUANTIDADE_AJUSTADA"]].copy()
    s["DATA"] = pd.to_datetime(s["DATA"], errors="coerce")
    s["PRO_CODIGO"] = s["PRO_CODIGO"].astype(str).str.strip()
    s["QUANTIDADE_AJUSTADA"] = pd.to_numeric(s["QUANTIDADE_AJUSTADA"], errors="coerce").fillna(0)
    if "TOTAL_LIQUIDO" in df_sai_fifo.columns:
        s["TOTAL_LIQUIDO"] = pd.to_numeric(df_sai_fifo["TOTAL_LIQUIDO"], errors="coerce").fillna(0).values
    else:
        s["TOTAL_LIQUIDO"] = 0.0
    s = s[(s["DATA"] >= data_ini) & (s["DATA"] <= hoje)]
    s["MES"] = s["DATA"].dt.to_period("M")

    vendas_mes = s.groupby(["PRO_CODIGO", "MES"])["QUANTIDADE_AJUSTADA"].sum()
    valor_12m = s.groupby("PRO_CODIGO")["TOTAL_LIQUIDO"].sum()

    # venda média por evento (linha de venda) — usada como referência do cap da VP
    linhas = s.groupby("PRO_CODIGO")["QUANTIDADE_AJUSTADA"].agg(["sum", "count"])
    media_evento = (linhas["sum"] / linhas["count"]).replace([np.inf, -np.inf], np.nan)

    # ---------- Venda perdida (cap por evento) ----------
    vp_mes = pd.Series(dtype=float)
    vp_total = pd.Series(dtype=float)
    qtd_capada = 0.0
    n_vp_janela = 0
    if df_vp is not None and not df_vp.empty:
        v = df_vp.rename(columns={c: str(c).upper() for c in df_vp.columns}).copy()
        v["PRO_CODIGO"] = v["PRO_CODIGO"].astype(str).str.strip()
        v["DATA"] = pd.to_datetime(v["DATA"], errors="coerce")
        v["QUANTIDADE"] = pd.to_numeric(v["QUANTIDADE"], errors="coerce").fillna(0)
        v = v[(v["DATA"] >= data_ini) & (v["DATA"] <= hoje) & (v["QUANTIDADE"] > 0)]
        n_vp_janela = len(v)
        if not v.empty:
            cap = media_evento.reindex(v["PRO_CODIGO"].values).fillna(0).to_numpy() * VP_CAP_MULT
            cap = np.maximum(cap, VP_CAP_PISO)
            cap = np.minimum(cap, VP_CAP_TETO)
            q_orig = v["QUANTIDADE"].to_numpy(dtype=float)
            q_cap = np.minimum(q_orig, cap)
            qtd_capada = float((q_orig - q_cap).sum())
            v = v.assign(Q_CAP=q_cap)
            v["MES"] = v["DATA"].dt.to_period("M")
            vp_mes = v.groupby(["PRO_CODIGO", "MES"])["Q_CAP"].sum()
            vp_total = v.groupby("PRO_CODIGO")["Q_CAP"].sum()

    print(f"  - Venda perdida na janela ({janela_meses}m): {n_vp_janela} apontamentos; "
          f"quantidade aparada pelo cap: {qtd_capada:.0f} un")

    # ---------- Demanda mensal real = vendas + venda perdida ----------
    demanda_mes = vendas_mes.add(vp_mes, fill_value=0)

    if demanda_mes.empty:
        return pd.DataFrame(columns=["PRO_CODIGO", "DEM_DIA_VENDAS", "DEM_DIA_REAL",
                                     "SIGMA_DEMANDA_DIA", "CV_DEMANDA",
                                     "VENDA_PERDIDA_12M", "VALOR_VENDIDO_12M",
                                     "ADI", "CV2_TAMANHO", "MEAN_SIZE_MES"])

    mat = demanda_mes.unstack(fill_value=0).reindex(columns=meses, fill_value=0)
    vendas_mat = (vendas_mes.unstack(fill_value=0).reindex(columns=meses, fill_value=0)
                  if not vendas_mes.empty else pd.DataFrame(index=mat.index))

    res = pd.DataFrame(index=mat.index)
    res["DEM_DIA_REAL"] = mat.sum(axis=1) / dias_janela
    sigma_mes = mat.std(axis=1, ddof=1) if n_meses > 1 else pd.Series(0.0, index=mat.index)
    media_mes = mat.mean(axis=1)
    # variância escala com o tempo -> sigma_dia = sigma_mes / sqrt(dias por mês)
    res["SIGMA_DEMANDA_DIA"] = sigma_mes / np.sqrt(dias_janela / n_meses)
    res["CV_DEMANDA"] = np.where(media_mes > 0, sigma_mes / media_mes, 0.0)

    # ---------- Padrão de demanda (Syntetos-Boylan-Croston) ----------
    # ADI = nº de meses / meses com venda ; CV² = variância/média² dos tamanhos
    nz = (mat > 0).sum(axis=1).astype(float)
    sums = mat.sum(axis=1).astype(float)
    sumsq = (mat * mat).sum(axis=1).astype(float)
    nz_safe = nz.replace(0, np.nan)
    mean_size = sums / nz_safe
    var_size = (sumsq / nz_safe) - mean_size ** 2
    res["ADI"] = (float(n_meses) / nz_safe).fillna(9999.0)
    res["CV2_TAMANHO"] = (var_size.clip(lower=0) / (mean_size ** 2)).where(mean_size > 0, 0.0).fillna(0.0)
    # tamanho médio do lote (qtd dos meses com venda) — base da Poisson composta
    res["MEAN_SIZE_MES"] = mean_size.fillna(0.0)
    res["VENDA_PERDIDA_12M"] = vp_total.reindex(mat.index).fillna(0) if not vp_total.empty else 0.0
    res["VALOR_VENDIDO_12M"] = valor_12m.reindex(mat.index).fillna(0)
    if not vendas_mat.empty:
        res["DEM_DIA_VENDAS"] = vendas_mat.reindex(index=mat.index).fillna(0).sum(axis=1) / dias_janela
    else:
        res["DEM_DIA_VENDAS"] = 0.0

    res = res.reset_index().rename(columns={res.index.name or "index": "PRO_CODIGO"})
    if "PRO_CODIGO" not in res.columns:
        res = res.rename(columns={res.columns[0]: "PRO_CODIGO"})
    return res


# Cobertura por classe: (dias_min, dias_max). A diferença vira o "dias de ciclo"
# (lote de reposição). Subgrupo 154 = giro mais lento.
REGRAS_DIAS = {
    "default": {"A": (20, 60), "B": (30, 90), "C": (45, 120), "D": (0, 45)},
    154:       {"A": (45, 120), "B": (60, 180), "C": (90, 240), "D": (0, 120)},
}

def _classificar_padrao(dem, adi, cv2):
    if (dem or 0) <= 0:
        return "Sem_Giro"
    try:
        adi = float(adi); cv2 = float(cv2)
    except (TypeError, ValueError):
        return "Suave"
    if adi < SBC_ADI and cv2 < SBC_CV2:
        return "Suave"
    if adi < SBC_ADI:
        return "Erratico"
    if cv2 < SBC_CV2:
        return "Intermitente"
    return "Grumoso"

def calcular_min_max(curva, dem, sigma_dia, padrao, sgr, data_max, cv2, msize, hoje):
    """
    Min/Máx (s,S) escolhendo o método pelo padrão de demanda:
      - Suave/Errático -> Normal: SS=Z·σ·√LT (teto N ciclos); min=d·LT+SS; max=min+d·ciclo
      - Intermitente/Grumoso -> Poisson composta: quantil da distribuição discreta
    `dem` já deve vir sazonalizada. Retorna dict (min, max, ss, z).
    """
    regra = REGRAS_DIAS.get(sgr, REGRAS_DIAS["default"])
    z = Z_POR_CURVA.get(curva, Z_POR_CURVA["C"])
    try: sigma_dia = float(sigma_dia) if not pd.isna(sigma_dia) else 0.0
    except (TypeError, ValueError): sigma_dia = 0.0
    try: dem = float(dem) if not pd.isna(dem) else 0.0
    except (TypeError, ValueError): dem = 0.0
    if dem <= 0 or curva not in regra:
        return {"ESTOQUE_MIN_BASE": 0, "ESTOQUE_MAX_BASE": 0, "ESTOQUE_SEGURANCA": 0, "NIVEL_SERVICO_Z": 0.0}
    dmin, dmax = regra[curva]; dias_ciclo = max(dmax - dmin, 1)
    if padrao in ("Intermitente", "Grumoso"):
        dem_c = dem * (1 - ALPHA_CROSTON / 2.0); p = NS_POR_CURVA.get(curva, 0.90)
        try: cv2 = float(cv2 or 0.0); msize = float(msize or 0.0)
        except (TypeError, ValueError): cv2, msize = 0.0, 0.0
        disp = max(msize * (1.0 + cv2), 1.0)
        media_lt = dem_c * LEAD_TIME_DIAS; media_ltc = dem_c * (LEAD_TIME_DIAS + dias_ciclo)
        rop = _quantil_demanda(media_lt, media_lt * disp, p, z)
        S = _quantil_demanda(media_ltc, media_ltc * disp, p, z)
        est_min = int(rop); est_max = int(max(S, rop + 1)); est_ss = int(max(rop - media_lt, 0))
    else:
        ss = z * sigma_dia * np.sqrt(LEAD_TIME_DIAS)
        if SS_CAP_CICLOS and SS_CAP_CICLOS > 0:
            ss = min(ss, dem * dias_ciclo * SS_CAP_CICLOS)
        rop = dem * LEAD_TIME_DIAS + ss; max_nivel = rop + dem * dias_ciclo
        est_min = int(np.ceil(rop)); est_max = int(np.ceil(max_nivel)); est_ss = int(np.ceil(ss))
    dias_corte = 365 if sgr == 154 else 240
    if not pd.isna(data_max) and (hoje - data_max).days > dias_corte:
        est_min = 0; est_max = max(1, int(np.ceil(dem * 15))); est_ss = 0
    return {"ESTOQUE_MIN_BASE": est_min, "ESTOQUE_MAX_BASE": est_max,
            "ESTOQUE_SEGURANCA": est_ss, "NIVEL_SERVICO_Z": round(float(z), 4)}

METODO_LABEL = {"Suave": "Normal (Z·σ·√LT)", "Erratico": "Normal (Z·σ·√LT)",
                "Intermitente": "Croston+Poisson", "Grumoso": "Croston+Binomial Neg",
                "Sem_Giro": "—"}

def calcular_grupos_descricao(df_sai_fifo, df_vp, df_saldo_produto, hoje, indices_saz):
    """
    Cálculo CONSOLIDADO por grupo de produto (= descrição), somando a demanda de
    TODAS as marcas (substitutos), EXCLUINDO os produtos ORIGINAIS (encomenda).
    Reaproveita o mesmo motor de demanda/variabilidade, re-chaveando por descrição.
    Retorna DataFrame por GRUPO com min/máx/SS consolidados (efeito de pooling).
    """
    sp = df_saldo_produto.copy()
    sp["PRO_CODIGO"] = sp["PRO_CODIGO"].astype(str).str.strip()
    sp["IS_ORIG"] = sp["MAR_DESCRICAO"].apply(marca_eh_original) if "MAR_DESCRICAO" in sp.columns else False
    sp["GRUPO"] = sp["PRO_DESCRICAO"].astype(str).str.strip()
    nao = sp[(~sp["IS_ORIG"]) & (sp["GRUPO"] != "")].copy()
    if nao.empty:
        return pd.DataFrame()
    pro2grp = nao.set_index("PRO_CODIGO")["GRUPO"].to_dict()

    sai = df_sai_fifo.copy()
    sai["PRO_CODIGO"] = sai["PRO_CODIGO"].astype(str).str.strip()
    sai["GRP"] = sai["PRO_CODIGO"].map(pro2grp)
    sai = sai.dropna(subset=["GRP"]).copy()
    sai["DATA"] = pd.to_datetime(sai["DATA"], errors="coerce")
    dmax_grp = sai.groupby("GRP")["DATA"].max()
    sai["PRO_CODIGO"] = sai["GRP"]  # re-chaveia: grupo vira o "produto"

    vp = pd.DataFrame()
    if df_vp is not None and not df_vp.empty:
        vp = df_vp.rename(columns={c: str(c).upper() for c in df_vp.columns}).copy()
        vp["PRO_CODIGO"] = vp["PRO_CODIGO"].astype(str).str.strip()
        vp["GRP"] = vp["PRO_CODIGO"].map(pro2grp)
        vp = vp.dropna(subset=["GRP"]).copy()
        vp["PRO_CODIGO"] = vp["GRP"]

    g = calcular_demanda_recente_e_variabilidade(sai, vp, hoje)
    if g is None or g.empty:
        return pd.DataFrame()
    g = g.rename(columns={"PRO_CODIGO": "GRUPO"})

    grp_sgr = nao.groupby("GRUPO")["SGR_CODIGO"].agg(lambda s: s.dropna().iloc[0] if s.dropna().size else None)
    grp_est = pd.to_numeric(nao["ESTOQUE_DISPONIVEL"], errors="coerce").fillna(0).groupby(nao["GRUPO"]).sum()
    grp_n = nao.groupby("GRUPO")["PRO_CODIGO"].nunique()

    g["SGR_CODIGO"] = g["GRUPO"].map(grp_sgr)
    g["DATA_MAX_VENDA"] = g["GRUPO"].map(dmax_grp)
    g["GRUPO_ESTOQUE_DISPONIVEL"] = g["GRUPO"].map(grp_est).fillna(0.0)
    g["GRUPO_QTD_ITENS"] = g["GRUPO"].map(grp_n).fillna(0).astype(int)

    # ABC sobre o valor 12m do grupo
    g = g.sort_values("VALOR_VENDIDO_12M", ascending=False).reset_index(drop=True)
    tot = g["VALOR_VENDIDO_12M"].sum()
    g["_PCT"] = (g["VALOR_VENDIDO_12M"].cumsum() / tot * 100) if tot > 0 else 0
    g["GRUPO_CURVA"] = g["_PCT"].apply(lambda p: "A" if p <= 70 else "B" if p <= 90 else "C" if p <= 97 else "D")
    g["GRUPO_PADRAO"] = g.apply(lambda r: _classificar_padrao(r["DEM_DIA_REAL"], r.get("ADI"), r.get("CV2_TAMANHO")), axis=1)

    horiz = LEAD_TIME_DIAS + 60
    def _fs(sgr):
        try: k = int(sgr)
        except (TypeError, ValueError): return 1.0
        return _fator_sazonal_forward(indices_saz.get(k), hoje, horiz)
    g["GRUPO_FATOR_SAZONAL"] = g["SGR_CODIGO"].apply(_fs)
    g["GRUPO_DEMANDA_DIA"] = pd.to_numeric(g["DEM_DIA_REAL"], errors="coerce").fillna(0.0) * g["GRUPO_FATOR_SAZONAL"]

    res = g.apply(lambda r: calcular_min_max(r["GRUPO_CURVA"], r["GRUPO_DEMANDA_DIA"], r["SIGMA_DEMANDA_DIA"],
                  r["GRUPO_PADRAO"], r["SGR_CODIGO"], r["DATA_MAX_VENDA"], r.get("CV2_TAMANHO"),
                  r.get("MEAN_SIZE_MES"), hoje), axis=1)
    g["GRUPO_ESTOQUE_MIN"] = [x["ESTOQUE_MIN_BASE"] for x in res]
    g["GRUPO_ESTOQUE_MAX"] = [x["ESTOQUE_MAX_BASE"] for x in res]
    g["GRUPO_ESTOQUE_SEGURANCA"] = [x["ESTOQUE_SEGURANCA"] for x in res]
    g["GRUPO_METODO"] = g["GRUPO_PADRAO"].map(METODO_LABEL).fillna("Normal (Z·σ·√LT)")

    cols = ["GRUPO", "GRUPO_QTD_ITENS", "GRUPO_ESTOQUE_DISPONIVEL", "GRUPO_DEMANDA_DIA",
            "GRUPO_FATOR_SAZONAL", "GRUPO_CURVA", "GRUPO_PADRAO", "GRUPO_METODO",
            "GRUPO_ESTOQUE_MIN", "GRUPO_ESTOQUE_MAX", "GRUPO_ESTOQUE_SEGURANCA"]
    return g[cols]


def calcular_metricas_e_classificar(df_sai_fifo: pd.DataFrame,
                                    df_ent_valid: pd.DataFrame,
                                    df_saldo_produto: pd.DataFrame,
                                    df_vp: pd.DataFrame = None,
                                    vendas_mensais_extra: pd.DataFrame = None) -> pd.DataFrame:
    """
    Calcula métricas (demanda 12m + venda perdida), ABC/XYZ, padrão de demanda,
    sazonalidade e min/máx. `vendas_mensais_extra` (modo incremental) traz os meses
    congelados do pacote para a sazonalidade.
    """
    df = df_sai_fifo.copy()
    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
    df["DATA_COMPRA"] = pd.to_datetime(df["DATA_COMPRA"], errors="coerce")

    # Diferença em dias entre saída e compra
    df["DPM"] = (df["DATA"] - df["DATA_COMPRA"]).dt.days

    hoje = pd.Timestamp.today().normalize()

    metricas = []
    print("\nCalculando métricas por produto...")

    for cod, grp in df.groupby("PRO_CODIGO"):
        grp_valid = grp.dropna(subset=["QUANTIDADE_AJUSTADA"])
        if grp_valid.empty:
            continue

        grp_dpm = grp_valid.dropna(subset=["DPM"])
        if grp_dpm.empty:
            tempo_medio = np.nan
        else:
            tempo_medio = np.average(grp_dpm["DPM"], weights=grp_dpm["QUANTIDADE_AJUSTADA"])

        qtd_vendida = grp_valid["QUANTIDADE_AJUSTADA"].sum()

        data_min = grp_valid["DATA"].min()
        data_max = grp_valid["DATA"].max()

        num_vendas = len(grp_valid)

        # Vendas dos últimos 12m (informativo). A demanda do modelo vem de
        # calcular_demanda_recente_e_variabilidade (janela 12m + venda perdida);
        # a tendência/sazonalidade do passado foi removida (sazonalidade forward
        # já ajusta a demanda do próximo período).
        data_12m_ini = hoje - pd.DateOffset(months=12)
        mask_12m = (grp_valid["DATA"] >= data_12m_ini) & (grp_valid["DATA"] <= hoje)
        vendas_ult_12m = grp_valid.loc[mask_12m, "QUANTIDADE_AJUSTADA"].sum()
        num_vendas_12m = int(mask_12m.sum())

        metricas.append({
            "PRO_CODIGO": cod,
            "TEMPO_MEDIO_ESTOQUE": tempo_medio,
            "QTD_VENDIDA": qtd_vendida,
            "DATA_MIN_VENDA": data_min,
            "DATA_MAX_VENDA": data_max,
            "NUM_VENDAS": num_vendas,
            "VENDAS_ULT_12M": vendas_ult_12m,
            "NUM_VENDAS_12M": num_vendas_12m,
        })

    df_met = pd.DataFrame(metricas)
    
    # ==========================================
    # ADICIONAR PRODUTOS QUE SÓ TÊM ENTRADAS (SEM VENDAS)
    # ==========================================
    # Identifica produtos que existem no saldo mas não têm vendas
    # Esses produtos vão ter min/max = 0 mas precisam estar no banco
    if not df_saldo_produto.empty:
        # Produtos com estoque
        produtos_com_estoque = set(df_saldo_produto["PRO_CODIGO"].astype(str).str.strip().unique())
        
        # Produtos já processados (com vendas)
        if not df_met.empty:
            produtos_processados = set(df_met["PRO_CODIGO"].astype(str).str.strip().unique())
        else:
            produtos_processados = set()
        
        # Produtos que só têm entradas (diferença entre os dois sets)
        produtos_so_entradas = produtos_com_estoque - produtos_processados
        
        if produtos_so_entradas:
            print(f"\nEncontrados {len(produtos_so_entradas)} produtos com apenas entradas (sem vendas)...")
            print("Esses produtos serão salvos com estoque_min_sugerido=0 e estoque_max_sugerido=0")
            
            # Criar registros mínimos para produtos só com entradas
            metricas_so_entradas = []
            for cod in produtos_so_entradas:
                metricas_so_entradas.append({
                    "PRO_CODIGO": cod,
                    "TEMPO_MEDIO_ESTOQUE": np.nan,
                    "QTD_VENDIDA": 0,
                    "DATA_MIN_VENDA": pd.NaT,
                    "DATA_MAX_VENDA": pd.NaT,
                    "NUM_VENDAS": 0,
                    "VENDAS_ULT_12M": 0,
                    "NUM_VENDAS_12M": 0,
                })
            
            # Adicionar ao DataFrame de métricas
            df_so_entradas = pd.DataFrame(metricas_so_entradas)
            df_met = pd.concat([df_met, df_so_entradas], ignore_index=True)
            print(f"Adicionados {len(metricas_so_entradas)} produtos sem vendas ao DataFrame de métricas")
    
    if df_met.empty:
        return df_met

    # ==========================================
    # DEMANDA RECENTE + VARIABILIDADE (janela 12m, inclui VENDA_PERDIDA)
    #   - substitui a demanda média desde 2005 pela demanda da janela recente
    #   - traz sigma diário (estoque de segurança) e CV (classe XYZ)
    # ==========================================
    print("Calculando demanda recente, venda perdida e variabilidade...")
    df_met["PRO_CODIGO"] = df_met["PRO_CODIGO"].astype(str).str.strip()
    df_rec = calcular_demanda_recente_e_variabilidade(df_sai_fifo, df_vp, hoje)

    cols_rec = ["DEM_DIA_VENDAS", "DEM_DIA_REAL", "SIGMA_DEMANDA_DIA",
                "CV_DEMANDA", "VENDA_PERDIDA_12M", "VALOR_VENDIDO_12M",
                "ADI", "CV2_TAMANHO", "MEAN_SIZE_MES"]
    if df_rec is not None and not df_rec.empty:
        df_rec["PRO_CODIGO"] = df_rec["PRO_CODIGO"].astype(str).str.strip()
        df_met = df_met.merge(df_rec[["PRO_CODIGO"] + cols_rec], on="PRO_CODIGO", how="left")
    for c in cols_rec:
        if c not in df_met.columns:
            df_met[c] = 0.0
        df_met[c] = pd.to_numeric(df_met[c], errors="coerce").fillna(0.0)

    # Demanda de reposição = demanda recente (vendas-only para exibição)
    df_met["DEMANDA_MEDIA_DIA"] = df_met["DEM_DIA_VENDAS"]
    df_met["LEAD_TIME_DIAS"] = LEAD_TIME_DIAS

    # ==========================================
    # DEMANDA AJUSTADA = demanda real recente (vendas + venda perdida observada)
    #   A antiga "ruptura em 2 anos" (pivot diário) e a inflação por
    #   (1 + dias_ruptura/730) foram REMOVIDAS: a venda perdida já corrige a
    #   demanda censurada na própria janela de 12m.
    # ==========================================
    df_saldo_produto["PRO_CODIGO"] = df_saldo_produto["PRO_CODIGO"].astype(str).str.strip()
    df_met["DEMANDA_MEDIA_DIA_AJUSTADA"] = pd.to_numeric(df_met["DEM_DIA_REAL"], errors="coerce").fillna(0.0)

    # ==========================================
    # (MUITO IMPORTANTE) MERGE DO SALDO AQUI
    # ==========================================
    colunas_saldo = [
        "PRO_CODIGO", "PRO_DESCRICAO", "SGR_CODIGO", "SGR_DESCRICAO",
        "ESTOQUE_DISPONIVEL", "MAR_DESCRICAO",
        "FORNECEDOR1", "FORNECEDOR2", "FORNECEDOR3",
    ]
    colunas_saldo = [c for c in colunas_saldo if c in df_saldo_produto.columns]

    df_met["PRO_CODIGO"] = df_met["PRO_CODIGO"].astype(str).str.strip()
    df_saldo_produto["PRO_CODIGO"] = df_saldo_produto["PRO_CODIGO"].astype(str).str.strip()

    df_met = df_met.merge(df_saldo_produto[colunas_saldo], on="PRO_CODIGO", how="left")

    # ==========================================
    # Curva ABC (sobre valor vendido dos últimos 12 meses, não vitalício)
    # ==========================================
    df_met["VALOR_VENDIDO_12M"] = pd.to_numeric(df_met["VALOR_VENDIDO_12M"], errors="coerce").fillna(0)
    df_met = df_met.sort_values("VALOR_VENDIDO_12M", ascending=False).reset_index(drop=True)

    total_valor = df_met["VALOR_VENDIDO_12M"].sum()
    df_met["PCT_ACUM_VALOR"] = (df_met["VALOR_VENDIDO_12M"].cumsum() / total_valor * 100) if total_valor > 0 else 0

    def classificar_abc(pct):
        if pct <= 70:
            return "A"
        elif pct <= 90:
            return "B"
        elif pct <= 97:
            return "C"
        else:
            return "D"

    df_met["CURVA_ABC"] = df_met["PCT_ACUM_VALOR"].apply(classificar_abc)

    # ==========================================
    # Classe XYZ (previsibilidade da demanda pelo coeficiente de variação)
    #   X = previsível | Y = média | Z = errática
    # ==========================================
    def classificar_xyz(cv):
        if pd.isna(cv) or cv <= XYZ_LIMIAR_X:
            return "X"
        elif cv <= XYZ_LIMIAR_Y:
            return "Y"
        else:
            return "Z"

    df_met["CLASSE_XYZ"] = df_met["CV_DEMANDA"].apply(classificar_xyz)
    # Item sem demanda recente não tem previsibilidade definida (não é "X").
    df_met.loc[pd.to_numeric(df_met["DEM_DIA_REAL"], errors="coerce").fillna(0) <= 0, "CLASSE_XYZ"] = None

    # ==========================================
    # Categoria estocagem
    # ==========================================
    def cat_estocagem(t):
        if pd.isna(t):
            return "Sem Dados"
        if t <= 60:
            return "Rápido"
        elif t <= 120:
            return "Médio"
        elif t <= 240:
            return "Lento"
        else:
            return "Obsoleto"

    df_met["CATEGORIA_ESTOCAGEM"] = df_met["TEMPO_MEDIO_ESTOQUE"].apply(cat_estocagem)

    # ==========================================
    # SAZONALIDADE (índice por subgrupo) + PADRÃO DE DEMANDA (SBC)
    # ==========================================
    print("Calculando sazonalidade por subgrupo e padrão de demanda...")
    indices_saz = calcular_indices_sazonais(df_sai_fifo, df_saldo_produto, hoje,
                                            vendas_mensais_extra=vendas_mensais_extra)
    n_saz_ok = sum(1 for v in indices_saz.values() if v.get("confiavel"))
    print(f"  - Subgrupos sazonais confiáveis: {n_saz_ok} de {len(indices_saz)}")

    horizonte_saz = LEAD_TIME_DIAS + 60
    def _fator_saz(sgr):
        try:
            key = int(sgr)
        except (TypeError, ValueError):
            return 1.0
        return _fator_sazonal_forward(indices_saz.get(key), hoje, horizonte_saz)

    df_met["FATOR_SAZONAL"] = df_met["SGR_CODIGO"].apply(_fator_saz)
    df_met["DEMANDA_PLANEJAMENTO_DIA"] = (
        pd.to_numeric(df_met["DEMANDA_MEDIA_DIA_AJUSTADA"], errors="coerce").fillna(0.0)
        * df_met["FATOR_SAZONAL"]
    )

    def _padrao_demanda(row):
        dem = row.get("DEMANDA_MEDIA_DIA_AJUSTADA", 0) or 0
        if dem <= 0:
            return "Sem_Giro"
        try:
            adi = float(row.get("ADI", 9999)); cv2 = float(row.get("CV2_TAMANHO", 0))
        except (TypeError, ValueError):
            return "Suave"
        if adi < SBC_ADI and cv2 < SBC_CV2:
            return "Suave"
        if adi < SBC_ADI and cv2 >= SBC_CV2:
            return "Erratico"
        if adi >= SBC_ADI and cv2 < SBC_CV2:
            return "Intermitente"
        return "Grumoso"

    df_met["PADRAO_DEMANDA"] = df_met.apply(_padrao_demanda, axis=1)
    df_met["METODO_REPOSICAO"] = df_met["PADRAO_DEMANDA"].map({
        "Suave": "Normal (Z·σ·√LT)",
        "Erratico": "Normal (Z·σ·√LT)",
        "Intermitente": "Croston+Poisson",
        "Grumoso": "Croston+Binomial Neg",
        "Sem_Giro": "—",
    }).fillna("Normal (Z·σ·√LT)")

    # ==========================================
    # Min/Max base
    #   A camada "ajustado" (multiplicador por fator de tendência) foi REMOVIDA:
    #   a sazonalidade forward já está embutida em DEMANDA_PLANEJAMENTO_DIA, então
    #   o SUGERIDO parte direto da BASE (sem dupla contagem).
    # ==========================================
    LEAD_TIME = LEAD_TIME_DIAS

    regras_dias = REGRAS_DIAS  # alias da tabela do módulo (usada em montar_descricao)

    def calc_min_max_base(row):
        return pd.Series(calcular_min_max(
            row["CURVA_ABC"], row.get("DEMANDA_PLANEJAMENTO_DIA", 0.0),
            row.get("SIGMA_DEMANDA_DIA", 0.0), row.get("PADRAO_DEMANDA", "Suave"),
            row.get("SGR_CODIGO", None), row["DATA_MAX_VENDA"],
            row.get("CV2_TAMANHO", 0.0), row.get("MEAN_SIZE_MES", 0.0), hoje))

    base_minmax = df_met.apply(calc_min_max_base, axis=1)
    df_met = pd.concat([df_met, base_minmax], axis=1)

    # ==========================================
    # Pouco histórico / Sob demanda / Normal -> SUGERIDO (= BASE)
    # ==========================================
    def ajustar_pouco_historico(row):
        # num_vendas vitalício = portão de esparsidade (mantém o modelo SS para
        # quem tem histórico). A MÉDIA do pouco-histórico e o "sem giro" passam a
        # olhar a JANELA de 12m, para que lançamentos atípicos antigos (ex.: estorno
        # de entrada errada) não inflem o máximo. Ver produto 8130.
        num_vendas = row["NUM_VENDAS"]
        num_vendas_12m = int(row.get("NUM_VENDAS_12M", 0) or 0)
        qtd_12m = float(row.get("VENDAS_ULT_12M", 0) or 0)

        # Nunca vendeu -> base
        if num_vendas is None or num_vendas <= 0:
            return pd.Series({
                "ESTOQUE_MIN_SUGERIDO": row["ESTOQUE_MIN_BASE"],
                "ESTOQUE_MAX_SUGERIDO": row["ESTOQUE_MAX_BASE"],
                "TIPO_PLANEJAMENTO": "Sem_Historico",
            })

        # Tem histórico, mas SEM venda nos últimos 12m -> usa a base (≈0), não a
        # média vitalícia (que pode estar contaminada por evento antigo).
        if num_vendas_12m <= 0:
            return pd.Series({
                "ESTOQUE_MIN_SUGERIDO": row["ESTOQUE_MIN_BASE"],
                "ESTOQUE_MAX_SUGERIDO": row["ESTOQUE_MAX_BASE"],
                "TIPO_PLANEJAMENTO": "Sem_Giro_Recente",
            })

        if num_vendas <= 10:
            # média da venda na JANELA de 12m (não vitalícia)
            qtd_media_venda = qtd_12m / num_vendas_12m if num_vendas_12m > 0 else 0

            if not pd.isna(row["TEMPO_MEDIO_ESTOQUE"]) and row["TEMPO_MEDIO_ESTOQUE"] <= 5:
                min_sug = 0
                max_sug = max(1, int(np.ceil(qtd_media_venda * 1.5)))
                tipo = "Sob_Demanda"
            else:
                min_sug = 0
                max_sug = max(1, int(np.ceil(qtd_media_venda * 2)))
                tipo = "Pouco_Historico"

            return pd.Series({
                "ESTOQUE_MIN_SUGERIDO": min_sug,
                "ESTOQUE_MAX_SUGERIDO": max_sug,
                "TIPO_PLANEJAMENTO": tipo,
            })

        return pd.Series({
            "ESTOQUE_MIN_SUGERIDO": row["ESTOQUE_MIN_BASE"],
            "ESTOQUE_MAX_SUGERIDO": row["ESTOQUE_MAX_BASE"],
            "TIPO_PLANEJAMENTO": "Normal",
        })

    ajuste_hist = df_met.apply(ajustar_pouco_historico, axis=1)
    df_met = pd.concat([df_met, ajuste_hist], axis=1)

    # ==========================================
    # PRODUTOS ORIGINAIS (encomenda) + CÁLCULO CONSOLIDADO POR GRUPO (descrição)
    #  - originais (montadora/genuíno): viram "Sob Encomenda", sem min/máx estocado
    #  - demais: agrupados por descrição, demanda consolidada entre marcas (pooling)
    # ==========================================
    print("Aplicando regra de originais e cálculo consolidado por grupo...")
    if "MAR_DESCRICAO" in df_met.columns:
        df_met["SOB_ENCOMENDA"] = df_met["MAR_DESCRICAO"].apply(marca_eh_original)
    else:
        df_met["SOB_ENCOMENDA"] = False
    df_met["GRUPO_CHAVE"] = np.where(
        df_met["SOB_ENCOMENDA"],
        None,
        df_met["PRO_DESCRICAO"].astype(str).str.strip() if "PRO_DESCRICAO" in df_met.columns else None,
    )
    mask_orig = df_met["SOB_ENCOMENDA"] == True  # noqa: E712
    for col in ["ESTOQUE_MIN_SUGERIDO", "ESTOQUE_MAX_SUGERIDO", "ESTOQUE_SEGURANCA",
                "ESTOQUE_MIN_BASE", "ESTOQUE_MAX_BASE"]:
        if col in df_met.columns:
            df_met.loc[mask_orig, col] = 0
    df_met.loc[mask_orig, "TIPO_PLANEJAMENTO"] = "Sob_Encomenda"
    df_met.loc[mask_orig, "METODO_REPOSICAO"] = "Sob Encomenda"
    print(f"  - Produtos originais (Sob Encomenda): {int(mask_orig.sum())}")

    try:
        df_grp = calcular_grupos_descricao(df_sai_fifo, df_vp, df_saldo_produto, hoje, indices_saz)
        if df_grp is not None and not df_grp.empty:
            print(f"  - Grupos de produto (consolidados): {len(df_grp)}")
            df_met = df_met.merge(df_grp, left_on="GRUPO_CHAVE", right_on="GRUPO", how="left")
            if "GRUPO" in df_met.columns:
                df_met = df_met.drop(columns=["GRUPO"])
    except Exception as e:
        print(f"  - AVISO: falha no cálculo por grupo ({e}). Seguindo sem consolidado.")

    # ==========================================
    # AGORA SIM: AGRUPAMENTO (no lugar correto)
    # ==========================================
    df_met = aplicar_analise_agrupada(df_met)

    # ==========================================
    # Remover itens velhos sem estoque (após saldo já existir)
    # ==========================================
    corte_data = pd.Timestamp("2020-01-01")
    df_met["ESTOQUE_DISPONIVEL"] = pd.to_numeric(df_met["ESTOQUE_DISPONIVEL"], errors="coerce").fillna(0)

    mask_velho_sem_estoque = (
        (df_met["DATA_MAX_VENDA"] < corte_data) &
        (df_met["ESTOQUE_DISPONIVEL"] <= 0)
    )
    df_met = df_met.loc[~mask_velho_sem_estoque].copy()

    # ==========================================
    # Descrição (por último, já com grupo pronto)
    # ==========================================
    def montar_descricao(row):
        curva = row["CURVA_ABC"]
        cat = row["CATEGORIA_ESTOCAGEM"]
        tipo = row["TIPO_PLANEJAMENTO"]
        dem_orig = row["DEMANDA_MEDIA_DIA"]
        dem_ajus = row["DEMANDA_MEDIA_DIA_AJUSTADA"]
        num_vendas = row["NUM_VENDAS"]
        est_min_base = row["ESTOQUE_MIN_BASE"]
        est_max_base = row["ESTOQUE_MAX_BASE"]
        est_min_final = row["ESTOQUE_MIN_SUGERIDO"]
        est_max_final = row["ESTOQUE_MAX_SUGERIDO"]
        est_atual = row["ESTOQUE_DISPONIVEL"]

        partes = []

        if not pd.isna(dem_orig):
            partes.append(
                f"Produto curva {curva}, categoria '{cat}', com {num_vendas} vendas e demanda média original "
                f"de {dem_orig:.3f} un/dia."
            )
        else:
            partes.append(
                f"Produto curva {curva}, categoria '{cat}', com {num_vendas} vendas no período."
            )

        vp12 = row.get("VENDA_PERDIDA_12M", 0) or 0
        if vp12 > 0:
            partes.append(
                f"Demanda real (vendas + {float(vp12):.0f} un de venda perdida em 12m) = {dem_ajus:.3f} un/dia."
            )

        sgr = row.get("SGR_CODIGO", None)
        regra_selecionada = regras_dias.get(sgr, regras_dias["default"])

        if curva in regra_selecionada:
            dias_min, dias_max = regra_selecionada[curva]
            dias_ciclo = max(dias_max - dias_min, 1)
            ss = row.get("ESTOQUE_SEGURANCA", 0)
            z = row.get("NIVEL_SERVICO_Z", 0)
            xyz = row.get("CLASSE_XYZ", "")
            partes.append(
                f"Mínimo (ponto de pedido) = demanda x {LEAD_TIME}d de lead time + estoque de segurança "
                f"({ss} un; classe {curva}/{xyz}, Z={z}); máximo cobre +{dias_ciclo}d de ciclo. "
                f"Base {est_min_base}-{est_max_base} un."
            )
            metodo = row.get("METODO_REPOSICAO", "")
            fs = row.get("FATOR_SAZONAL", 1.0) or 1.0
            extra = f"Método de reposição: {metodo}."
            try:
                if abs(float(fs) - 1.0) >= 0.05:
                    extra += f" Ajuste sazonal do próximo período: x{float(fs):.2f}."
            except (TypeError, ValueError):
                pass
            partes.append(extra)

        if tipo == "Normal":
            partes.append(
                f"Planejamento 'Normal': sugerido final {est_min_final}-{est_max_final} un."
            )
        elif tipo == "Sob_Demanda":
            partes.append("Planejamento 'Sob Demanda' por poucas vendas e giro muito rápido.")
        elif tipo == "Pouco_Historico":
            partes.append("Planejamento 'Pouco Histórico' por baixa quantidade de vendas.")

        if not pd.isna(est_atual):
            partes.append(f"Estoque atual: {est_atual:.0f} un.")

        # Info do grupo (se existir)
        if pd.notna(row.get("group_id", np.nan)):
            grp_est = row.get("GRP_ESTOQUE_DISPONIVEL", np.nan)
            grp_vnd = row.get("GRP_QTD_VENDIDA", np.nan)
            prop = row.get("RATEIO_PROP_GRUPO", np.nan)

            txt = f"Análise unificada do grupo {str(row['group_id'])[:8]}."
            if pd.notna(grp_est):
                txt += f" Estoque grupo: {float(grp_est):.0f}."
            if pd.notna(grp_vnd):
                txt += f" Venda grupo: {float(grp_vnd):.0f}."
            if pd.notna(prop):
                txt += f" Rateio do item: {float(prop)*100:.1f}%."
            partes.append(txt)

        return " ".join(partes)

    df_met["DESCRICAO_CALCULO_ESTOQUE"] = df_met.apply(montar_descricao, axis=1)

    return df_met



# ==========================================
# FIFO DO ESTOQUE ATUAL
# ==========================================

def calcular_fifo_saldo_atual(df_ent_valid: pd.DataFrame,
                              df_sai_fifo: pd.DataFrame,
                              df_saldo_produto: pd.DataFrame):
    """
    Calcula, para cada produto, de quais entradas FIFO é composto o estoque atual.
    """
    camadas = []
    divergencias = []

    desc_map = (
        df_saldo_produto[["PRO_CODIGO", "PRO_DESCRICAO"]]
        .drop_duplicates()
        .set_index("PRO_CODIGO")["PRO_DESCRICAO"]
        .to_dict()
    )

    saldo_map = (
        df_saldo_produto[["PRO_CODIGO", "ESTOQUE_DISPONIVEL"]]
        .drop_duplicates()
        .set_index("PRO_CODIGO")["ESTOQUE_DISPONIVEL"]
        .to_dict()
    )

    produtos_saldo = df_saldo_produto["PRO_CODIGO"].unique()
    total_prod = len(produtos_saldo)
    print(f"\nCalculando camadas FIFO do estoque atual para {total_prod} produtos...")

    for i, cod in enumerate(produtos_saldo, start=1):
        if i % 500 == 0:
            print(f"  [{i}/{total_prod}] Processando...")

        estoque_disp = saldo_map.get(cod, 0)
        if estoque_disp is None or estoque_disp <= 0:
            continue

        if "LANCTO" in df_ent_valid.columns:
            entradas = (
                df_ent_valid[df_ent_valid["PRO_CODIGO"] == cod]
                .sort_values(["DATA", "LANCTO"])
                .copy()
            )
        else:
            entradas = (
                df_ent_valid[df_ent_valid["PRO_CODIGO"] == cod]
                .sort_values("DATA")
                .copy()
            )

        if entradas.empty:
            divergencias.append({
                "PRO_CODIGO": cod,
                "MOTIVO": "Sem entradas para o produto, mas com ESTOQUE_DISPONIVEL",
                "ESTOQUE_DISPONIVEL": estoque_disp,
                "ESTOQUE_FIFO_CALC": 0,
            })
            continue

        saidas = df_sai_fifo[df_sai_fifo["PRO_CODIGO"] == cod]
        total_saida = saidas["QUANTIDADE_AJUSTADA"].sum() if not saidas.empty else 0
        total_entrada = entradas["QUANTIDADE"].sum()
        estoque_fifo_teorico = total_entrada - total_saida

        # Pequena tolerância para float
        if not np.isclose(estoque_fifo_teorico, estoque_disp, atol=0.0001):
            divergencias.append({
                "PRO_CODIGO": cod,
                "MOTIVO": "Divergência entre saldo FIFO teórico e ESTOQUE_DISPONIVEL",
                "ESTOQUE_DISPONIVEL": estoque_disp,
                "ESTOQUE_FIFO_CALC": estoque_fifo_teorico,
            })

        saldo_para_distribuir = estoque_disp
        if saldo_para_distribuir <= 0:
            continue

        sold_left = total_saida
        layer_index = 0

        for _, ent in entradas.iterrows():
            qtd_ent = ent["QUANTIDADE"]
            data_ent = ent["DATA"]

            if pd.isna(qtd_ent) or qtd_ent <= 0:
                continue

            if sold_left > 0:
                if sold_left >= qtd_ent:
                    sold_left -= qtd_ent
                    restante_entrada = 0
                else:
                    restante_entrada = qtd_ent - sold_left
                    sold_left = 0
            else:
                restante_entrada = qtd_ent

            if restante_entrada > 0 and saldo_para_distribuir > 0:
                qtd_camada = min(restante_entrada, saldo_para_distribuir)
                saldo_para_distribuir -= qtd_camada

                layer_index += 1
                camadas.append({
                    "PRO_CODIGO": cod,
                    "PRO_DESCRICAO": desc_map.get(cod, ""),
                    "ESTOQUE_DISPONIVEL": estoque_disp,
                    "LAYER_INDEX": layer_index,
                    "DATA_COMPRA_RESIDUAL": data_ent,
                    "QTD_RESTANTE": qtd_camada,
                })

            if saldo_para_distribuir <= 0:
                break

    df_camadas_long = pd.DataFrame(camadas)
    df_div = pd.DataFrame(divergencias)

    registros = []
    if not df_camadas_long.empty:
        grouped = df_camadas_long.sort_values(
            ["PRO_CODIGO", "LAYER_INDEX", "DATA_COMPRA_RESIDUAL"]
        ).groupby("PRO_CODIGO")

        for cod, grp in grouped:
            rec = {
                "PRO_CODIGO": cod,
                "PRO_DESCRICAO": grp["PRO_DESCRICAO"].iloc[0],
                "ESTOQUE_DISPONIVEL": grp["ESTOQUE_DISPONIVEL"].iloc[0],
                "ESTOQUE_SOMA_CAMADAS": grp["QTD_RESTANTE"].sum(),
            }
            for _, row in grp.iterrows():
                idx = int(row["LAYER_INDEX"])
                rec[f"DATA_COMPRA_{idx}"] = row["DATA_COMPRA_RESIDUAL"]
                rec[f"QTD_RESTANTE_{idx}"] = row["QTD_RESTANTE"]
            registros.append(rec)

    df_camadas_wide = pd.DataFrame(registros)
    return df_camadas_long, df_camadas_wide, df_div


# ==========================================
# GERAÇÃO DE RELATÓRIO DE ALTERAÇÕES
# ==========================================

def detectar_alteracoes_via_banco(df_atual: pd.DataFrame):
    """
    Compara o DataFrame atual com a última execução gravada no PostgreSQL.
    Retorna:
      - df_atual atualizado com a coluna 'TEVE_ALTERACAO_ANALISE'
      - df_mudancas (para relatório/email)
    """
    print("\nVerificando alterações em relação à última análise no banco...")
    
    # 1. Buscar última análise no banco
    engine = get_postgres_engine()
    sql_ultima = """
        SELECT pro_codigo, estoque_min_sugerido, estoque_max_sugerido, curva_abc
        FROM com_fifo_completo
        WHERE data_processamento = (
            SELECT MAX(data_processamento) FROM com_fifo_completo
        )
    """
    try:
        df_ant = pd.read_sql(sql_ultima, engine)
    except Exception as e:
        print(f"Erro ao buscar análise anterior: {e}")
        df_atual["TEVE_ALTERACAO_ANALISE"] = False
        return df_atual, pd.DataFrame()

    if df_ant.empty:
        print("Nenhuma análise anterior encontrada no banco.")
        df_atual["TEVE_ALTERACAO_ANALISE"] = True # Primeira vez assumimos alteração/novo
        # Mas para não spammar logs de "Novo Produto", podemos tratar diff.
        # Vamos marcar como True para novos produtos também.
        return df_atual, pd.DataFrame()

    # Preparar para merge
    # Converter pro_codigo para string/mesmo tipo
    df_atual["PRO_CODIGO"] = df_atual["PRO_CODIGO"].astype(str)
    df_ant["pro_codigo"] = df_ant["pro_codigo"].astype(str)
    
    df_ant = df_ant.rename(columns={
        "pro_codigo": "PRO_CODIGO",
        "estoque_min_sugerido": "MIN_ANT",
        "estoque_max_sugerido": "MAX_ANT",
        "curva_abc": "ABC_ANT"
    })
    
    # Merge
    df_merge = df_atual.merge(df_ant, on="PRO_CODIGO", how="left")
    
    mudancas = []
    
    def verificar_linha(row):
        # Se é produto novo (não tem anterior)
        if pd.isna(row["MIN_ANT"]):
            return True, "NOVO PRODUTO", "Novo na análise", {}
        
        diffs = []
        changed = False
        changes = {}
        
        # Comparar Min (tratar NaN como diferente de numero)
        atual_min = row["ESTOQUE_MIN_SUGERIDO"]
        ant_min = row["MIN_ANT"]
        if atual_min != ant_min:
            diffs.append(f"Min: {float(ant_min):.0f} -> {atual_min}")
            # Ensure serialization friendly types
            changes["estoque_min_sugerido"] = {"old": float(ant_min), "new": float(atual_min) if atual_min is not None else 0}
            changed = True
            
        # Comparar Max
        atual_max = row["ESTOQUE_MAX_SUGERIDO"]
        ant_max = row["MAX_ANT"]
        if atual_max != ant_max:
            diffs.append(f"Max: {float(ant_max):.0f} -> {atual_max}")
            changes["estoque_max_sugerido"] = {"old": float(ant_max), "new": float(atual_max) if atual_max is not None else 0}
            changed = True
            
        # Comparar ABC
        atual_abc = str(row["CURVA_ABC"])
        ant_abc = str(row["ABC_ANT"])
        if atual_abc != ant_abc:
            diffs.append(f"ABC: {ant_abc} -> {atual_abc}")
            changes["curva_abc"] = {"old": ant_abc, "new": atual_abc}
            changed = True
            
        if changed:
            return True, "ALTERADO", "; ".join(diffs), changes
            
        return False, None, None, {}

    # Aplicar verificação
    resultados = df_merge.apply(verificar_linha, axis=1)
    
    # Extrair resultados
    df_atual["TEVE_ALTERACAO_ANALISE"] = [res[0] for res in resultados]
    df_atual["dados_alteracao_json"] = [json.dumps(res[3]) if res[0] and res[3] else None for res in resultados]
    
    # Montar df_mudancas para relatorio
    for idx, (changed, tipo, detalhes, _) in enumerate(resultados):
        if changed and tipo == "ALTERADO": # Só listamos alterações de produtos existentes no email para não poluir
             mudancas.append({
                 "PRO_CODIGO": df_atual.iloc[idx]["PRO_CODIGO"],
                 "PRO_DESCRICAO": df_atual.iloc[idx].get("PRO_DESCRICAO", ""),
                 "TIPO_MUDANCA": tipo,
                 "DETALHES": detalhes
             })
             
    df_mudancas = pd.DataFrame(mudancas)
    print(f"Detectadas {len(df_mudancas)} alterações relevantes em produtos já existentes.")
    
    return df_atual, df_mudancas
    

# ==========================================
# ENVIO DE E-MAIL
# ==========================================

def enviar_email_relatorio(arquivo_anexo, df_mudancas):
    if "exemplo.com" in EMAIL_SENDER or not EMAIL_PASSWORD:
        print("\n[AVISO] Configurações de e-mail não preenchidas. O e-mail não será enviado.")
        return

    print("\nPreparando envio de e-mail...")
    
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = f"Relatório FIFO e Mudanças de Estoque - {datetime.date.today()}"
    
    qtd_mudancas = len(df_mudancas) if not df_mudancas.empty else 0
    
    body = f"""
    Olá,
    
    Segue anexo o relatório atualizado de análise de estoque (FIFO).
    
    Resumo:
    - Data da execução: {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}
    - Produtos com alteração de perfil (ABC/Min/Max/Categoria): {qtd_mudancas}
    
    O arquivo Excel contém a aba 'RELATORIO_MUDANCAS' detalhando o que mudou.
    
    Atenciosamente,
    Robô de Estoque
    """
    
    msg.attach(MIMEText(body, 'plain'))
    
    # Anexo
    filename = arquivo_anexo.name
    with open(arquivo_anexo, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )
    msg.attach(part)
    
    try:
        server = smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, text)
        server.quit()
        print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")


# ==========================================
# FUNÇÕES DE SALVAMENTO NO POSTGRESQL
# ==========================================

def verificar_tabela_postgres():
    """
    Verifica se a tabela com_fifo_completo existe e mostra algumas informações básicas
    """
    try:
        engine = get_postgres_engine()
        
        with engine.connect() as conn:
            # Verifica se a tabela existe
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'com_fifo_completo'
                );
            """))
            existe = result.scalar()
            
            if existe:
                # Conta registros
                result = conn.execute(text("SELECT COUNT(*) FROM com_fifo_completo"))
                count = result.scalar()
                
                # Data do último processamento
                result = conn.execute(text("""
                    SELECT MAX(data_processamento) 
                    FROM com_fifo_completo
                """))
                ultima_data = result.scalar()
                
                print(f"✓ Tabela com_fifo_completo existe")
                print(f"✓ Total de registros: {count}")
                print(f"✓ Último processamento: {ultima_data}")
                
                # Mostra amostra dos dados
                if count > 0:
                    result = conn.execute(text("""
                        SELECT pro_codigo, pro_descricao, curva_abc, estoque_min_sugerido, estoque_max_sugerido
                        FROM com_fifo_completo 
                        WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
                        ORDER BY qtd_vendida DESC
                        LIMIT 5
                    """))
                    
                    print("\nAmostra dos 5 produtos com maior volume de vendas:")
                    print("CÓDIGO\tDESCRIÇÃO\tABC\tMÍN\tMÁX")
                    print("-" * 80)
                    for row in result:
                        desc = (row[1][:30] + "...") if len(str(row[1] or "")) > 30 else (row[1] or "")
                        print(f"{row[0]}\t{desc}\t{row[2]}\t{row[3]}\t{row[4]}")
                
            else:
                print("❌ Tabela com_fifo_completo não existe")
                
    except Exception as e:
        print(f"Erro ao verificar tabela: {e}")


def salvar_metricas_postgres(df_metricas):
    """
    Salva as métricas calculadas na tabela com_fifo_completo do PostgreSQL
    Mantém apenas as últimas 2 execuções (Penúltima e Última)
    """
    if df_metricas.empty:
        print("DataFrame de métricas vazio. Nada para salvar.")
        return
    
    # Cria uma cópia do DataFrame para não modificar o original
    df_save = df_metricas.copy()
    
    # Adiciona timestamp do processamento
    df_save['data_processamento'] = datetime.datetime.now()
    
    # Converte colunas de data para datetime se necessário
    date_columns = ['DATA_MIN_VENDA', 'DATA_MAX_VENDA']
    for col in date_columns:
        if col in df_save.columns:
            df_save[col] = pd.to_datetime(df_save[col], errors='coerce')
    
    # Mapeia as colunas do DataFrame para as colunas da tabela
    column_mapping = {
        'PRO_CODIGO': 'pro_codigo',
        'TEMPO_MEDIO_ESTOQUE': 'tempo_medio_estoque',
        'QTD_VENDIDA': 'qtd_vendida',
        'VALOR_VENDIDO': 'valor_vendido', 
        'DATA_MIN_VENDA': 'data_min_venda',
        'DATA_MAX_VENDA': 'data_max_venda',
        'PERIODO_DIAS': 'periodo_dias',
        'DEMANDA_MEDIA_DIA': 'demanda_media_dia',
        'NUM_VENDAS': 'num_vendas',
        'VENDAS_ULT_12M': 'vendas_ult_12m',
        'VENDAS_12M_ANT': 'vendas_12m_ant',
        'FATOR_TENDENCIA': 'fator_tendencia',
        'TENDENCIA_LABEL': 'tendencia_label',
        'DIAS_RUPTURA': 'dias_ruptura',
        'DEMANDA_MEDIA_DIA_AJUSTADA': 'demanda_media_dia_ajustada',
        'PRO_DESCRICAO': 'pro_descricao',
        'ESTOQUE_DISPONIVEL': 'estoque_disponivel',
        'ESTOQUE_DISPONIVEL': 'estoque_disponivel',
        'MAR_DESCRICAO': 'mar_descricao',
        'SGR_CODIGO': 'sgr_codigo',
        'SGR_DESCRICAO': 'sgr_descricao',
        'group_id': 'group_id',
        'FORNECEDOR1': 'fornecedor1',
        'FORNECEDOR2': 'fornecedor2',
        'FORNECEDOR3': 'fornecedor3',
        'PCT_ACUM_VALOR': 'pct_acum_valor',
        'CURVA_ABC': 'curva_abc',
        'CURVA_ABC': 'curva_abc',
        'CATEGORIA_ESTOCAGEM': 'categoria_estocagem',
        'ESTOQUE_MIN_BASE': 'estoque_min_base',
        'ESTOQUE_MAX_BASE': 'estoque_max_base',
        'FATOR_AJUSTE_TENDENCIA': 'fator_ajuste_tendencia',
        'ESTOQUE_MIN_AJUSTADO': 'estoque_min_ajustado',
        'ESTOQUE_MAX_AJUSTADO': 'estoque_max_ajustado',
        'ESTOQUE_MIN_SUGERIDO': 'estoque_min_sugerido',
        'ESTOQUE_MAX_SUGERIDO': 'estoque_max_sugerido',
        'TIPO_PLANEJAMENTO': 'tipo_planejamento',
        'ALERTA_TENDENCIA_ALTA': 'alerta_tendencia_alta',
        'DESCRICAO_CALCULO_ESTOQUE': 'descricao_calculo_estoque',
        'TEVE_ALTERACAO_ANALISE': 'teve_alteracao_analise',
        'GRP_ESTOQUE_DISPONIVEL': 'grp_estoque_disponivel',
        'GRP_QTD_VENDIDA': 'grp_qtd_vendida',
        'GRP_VALOR_VENDIDO': 'grp_valor_vendido',
        'GRP_NUM_VENDAS': 'grp_num_vendas',
        'GRP_VENDAS_ULT_12M': 'grp_vendas_ult_12m',
        'GRP_VENDAS_12M_ANT': 'grp_vendas_12m_ant',
        'GRP_ESTOQUE_MIN_BASE': 'grp_estoque_min_base',
        'GRP_ESTOQUE_MAX_BASE': 'grp_estoque_max_base',
        'GRP_ESTOQUE_MIN_AJUSTADO': 'grp_estoque_min_ajustado',
        'GRP_ESTOQUE_MAX_AJUSTADO': 'grp_estoque_max_ajustado',
        'GRP_ESTOQUE_MIN_SUGERIDO': 'grp_estoque_min_sugerido',
        'GRP_ESTOQUE_MAX_SUGERIDO': 'grp_estoque_max_sugerido',
        'GRP_DEMANDA_MEDIA_DIA': 'grp_demanda_media_dia',
        'RATEIO_PROP_GRUPO': 'rateio_prop_grupo',
        'TEMPO_MEDIO_SALDO_ATUAL': 'tempo_medio_saldo_atual',
        'CATEGORIA_SALDO_ATUAL': 'categoria_saldo_atual',
        'PRO_REFERENCIA': 'pro_referencia',
        'DEM_DIA_REAL': 'demanda_real_dia',
        'SIGMA_DEMANDA_DIA': 'sigma_demanda_dia',
        'CV_DEMANDA': 'cv_demanda',
        'CLASSE_XYZ': 'classe_xyz',
        'ESTOQUE_SEGURANCA': 'estoque_seguranca',
        'NIVEL_SERVICO_Z': 'nivel_servico_z',
        'LEAD_TIME_DIAS': 'lead_time_dias',
        'VENDA_PERDIDA_12M': 'venda_perdida_12m',
        'VALOR_VENDIDO_12M': 'valor_vendido_12m',
        'PADRAO_DEMANDA': 'padrao_demanda',
        'METODO_REPOSICAO': 'metodo_reposicao',
        'FATOR_SAZONAL': 'fator_sazonal',
        'DEMANDA_PLANEJAMENTO_DIA': 'demanda_planejamento_dia',
        'SOB_ENCOMENDA': 'sob_encomenda',
        'GRUPO_CHAVE': 'grupo_chave',
        'GRUPO_QTD_ITENS': 'grupo_qtd_itens',
        'GRUPO_ESTOQUE_DISPONIVEL': 'grupo_estoque_disponivel',
        'GRUPO_DEMANDA_DIA': 'grupo_demanda_dia',
        'GRUPO_FATOR_SAZONAL': 'grupo_fator_sazonal',
        'GRUPO_CURVA': 'grupo_curva',
        'GRUPO_PADRAO': 'grupo_padrao',
        'GRUPO_METODO': 'grupo_metodo',
        'GRUPO_ESTOQUE_MIN': 'grupo_estoque_min',
        'GRUPO_ESTOQUE_MAX': 'grupo_estoque_max',
        'GRUPO_ESTOQUE_SEGURANCA': 'grupo_estoque_seguranca',
        'dados_alteracao_json': 'dados_alteracao_json'
    }
    
    # Renomeia as colunas
    df_save = df_save.rename(columns=column_mapping)
    
    # Lista das colunas da tabela na ordem correta
    table_columns = [
        'data_processamento', 'pro_codigo', 'tempo_medio_estoque', 'qtd_vendida',
        'valor_vendido', 'data_min_venda', 'data_max_venda', 'periodo_dias',
        'demanda_media_dia', 'num_vendas', 'vendas_ult_12m', 'vendas_12m_ant',
        'fator_tendencia', 'tendencia_label', 'dias_ruptura', 'demanda_media_dia_ajustada',
        'pro_descricao', 'estoque_disponivel', 'mar_descricao', 'sgr_codigo', 'sgr_descricao', 'group_id', 'fornecedor1',
        'fornecedor2', 'fornecedor3', 'pct_acum_valor', 'curva_abc',
        'categoria_estocagem', 'estoque_min_base', 'estoque_max_base',
        'fator_ajuste_tendencia', 'estoque_min_ajustado', 'estoque_max_ajustado',
        'estoque_min_sugerido', 'estoque_max_sugerido', 'tipo_planejamento',
        'alerta_tendencia_alta', 'descricao_calculo_estoque', 'teve_alteracao_analise',
        'grp_estoque_disponivel', 'grp_qtd_vendida', 'grp_valor_vendido', 'grp_num_vendas',
        'grp_vendas_ult_12m', 'grp_vendas_12m_ant', 'grp_estoque_min_base', 'grp_estoque_max_base',
        'grp_estoque_min_ajustado', 'grp_estoque_max_ajustado', 'grp_estoque_min_sugerido',
        'grp_estoque_max_sugerido', 'grp_demanda_media_dia', 'rateio_prop_grupo',
        'tempo_medio_saldo_atual', 'categoria_saldo_atual',
        'pro_referencia',
        'demanda_real_dia', 'sigma_demanda_dia', 'cv_demanda', 'classe_xyz',
        'estoque_seguranca', 'nivel_servico_z', 'lead_time_dias',
        'venda_perdida_12m', 'valor_vendido_12m',
        'padrao_demanda', 'metodo_reposicao', 'fator_sazonal', 'demanda_planejamento_dia',
        'sob_encomenda', 'grupo_chave', 'grupo_qtd_itens', 'grupo_estoque_disponivel',
        'grupo_demanda_dia', 'grupo_fator_sazonal', 'grupo_curva', 'grupo_padrao',
        'grupo_metodo', 'grupo_estoque_min', 'grupo_estoque_max', 'grupo_estoque_seguranca',
        'dados_alteracao_json'
    ]
    
    # Garante que todas as colunas necessárias existem (adiciona como None se não existir)
    for col in table_columns:
        if col not in df_save.columns:
            df_save[col] = None
    
    # Seleciona apenas as colunas necessárias na ordem correta
    df_save = df_save[table_columns]
    
    try:
        engine = get_postgres_engine()
        
        # Limpa dados antigos da mesma data (se existir)
        hoje = datetime.date.today()
        with engine.connect() as conn:
            conn.execute(
                text("DELETE FROM com_fifo_completo WHERE DATE(data_processamento) = :data"),
                {"data": hoje}
            )
            conn.commit()
        
        # Salva os novos dados
        df_save.to_sql(
            'com_fifo_completo', 
            engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Salvos {len(df_save)} registros na tabela com_fifo_completo do PostgreSQL")
        
        # === LIMPEZA: Manter apenas as últimas 2 datas de análise ===
        with engine.connect() as conn:
            # Busca todas as datas distintas ordenadas da mais recente para a mais antiga
            result = conn.execute(text("SELECT DISTINCT data_processamento FROM com_fifo_completo ORDER BY data_processamento DESC"))
            dates = [row[0] for row in result]
            
            # Se houver mais de 2 datas, remove as antigas
            if len(dates) > 2:
                # A partir da 3ª data (índice 2), todas devem ser excluídas
                cutoff_date = dates[1] # Mantém índice 0 e 1, cutoff é a segunda mais recente (limite inferior inclusivo para o DELETE é tudo MENOR que ela? Não, temos timestamps exatos)
                
                # Vamos deletar tudo que não seja as 2 primeiras datas
                dates_to_keep = dates[:2]
                
                # Formata para string para usar na query (ou passamos parametro)
                # Parametro Lista/Tuple pode ser chato, vamos deletar onde data < dates[1]
                # Se dates[1] é a penúltima, queremos manter ela e a dates[0].
                # Então removemos tudo onde data_processamento < dates[1].
                
                print(f"Limpando análises antigas. Mantendo apenas execuções de {dates[0]} e {dates[1]}")
                
                conn.execute(
                    text("DELETE FROM com_fifo_completo WHERE data_processamento < :cutoff"),
                    {"cutoff": cutoff_date}
                )
                conn.commit()
                print("Limpeza concluída.")
        
    except Exception as e:
        print(f"Erro ao salvar no PostgreSQL: {e}")
        raise


def salvar_dados_postgres(df_metricas, df_mudancas, df_long):
    """
    Salva os dados das métricas na tabela com_fifo_completo do PostgreSQL
    """
    print("Salvando dados no PostgreSQL...")
    
    # Cria a tabela se não existir
    criar_tabela_postgres()
    
    # Salva apenas as métricas principais na tabela
    salvar_metricas_postgres(df_metricas)
    
    print("Dados salvos com sucesso no PostgreSQL!")


def salvar_distribuicao_fifo_postgres(df_long):
    """
    Salva a distribuição detalhada do FIFO (lotes residuais) na tabela com_data_saldo_produto.
    Estratégia: Limpar tabela e reinserir (Snapshot atual).
    """
    if df_long.empty:
        return

    print("Salvando distribuição FIFO (lotes) no PostgreSQL...")
    engine = get_postgres_engine()
    
    # Preparar DataFrame
    # df_long columns expected: PRO_CODIGO, DATA_COMPRA_RESIDUAL, QTD_RESTANTE, ...
    
    df_save = df_long[["PRO_CODIGO", "DATA_COMPRA_RESIDUAL", "QTD_RESTANTE"]].copy()
    df_save.columns = ["pro_codigo", "data_compra", "saldo_residual"]
    
    # Converter datas
    df_save["data_compra"] = pd.to_datetime(df_save["data_compra"], errors='coerce')
    
    try:
        with engine.connect() as conn:
            # Opção 1: Truncate (mais rápido se for atualizar tudo)
            # Opção 2: Delete by pro_codigo se for parcial (mas aqui estamos rodando tudo)
            # Vamos usar Truncate/Delete All para garantir snapshot limpo
            conn.execute(text("TRUNCATE TABLE com_data_saldo_produto RESTART IDENTITY"))
            conn.commit()
            
        df_save.to_sql(
            'com_data_saldo_produto',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"Salvos {len(df_save)} lotes residuais em com_data_saldo_produto.")
        
    except Exception as e:
        print(f"Erro ao salvar distribuição FIFO: {e}")


# ==========================================
# EXECUÇÃO DO JOB
# ==========================================

def atualizar_compras_fornecedor_mongo():
    """
    Agrega no ERP a lista PRODUTO -> FORNECEDORES de quem JÁ COMPRAMOS
    (nfe_itens + nf_entrada + fornecedores), exclui o DEPÓSITO interno e notas
    canceladas, e grava no Mongo (coleção compras_fornecedor). A tela "Comprar
    agora" lê disso (rápido), em vez de rodar essa consulta pesada a cada request.
    """
    print("Atualizando lista produto x fornecedor (histórico de compra) no Mongo...")
    inner = ("SELECT i.pro_codigo, f.for_nome, SUM(i.quantidade) AS qtd "
             "FROM nfe_itens i "
             "JOIN nf_entrada e ON e.empresa = i.empresa AND e.nfe = i.nfe "
             "JOIN fornecedores f ON f.empresa = e.empresa AND f.for_codigo = e.for_codigo "
             "WHERE i.empresa = 3 AND e.dt_cancelamento IS NULL "
             "AND f.for_nome NOT LIKE '%(DEPOSITO)%' "
             "GROUP BY i.pro_codigo, f.for_nome")
    query = f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39) * 2)}')"
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    df.columns = [str(c).upper() for c in df.columns]  # OPENQUERY devolve MAIÚSCULAS
    mapa = {}
    for cod, nome, qtd in zip(df["PRO_CODIGO"], df["FOR_NOME"], df["QTD"]):
        if cod is None:
            continue
        mapa.setdefault(str(cod).strip(), []).append((str(nome).strip(), float(qtd or 0)))
    for c in mapa:
        mapa[c].sort(key=lambda x: -x[1])

    import empacotamento as emp
    n = emp.salvar_compras_fornecedor(mapa)
    print(f"  - {n} produtos x fornecedor gravados (fonte: {len(df)} pares).")
    return n


def run_job():
    print(f"\n=== INICIANDO JOB DE ANÁLISE FIFO: {datetime.datetime.now()} ===")

    import empacotamento as emp
    hoje_ts = pd.Timestamp.today().normalize()

    # === MODO EMPACOTAMENTO (Fase 2, gated por EMPAC_ENABLED) ===
    emp_on = os.getenv("EMPAC_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
    pack_store = None
    packs_docs = {}        # {pro_codigo: {corte, camadas, vendas_mensais}}
    packs_cam = {}         # {pro_codigo: [{data_compra, qtd}]}  -> semeia o motor FIFO
    corte_pack = None
    modo_incremental = False
    if emp_on:
        try:
            pack_store = emp.MongoPackStore()
            meta_pack = pack_store.meta()
            if meta_pack.get("corte"):
                modo_incremental = True
                corte_pack = meta_pack["corte"]
                packs_docs = pack_store.carregar()
                packs_cam = {c: emp._cam_from_doc(d.get("camadas", []))
                             for c, d in packs_docs.items()}
                print(f"  [EMPAC] modo INCREMENTAL — corte={corte_pack}, "
                      f"{len(packs_docs)} produtos no pacote (janela {emp.JANELA_MESES}m)")
            else:
                print("  [EMPAC] habilitado, pacote vazio -> BACKFILL (carga completa desta vez)")
        except Exception as e:
            print(f"  [EMPAC] AVISO: store indisponível ({e}). Caindo para carga completa.")
            emp_on = False; pack_store = None; modo_incremental = False

    # 1) Carregar dados (janela no modo incremental; completa caso contrário)
    df_saidas, df_ent, df_dev, df_saldo_produto, df_vp = carregar_dados_do_banco(
        corte=corte_pack if modo_incremental else None)
    
    # === CORREÇÃO DE TIPOS: Garantir que PRO_CODIGO seja string em todos os DataFrames ===
    df_saidas["PRO_CODIGO"] = df_saidas["PRO_CODIGO"].astype(str).str.strip()
    df_ent["PRO_CODIGO"] = df_ent["PRO_CODIGO"].astype(str).str.strip()
    if not df_dev.empty:
        df_dev["PRO_CODIGO"] = df_dev["PRO_CODIGO"].astype(str).str.strip()
    df_saldo_produto["PRO_CODIGO"] = df_saldo_produto["PRO_CODIGO"].astype(str).str.strip()
    # ===================================================================================

    # 2) Processamento de limpeza (Simplificado aqui, puxando lógica do script original)
    df_sai = df_saidas.copy()
    df_sai["QUANTIDADE"] = pd.to_numeric(df_sai.get("QUANTIDADE"), errors="coerce")
    df_sai["DATA"] = pd.to_datetime(df_sai.get("DATA"), errors="coerce")
    df_ent["QUANTIDADE"] = pd.to_numeric(df_ent.get("QUANTIDADE"), errors="coerce")
    df_ent["DATA"] = pd.to_datetime(df_ent.get("DATA"), errors="coerce")
    
    if not df_dev.empty:
        df_dev["QTDE_DEVOLVIDA"] = pd.to_numeric(df_dev.get("QTDE_DEVOLVIDA"), errors="coerce")
        dev_agg = df_dev.groupby(["NFS", "PRO_CODIGO"], as_index=False)["QTDE_DEVOLVIDA"].sum()
        df_sai = df_sai.merge(dev_agg, left_on=["NFS", "PRO_CODIGO"], right_on=["NFS", "PRO_CODIGO"], how="left")
        df_sai["QTDE_DEVOLVIDA"] = df_sai["QTDE_DEVOLVIDA"].fillna(0)
    else:
        df_sai["QTDE_DEVOLVIDA"] = 0
        
    df_sai["QUANTIDADE_AJUSTADA"] = df_sai["QUANTIDADE"] - df_sai["QTDE_DEVOLVIDA"]
    df_sai_valid = df_sai[df_sai["QUANTIDADE_AJUSTADA"] > 0].copy()
    
    # Recalcula acumulado pro FIFO
    df_sai_valid = df_sai_valid.sort_values(["PRO_CODIGO", "DATA"])
    df_sai_valid["QTD_ACUMULADA"] = df_sai_valid.groupby("PRO_CODIGO")["QUANTIDADE_AJUSTADA"].cumsum()
    
    df_ent_valid = df_ent.copy() # (Assumindo entradas limpas ou simplificando a limpeza para brevidade)
    # Reimplementar limpeza de CNE/NFE se crítico (mantido simples aqui pelo tamanho do prompt, mas ideal manter lógica original)
    # ... Lógica original de limpeza de entradas ...
    # REPLICANDO A LOGICA DE ENTRADAS DO SCRIPT ORIGINAL DE FORMA SIMPLIFICADA:
    origem_ent = df_ent["ORIGEM"].astype(str).str.upper()
    mask_cne = origem_ent == "CNE"
    nfe_com_cne = df_ent.loc[mask_cne, "NFE"].dropna().unique()
    mask_entrada_valida = (
        ((origem_ent == "NFE") & (~df_ent["NFE"].isin(nfe_com_cne)))
        | (origem_ent.isin(["LIA", "CDE"]))
    )
    df_ent_valid = df_ent.loc[mask_entrada_valida].copy()
    df_ent_valid = df_ent_valid.sort_values(["PRO_CODIGO", "DATA"])
    df_ent_valid["QTD_ENTRADA_ACUMULADA"] = df_ent_valid.groupby("PRO_CODIGO")["QUANTIDADE"].cumsum()
    
    
    # 3) FIFO Core — MOTOR ÚNICO por camadas (semeado pelo pacote no modo incremental).
    #    Produz, num só passe: DATA_COMPRA por venda (data média ponderada) e as
    #    camadas residuais do estoque atual (df_long), reconciliadas ao saldo do ERP.
    print("Processando FIFO (motor por camadas)...")
    df_sai_fifo = df_sai_valid.copy()
    data_compra, df_long, df_div = emp.fifo_por_camadas(
        df_ent_valid, df_sai_fifo, df_saldo_produto,
        packs=packs_cam if modo_incremental else None, hoje=hoje_ts)
    df_sai_fifo["DATA_COMPRA"] = data_compra
    df_wide = pd.DataFrame()
    if not df_div.empty:
        print(f"  [FIFO] {len(df_div)} produtos com ajuste de inventário (saldo x movimento) reconciliados.")

    # Sazonalidade: no modo incremental, traz os meses congelados do pacote.
    vendas_mensais_extra = (emp.vendas_mensais_subgrupo(packs_docs, df_saldo_produto)
                            if modo_incremental else None)

    # 4) Metricas
    df_metricas = calcular_metricas_e_classificar(
        df_sai_fifo, df_ent_valid, df_saldo_produto, df_vp,
        vendas_mensais_extra=vendas_mensais_extra)
    
    # === ETAPA NOVA: Calcular Idade Média do Saldo Atual e Classificar ===
    # Agrupar df_long por produto para calcular weighted average age
    if not df_long.empty:
        # Calcular Idade de cada camada
        # Age = (Hoje - DataCompra).days
        hoje = pd.Timestamp.today().normalize()
        df_long["DATA_COMPRA_RESIDUAL"] = pd.to_datetime(df_long["DATA_COMPRA_RESIDUAL"], errors='coerce')
        df_long["LAYER_AGE_DAYS"] = (hoje - df_long["DATA_COMPRA_RESIDUAL"]).dt.days.fillna(0)
        
        # Weighted Avg per Protocol
        # Weighted Age = Sum(Age * Qty) / Sum(Qty)
        
        # Helper storage
        prod_age_map = {}
        
        for cod, grp in df_long.groupby("PRO_CODIGO"):
            total_qty = grp["QTD_RESTANTE"].sum()
            if total_qty > 0:
                weighted_sum = (grp["LAYER_AGE_DAYS"] * grp["QTD_RESTANTE"]).sum()
                avg_age = weighted_sum / total_qty
                prod_age_map[cod] = avg_age
            else:
                prod_age_map[cod] = 0 # Should not happen if filtered, but safety
        
        # Mapear para df_metricas
        # Função cat_estocagem já existe no escopo (definida dentro de calcular_metricas_e_classificar... ops, escopo fechado)
        # Precisamos redefinir ou extrair a lógica de cat_estocagem.
        # Vamos redefinir aqui para facilidade.
        def get_cat_saldo(t):
            if pd.isna(t): return None
            if t <= 60: return "Rápido"
            elif t <= 120: return "Médio"
            elif t <= 240: return "Lento"
            else: return "Obsoleto"
            
        df_metricas["TEMPO_MEDIO_SALDO_ATUAL"] = df_metricas["PRO_CODIGO"].map(prod_age_map)
        df_metricas["CATEGORIA_SALDO_ATUAL"] = df_metricas["TEMPO_MEDIO_SALDO_ATUAL"].apply(get_cat_saldo)
        
    else:
        df_metricas["TEMPO_MEDIO_SALDO_ATUAL"] = None
        df_metricas["CATEGORIA_SALDO_ATUAL"] = None
    
    # 6) Comparação com Anterior
    # 6) Comparação com Anterior (AGORA VIA BANCO)
    df_metricas, df_mudancas = detectar_alteracoes_via_banco(df_metricas)
    
    # 7) Salvar no PostgreSQL
    print("Salvando dados no PostgreSQL...")
    try:
        salvar_dados_postgres(df_metricas, df_mudancas, df_long)
        salvar_distribuicao_fifo_postgres(df_long)
    except Exception as e:
        print(f"Erro ao salvar no PostgreSQL: {e}")
        return

    # 7b) EMPACOTAMENTO: backfill (1ª vez) ou fold incremental do corte -> grava no Mongo
    if emp_on and pack_store is not None:
        try:
            if modo_incremental:
                novo_corte = emp.corte_da_janela(hoje_ts.date())
                if pd.Timestamp(novo_corte) > pd.Timestamp(corte_pack):
                    docs_novos = emp.avancar_corte(packs_docs, df_ent_valid, df_sai_valid,
                                                   corte_pack, novo_corte)
                    pack_store.salvar(docs_novos, {"corte": novo_corte.strftime(emp._FMT),
                                                   "janela_meses": emp.JANELA_MESES,
                                                   "atualizado_em": str(hoje_ts.date())})
                    print(f"  [EMPAC] corte avançado {corte_pack} -> {novo_corte} "
                          f"({len(docs_novos)} produtos)")
                else:
                    print(f"  [EMPAC] corte inalterado ({corte_pack}); nada a avançar.")
            else:
                docs, T = emp.computar_pack(df_ent_valid, df_sai_valid, hoje_ts.date())
                pack_store.salvar(docs, {"corte": T.strftime(emp._FMT),
                                         "janela_meses": emp.JANELA_MESES,
                                         "atualizado_em": str(hoje_ts.date())})
                print(f"  [EMPAC] BACKFILL gravado no Mongo: {len(docs)} produtos, corte={T}")
        except Exception as e:
            print(f"  [EMPAC] AVISO: falha ao gravar pacote: {e}")

    # 8) Opcional: Ainda salvar Excel para backup/compatibilidade
    print(f"Salvando backup em Excel: {ARQUIVO_SAIDA}")
    try:
        if ARQUIVO_SAIDA.exists(): os.remove(ARQUIVO_SAIDA)
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
            df_metricas.to_excel(writer, sheet_name="ANALISE_ATUAL", index=False)
            if not df_mudancas.empty:
                df_mudancas.to_excel(writer, sheet_name="RELATORIO_MUDANCAS", index=False)
            df_long.to_excel(writer, sheet_name="FIFO_SALDO", index=False)
            # Outras abas opcionais
            
    except Exception as e:
        print(f"Erro ao salvar Excel de backup: {e}")
        # Não retorna erro aqui, pois o principal (PostgreSQL) já foi salvo

    # 9) Enviar e-mail
    enviar_email_relatorio(ARQUIVO_SAIDA, df_mudancas)
    
    # 10) Verificar dados salvos
    print("\n" + "="*50)
    print("VERIFICAÇÃO FINAL DA TABELA")
    print("="*50)
    verificar_tabela_postgres()
    
    # 11) Auto-Agrupamento Similares
    agrupar_similares_automaticamente()

    # 12) Lista produto x fornecedor (histórico de compra) -> Mongo (tela Comprar agora)
    try:
        atualizar_compras_fornecedor_mongo()
    except Exception as e:
        print(f"AVISO: falha ao atualizar compras x fornecedor no Mongo: {e}")
    
    print("Job finalizado com sucesso.")


    print("Dados salvos com sucesso no PostgreSQL!")

# --------------------------------------------------------------------------------------
# AUTO-AGRUPAMENTO
# --------------------------------------------------------------------------------------
def agrupar_similares_automaticamente():
    """
    Identifica produtos com descrições idênticas e cria grupos na tabela com_relacionamento_itens.
    Executa apenas para itens que ainda não possuem relacionamento.
    """
    print("Iniciando auto-agrupamento de produtos similares...")
    try:
        engine = get_postgres_engine()
        with engine.connect() as conn:
            # 1. Encontrar descrições duplicadas
            # Considera apenas itens ativos na analise (presentes em com_fifo_completo)
            # Otimização: Buscar apenas descrições que tenham itens NÃO AGRUPADOS ou SEM GRUPO COMPLETO
            # JOIN com a tabela de relacionamento para ver quem já tem grupo.
            # Condição HAVING: Tem duplicatas (COUNT > 1) E (Contagem de Grupos < Contagem de Itens)
            # Ou seja, pelo menos um item da descrição não tem registro na tabela de relacionamento.
            sql_duplicados = text("""
                SELECT f.pro_descricao, COUNT(f.pro_codigo) as qtd
                FROM com_fifo_completo f
                LEFT JOIN com_relacionamento_itens r ON f.pro_codigo = r.pro_codigo
                WHERE f.pro_descricao IS NOT NULL AND f.pro_descricao <> ''
                GROUP BY f.pro_descricao 
                HAVING COUNT(f.pro_codigo) > 1 
                   AND COUNT(r.group_id) < COUNT(f.pro_codigo)
            """)
            result = conn.execute(sql_duplicados).fetchall()
            
            if not result:
                print("Nenhuma descrição duplicada encontrada para agrupamento.")
                return

            print(f"Encontrados {len(result)} descrições com duplicatas.")
            
            novos_grupos = 0
            novos_itens = 0
            
            for i, row in enumerate(result):
                if i % 100 == 0:
                    print(f"  Processando grupo {i}/{len(result)}...")
                    conn.commit() # Commit parcial para não perder tudo se cair
                
                descricao = row[0]
                if not descricao: continue
                
                # Buscar produtos com essa descricao
                sql_prods = text("SELECT pro_codigo FROM com_fifo_completo WHERE pro_descricao = :desc")
                prods = conn.execute(sql_prods, {"desc": descricao}).fetchall()
                pro_codigos = [p[0] for p in prods]
                
                if not pro_codigos: continue
                
                # Verificar se algum já tem grupo
                # Busca grupos já existentes para esses códigos
                existing_groups = set()
                for pc in pro_codigos:
                    g = conn.execute(text("SELECT group_id FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": pc}).scalar()
                    if g:
                        existing_groups.add(g)
                
                # Determinar ID do grupo
                group_id = None
                if len(existing_groups) == 1:
                    # Todos (que tem grupo) estão no mesmo grupo. Usar esse.
                    group_id = list(existing_groups)[0]
                elif len(existing_groups) > 1:
                    # Mais de um grupo existente.
                    # Simplificação: Usar o primeiro encontrado e mover os outros (merge) ou ignorar?
                    # Vamos usar o primeiro e agrupar os 'sem grupo' nele. Não vamos fazer merge de grupos existentes agora para evitar complexidade.
                    group_id = list(existing_groups)[0]
                else:
                    # Nenhum tem grupo. Criar novo.
                    group_id = str(uuid.uuid4())
                    novos_grupos += 1
                
                # Inserir itens que não estão em grupo nenhum
                # Se já estiverem em OUTRO grupo, mantemos lá (respeita decisão manual anterior?)
                # Para "Auto", vamos apenas preencher os vazios.
                
                for pc in pro_codigos:
                    curr = conn.execute(text("SELECT group_id FROM com_relacionamento_itens WHERE pro_codigo = :c"), {"c": pc}).scalar()
                    if not curr:
                        # Inserir
                        conn.execute(text("INSERT INTO com_relacionamento_itens (group_id, pro_codigo) VALUES (:g, :c)"), {"g": group_id, "c": pc})
                        novos_itens += 1
            
            conn.commit()
            print(f"Auto-agrupamento concluído: {novos_grupos} novos grupos, {novos_itens} produtos vinculados.")
            
    except Exception as e:
        print(f"Erro no auto-agrupamento: {e}")

# ==========================================
# LOOP DE SERVIÇO
# ==========================================

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

def start_service():
    print(f"Iniciando serviço de monitoramento FIFO. Agendado para todo DOMINGO as 14:00.")
    
    # Garantir que pasta data existe no caminho configurado
    if ARQUIVO_ESTADO.parent.name == "data":
        os.makedirs(ARQUIVO_ESTADO.parent, exist_ok=True)

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
                    print("Agendamento: Nenhuma execução anterior registrada. Rodando job de Domingo...")
                    should_run = True
                else:
                    try:
                        last_run = datetime.datetime.fromisoformat(last_run_str)
                        # Se a última execução não foi HOJE, então roda.
                        # Garante apenas uma execução no Domingo.
                        if last_run.date() != now.date():
                            print(f"Agendamento: Última execução foi {last_run}. Rodando job de Domingo agora...")
                            should_run = True
                        else:
                            # Já rodou hoje, aguarda próxima semana
                            pass
                    except ValueError:
                        print("Agendamento: Erro ao ler data anterior. Forçando execução...")
                        should_run = True

                if should_run:
                    try:
                        print(f"Iniciando execução agendada: {datetime.datetime.now()}")
                        run_job()
                        
                        # Atualiza estado após sucesso
                        state = load_state()
                        state["last_run"] = datetime.datetime.now().isoformat()
                        save_state(state)
                        print("Job de Domingo concluído com sucesso.")
                    except Exception as e:
                        print(f"Erro fatal no job agendado: {e}")
            
            else:
                pass
                # Se desejar logs verbose:
                # print(f"Aguardando Domingo 14hs. Agora: {now}")

        except Exception as e:
            print(f"Erro no loop principal do serviço: {e}")

        # Verifica a cada 10 minutos (600s) para não perder a janela, mas sem busy-wait
        time.sleep(600) 

if __name__ == "__main__":
    # Se o usuário passar argumento "run", roda direto
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run_job()
    elif len(sys.argv) > 1 and sys.argv[1] == "check":
        # Verifica apenas a tabela
        verificar_tabela_postgres()
    elif len(sys.argv) > 1 and sys.argv[1] == "create":
        # Cria apenas a tabela
        criar_tabela_postgres()
        print("Tabela criada/verificada com sucesso!")
    elif len(sys.argv) > 1 and sys.argv[1] == "compras-hist":
        # Atualiza só a lista produto x fornecedor no Mongo (sem a análise completa)
        atualizar_compras_fornecedor_mongo()
    else:
        start_service()