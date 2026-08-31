# -*- coding: utf-8 -*-
"""Configuracao central: .env e constantes de conexao/consulta."""
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://usuario:senha@host:5432/database")

SQL_HOST = os.getenv('SQL_HOST', '127.0.0.1')
SQL_PORT = os.getenv('SQL_PORT', '1433')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'master')
SQL_USER = os.getenv('SQL_USER', 'sa')
SQL_PASSWORD = os.getenv('SQL_PASSWORD', 'senha_secreta')
TDS_VERSION = os.getenv('TDS_VERSION', '7.4')

# Fornecedor "do produto" para filtros: principal do HISTÓRICO de compra (Mongo,
# gravado pelo worker), com fallback no fornecedor 1 do cadastro. A MESMA
# expressão é usada na variável {{fornecedor}} dos cards do Metabase — o drill
# do painel soma exatamente o card.
FORN_EXPR = "COALESCE(NULLIF(fornecedor_principal,''), fornecedor1)"

# ==========================================
# SETUP
# ==========================================
