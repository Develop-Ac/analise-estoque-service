# -*- coding: utf-8 -*-
"""Conexoes: engine unico do Postgres e ODBC do SQL Server (OPENQUERY)."""
import os
import pyodbc
from sqlalchemy import create_engine
from config import POSTGRES_URL, SQL_HOST, SQL_PORT, SQL_DATABASE, SQL_USER, SQL_PASSWORD, TDS_VERSION

# Engine ÚNICO do processo. Criar engine por request abre um pool novo a cada
# chamada (conexões novas no Postgres + engines nunca descartados vazando).
# pool_pre_ping testa a conexão antes de entregar — um restart do Postgres
# derruba a conexão do pool sem derrubar o request seguinte.
_pg_engine = None


def get_pg_engine():
    global _pg_engine
    if _pg_engine is None:
        # SQLAlchemy 2.x não aceita o alias 'postgres://' — normaliza
        url = POSTGRES_URL.replace("postgres://", "postgresql://")
        _pg_engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=int(os.getenv("PG_POOL_SIZE") or 10),
            max_overflow=int(os.getenv("PG_MAX_OVERFLOW") or 10),
            pool_recycle=1800,
        )
    return _pg_engine


def get_db_connection():
    # .close() na conexão devolve ao pool (não fecha a física)
    return get_pg_engine().connect()

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
    # timeout de LOGIN: SQL Server fora do ar não pode segurar a thread
    # indefinidamente (o timeout de query é setado por consulta, via conn.timeout)
    return pyodbc.connect(conn_str, timeout=int(os.getenv('SQL_LOGIN_TIMEOUT_S') or 10))

# =============================================================================
