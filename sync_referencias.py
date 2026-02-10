
import os
import pyodbc
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONFIGURAÇÕES
# ==========================================

# Configurações PostgreSQL
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://usuario:senha@host:5432/database")

# ==========================================
# CONEXÕES
# ==========================================

def get_connection():
    server = os.getenv('SQL_HOST', '127.0.0.1')
    port = os.getenv('SQL_PORT', '1433')
    database = os.getenv('SQL_DATABASE', 'master')
    user = os.getenv('SQL_USER', 'sa')
    password = os.getenv('SQL_PASSWORD', 'senha_secreta')
    sql_driver = os.getenv('SQL_DRIVER', '{FreeTDS}')

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
        conn_str = (
            f"DRIVER={sql_driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={{{user}}};"
            f"PWD={{{password}}};"
        )
    
    print(f"DEBUG: Conectando ao ERP com DRIVER={sql_driver}, SERVER={server}...")
    return pyodbc.connect(conn_str)

def get_postgres_engine():
    final_url = POSTGRES_URL.replace("postgres://", "postgresql://")
    return create_engine(final_url)

# ==========================================
# LÓGICA DE ATUALIZAÇÃO
# ==========================================

def sync_referencias():
    print("Iniciando sincronização de referências...")
    
    # 1. Buscar referências no ERP (SQL Server)
    sql_erp = """
    SELECT * FROM OPENQUERY(CONSULTA, '
        SELECT 
            pro_codigo,
            referencia as pro_referencia
        FROM produtos
        WHERE empresa = 3
    ')
    """
    
    try:
        print("Buscando dados no ERP...")
        conn_erp = get_connection()
        cursor_erp = conn_erp.cursor()
        cursor_erp.execute(sql_erp)
        rows = cursor_erp.fetchall()
        
        # Criar dicionário {pro_codigo: pro_referencia}
        referencias = {}
        for row in rows:
            # row[0] = pro_codigo, row[1] = pro_referencia
            if row[1]: # Apenas se tiver referência
                cod = str(row[0]).strip()
                ref = str(row[1]).strip()
                referencias[cod] = ref
                
        print(f"Encontradas {len(referencias)} referências no ERP.")
        conn_erp.close()
        
        if not referencias:
            print("Nenhuma referência encontrada. Encerrando.")
            return

        # 2. Atualizar no PostgreSQL
        print("Atualizando PostgreSQL...")
        engine_pg = get_postgres_engine()
        
        with engine_pg.connect() as conn_pg:
            # Verificar se a coluna pro_referencia existe (só pra garantir, embora já deva existir)
            check_col = text("SELECT count(*) FROM information_schema.columns WHERE table_name='com_fifo_completo' AND column_name='pro_referencia'")
            if conn_pg.execute(check_col).scalar() == 0:
                print("Criando coluna pro_referencia...")
                conn_pg.execute(text("ALTER TABLE com_fifo_completo ADD COLUMN pro_referencia VARCHAR(50)"))
                conn_pg.commit()

            # Atualizar em lote é mais rápido, mas vamos fazer update simples primeiro para garantir
            # Para otimizar, poderiamos usar uma tabela temporária, mas para alguns milhares de produtos o update direto via sqlalchemy/transação resolve.
            
            # Vamos usar uma abordagem de criar uma tabela temporária de valores para update em massa (FROM values)
            # Ou transaction com múltiplos updates.
            
            print("Iniciando updates no banco PostgreSQL...")
            
            # Preparar o batch de updates
            # UPDATE com_fifo_completo SET pro_referencia = :ref WHERE pro_codigo = :cod
            
            # Para não travar, vamos fazer em batches de 1000
            batch_size = 1000
            updates = []
            count = 0
            total_updated = 0
            
            for cod, ref in referencias.items():
                updates.append({"ref": ref, "cod": cod})
                count += 1
                
                if count >= batch_size:
                    stmt = text("UPDATE com_fifo_completo SET pro_referencia = :ref WHERE pro_codigo = :cod")
                    conn_pg.execute(stmt, updates)
                    conn_pg.commit()
                    total_updated += count
                    print(f"Processados {total_updated} registros...")
                    updates = []
                    count = 0
            
            if updates:
                stmt = text("UPDATE com_fifo_completo SET pro_referencia = :ref WHERE pro_codigo = :cod")
                conn_pg.execute(stmt, updates)
                conn_pg.commit()
                total_updated += count
                
            print(f"Sincronização concluída! Total processado: {total_updated}")

    except Exception as e:
        print(f"Erro durante a sincronização: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    sync_referencias()
