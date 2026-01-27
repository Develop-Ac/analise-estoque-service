from main import get_postgres_engine, get_connection
import pandas as pd
from sqlalchemy import text
import sys

def sync_subgroups():
    print("Iniciando sincronização de Subgrupos faltantes...")
    
    engine = get_postgres_engine()
    
    # 1. Buscar produtos COM sgr_codigo NULO ou sgr_descricao NULO na tabela local
    print("Buscando produtos locais com subgrupo pendente...")
    try:
        with engine.connect() as conn:
            # Lista de produtos que precisam de update
            result = conn.execute(text("SELECT DISTINCT pro_codigo FROM com_fifo_completo WHERE sgr_codigo IS NULL OR sgr_descricao IS NULL")).fetchall()
            products_to_update = [row[0] for row in result]
            
        if not products_to_update:
            print("Nenhum produto precisa de atualização de subgrupo.")
            return

        print(f"Encontrados {len(products_to_update)} produtos para atualizar.")

        # 2. Buscar dados no ERP (SQL Server) via OPENQUERY
        # Devido ao limite de tamanho de query do OPENQUERY, vamos fazer em batches se necessário.
        # Mas vamos tentar buscar TUDO do ERP e filtrar localmente, pode ser mais leve que listas gigantes no IN.
        
        print("Consultando ERP (OPENQUERY) para buscar todos subgrupos...")
        
        sql_erp = """
            SELECT * FROM OPENQUERY(CONSULTA, '
                SELECT 
                    pro.pro_codigo, 
                    pro.subgrp_codigo, 
                    sub.subgrp_descricao
                FROM produtos pro
                LEFT JOIN produtos_subgrupos sub 
                    ON sub.empresa = pro.empresa 
                    AND sub.subgrp_codigo = pro.subgrp_codigo
                WHERE pro.empresa = 3
            ')
        """
        
        conn_odbc = get_connection()
        df_erp = pd.read_sql(sql_erp, conn_odbc)
        conn_odbc.close()
        
        print(f"Dados ERP carregados: {len(df_erp)} registros.")
        
        # Normalizar colunas
        df_erp.columns = [c.upper() for c in df_erp.columns]
        # O retorno do OPENQUERY pode variar case, vamos garantir nomes
        # Esperado: PRO_CODIGO, SUBGRP_CODIGO, SUBGRP_DESCRICAO
        
        # Limpeza
        df_erp['PRO_CODIGO'] = df_erp['PRO_CODIGO'].astype(str).str.strip()
        
        # Filtra apenas os que precisamos atualizar
        df_updates = df_erp[df_erp['PRO_CODIGO'].isin([p.strip() for p in products_to_update])].copy()
        
        print(f"Registros correspondentes encontrados: {len(df_updates)}")
        
        if df_updates.empty:
            print("Nenhum dado encontrado no ERP para os códigos locais pendentes.")
            return

        # 3. Update no Postgres
        print("Aplicando atualizações no PostgreSQL...")
        
        # Vamos fazer update um a um ou em batch? 
        # Para garantir integridade e simplicidade, vamos iterar e fazer updates diretos.
        # Se for muito lento, podemos otimizar com temp table.
        
        count = 0
        with engine.begin() as conn: # Transaction
            for idx, row in df_updates.iterrows():
                pc = row['PRO_CODIGO']
                sg_cod = row['SUBGRP_CODIGO']
                sg_desc = row['SUBGRP_DESCRICAO']
                
                # Tratar NaNs
                if pd.isna(sg_cod): sg_cod = None
                else: sg_cod = int(sg_cod)
                
                if pd.isna(sg_desc): sg_desc = None
                else: sg_desc = str(sg_desc).strip()
                
                if sg_cod is not None:
                    conn.execute(
                        text("UPDATE com_fifo_completo SET sgr_codigo = :sc, sgr_descricao = :sd WHERE pro_codigo = :pc"),
                        {"sc": sg_cod, "sd": sg_desc, "pc": pc}
                    )
                    count += 1
                    
        print(f"Sucesso! {count} produtos atualizados com dados de subgrupo.")
        
    except Exception as e:
        print(f"Erro durante sincronização: {e}")
        raise e

if __name__ == "__main__":
    sync_subgroups()
