from api import listar_analise
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json

load_dotenv()

# Setup similar to api.py
db_url = os.getenv("POSTGRES_URL")
if not db_url:
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    host = os.getenv("SQL_HOST")
    port = os.getenv("SQL_PORT")
    dbname = os.getenv("SQL_DATABASE")
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

engine = create_engine(db_url)

def test_debug():
    with engine.connect() as conn:
        pro_codigo = "11344"
        gid_res = conn.execute(text(f"SELECT group_id FROM com_relacionamento_itens WHERE pro_codigo = '{pro_codigo}'")).fetchone()
        
        if not gid_res:
            print(f"Product {pro_codigo} not found in relationship table.")
            return
            
        group_id = gid_res[0]
        print(f"Testing with Product: {pro_codigo}, Group: {group_id}")

        # Call the function
        try:
            response = listar_analise(
                page=1, 
                limit=10, 
                pro_codigos=str(pro_codigo)
            )
            
            # Check the data for that item
            found = False
            for item in response['data']:
                if str(item['pro_codigo']) == str(pro_codigo):
                    found = True
                    print("\n--- API RESPONSE DATA ---")
                    print(json.dumps({
                        "pro_codigo": item.get('pro_codigo'),
                        "group_id": item.get('group_id'),
                        "group_count": item.get('group_count'),
                        "estoque_disponivel": item.get('estoque_disponivel'),
                        "grp_estoque_disponivel": item.get('grp_estoque_disponivel'),
                        "grp_media_dia": item.get('grp_media_dia'),
                        "grp_estoque_min_sugerido": item.get('grp_estoque_min_sugerido'),
                        "grp_estoque_max_sugerido": item.get('grp_estoque_max_sugerido')
                    }, indent=2))
                    
                    # Check DB directly
                    print("\n--- CHECK ---")
                    sums = conn.execute(text(f"""
                        SELECT 
                            count(*) as count,
                            sum(fifo.estoque_disponivel) as sum_est,
                            sum(fifo.demanda_media_dia_ajustada) as sum_dem,
                            sum(fifo.estoque_min_sugerido) as sum_min,
                            sum(fifo.estoque_max_sugerido) as sum_max
                        FROM com_fifo_completo fifo
                        JOIN com_relacionamento_itens rel ON fifo.pro_codigo = rel.pro_codigo
                        WHERE rel.group_id = '{group_id}'
                    """)).mappings().one()
                    
                    print(f"DB CACHED SUMS: Est={sums['sum_est']} (If this is 0 but API > 0, Realtime is working!)")
                    print(f"API RES (REALTIME): Est={item.get('grp_estoque_disponivel')}")
                    print(f"API Item GroupID: {item.get('group_id')}")
            
            if not found:
                print("Item not found in API response.")
                
        except Exception as e:
            print(f"API Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_debug()
