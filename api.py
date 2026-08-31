# -*- coding: utf-8 -*-
"""Montagem do app: middlewares, rotas e scheduler de background.
As regras moram nos modulos (config, infra_db, erp_api, modelos,
estoque_rt, fornecedor_info, sugestao_compra) e nas rotas_*."""
import datetime
import os
import threading
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

import config  # noqa: F401  (carrega o .env antes de tudo)
from estado_job import ARQUIVO_ESTADO, load_state, save_state
from infra_db import get_db_connection

BACKGROUND_Running = False

app = FastAPI(title="API Analise Estoque", description="API Read-only para dados de analise de estoque")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


import rotas_analise
import rotas_promo
import rotas_similares
import rotas_catalogos
import rotas_compras
import rotas_produto
import rotas_sistema

app.include_router(rotas_analise.router)
app.include_router(rotas_promo.router)
app.include_router(rotas_similares.router)
app.include_router(rotas_catalogos.router)
app.include_router(rotas_compras.router)
app.include_router(rotas_produto.router)
app.include_router(rotas_sistema.router)

def background_scheduler():
    global BACKGROUND_Running
    print("Iniciando scheduler de background (Regra: Domingo >= 14:00)...")
    print(f"Monitorando arquivo de estado: {ARQUIVO_ESTADO}")
    
    # Garantir que pasta data existe
    if ARQUIVO_ESTADO.parent.name == "data":
        try:
            os.makedirs(ARQUIVO_ESTADO.parent, exist_ok=True)
        except Exception:
            pass

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
                    print(f"Agendamento: Nenhuma execução registrada. Domingo detectado. Rodando...")
                    should_run = True
                else:
                    try:
                        last_run = datetime.datetime.fromisoformat(last_run_str)
                        # Se a última execução não foi HOJE, então roda.
                        if last_run.date() != now.date():
                            print(f"Agendamento: Última execução foi {last_run}. Rodando job de Domingo agora...")
                            should_run = True
                        else:
                            # Já rodou hoje
                            pass
                    except ValueError:
                        print("Agendamento: Erro ao parsear data anterior. Forçando execução de Domingo...")
                        should_run = True
                
                if should_run:
                    if not BACKGROUND_Running:
                        BACKGROUND_Running = True
                        try:
                            print(f">>> Iniciando execução do Job FIFO: {now}")
                            run_job()
                            state = load_state()
                            state["last_run"] = datetime.datetime.now().isoformat()
                            save_state(state)
                            print(">>> Job FIFO finalizado com sucesso.")
                        except Exception as e:
                            print(f"Erro ao rodar job: {e}")
                        finally:
                            BACKGROUND_Running = False
                    else:
                        print("Job já está rodando. Ignorando trigger.")
            else:
                # Opcional: Log apenas 1 vez por hora se nao for domingo
                # if now.minute == 0:
                #    print(f"Aguardando Domingo 14hs. Agora: {now}")
                pass
                
        except Exception as e:
            print(f"Erro no background scheduler: {e}")
            BACKGROUND_Running = False
            
        # Verifica a cada 10 minutos (600s) para não perder a janela, mas sem busy-wait e sem flood
        time.sleep(600) 

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

