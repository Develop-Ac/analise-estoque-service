# -*- coding: utf-8 -*-
"""Rotas de sistema: pagina inicial e healthcheck."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text

import threading

import estado_job
from estado_job import load_state

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
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

@router.get("/health")
def health_check():
    return {"status": "ok"}


@router.get("/sistema/job")
def job_status():
    """Estado do job semanal para a tela Sistema→ETL da intranet: execução em
    curso (etapa atual), última execução fechada (etapas com duração, produtos,
    alterações), histórico recente e próxima execução agendada."""
    return estado_job.resumo_para_tela()


@router.post("/sistema/job/rodar")
def job_rodar():
    """Dispara a análise agora, em thread. O job leva de minutos a mais de uma
    hora e lê o ERP pelo linked server: por isso recusa disparo concorrente."""
    if estado_job.em_execucao():
        raise HTTPException(status_code=409, detail="Análise já em execução.")
    from main import run_job   # lazy: main.py carrega pandas/ODBC no import

    def _rodar():
        try:
            run_job()
        except Exception as e:  # run_job já registrou o erro no estado
            print(f"Erro na execução manual do job: {e}")

    threading.Thread(target=_rodar, name="analise-estoque-manual", daemon=True).start()
    return {"iniciado": True, "inicio": estado_job.load_state().get("rodando", {}).get("inicio")}

