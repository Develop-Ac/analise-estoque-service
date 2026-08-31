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

