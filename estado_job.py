# -*- coding: utf-8 -*-
"""Estado persistido do job semanal (última execução) — lido pelo scheduler e
pela página inicial. Arquivo JSON em data/ para sobreviver a restart do container."""
import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ARQUIVO_ESTADO = BASE_DIR / "data" / "fifo_service_state.json"
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS') or 7)


def load_state():
    if not ARQUIVO_ESTADO.exists():
        print(f"DEBUG: Arquivo de estado não encontrado em {ARQUIVO_ESTADO}")
        return {}
    try:
        with open(ARQUIVO_ESTADO, "r") as f:
            state = json.load(f)
            print(f"DEBUG: Estado carregado de {ARQUIVO_ESTADO}: {state}")
            return state
    except Exception as e:
        print(f"DEBUG: Erro ao ler estado: {e}")
        return {}


def save_state(state):
    try:
        with open(ARQUIVO_ESTADO, "w") as f:
            json.dump(state, f)
            print(f"DEBUG: Estado salvo em {ARQUIVO_ESTADO}")
    except Exception as e:
        print(f"Erro ao salvar estado: {e}")
