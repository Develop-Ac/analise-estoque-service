# -*- coding: utf-8 -*-
"""Estado persistido do job semanal — lido pelo scheduler, pela página inicial
e pela tela Sistema→ETL da intranet (GET /sistema/job).

Arquivo JSON em data/ para sobreviver a restart do container. Guarda:
- last_run: ISO da última execução concluída (o scheduler decide por ela);
- rodando: execução em curso (início, etapa atual, etapas já fechadas) ou null;
- ultima: última execução fechada (status ok|erro, etapas com duração, contagens);
- historico: as últimas HISTORICO_MAX execuções, mais recente primeiro.

O job roda em thread dentro do processo da API; o lock abaixo é o que impede o
botão "Rodar" da tela e o scheduler de domingo dispararem duas análises juntas.
"""
import datetime
import json
import os
import threading
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ARQUIVO_ESTADO = BASE_DIR / "data" / "fifo_service_state.json"
INTERVALO_DIAS = int(os.getenv('INTERVALO_DIAS') or 7)
HISTORICO_MAX = 20
HORA_AGENDADA = 14      # domingo >= 14:00 (regra do scheduler)

_lock = threading.Lock()
_em_execucao = False


def _agora():
    return datetime.datetime.now().isoformat(timespec="seconds")


def load_state():
    if not ARQUIVO_ESTADO.exists():
        print(f"DEBUG: Arquivo de estado não encontrado em {ARQUIVO_ESTADO}")
        return {}
    try:
        with open(ARQUIVO_ESTADO, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"DEBUG: Erro ao ler estado: {e}")
        return {}


def save_state(state):
    try:
        ARQUIVO_ESTADO.parent.mkdir(parents=True, exist_ok=True)
        with open(ARQUIVO_ESTADO, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        print(f"Erro ao salvar estado: {e}")


# ------------------------------------------------------------------------------
# Ciclo de vida de uma execução (chamado pelo run_job do main.py)
# ------------------------------------------------------------------------------

def em_execucao():
    return _em_execucao


def registrar_inicio(modo=None):
    """Abre a execução. Devolve False se já há uma em curso (não abre outra)."""
    global _em_execucao
    with _lock:
        if _em_execucao:
            return False
        _em_execucao = True
    state = load_state()
    state["rodando"] = {"inicio": _agora(), "etapa_atual": None, "etapas": [], "modo": modo}
    save_state(state)
    return True


def registrar_etapa(nome):
    """Fecha a etapa anterior (com duração) e abre a próxima."""
    state = load_state()
    rod = state.get("rodando")
    if not rod:
        return
    _fechar_etapa(rod)
    rod["etapa_atual"] = {"nome": nome, "inicio": _agora()}
    save_state(state)


def anotar(**campos):
    """Anexa números da execução em curso (produtos, alteracoes, modo...)."""
    state = load_state()
    rod = state.get("rodando")
    if not rod:
        return
    rod.update({k: v for k, v in campos.items() if v is not None})
    save_state(state)


def registrar_fim(status, erro=None):
    """Fecha a execução. Idempotente: a segunda chamada não faz nada — o run_job
    fecha com 'erro' em pontos de saída antecipada e o wrapper fecha com 'ok'."""
    global _em_execucao
    state = load_state()
    rod = state.pop("rodando", None)
    if rod:
        _fechar_etapa(rod)
        fim = _agora()
        try:
            dur = (datetime.datetime.fromisoformat(fim)
                   - datetime.datetime.fromisoformat(rod["inicio"])).total_seconds()
        except Exception:
            dur = None
        registro = {
            "inicio": rod.get("inicio"),
            "fim": fim,
            "status": status,
            "duracao_s": round(dur) if dur is not None else None,
            "etapas": rod.get("etapas", []),
            "erro": (str(erro)[:500] if erro else None),
            "produtos": rod.get("produtos"),
            "alteracoes": rod.get("alteracoes"),
            "modo": rod.get("modo"),
        }
        state["ultima"] = registro
        hist = [registro] + [h for h in state.get("historico", []) if isinstance(h, dict)]
        state["historico"] = hist[:HISTORICO_MAX]
        if status == "ok":
            state["last_run"] = fim
        save_state(state)
    with _lock:
        _em_execucao = False


def _fechar_etapa(rod):
    atual = rod.get("etapa_atual")
    if not atual:
        return
    try:
        seg = (datetime.datetime.now()
               - datetime.datetime.fromisoformat(atual["inicio"])).total_seconds()
    except Exception:
        seg = None
    rod.setdefault("etapas", []).append({"nome": atual["nome"], "seg": round(seg, 1) if seg is not None else None})
    rod["etapa_atual"] = None


# ------------------------------------------------------------------------------
# Leitura para a tela
# ------------------------------------------------------------------------------

def proxima_execucao(state=None):
    """Próximo domingo >= 14:00 em que o scheduler dispara (ISO) — hoje mesmo se
    for domingo antes das 14h e ainda não rodou hoje."""
    state = state if state is not None else load_state()
    agora = datetime.datetime.now()
    hoje = agora.date()
    ja_rodou_hoje = False
    try:
        lr = state.get("last_run")
        ja_rodou_hoje = bool(lr) and datetime.datetime.fromisoformat(lr).date() == hoje
    except Exception:
        pass
    dias_ate_domingo = (6 - hoje.weekday()) % 7
    candidato = datetime.datetime.combine(hoje + datetime.timedelta(days=dias_ate_domingo),
                                          datetime.time(HORA_AGENDADA, 0))
    if dias_ate_domingo == 0 and (ja_rodou_hoje or agora >= candidato):
        candidato = candidato + datetime.timedelta(days=7)
    return candidato.isoformat(timespec="seconds")


def resumo_para_tela():
    state = load_state()
    rod = state.get("rodando")
    if rod and not _em_execucao:
        # Container reiniciou no meio de uma execução: o registro ficou aberto
        # sem processo por trás. Mostrar como interrompida, não como "rodando".
        rod = dict(rod, interrompida=True)
    return {
        "rodando": rod if _em_execucao else None,
        "interrompida": rod if (rod and not _em_execucao) else None,
        "ultima": state.get("ultima"),
        "historico": state.get("historico", []),
        "last_run": state.get("last_run"),
        "agenda": {
            "regra": "domingo a partir das 14:00 (uma execução por domingo)",
            "intervalo_dias": INTERVALO_DIAS,
            "proxima": proxima_execucao(state),
        },
    }
