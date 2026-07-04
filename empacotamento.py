# -*- coding: utf-8 -*-
"""
Empacotamento INCREMENTAL do estado FIFO (Fase 2).

Evita reler ~6 anos do ERP a cada execução. Em vez disso:
  - ZONA CONGELADA (Mongo): por produto, as camadas FIFO abertas numa data de
    corte T + as vendas mensais (para a sazonalidade dos anos antigos). Movimento
    < T é imutável -> empacotado UMA vez (backfill) e nunca mais relido.
  - JANELA RAW (relê do ERP todo run): movimentos de T..hoje (janela = 24 meses),
    o que absorve lançamentos retroativos recentes.

A identidade que garante corretude:  Σcamadas(T) + Σent_janela − Σsai_janela = saldo_atual.
A equivalência (pacote+janela == carga completa) está provada no POC (poc_pack_fifo).

Integração: as camadas do pacote viram "entradas sintéticas" (com a data de compra
real) e são concatenadas ANTES das entradas da janela; assim o FIFO do main.py roda
sem alteração. Ver `to_entradas_sinteticas`.
"""
from __future__ import annotations

import os
import json
from abc import ABC, abstractmethod
from collections import deque
from datetime import date, datetime

import pandas as pd

# ----------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------
# Janela raw (meses relidos do ERP a cada execução). Só meses mais antigos que
# a janela são congelados no pacote.
# 12 meses = período de GARANTIA contra lançamentos retroativos (para trás de
# 12m o ERP não muda) e cobre exatamente a janela de demanda do modelo — a
# demanda/σ/ABC/margem saem 100% do dado cru; o pacote fornece o resto
# (camadas FIFO, sazonalidade antiga e estatísticas vitalícias).
JANELA_MESES = int(os.getenv("EMPAC_JANELA_MESES") or 12)
MONGO_URL = os.getenv("MONGO_URL", "mongodb://admin:admin@automacao_mongo-db:27017/?tls=false")
MONGO_DB = os.getenv("MONGO_DB", "analise_estoque")
MONGO_COL = os.getenv("MONGO_COL", "fifo_pack")

_FMT = "%Y-%m-%d"


def primeiro_dia_mes(d: date) -> date:
    return date(d.year, d.month, 1)


def add_meses(d: date, n: int) -> date:
    """Soma n meses (n pode ser negativo) preservando o dia 1."""
    total = (d.year * 12 + (d.month - 1)) + n
    ano, mes = divmod(total, 12)
    return date(ano, mes + 1, 1)


def corte_da_janela(hoje: date, janela_meses: int = JANELA_MESES) -> date:
    """Data de corte T = primeiro dia do mês, `janela_meses` meses atrás."""
    return add_meses(primeiro_dia_mes(hoje), -janela_meses)


def mes_chave(d) -> str:
    d = pd.Timestamp(d)
    return f"{d.year:04d}-{d.month:02d}"


# ----------------------------------------------------------------------
# Núcleo FIFO por camadas (layer model) — provado no POC
# ----------------------------------------------------------------------
def processar_fifo(camadas_iniciais, eventos):
    """
    camadas_iniciais: [{'data_compra': date/Timestamp, 'qtd': float}]
    eventos: lista cronológica de {'tipo':'E'|'S', 'data', 'qtd'}
    Retorna (vendas, camadas_residuais):
      vendas: [{'data', 'qtd', 'consumos':[(data_compra, qtd)], 'faltou': float}]
      camadas_residuais: [{'data_compra', 'qtd'}] (FIFO; mais antigo primeiro)
    """
    fila = deque([[pd.Timestamp(c['data_compra']), float(c['qtd'])]
                  for c in camadas_iniciais if float(c.get('qtd', 0)) > 0])
    vendas = []
    for ev in eventos:
        if ev['tipo'] == 'E':
            if ev['qtd'] > 0:
                fila.append([pd.Timestamp(ev['data']), float(ev['qtd'])])
        else:
            q = float(ev['qtd']); consumos = []
            while q > 1e-9 and fila:
                layer = fila[0]
                take = min(layer[1], q)
                consumos.append((layer[0], take))
                layer[1] -= take; q -= take
                if layer[1] <= 1e-9:
                    fila.popleft()
            vendas.append({'data': pd.Timestamp(ev['data']), 'qtd': float(ev['qtd']),
                           'consumos': consumos, 'faltou': q})
    camadas = [{'data_compra': d, 'qtd': qr} for d, qr in fila]
    return vendas, camadas


def _eventos(df_ent, df_sai):
    """Constrói a lista cronológica de eventos a partir de DataFrames de mov.
    df_ent: colunas PRO_CODIGO, DATA, QUANTIDADE
    df_sai: colunas PRO_CODIGO, DATA, QUANTIDADE_AJUSTADA
    Mesma data: ENTRADA antes de SAÍDA.
    """
    evs = []
    if df_ent is not None and not df_ent.empty:
        for d, q in zip(df_ent["DATA"], df_ent["QUANTIDADE"]):
            evs.append({'tipo': 'E', 'data': d, 'qtd': float(q or 0)})
    if df_sai is not None and not df_sai.empty:
        for d, q in zip(df_sai["DATA"], df_sai["QUANTIDADE_AJUSTADA"]):
            evs.append({'tipo': 'S', 'data': d, 'qtd': float(q or 0)})
    evs.sort(key=lambda e: (pd.Timestamp(e['data']), 0 if e['tipo'] == 'E' else 1))
    return evs


def camadas_no_corte(df_ent_prod, df_sai_prod, T):
    """Camadas abertas em T (processa só movimento com data < T)."""
    Tt = pd.Timestamp(T)
    evs = [e for e in _eventos(df_ent_prod, df_sai_prod) if pd.Timestamp(e['data']) < Tt]
    _, cam = processar_fifo([], evs)
    return cam


def reconciliar_camadas(camadas, saldo_atual, hoje):
    """
    Ajuste de inventário: força Σcamadas == saldo_atual confiando no ERP.
      - sobra (saldo > Σcamadas): acrescenta camada com data de hoje.
      - falta (saldo < Σcamadas): consome FIFO (mais antigas primeiro).
    Retorna (camadas_ajustadas, divergencia) onde divergencia = saldo - Σcamadas (0 se ok).
    """
    soma = sum(float(c['qtd']) for c in camadas)
    diff = float(saldo_atual) - soma
    if abs(diff) < 1e-6:
        return camadas, 0.0
    cam = [dict(c) for c in camadas]
    if diff > 0:
        cam.append({'data_compra': pd.Timestamp(hoje), 'qtd': diff})
    else:
        faltam = -diff
        i = 0
        while faltam > 1e-9 and i < len(cam):
            take = min(cam[i]['qtd'], faltam)
            cam[i]['qtd'] -= take; faltam -= take
            i += 1
        cam = [c for c in cam if c['qtd'] > 1e-9]
    return cam, diff


# ----------------------------------------------------------------------
# Stores
# ----------------------------------------------------------------------
class PackStore(ABC):
    @abstractmethod
    def carregar(self) -> dict:
        """Retorna {pro_codigo: doc}. doc = {corte, camadas, vendas_mensais}."""

    @abstractmethod
    def salvar(self, docs: dict, meta: dict):
        """Persiste {pro_codigo: doc} e metadados (ex.: corte global)."""

    @abstractmethod
    def meta(self) -> dict:
        """Metadados globais (ex.: {'corte': 'YYYY-MM-DD'}). {} se vazio."""


class FilePackStore(PackStore):
    """Store em arquivo JSON — para desenvolvimento/teste local (sem Mongo)."""

    def __init__(self, path):
        self.path = path

    def _read(self):
        if not os.path.exists(self.path):
            return {"_meta": {}, "docs": {}}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def carregar(self) -> dict:
        return self._read().get("docs", {})

    def meta(self) -> dict:
        return self._read().get("_meta", {})

    def salvar(self, docs: dict, meta: dict):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"_meta": meta, "docs": docs}, f, ensure_ascii=False, default=str)


class MongoPackStore(PackStore):
    """Adaptador Mongo. Só funciona DE DENTRO da rede docker (serviço)."""

    def __init__(self, url=MONGO_URL, db=MONGO_DB, col=MONGO_COL):
        import pymongo  # import tardio: só quando usado em produção
        self._cli = pymongo.MongoClient(url, serverSelectionTimeoutMS=8000)
        self._col = self._cli[db][col]
        self._meta_col = self._cli[db][col + "_meta"]

    def carregar(self) -> dict:
        return {d["_id"]: {"corte": d.get("corte"),
                           "camadas": d.get("camadas", []),
                           "vendas_mensais": d.get("vendas_mensais", {}),
                           # estatísticas vitalícias (pack v2; ausentes em pack v1 -> defaults)
                           "n_vendas": d.get("n_vendas", 0),
                           "qtd_vendida": d.get("qtd_vendida", 0.0),
                           "data_min_venda": d.get("data_min_venda"),
                           "data_max_venda": d.get("data_max_venda"),
                           "dpm_num": d.get("dpm_num", 0.0),
                           "dpm_den": d.get("dpm_den", 0.0)}
                for d in self._col.find({})}

    def meta(self) -> dict:
        m = self._meta_col.find_one({"_id": "global"})
        return {k: v for k, v in (m or {}).items() if k != "_id"}

    def salvar(self, docs: dict, meta: dict):
        from pymongo import ReplaceOne
        ops = [ReplaceOne({"_id": cod}, {"_id": cod, **doc}, upsert=True)
               for cod, doc in docs.items()]
        if ops:
            self._col.bulk_write(ops, ordered=False)
        self._meta_col.replace_one({"_id": "global"}, {"_id": "global", **meta}, upsert=True)


# ----------------------------------------------------------------------
# Serialização das camadas
# ----------------------------------------------------------------------
def _cam_to_doc(camadas):
    return [{"data_compra": pd.Timestamp(c["data_compra"]).strftime(_FMT),
             "qtd": round(float(c["qtd"]), 6)} for c in camadas]


def _cam_from_doc(camadas_doc):
    return [{"data_compra": pd.Timestamp(c["data_compra"]), "qtd": float(c["qtd"])}
            for c in camadas_doc]


# ----------------------------------------------------------------------
# Backfill (1×) e avanço incremental
# ----------------------------------------------------------------------
def _vendas_mensais_por_produto(df_sai, ate_exclusivo):
    """Somatório de QUANTIDADE_AJUSTADA por (PRO_CODIGO, 'YYYY-MM') com data < ate_exclusivo."""
    s = df_sai[["PRO_CODIGO", "DATA", "QUANTIDADE_AJUSTADA"]].copy()
    s["DATA"] = pd.to_datetime(s["DATA"], errors="coerce")
    s = s[s["DATA"] < pd.Timestamp(ate_exclusivo)]
    if s.empty:
        return {}
    s["MES"] = s["DATA"].dt.strftime("%Y-%m")
    g = s.groupby(["PRO_CODIGO", "MES"])["QUANTIDADE_AJUSTADA"].sum()
    out = {}
    for (cod, mes), v in g.items():
        out.setdefault(str(cod).strip(), {})[mes] = round(float(v), 4)
    return out


def _dpm_de_vendas(vendas):
    """
    Contribuições do "tempo em estoque" (DPM = dias entre a compra e a venda)
    das vendas processadas pelo FIFO:
      dpm_num = Σ sobre consumos ((data_venda − data_compra).dias × qtd_consumida)
      dpm_den = Σ qtd_consumida
    Equivale exatamente ao DPM médio ponderado do main.py (média das datas de
    compra ponderada pela qtd é linear).
    """
    num = 0.0
    den = 0.0
    for v in vendas:
        # Mesma regra do main.py: venda PARCIALMENTE atendida (faltou estoque)
        # fica sem data_compra e NÃO entra no tempo médio.
        if float(v.get("faltou", 0.0)) > 1e-9:
            continue
        dv = pd.Timestamp(v["data"])
        for dc, q in v.get("consumos", []):
            num += (dv - pd.Timestamp(dc)).days * float(q)
            den += float(q)
    return num, den


def _stats_vendas(df_sai_prod, ate_exclusivo):
    """Estatísticas vitalícias das vendas < corte: nº de linhas, qtd, datas min/max."""
    if df_sai_prod is None or df_sai_prod.empty:
        return {"n_vendas": 0, "qtd_vendida": 0.0, "data_min_venda": None, "data_max_venda": None}
    s = df_sai_prod[df_sai_prod["DATA"] < pd.Timestamp(ate_exclusivo)]
    if s.empty:
        return {"n_vendas": 0, "qtd_vendida": 0.0, "data_min_venda": None, "data_max_venda": None}
    return {
        "n_vendas": int(len(s)),
        "qtd_vendida": round(float(s["QUANTIDADE_AJUSTADA"].sum()), 4),
        "data_min_venda": pd.Timestamp(s["DATA"].min()).strftime(_FMT),
        "data_max_venda": pd.Timestamp(s["DATA"].max()).strftime(_FMT),
    }


_STATS_VAZIO = {"n_vendas": 0, "qtd_vendida": 0.0, "data_min_venda": None,
                "data_max_venda": None, "dpm_num": 0.0, "dpm_den": 0.0}


def computar_pack(df_ent, df_sai, hoje, janela_meses=JANELA_MESES):
    """
    BACKFILL: a partir da carga COMPLETA, calcula o pacote no corte T.
      camadas abertas em T + vendas mensais congeladas (< T) + estatísticas
      vitalícias (nº de vendas, qtd, datas min/max, tempo médio em estoque) —
      necessárias para o mín/máx não mudar quando a janela raw encolhe p/ 12m.
    Retorna (docs, corte).
    """
    T = corte_da_janela(hoje, janela_meses)
    Ts = T.strftime(_FMT)
    Tt = pd.Timestamp(T)
    de = df_ent.copy(); de["PRO_CODIGO"] = de["PRO_CODIGO"].astype(str).str.strip()
    de["DATA"] = pd.to_datetime(de["DATA"], errors="coerce")
    ds = df_sai.copy(); ds["PRO_CODIGO"] = ds["PRO_CODIGO"].astype(str).str.strip()
    ds["DATA"] = pd.to_datetime(ds["DATA"], errors="coerce")

    vmensais = _vendas_mensais_por_produto(ds, T)

    ent_por = {c: g for c, g in de.groupby("PRO_CODIGO")}
    sai_por = {c: g for c, g in ds.groupby("PRO_CODIGO")}
    produtos = set(ent_por) | set(sai_por) | set(vmensais)

    docs = {}
    for cod in produtos:
        gs = sai_por.get(cod)
        evs = [e for e in _eventos(ent_por.get(cod), gs) if pd.Timestamp(e['data']) < Tt]
        vendas, cam = processar_fifo([], evs)
        dpm_num, dpm_den = _dpm_de_vendas(vendas)
        docs[cod] = {
            "corte": Ts,
            "camadas": _cam_to_doc(cam),
            "vendas_mensais": vmensais.get(cod, {}),
            **_stats_vendas(gs, T),
            "dpm_num": round(dpm_num, 2),
            "dpm_den": round(dpm_den, 4),
        }
    return docs, T


def avancar_corte(docs, df_ent_janela, df_sai_janela, corte_antigo, corte_novo):
    """
    FOLD INCREMENTAL: dobra os meses [corte_antigo, corte_novo) para dentro do pacote,
    usando SÓ os movimentos da janela já carregada (sem reler o ERP).
    Atualiza camadas e vendas_mensais de cada produto. Retorna docs atualizados.
    """
    Ta, Tn = pd.Timestamp(corte_antigo), pd.Timestamp(corte_novo)
    de = df_ent_janela.copy(); de["PRO_CODIGO"] = de["PRO_CODIGO"].astype(str).str.strip()
    de["DATA"] = pd.to_datetime(de["DATA"], errors="coerce")
    ds = df_sai_janela.copy(); ds["PRO_CODIGO"] = ds["PRO_CODIGO"].astype(str).str.strip()
    ds["DATA"] = pd.to_datetime(ds["DATA"], errors="coerce")

    de_f = de[(de["DATA"] >= Ta) & (de["DATA"] < Tn)]
    ds_f = ds[(ds["DATA"] >= Ta) & (ds["DATA"] < Tn)]

    ent_por = {c: g for c, g in de_f.groupby("PRO_CODIGO")}
    sai_por = {c: g for c, g in ds_f.groupby("PRO_CODIGO")}
    produtos = set(docs) | set(ent_por) | set(sai_por)

    novos = {}
    for cod in produtos:
        doc = docs.get(cod, {"camadas": [], "vendas_mensais": {}, **_STATS_VAZIO})
        cam0 = _cam_from_doc(doc.get("camadas", []))
        evs = _eventos(ent_por.get(cod), sai_por.get(cod))
        vendas, cam1 = processar_fifo(cam0, evs)
        # vendas mensais do mês congelado
        vm = dict(doc.get("vendas_mensais", {}))
        gsai = sai_por.get(cod)
        n_v = int(doc.get("n_vendas", 0) or 0)
        q_v = float(doc.get("qtd_vendida", 0.0) or 0.0)
        dmin = doc.get("data_min_venda")
        dmax = doc.get("data_max_venda")
        if gsai is not None and not gsai.empty:
            tmp = gsai.copy()
            tmp["MES"] = tmp["DATA"].dt.strftime("%Y-%m")
            for mes, v in tmp.groupby("MES")["QUANTIDADE_AJUSTADA"].sum().items():
                vm[mes] = round(vm.get(mes, 0.0) + float(v), 4)
            n_v += int(len(tmp))
            q_v += float(tmp["QUANTIDADE_AJUSTADA"].sum())
            f_min = pd.Timestamp(tmp["DATA"].min()).strftime(_FMT)
            f_max = pd.Timestamp(tmp["DATA"].max()).strftime(_FMT)
            dmin = min(dmin, f_min) if dmin else f_min
            dmax = max(dmax, f_max) if dmax else f_max
        d_num, d_den = _dpm_de_vendas(vendas)
        novos[cod] = {"corte": Tn.strftime(_FMT), "camadas": _cam_to_doc(cam1),
                      "vendas_mensais": vm,
                      "n_vendas": n_v, "qtd_vendida": round(q_v, 4),
                      "data_min_venda": dmin, "data_max_venda": dmax,
                      "dpm_num": round(float(doc.get("dpm_num", 0.0) or 0.0) + d_num, 2),
                      "dpm_den": round(float(doc.get("dpm_den", 0.0) or 0.0) + d_den, 4)}
    return novos


# ----------------------------------------------------------------------
# Injeção no FIFO do main.py
# ----------------------------------------------------------------------
def to_entradas_sinteticas(docs):
    """
    Converte as camadas abertas do pacote em linhas de ENTRADA sintéticas
    (DATA = data_compra real, QUANTIDADE = qtd, ORIGEM = 'PACK'), para concatenar
    ANTES das entradas da janela. Assim o FIFO existente roda sem alteração.
    """
    rows = []
    for cod, doc in docs.items():
        for c in doc.get("camadas", []):
            rows.append({"PRO_CODIGO": str(cod).strip(),
                         "DATA": pd.Timestamp(c["data_compra"]),
                         "QUANTIDADE": float(c["qtd"]),
                         "ORIGEM": "PACK"})
    cols = ["PRO_CODIGO", "DATA", "QUANTIDADE", "ORIGEM"]
    return pd.DataFrame(rows, columns=cols)


# ----------------------------------------------------------------------
# Lista produto -> fornecedores (histórico de compra) no Mongo
# ----------------------------------------------------------------------
COMPRAS_COL = os.getenv("MONGO_COMPRAS_COL", "compras_fornecedor")
COMPRAS_FILE = os.getenv("COMPRAS_HIST_FILE")  # se setado, usa arquivo JSON (dev/teste)


def salvar_compras_fornecedor(mapa):
    """
    Grava {pro_codigo: [(for_nome, qtd), ...]} (já ordenado desc) no Mongo
    (coleção compras_fornecedor), 1 doc por produto. Substitui tudo.
    Em dev, se COMPRAS_HIST_FILE estiver setado, grava num JSON.
    Retorna a quantidade de produtos gravados.
    """
    if COMPRAS_FILE:
        with open(COMPRAS_FILE, "w", encoding="utf-8") as f:
            json.dump({c: [[n, round(float(q), 2)] for n, q in lst] for c, lst in mapa.items()},
                      f, ensure_ascii=False)
        return len(mapa)
    import pymongo
    cli = pymongo.MongoClient(MONGO_URL, serverSelectionTimeoutMS=8000)
    col = cli[MONGO_DB][COMPRAS_COL]
    col.delete_many({})
    docs = [{"_id": str(cod), "f": [[n, round(float(q), 2)] for n, q in lst]}
            for cod, lst in mapa.items() if lst]
    if docs:
        for i in range(0, len(docs), 5000):
            col.insert_many(docs[i:i + 5000], ordered=False)
    return len(docs)


def carregar_compras_fornecedor():
    """Lê o mapa produto -> [(for_nome, qtd), ...] do Mongo (ou do JSON em dev)."""
    if COMPRAS_FILE and os.path.exists(COMPRAS_FILE):
        with open(COMPRAS_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        return {str(c): [(x[0], float(x[1])) for x in lst] for c, lst in raw.items()}
    import pymongo
    cli = pymongo.MongoClient(MONGO_URL, serverSelectionTimeoutMS=8000)
    col = cli[MONGO_DB][COMPRAS_COL]
    return {d["_id"]: [(x[0], float(x[1])) for x in d.get("f", [])] for d in col.find({})}


def fifo_por_camadas(df_ent, df_sai, df_saldo, packs=None, hoje=None):
    """
    MOTOR FIFO ÚNICO (layer model). Substitui o loop de DATA_COMPRA do run_job e o
    calcular_fifo_saldo_atual: num único passe cronológico por produto produz
      - data_compra por LINHA de venda (data de compra média PONDERADA por qtd da
        venda; preserva o tempo médio ponderado exato — NaT se faltou estoque);
      - camadas residuais do estoque atual, RECONCILIADAS ao saldo do ERP.

    packs: dict {pro_codigo: [{data_compra, qtd}]} (camadas iniciais; None/{} = começa vazio,
           equivalente à carga completa desde a origem).

    Retorna (data_compra, df_long, df_div):
      data_compra: pd.Series alinhada ao índice de df_sai
      df_long: PRO_CODIGO, DATA_COMPRA_RESIDUAL, QTD_RESTANTE, ESTOQUE_DISPONIVEL, LAYER_INDEX
      df_div:  PRO_CODIGO, DIVERGENCIA (saldo - Σcamadas teóricas)
    """
    import numpy as np
    packs = packs or {}
    if hoje is None:
        hoje = pd.Timestamp.today().normalize()
    hoje = pd.Timestamp(hoje)

    de = df_ent.copy()
    de["PRO_CODIGO"] = de["PRO_CODIGO"].astype(str).str.strip()
    de["DATA"] = pd.to_datetime(de["DATA"], errors="coerce")
    if "LANCTO" not in de.columns:
        de["LANCTO"] = 0
    ds = df_sai.copy()
    ds["PRO_CODIGO"] = ds["PRO_CODIGO"].astype(str).str.strip()
    ds["DATA"] = pd.to_datetime(ds["DATA"], errors="coerce")

    saldo_map = (df_saldo[["PRO_CODIGO", "ESTOQUE_DISPONIVEL"]].drop_duplicates()
                 .assign(PRO_CODIGO=lambda d: d["PRO_CODIGO"].astype(str).str.strip())
                 .set_index("PRO_CODIGO")["ESTOQUE_DISPONIVEL"].to_dict())

    ent_por = {c: g for c, g in de.groupby("PRO_CODIGO")}
    sai_por = {c: g for c, g in ds.groupby("PRO_CODIGO")}
    produtos = set(ent_por) | set(sai_por) | set(saldo_map) | set(packs)

    data_compra = pd.Series(pd.NaT, index=ds.index, dtype="datetime64[ns]")
    long_rows = []
    div_rows = []

    for cod in produtos:
        # eventos cronológicos: entrada antes de saída na mesma data
        eventos = []
        ge = ent_por.get(cod)
        if ge is not None:
            for d, q, lan in zip(ge["DATA"], ge["QUANTIDADE"], ge["LANCTO"]):
                eventos.append((pd.Timestamp(d), 0, float(lan or 0), 'E', float(q or 0), None))
        gs = sai_por.get(cod)
        if gs is not None:
            for idx, d, q in zip(gs.index, gs["DATA"], gs["QUANTIDADE_AJUSTADA"]):
                eventos.append((pd.Timestamp(d), 1, 0.0, 'S', float(q or 0), idx))
        eventos.sort(key=lambda e: (e[0], e[1], e[2]))

        fila = deque([[pd.Timestamp(c['data_compra']), float(c['qtd'])]
                      for c in packs.get(cod, []) if float(c.get('qtd', 0)) > 0])
        for ts, _, _, tipo, qtd, idx in eventos:
            if tipo == 'E':
                if qtd > 0:
                    fila.append([ts, qtd])
            else:
                q = qtd
                num = 0.0   # Σ(value_ns * qtd) p/ data média ponderada
                den = 0.0
                while q > 1e-9 and fila:
                    layer = fila[0]
                    take = min(layer[1], q)
                    num += layer[0].value * take
                    den += take
                    layer[1] -= take; q -= take
                    if layer[1] <= 1e-9:
                        fila.popleft()
                if den > 0 and q <= 1e-9:
                    data_compra.loc[idx] = pd.Timestamp(int(round(num / den)))
                # se q>0 (faltou estoque) deixa NaT

        # camadas residuais (estoque teórico) -> reconcilia ao saldo do ERP
        camadas = [{'data_compra': d, 'qtd': qr} for d, qr in fila]
        saldo = float(saldo_map.get(cod, 0) or 0)
        cam_rec, div = reconciliar_camadas(camadas, saldo, hoje)
        if abs(div) > 1e-6:
            div_rows.append({"PRO_CODIGO": cod, "DIVERGENCIA": div})
        for i, c in enumerate(cam_rec, start=1):
            if c['qtd'] > 1e-9:
                long_rows.append({"PRO_CODIGO": cod, "LAYER_INDEX": i,
                                  "DATA_COMPRA_RESIDUAL": pd.Timestamp(c['data_compra']),
                                  "QTD_RESTANTE": float(c['qtd']),
                                  "ESTOQUE_DISPONIVEL": saldo})

    df_long = pd.DataFrame(long_rows, columns=["PRO_CODIGO", "LAYER_INDEX",
                                               "DATA_COMPRA_RESIDUAL", "QTD_RESTANTE",
                                               "ESTOQUE_DISPONIVEL"])
    df_div = pd.DataFrame(div_rows, columns=["PRO_CODIGO", "DIVERGENCIA"])
    return data_compra, df_long, df_div


def stats_por_produto(docs):
    """
    Estatísticas vitalícias CONGELADAS do pacote, por produto, para o
    calcular_metricas_e_classificar combinar com a janela raw:
      PACK_N_VENDAS, PACK_QTD_VENDIDA, PACK_DATA_MIN, PACK_DATA_MAX,
      PACK_DPM_NUM, PACK_DPM_DEN.
    Pacotes v1 (sem os campos) rendem zeros — o resultado degrada para o
    comportamento antigo até o corte avançar uma vez (fold grava v2).
    """
    rows = []
    for cod, doc in docs.items():
        rows.append({
            "PRO_CODIGO": str(cod).strip(),
            "PACK_N_VENDAS": int(doc.get("n_vendas", 0) or 0),
            "PACK_QTD_VENDIDA": float(doc.get("qtd_vendida", 0.0) or 0.0),
            "PACK_DATA_MIN": pd.to_datetime(doc.get("data_min_venda"), errors="coerce"),
            "PACK_DATA_MAX": pd.to_datetime(doc.get("data_max_venda"), errors="coerce"),
            "PACK_DPM_NUM": float(doc.get("dpm_num", 0.0) or 0.0),
            "PACK_DPM_DEN": float(doc.get("dpm_den", 0.0) or 0.0),
        })
    return pd.DataFrame(rows, columns=["PRO_CODIGO", "PACK_N_VENDAS", "PACK_QTD_VENDIDA",
                                       "PACK_DATA_MIN", "PACK_DATA_MAX",
                                       "PACK_DPM_NUM", "PACK_DPM_DEN"])


def vendas_mensais_subgrupo(docs, df_saldo_produto):
    """
    Agrega as vendas mensais CONGELADAS do pacote por (SGR_CODIGO, ano, mês),
    para alimentar a sazonalidade junto com os meses da janela.
    Retorna DataFrame: SGR_CODIGO, ANO, MES, QTD.
    """
    sp = df_saldo_produto[["PRO_CODIGO", "SGR_CODIGO"]].dropna(subset=["SGR_CODIGO"]).copy()
    sp["PRO_CODIGO"] = sp["PRO_CODIGO"].astype(str).str.strip()
    pro2sgr = sp.set_index("PRO_CODIGO")["SGR_CODIGO"].to_dict()
    rows = []
    for cod, doc in docs.items():
        sgr = pro2sgr.get(str(cod).strip())
        if sgr is None:
            continue
        for mes, q in doc.get("vendas_mensais", {}).items():
            ano, m = mes.split("-")
            rows.append({"SGR_CODIGO": sgr, "ANO": int(ano), "MES": int(m), "QTD": float(q)})
    return pd.DataFrame(rows, columns=["SGR_CODIGO", "ANO", "MES", "QTD"])
