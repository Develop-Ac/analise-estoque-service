# -*- coding: utf-8 -*-
"""Análise unificada SÓ de um grupo de similares (tela, logo após "Vincular").

Roda o mesmo motor do consolidado (main.calcular_grupos_descricao) apenas com os
produtos do grupo, lidos do ERP na hora, e grava só nas linhas deles do último
snapshot de com_fifo_completo. O que depende da base inteira vem da última rodada
completa:
  - curva ABC do grupo: posicionada contra o valor 12m dos DEMAIS grupos;
  - índices sazonais do subgrupo: tabela com_indice_sazonal (gravada pela rodada);
  - custo unitário e última venda dos membros: o próprio snapshot.
A próxima rodada completa recalcula tudo do zero.

Membros = mesma regra da rodada: produtos ativos do cadastro, não originais, com a
mesma chave (descrição + linha da marca) depois de unir os vínculos manuais. Por
isso a busca fecha por descrição E por vínculo até estabilizar.
"""
import json
import re

import pandas as pd
from sqlalchemy import text

from infra_db import get_db_connection, get_pg_engine, get_sql_connection
from vinculos_similares import carregar_vinculos_manuais, unificar_chaves

_COD_OK = re.compile(r"^[A-Za-z0-9._-]{1,50}$")
MAX_PRODUTOS = 500          # trava de segurança da busca por descrição/vínculo
_SNAPSHOT = "data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)"

# Colunas que calcular_grupos_descricao devolve -> com_fifo_completo (mesmo nome em minúsculas)
_COLS_GRUPO = ["GRUPO_QTD_ITENS", "GRUPO_ESTOQUE_DISPONIVEL", "GRUPO_DEMANDA_DIA",
               "GRUPO_FATOR_SAZONAL", "GRUPO_CURVA", "GRUPO_PADRAO", "GRUPO_METODO",
               "GRUPO_ESTOQUE_MIN", "GRUPO_ESTOQUE_MAX", "GRUPO_ESTOQUE_SEGURANCA",
               "GRUPO_MEAN_SIZE", "GRUPO_CV2", "GRUPO_LEAD_TIME_DIAS",
               "GRUPO_NIVEL_SERVICO_CUSTO", "GRUPO_ESTOQUE_MIN_CUSTO", "GRUPO_ESTOQUE_MAX_CUSTO",
               "GRUPO_MARGEM_PCT"]


# ------------------------------------------------------------------
# Índices sazonais (gravados pela rodada completa)
# ------------------------------------------------------------------
def salvar_indices_sazonais(indices_saz):
    """Substitui com_indice_sazonal pelos índices da rodada. Falha só avisa."""
    try:
        with get_pg_engine().begin() as conn:
            conn.execute(text("DELETE FROM com_indice_sazonal"))
            for sgr, dados in (indices_saz or {}).items():
                conn.execute(text("INSERT INTO com_indice_sazonal (sgr_codigo, dados) VALUES (:s, :d)"),
                             {"s": int(sgr), "d": json.dumps(dados)})
        print(f"  - Índices sazonais gravados: {len(indices_saz or {})} subgrupos.")
    except Exception as e:
        print(f"  - AVISO: índices sazonais não gravados ({e}); análise de grupo fica sem sazonalidade.")


def carregar_indices_sazonais():
    try:
        with get_db_connection() as conn:
            rows = conn.execute(text("SELECT sgr_codigo, dados FROM com_indice_sazonal")).fetchall()
    except Exception as e:
        print(f"AVISO: com_indice_sazonal indisponível ({e})")
        return {}
    return {int(s): (d if isinstance(d, dict) else json.loads(d)) for s, d in rows}


# ------------------------------------------------------------------
# ERP (OPENQUERY) — mesmas consultas de main.carregar_dados_do_banco, filtradas
# ------------------------------------------------------------------
def _lit(v):
    return "'" + str(v).replace("'", "''") + "'"


def _in(valores):
    return ", ".join(_lit(v) for v in sorted(valores))


def _openquery(conn, inner):
    df = pd.read_sql(f"SELECT * FROM OPENQUERY(CONSULTA, '{inner.replace(chr(39), chr(39) * 2)}')", conn)
    df.columns = [str(c).upper() for c in df.columns]
    return df


def _ler_produtos(conn, codigos, descricoes):
    cond = [f"pro.pro_codigo IN ({_in(codigos)})"] if codigos else []
    if descricoes:
        cond.append(f"pro.pro_descricao IN ({_in(descricoes)})")
    df = _openquery(conn, f"""
        SELECT pro.pro_codigo, pro.pro_descricao, pro.subgrp_codigo AS sgr_codigo,
               pro.estoque_disponivel, mar.mar_descricao, f1.for_nome AS fornecedor1
        FROM produtos pro
        LEFT JOIN marcas mar ON mar.empresa = pro.empresa AND mar.mar_codigo = pro.mar_codigo
        LEFT JOIN fornecedores f1 ON f1.empresa = pro.empresa AND f1.for_codigo = pro.for_codigo
        WHERE pro.empresa = 3
          AND UPPER(pro.inativo) = 'N'
          AND UPPER(pro.comercializavel) = 'S'
          AND ({' OR '.join(cond)})""")
    df["PRO_CODIGO"] = df["PRO_CODIGO"].astype(str).str.strip()
    return df


def _ler_movimento(conn, codigos, ini):
    """Saídas válidas (sem cancelamento, menos devolução) e venda perdida dos membros."""
    ini_s = ini.strftime("%Y-%m-%d")
    sai = _openquery(conn, f"""
        SELECT LE.pro_codigo, LE.nfs, LE.preco_custo, LE.total_liquido, LE.data, LE.quantidade
        FROM lanctos_estoque LE
        WHERE LE.data >= '{ini_s}'
          AND LE.empresa = 3
          AND LE.origem IN ('NFS','EVF','EFD')
          AND LE.pro_codigo IN ({_in(codigos)})
          AND NOT EXISTS (SELECT 1 FROM lanctos_estoque C
                          WHERE C.empresa = LE.empresa AND C.nfs = LE.nfs
                            AND C.origem = 'CNS' AND C.data >= '{ini_s}')""")
    dev = _openquery(conn, f"""
        SELECT nfsi.nfs, nfsi.pro_codigo, nfsi.qtde_devolvida
        FROM nfs_itens nfsi
        WHERE nfsi.qtde_devolvida > 0 AND nfsi.empresa = 3
          AND nfsi.pro_codigo IN ({_in(codigos)})""")
    try:
        vp = _openquery(conn, f"""
            SELECT vp.pro_codigo, vp.quantidade, vp.data FROM venda_perdida vp
            WHERE vp.empresa = 3 AND vp.quantidade > 0 AND vp.pro_codigo IN ({_in(codigos)})""")
    except Exception as e:
        print(f"AVISO: VENDA_PERDIDA indisponível ({e})")
        vp = pd.DataFrame(columns=["PRO_CODIGO", "QUANTIDADE", "DATA"])
    try:
        vpi = pd.read_sql(text("SELECT pro_codigo, quantidade, created_at::date AS data FROM ven_venda_perdida "
                               "WHERE quantidade > 0 AND pro_codigo::text = ANY(:c)"),
                          get_pg_engine(), params={"c": list(codigos)})
        vpi.columns = [str(c).upper() for c in vpi.columns]
        vp = pd.concat([vp, vpi], ignore_index=True)
    except Exception as e:
        print(f"AVISO: venda perdida da intranet indisponível ({e})")

    # Mesma limpeza do _executar_job
    for df in (sai, dev, vp):
        if "PRO_CODIGO" in df.columns:
            df["PRO_CODIGO"] = df["PRO_CODIGO"].astype(str).str.strip()
    sai["QUANTIDADE"] = pd.to_numeric(sai["QUANTIDADE"], errors="coerce")
    sai["DATA"] = pd.to_datetime(sai["DATA"], errors="coerce")
    if not dev.empty:
        dev["QTDE_DEVOLVIDA"] = pd.to_numeric(dev["QTDE_DEVOLVIDA"], errors="coerce")
        dev_agg = dev.groupby(["NFS", "PRO_CODIGO"], as_index=False)["QTDE_DEVOLVIDA"].sum()
        sai = sai.merge(dev_agg, on=["NFS", "PRO_CODIGO"], how="left")
        sai["QTDE_DEVOLVIDA"] = sai["QTDE_DEVOLVIDA"].fillna(0)
    else:
        sai["QTDE_DEVOLVIDA"] = 0
    sai["QUANTIDADE_AJUSTADA"] = sai["QUANTIDADE"] - sai["QTDE_DEVOLVIDA"]
    return sai[sai["QUANTIDADE_AJUSTADA"] > 0].copy(), vp


# ------------------------------------------------------------------
# Membros do grupo
# ------------------------------------------------------------------
def fechar_membros(codigos, vinculos, ler_produtos):
    """Cadastro de tudo que pode cair no(s) grupo(s) dos `codigos`: fecha por
    descrição (mesma chave) e por vínculo manual até não crescer mais.
    `ler_produtos(codigos, descricoes)` -> DataFrame com PRO_CODIGO/PRO_DESCRICAO."""
    cods, descs = set(codigos), set()
    while True:
        sp = ler_produtos(cods, descs)
        carregados = set(sp["PRO_CODIGO"])
        novos_descs = {d for d in sp["PRO_DESCRICAO"].dropna() if str(d).strip()} - descs
        novos_cods = {p for v in vinculos if carregados.intersection(v) for p in v} - cods
        if not novos_descs and not novos_cods:
            return sp, [v for v in vinculos if carregados.intersection(v)]
        descs |= novos_descs
        cods |= novos_cods
        if len(cods) + len(descs) > MAX_PRODUTOS:
            raise ValueError(f"Grupo grande demais para análise parcial (> {MAX_PRODUTOS} produtos).")


def _sem_nan(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v.item() if hasattr(v, "item") else v


# ------------------------------------------------------------------
# Entrada
# ------------------------------------------------------------------
def analisar_grupo(pro_codigos):
    """Recalcula o consolidado do(s) grupo(s) dos produtos e grava no snapshot atual."""
    import main  # lazy: carrega pandas/ODBC e o motor

    codigos = {str(c).strip() for c in pro_codigos if str(c).strip()}
    invalidos = [c for c in codigos if not _COD_OK.match(c)]
    if invalidos:
        raise ValueError(f"Código(s) inválido(s): {', '.join(sorted(invalidos))}")
    if not codigos:
        raise ValueError("Lista de produtos vazia")

    avisos = []
    hoje = pd.Timestamp.today().normalize()
    mapa_linha = main.carregar_marca_linha()
    vinculos = [[str(p).strip() for p in v] for v in carregar_vinculos_manuais(get_pg_engine())]

    sql_conn = get_sql_connection()
    try:
        sp, vinc_rel = fechar_membros(codigos, vinculos, lambda c, d: _ler_produtos(sql_conn, c, d))

        # Chaves como na rodada: original fica fora; vínculo manual une as chaves.
        sp["IS_ORIG"] = sp["MAR_DESCRICAO"].apply(main.marca_eh_original)
        sp["GRUPO"] = [main.grupo_chave_com_linha(d, m, mapa_linha) or ""
                       for d, m in zip(sp["PRO_DESCRICAO"], sp["MAR_DESCRICAO"])]
        nao = sp[(~sp["IS_ORIG"]) & (sp["GRUPO"] != "")]
        remap = unificar_chaves(dict(zip(nao["PRO_CODIGO"], nao["GRUPO"])), vinc_rel)
        chave_final = {p: remap.get(k, k) for p, k in zip(nao["PRO_CODIGO"], nao["GRUPO"])}
        alvos = {chave_final[c] for c in codigos if c in chave_final}
        fora = sorted(codigos - set(chave_final))
        if fora:
            avisos.append(f"Sem grupo (original, inativo ou sem descrição): {', '.join(fora)}")
        if not alvos:
            return {"status": "success", "grupos": [], "avisos": avisos}

        membros = sorted(p for p, k in chave_final.items() if k in alvos)
        ini = (hoje - pd.DateOffset(months=main.JANELA_DEMANDA_MESES)).normalize().replace(day=1)
        df_sai, df_vp = _ler_movimento(sql_conn, membros, ini)
    finally:
        sql_conn.close()

    with get_db_connection() as pg:
        snap = pd.DataFrame(pg.execute(text(
            f"SELECT pro_codigo, grupo_chave, custo_unitario, data_max_venda FROM com_fifo_completo "
            f"WHERE {_SNAPSHOT} AND pro_codigo = ANY(:c)"), {"c": membros}).mappings().all())
        if snap.empty:
            snap = pd.DataFrame(columns=["pro_codigo", "grupo_chave", "custo_unitario", "data_max_venda"])
        snap["pro_codigo"] = snap["pro_codigo"].astype(str).str.strip()
        # Chave gravada na última rodada, por grupo alvo (sai da curva base e é re-etiquetada)
        antigas_por_grupo = {gk: sorted({gk} | set(snap.loc[snap["pro_codigo"].map(chave_final) == gk,
                                                              "grupo_chave"].dropna()))
                             for gk in alvos}
        chaves_antigas = sorted({k for ks in antigas_por_grupo.values() for k in ks})
        base = [float(r[0] or 0) for r in pg.execute(text(
            f"SELECT SUM(COALESCE(valor_vendido_12m, 0)) FROM com_fifo_completo "
            f"WHERE {_SNAPSHOT} AND grupo_chave IS NOT NULL AND NOT (grupo_chave = ANY(:k)) "
            f"GROUP BY grupo_chave"), {"k": chaves_antigas}).fetchall()]

    indices_saz = carregar_indices_sazonais()
    if not indices_saz:
        avisos.append("Sazonalidade indisponível: rode a análise completa uma vez após aplicar o SQL.")
    custo = pd.to_numeric(snap.set_index("pro_codigo")["custo_unitario"], errors="coerce").dropna()
    data_max = snap.set_index("pro_codigo")["data_max_venda"]

    g = main.calcular_grupos_descricao(
        df_sai, df_vp, sp[sp["PRO_CODIGO"].isin(membros)].drop(columns=["IS_ORIG", "GRUPO"]),
        hoje, indices_saz,
        params_forn=main.carregar_parametros_fornecedor(),
        mapa_compras=main.carregar_mapa_compras_fornecedor(),
        mapa_linha=mapa_linha, custo_por_produto=custo, data_max_por_produto=data_max,
        remap_chaves=remap, valores_base=base)
    if g is None or g.empty:
        avisos.append("Grupo sem venda na janela: nada a consolidar.")
        return {"status": "success", "grupos": [], "avisos": avisos}

    sets = ", ".join(f"{c.lower()} = :{c.lower()}" for c in _COLS_GRUPO)
    resumo = []
    with get_db_connection() as pg:
        trans = pg.begin()
        for _, r in g[g["GRUPO"].isin(alvos)].iterrows():
            gk = r["GRUPO"]
            cods_g = [p for p in membros if chave_final[p] == gk]
            params = {c.lower(): _sem_nan(r.get(c)) for c in _COLS_GRUPO}
            params.update({"gk": gk, "cods": cods_g, "antigas": antigas_por_grupo[gk]})
            # Linhas do snapshot com chave antiga e fora do cadastro ativo vão junto:
            # senão sobrariam como um grupo fantasma com números velhos.
            pg.execute(text(
                f"UPDATE com_fifo_completo SET grupo_chave = :gk, {sets} "
                f"WHERE {_SNAPSHOT} AND (pro_codigo = ANY(:cods) OR grupo_chave = ANY(:antigas))"),
                params)
            resumo.append({"grupo_chave": gk, "qtd_itens": params["grupo_qtd_itens"],
                           "curva": params["grupo_curva"], "estoque_min": params["grupo_estoque_min"],
                           "estoque_max": params["grupo_estoque_max"], "membros": cods_g})
        trans.commit()
    return {"status": "success", "grupos": resumo, "avisos": avisos}
