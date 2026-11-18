# -*- coding: utf-8 -*-
"""Lista de promoção por valor parado: /promo/plan, /promo/export, /promo/acao/{pro_codigo},
e o lote semanal de sugestões de oferta do atacado: /promo/lote, /promo/lote/recusar,
/promo/lote/desfazer-recusa e /promo/lote/acompanhamento; e os itens pequenos (abaixo do mínimo do
lote): /promo/pequenos (ranking por família ou marca) e /promo/plan com `pequeno_valor`.

Lote semanal (com_promo_lote): montado e gravado na primeira abertura da semana (segunda-feira,
fuso de Cuiabá) a partir da visão agrupada, sem os produtos já numa oferta não encerrada nem os
recusados nas últimas semanas (promo_regra.montar_lote / corte_recusa). Depois de gravado não é
recomposto: a recusa só marca o item.

A regra (ação, público, preço) mora em promo_regra.py; aqui só se lê o com_fifo_completo,
aplica filtros/paginação/totais e grava a troca manual e as campanhas exportadas.
Nada é gravado no ERP — a carga da promoção continua no Celta.

Duas visões (`grouped_view`): individual (cada produto contra o próprio máximo) e agrupada
(o grupo unificado — marcas substitutas — contra o máximo, a cobertura e a curva do grupo;
ver promo_regra.avaliar). Produto sem grupo é avaliado sozinho nas duas.
"""
import io
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from config import FORN_EXPR
from estoque_rt import _get_stock_batches
from fornecedor_info import _expandir_fornecedores
from infra_db import get_db_connection
from analise_nota import hoje_cuiaba
from modelos import PromoAcaoRequest, PromoLoteRecusaRequest, PromoPlanRequest
from promo_regra import ACOES, REGUA_PADRAO, VALIDADE_DIAS, avaliar, corte_recusa, montar_lote, pequenos, segunda
from rotas_analise import _filtro_busca

router = APIRouter()

ORDENACOES = {"valor_parado", "valor_excesso", "cobertura_meses", "dias_sem_venda", "caixa_potencial", "pro_descricao", "valor_vender"}


_SNAPSHOT = "data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)"
# Grupo unificado de verdade: mais de uma marca e consolidado calculado (grupo com venda).
_EM_GRUPO = "(f.grupo_chave IS NOT NULL AND COALESCE(f.grupo_qtd_itens,0) > 1 AND f.grupo_estoque_max IS NOT NULL)"
_CAMPOS_GRUPO = ("grupo_chave", "grupo_qtd_itens", "grupo_curva", "grupo_estoque_disponivel", "grupo_estoque_max",
                 "grupo_data_max_venda", "grupo_dias_sem_venda", "grupo_idade_saldo_dias")
_COLS_GRUPO_SQL = f""",
               {_EM_GRUPO} AS em_grupo, f.grupo_chave, f.grupo_qtd_itens, f.grupo_curva, f.grupo_estoque_disponivel,
               CEIL(f.grupo_estoque_max) AS grupo_estoque_max, f.grupo_demanda_dia, g.grupo_data_max_venda,
               (CURRENT_DATE - g.grupo_data_max_venda::date) AS grupo_dias_sem_venda, g.grupo_idade_saldo_dias"""
# Última venda do grupo = a mais recente entre TODAS as marcas (inclusive as sem saldo);
# idade do saldo = média das marcas ponderada pelo saldo.
_JOIN_GRUPO_SQL = f"""
        LEFT JOIN (SELECT grupo_chave AS gk, MAX(data_max_venda) AS grupo_data_max_venda,
                          SUM(tempo_medio_saldo_atual * estoque_disponivel)
                              FILTER (WHERE estoque_disponivel > 0 AND tempo_medio_saldo_atual IS NOT NULL)
                          / NULLIF(SUM(estoque_disponivel)
                              FILTER (WHERE estoque_disponivel > 0 AND tempo_medio_saldo_atual IS NOT NULL), 0) AS grupo_idade_saldo_dias
                   FROM com_fifo_completo WHERE {_SNAPSHOT} AND grupo_chave IS NOT NULL
                   GROUP BY grupo_chave) g ON g.gk = f.grupo_chave"""


def _tabela_existe(conn, nome):
    return conn.execute(text("SELECT to_regclass(:n)"), {"n": nome}).scalar() is not None


def _regua(conn):
    """Régua v3 do atacado (mesmo Postgres do vendas-service); seed embutido como plano B."""
    if not _tabela_existe(conn, "ven_regua_atacado"):
        return REGUA_PADRAO
    rows = conn.execute(text("SELECT classe, faixa, markup, desc_max FROM ven_regua_atacado WHERE ativo")).fetchall()
    return {(r[0], r[1]): (float(r[2]), float(r[3])) for r in rows} or REGUA_PADRAO


def _linhas(conn, req: PromoPlanRequest):
    """Todas as linhas candidatas (excesso > 0, sem custo ou na base do parado há 240 d —
    fin_plano_parado_base, quando existe) já avaliadas pela regra.
    Na visão agrupada o excesso que conta é o do grupo: entra a marca com saldo de um grupo
    acima do máximo, mesmo dentro do próprio máximo, e sai a que está acima do próprio máximo
    num grupo que, somado, não tem excesso."""
    excesso_item = "f.estoque_disponivel > CEIL(COALESCE(f.estoque_max_sugerido,0))"
    excesso = (f"(CASE WHEN {_EM_GRUPO} THEN COALESCE(f.grupo_estoque_disponivel,0) > CEIL(f.grupo_estoque_max) "
               f"ELSE {excesso_item} END)") if req.grouped_view else excesso_item
    tem_base = _tabela_existe(conn, "fin_plano_parado_base")
    filters = [f"f.{_SNAPSHOT}",
               "f.estoque_disponivel > 0",
               f"({excesso} OR COALESCE(f.custo_unitario,0) <= 0{' OR b.pro_codigo IS NOT NULL' if tem_base else ''})"]
    params = {}
    for campo, coluna, valores in (("sg", "sgr_descricao", req.subgroups), ("br", "mar_descricao", req.brands),
                                   ("cat", "categoria_saldo_atual", req.categories)):
        vals = [v.strip() for v in (valores or []) if v and v.strip()]
        if vals:
            keys = [f"{campo}_{i}" for i in range(len(vals))]
            params.update(zip(keys, vals))
            filters.append(f"f.{coluna} IN ({','.join(':' + k for k in keys)})")
    cl, pr = _filtro_busca(req.search, req.match_type, req.pro_codigos, "f.")
    filters.extend(cl)
    params.update(pr)
    # Fornecedor com os relacionados (matriz/filiais), mesma expressão do /analise
    nomes_f = [n for n in (req.fornecedores or []) if n and n.strip()]
    nomes_f = _expandir_fornecedores(nomes_f) if nomes_f else []
    if nomes_f:
        fkeys = {f"forn_{i}": n for i, n in enumerate(nomes_f)}
        params.update(fkeys)
        forn = FORN_EXPR.replace("fornecedor", "f.fornecedor")
        filters.append(f"UPPER(TRIM({forn})) IN ({', '.join(':' + k for k in fkeys)})")

    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name='com_fifo_completo'"))}
    canal = "f.qtd_varejo_12m, f.qtd_atacado_12m" if "qtd_varejo_12m" in cols else "NULL AS qtd_varejo_12m, NULL AS qtd_atacado_12m"
    tem_exc = _tabela_existe(conn, "ven_regua_item_excecao")
    tem_acao = _tabela_existe(conn, "com_promo_acao")
    sql = f"""
        SELECT f.id, f.pro_codigo, f.pro_descricao, f.pro_referencia, f.sgr_codigo, f.sgr_descricao,
               f.mar_descricao, f.fornecedor1, f.curva_abc, f.categoria_saldo_atual, f.tempo_medio_saldo_atual, f.group_id,
               f.estoque_disponivel, CEIL(COALESCE(f.estoque_max_sugerido,0)) AS estoque_max_sugerido,
               f.custo_unitario, f.custo_fonte, f.preco_venda_1, f.preco_venda_2, f.demanda_real_dia,
               f.data_max_venda, (CURRENT_DATE - f.data_max_venda::date) AS dias_sem_venda, {canal},
               {"(e.pro_codigo IS NOT NULL)" if tem_exc else "FALSE"} AS fora_regua,
               {"a.acao, a.preco_varejo AS preco_varejo_manual, a.preco_atacado AS preco_atacado_manual, a.observacao AS observacao_manual"
                if tem_acao else "NULL AS acao, NULL AS preco_varejo_manual, NULL AS preco_atacado_manual, NULL AS observacao_manual"},
               {"b.quantidade AS base_qtd, b.custo_unitario AS base_custo" if tem_base else "NULL AS base_qtd, NULL AS base_custo"}
               {_COLS_GRUPO_SQL if req.grouped_view else ""}
        FROM com_fifo_completo f
        {_JOIN_GRUPO_SQL if req.grouped_view else ""}
        {"LEFT JOIN ven_regua_item_excecao e ON e.pro_codigo::text = f.pro_codigo" if tem_exc else ""}
        {"LEFT JOIN com_promo_acao a ON a.pro_codigo = f.pro_codigo" if tem_acao else ""}
        {"LEFT JOIN fin_plano_parado_base b ON b.pro_codigo::text = f.pro_codigo" if tem_base else ""}
        WHERE {' AND '.join(filters)}
    """
    regua = _regua(conn)
    out = []
    for r in conn.execute(text(sql), params).mappings():
        # Decimal/date do Postgres → float/str: a regra faz conta e a resposta vira JSON
        d = {k: (float(v) if isinstance(v, Decimal) else str(v) if isinstance(v, (date, datetime)) else v)
             for k, v in dict(r).items()}
        d["acao_manual"] = d.pop("acao", None)
        grupo = None
        if req.grouped_view:
            if d.pop("em_grupo"):
                grupo = {"saldo": d["grupo_estoque_disponivel"], "maximo": d["grupo_estoque_max"],
                         "demanda_dia": d.pop("grupo_demanda_dia"), "curva": d["grupo_curva"],
                         "dias_sem_venda": d["grupo_dias_sem_venda"], "idade_saldo_dias": d["grupo_idade_saldo_dias"]}
            else:   # sem grupo de verdade (uma marca só ou grupo sem venda): avaliado sozinho
                d.pop("grupo_demanda_dia", None)
                d.update(dict.fromkeys(_CAMPOS_GRUPO))
        out.append(avaliar(d, regua, grupo))

    if req.so_parado:
        out = [x for x in out if x["na_base"]]
    if req.acoes:
        out = [x for x in out if x["acao"] in req.acoes]
    if req.publicos:
        out = [x for x in out if x["publico"] in req.publicos]
    if req.curvas:   # na visão agrupada, a curva que filtra é a do grupo (como na aba Análise)
        out = [x for x in out if (x.get("grupo_curva") or x.get("curva_abc") or "") in req.curvas]
    return out


def _agrupar(linhas):
    """Visão agrupada: uma linha por grupo unificado, com as marcas em `membros`, e uma por
    produto sem grupo. O cabeçalho leva os números do grupo e a soma dos valores das marcas."""
    out, grupos = [], {}
    for x in linhas:
        gk = x.get("grupo_chave")
        if not gk:
            out.append(x)
            continue
        g = grupos.get(gk)
        if g is None:
            g = grupos[gk] = {
                "is_grupo": True, "id": -x["id"], "pro_codigo": "", "pro_descricao": gk, "membros": [],
                "sgr_descricao": x.get("sgr_descricao"), "curva_abc": x.get("grupo_curva"),
                "estoque_disponivel": x.get("grupo_estoque_disponivel"), "estoque_max_sugerido": x.get("grupo_estoque_max"),
                "excesso_qtd": x.get("grupo_excesso_qtd"), "cobertura_meses": x.get("grupo_cobertura_meses"),
                "data_max_venda": x.get("grupo_data_max_venda"), "dias_sem_venda": x.get("grupo_dias_sem_venda"),
                "idade_saldo_dias": x.get("grupo_idade_saldo_dias"), **{c: x.get(c) for c in _CAMPOS_GRUPO},
            }
            out.append(g)
        g["membros"].append(x)
    for g in grupos.values():
        m = g["membros"]
        m.sort(key=lambda x: -x["valor_parado"])
        for campo in ("valor_parado", "valor_excesso", "caixa_potencial", "valor_vender", "valor_parado_240", "vender_qtd"):
            g[campo] = round(sum(x[campo] for x in m), 2)
        g["na_base"] = any(x["na_base"] for x in m)
        g["acoes"] = sorted({x["acao"] for x in m}, key=ACOES.index)
        g["acao"] = max(g["acoes"], key=[x["acao"] for x in m].count)
    return out


def _totais(linhas):
    valor = sum(x["valor_parado"] for x in linhas)
    custo_dem_mes = sum((x.get("demanda_real_dia") or 0) * 30.0 * (x.get("custo_unitario") or 0) for x in linhas)
    por_acao = {}
    for x in linhas:
        p = por_acao.setdefault(x["acao"], {"itens": 0, "valor_parado": 0.0, "valor_excesso": 0.0, "caixa_potencial": 0.0})
        p["itens"] += 1
        p["valor_parado"] += x["valor_parado"]
        p["valor_excesso"] += x["valor_excesso"]
        p["caixa_potencial"] += x["caixa_potencial"]
    return {
        "itens": len(linhas),
        "valor_parado": round(valor, 2),
        "valor_excesso": round(sum(x["valor_excesso"] for x in linhas), 2),
        "caixa_potencial": round(sum(x["caixa_potencial"] for x in linhas), 2),
        "valor_vender": round(sum(x["valor_vender"] for x in linhas), 2),
        "parado_240": {"itens": sum(1 for x in linhas if x["na_base"]),
                       "valor": round(sum(x["valor_parado_240"] for x in linhas), 2)},
        "meses_para_zerar": round(valor / custo_dem_mes, 1) if custo_dem_mes > 0 else None,
        "bonus_liquidacao": round(sum(x["vender_qtd"] * (x.get("bonus_liquidacao_unit") or 0)
                                      for x in linhas if x["acao"] == "giro_caixa" and x["publico"] != "varejo"), 2),
        "por_acao": {k: {kk: (round(vv, 2) if isinstance(vv, float) else vv) for kk, vv in v.items()} for k, v in por_acao.items()},
    }


@router.post("/promo/plan")
def planejar_promocao(req: PromoPlanRequest):
    conn = get_db_connection()
    try:
        if req.pequeno_valor:   # lista plana dos pequenos, avaliados na visão agrupada
            req.grouped_view = True
            itens = pequenos(_linhas(conn, req), _fora_da_sugestao(conn, segunda(hoje_cuiaba())))
            linhas = itens
        else:
            itens = _linhas(conn, req)
            linhas = _agrupar(itens) if req.grouped_view else itens
        chave = req.sort if req.sort in ORDENACOES else "valor_parado"
        reverso = (req.sort_dir or "desc").lower() != "asc"
        linhas.sort(key=lambda x: ((x.get(chave) is None), x.get(chave) if x.get(chave) is not None else 0), reverse=reverso)
        if reverso:  # None sempre por último, mesmo em ordem decrescente
            linhas.sort(key=lambda x: x.get(chave) is None)
        total = len(linhas)
        size = max(1, min(req.page_size or 100, 1000))
        page = max(1, req.page or 1)
        pagina = linhas[(page - 1) * size: page * size]
        produtos = [m for x in pagina for m in (x.get("membros") or [x])]
        lotes = _get_stock_batches([x["pro_codigo"] for x in produtos]) if produtos else {}
        for x in produtos:
            b = lotes.get(x["pro_codigo"], [])
            x["lotes_estoque"] = [l.model_dump() if hasattr(l, "model_dump") else l.dict() for l in b]
            x["estoque_obsoleto"] = sum(l.qtd for l in b if l.dias_em_estoque > 240)
        return {"items": pagina, "total": total, "page": page, "page_size": size,
                "total_pages": (total + size - 1) // size, "totais": _totais(itens)}
    except Exception as e:
        print(f"Erro promo plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


COLS_EXPORT = {
    "pro_codigo": "Código", "pro_descricao": "Descrição", "mar_descricao": "Marca", "sgr_descricao": "Subgrupo",
    "fornecedor1": "Fornecedor", "curva_abc": "Curva ABC",
    # só na visão agrupada (na individual as chaves não existem e as colunas não saem)
    "grupo_chave": "Grupo", "grupo_curva": "Curva do grupo", "grupo_qtd_itens": "Marcas no grupo",
    "grupo_estoque_disponivel": "Saldo do grupo", "grupo_estoque_max": "Máximo do grupo",
    "grupo_excesso_qtd": "Excesso do grupo (un)", "grupo_cobertura_meses": "Cobertura do grupo (meses)",
    "categoria_saldo_atual": "Tempo em estoque",
    "estoque_disponivel": "Saldo", "estoque_max_sugerido": "Máximo sugerido", "excesso_qtd": "Excesso (un)",
    "parado_qtd": "Parado +240 d (qtd)", "vender_qtd": "A vender (qtd)",
    "custo_unitario": "Custo (R$)", "valor_parado": "Valor parado (R$)", "valor_excesso": "Valor em excesso (R$)",
    "cobertura_meses": "Cobertura (meses)", "idade_saldo_dias": "Idade média do saldo (dias)", "data_max_venda": "Última venda", "dias_sem_venda": "Dias sem venda",
    "publico": "Público", "acao": "Ação", "acao_sugerida": "Ação sugerida", "acao_manual": "Ação manual?",
    "preco_venda_1": "Tabela 1 (R$)", "preco_varejo": "Preço promocional balcão (R$)", "desc_varejo_pct": "Desc. balcão (%)",
    "motivo_varejo": "Obs. balcão", "piso_varejo": "Piso balcão (R$)", "preco_venda_2": "Tabela 2 (R$)", "preco_atacado": "Preço promocional atacado (R$)",
    "desc_atacado_pct": "Desc. atacado (%)", "motivo_atacado": "Obs. atacado", "piso_atacado": "Piso atacado (R$)", "bonus_liquidacao_unit": "Bônus liquidação/un (R$)",
    "caixa_potencial": "Caixa potencial (R$)", "observacao_manual": "Observação",
}
TIPO_CAMPANHA = {"promocao": "promocao", "giro_caixa": "liquidacao"}
ACAO_ROTULO = {"giro_caixa": "Liquidação", "promocao": "Promoção", "vender_sem_repor": "Vender sem repor", "revisar_cadastro": "Revisar cadastro"}


def _registrar_campanhas(conn, carga, inicio, fim, usuario):
    """Guarda a campanha exportada: é por aqui que a bolsa e a comissão do vendedor sabem
    que a venda foi de promoção ou liquidação (cartão 'Bolsa e bônus do vendedor')."""
    if not _tabela_existe(conn, "com_promo_campanha") or carga.empty:
        return 0
    sql = text("""
        INSERT INTO com_promo_campanha (pro_codigo, tipo, preco_varejo, preco_atacado, inicio, fim, usuario)
        VALUES (:pro, :tipo, :pv, :pa, :ini, :fim, :usr)
        ON CONFLICT (pro_codigo, inicio) DO UPDATE SET tipo = EXCLUDED.tipo, preco_varejo = EXCLUDED.preco_varejo,
            preco_atacado = EXCLUDED.preco_atacado, fim = EXCLUDED.fim, usuario = EXCLUDED.usuario, criado_em = NOW()""")
    for _, r in carga.iterrows():
        conn.execute(sql, {"pro": r["PRO_CODIGO"], "tipo": r["TIPO"], "pv": r["PROM_VALOR"] or None,
                           "pa": r["PROM_VALOR2"] or None, "ini": inicio, "fim": fim, "usr": usuario})
    conn.commit()  # SQLAlchemy 2.x: a conexão já está em transação (autobegin) após os SELECTs
    return len(carga)


@router.post("/promo/export")
def exportar_promocao(req: PromoPlanRequest):
    conn = get_db_connection()
    try:
        linhas = _linhas(conn, req)
        if not linhas:
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado para exportação")
        if req.grouped_view:   # marcas do mesmo grupo juntas, grupos pelo valor parado somado
            soma = {}
            for x in linhas:
                if x.get("grupo_chave"):
                    soma[x["grupo_chave"]] = soma.get(x["grupo_chave"], 0.0) + x["valor_parado"]
            linhas.sort(key=lambda x: (-soma.get(x.get("grupo_chave"), x["valor_parado"]),
                                       x.get("grupo_chave") or "", -x["valor_parado"]))
        else:
            linhas.sort(key=lambda x: -x["valor_parado"])
        df = pd.DataFrame(linhas)
        df_lista = df[[c for c in COLS_EXPORT if c in df.columns]].rename(columns=COLS_EXPORT)
        for col in ("Ação", "Ação sugerida"):   # rótulo legível, igual ao da tela
            if col in df_lista.columns:
                df_lista[col] = df_lista[col].map(lambda v: ACAO_ROTULO.get(v, v))

        inicio, fim = date.today(), date.today() + timedelta(days=VALIDADE_DIAS)
        carga = df[df["acao"].isin(TIPO_CAMPANHA) & (df["preco_varejo"].notna() | df["preco_atacado"].notna())].copy()
        carga["PRO_CODIGO"] = carga["pro_codigo"]
        carga["PROM_VALOR"] = [r.preco_varejo if r.publico in ("varejo", "ambos") else None for r in carga.itertuples()]
        carga["PROM_VALOR2"] = [r.preco_atacado if r.publico in ("atacado", "ambos") else None for r in carga.itertuples()]
        carga = carga[carga["PROM_VALOR"].notna() | carga["PROM_VALOR2"].notna()].copy()
        carga["TIPO"] = carga["acao"].map(TIPO_CAMPANHA)
        carga["DATA_INICIAL"], carga["DATA_FINAL"] = inicio.strftime("%d/%m/%Y"), fim.strftime("%d/%m/%Y")
        df_carga = carga[["PRO_CODIGO", "pro_descricao", "TIPO", "PROM_VALOR", "PROM_VALOR2", "DATA_INICIAL", "DATA_FINAL"]] \
            .rename(columns={"pro_descricao": "DESCRICAO"})
        try:
            n = _registrar_campanhas(conn, carga, inicio, fim, req.usuario)
            print(f"promo export: {n} campanhas registradas em com_promo_campanha")
        except Exception as e:  # a planilha sai mesmo se o registro falhar
            print(f"AVISO promo export: campanhas não registradas ({e})")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_lista.to_excel(writer, index=False, sheet_name="Promocao")
            df_carga.to_excel(writer, index=False, sheet_name="Carga ERP")
        output.seek(0)
        return StreamingResponse(output, headers={"Content-Disposition": 'attachment; filename="lista_promocao.xlsx"'},
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Erro export promo: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.put("/promo/acao/{pro_codigo}")
def definir_acao(pro_codigo: str, req: PromoAcaoRequest):
    if req.acao not in ACOES:
        raise HTTPException(status_code=400, detail=f"acao deve ser uma de {ACOES}")
    conn = get_db_connection()
    try:
        if not _tabela_existe(conn, "com_promo_acao"):
            raise HTTPException(status_code=503, detail="Tabela com_promo_acao não existe (aplicar sql/2026-09-16_promocao_valor_parado.sql)")
        conn.execute(text("""
                INSERT INTO com_promo_acao (pro_codigo, acao, preco_varejo, preco_atacado, observacao, usuario, atualizado_em)
                VALUES (:pro, :acao, :pv, :pa, :obs, :usr, NOW())
                ON CONFLICT (pro_codigo) DO UPDATE SET acao = EXCLUDED.acao, preco_varejo = EXCLUDED.preco_varejo,
                    preco_atacado = EXCLUDED.preco_atacado, observacao = EXCLUDED.observacao, usuario = EXCLUDED.usuario,
                    atualizado_em = NOW()"""),
            {"pro": pro_codigo, "acao": req.acao, "pv": req.preco_varejo, "pa": req.preco_atacado,
             "obs": req.observacao, "usr": req.usuario})
        conn.commit()
        return {"ok": True, "pro_codigo": pro_codigo, "acao": req.acao}
    finally:
        conn.close()


@router.delete("/promo/acao/{pro_codigo}")
def limpar_acao(pro_codigo: str):
    conn = get_db_connection()
    try:
        if _tabela_existe(conn, "com_promo_acao"):
            conn.execute(text("DELETE FROM com_promo_acao WHERE pro_codigo = :pro"), {"pro": pro_codigo})
            conn.commit()
        return {"ok": True, "pro_codigo": pro_codigo}
    finally:
        conn.close()


_SEM_LOTE = "Tabela com_promo_lote não existe (aplicar sql/2026-11-17_promo_lote_semanal.sql)"
_COLS_LOTE = ("pro_codigo", "pro_descricao", "posicao", "acao", "sgr_descricao", "mar_descricao",
              "vender_qtd", "valor_vender", "preco_atacado")


def _fora_da_sugestao(conn, semana):
    """Produtos que não entram no lote: já numa oferta não encerrada (ven_oferta_item, do
    vendas-service, quando existe) ou recusados num lote desde corte_recusa(semana)."""
    fora = set()
    if _tabela_existe(conn, "ven_oferta_item"):
        fora |= {r[0] for r in conn.execute(text(
            "SELECT DISTINCT pro_codigo::text FROM ven_oferta_item WHERE status <> 'ENCERRADO'"))}
    fora |= {r[0] for r in conn.execute(text(
        "SELECT DISTINCT pro_codigo FROM com_promo_lote WHERE recusado_em IS NOT NULL AND semana >= :corte"),
        {"corte": corte_recusa(semana)})}
    return fora


@router.get("/promo/pequenos")
def ranking_pequenos(por: str = "familia"):
    """Itens pequenos (mesma seleção do /promo/plan com pequeno_valor) somados por família
    (sgr_descricao) ou marca (mar_descricao), do maior custo a vender para o menor."""
    if por not in ("familia", "marca"):
        raise HTTPException(status_code=400, detail="por deve ser familia ou marca")
    campo = "sgr_descricao" if por == "familia" else "mar_descricao"
    conn = get_db_connection()
    try:
        itens = pequenos(_linhas(conn, PromoPlanRequest(grouped_view=True)),
                         _fora_da_sugestao(conn, segunda(hoje_cuiaba())))
    finally:
        conn.close()
    grupos = {}
    for x in itens:
        g = grupos.setdefault(x.get(campo), {"nome": x.get(campo), "itens": 0, "valor_vender": 0.0})
        g["itens"] += 1
        g["valor_vender"] += x["valor_vender"]
    linhas = sorted(({**g, "valor_vender": round(g["valor_vender"], 2)} for g in grupos.values()),
                    key=lambda g: -g["valor_vender"])
    return {"por": por, "total": {"itens": len(itens), "valor_vender": round(sum(x["valor_vender"] for x in itens), 2)},
            "linhas": linhas}


def _garantir_lote(conn, semana):
    """Monta e grava o lote da semana se ainda não existe. A trava consultiva serializa duas
    primeiras aberturas simultâneas; o ON CONFLICT garante um item por produto na semana."""
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('com_promo_lote'))"))
    if conn.execute(text("SELECT 1 FROM com_promo_lote WHERE semana = :s LIMIT 1"), {"s": semana}).scalar():
        conn.commit()   # solta a trava (é de transação)
        return
    lote = montar_lote(_linhas(conn, PromoPlanRequest(grouped_view=True)), _fora_da_sugestao(conn, semana))
    sql = text("""
        INSERT INTO com_promo_lote (semana, posicao, pro_codigo, pro_descricao, acao, sgr_descricao, mar_descricao,
                                    vender_qtd, valor_vender, preco_atacado)
        VALUES (:semana, :posicao, :pro_codigo, :pro_descricao, :acao, :sgr_descricao, :mar_descricao,
                :vender_qtd, :valor_vender, :preco_atacado)
        ON CONFLICT (semana, pro_codigo) DO NOTHING""")
    for r in lote:
        conn.execute(sql, {"semana": semana, **{c: r.get(c) for c in _COLS_LOTE}})
    conn.commit()


def _ofertas_do_lote(conn, semana_ini, semana_fim):
    """Itens do lote que viraram oferta: (semana, pro_codigo) -> grupo e vendido.

    ven_oferta_item não guarda data de criação; o vendas-service grava inicio = dia da inclusão,
    então o vínculo é o item de oferta do produto com inicio dentro da semana do lote (assim o
    mesmo item não conta em várias semanas). Vendido = linhas de orçamento FECHADO que levaram o
    preço da oferta (fora_promocao = false), a mesma regra das unidades vendidas do vendas-service.
    Vários itens do mesmo produto na semana: soma o vendido e fica o grupo do item de maior id.
    Sem as tabelas das ofertas devolve {}."""
    if not (_tabela_existe(conn, "ven_oferta_item") and _tabela_existe(conn, "ven_oferta")):
        return {}
    tem_venda = conn.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = 'ven_orcamento_item' AND column_name = 'oferta_item_id'")).scalar()
    venda = """LEFT JOIN (SELECT oi.oferta_item_id, SUM(oi.quantidade) AS qtd, SUM(oi.total) AS valor
                   FROM ven_orcamento_item oi JOIN ven_orcamento oc ON oc.id = oi.orcamento_id
                   WHERE oi.oferta_item_id IS NOT NULL AND oi.fora_promocao = FALSE AND oc.status = 'FECHADO'
                   GROUP BY oi.oferta_item_id) v ON v.oferta_item_id = i.id""" if tem_venda else ""
    vend = "COALESCE(v.qtd, 0), COALESCE(v.valor, 0)" if tem_venda else "0, 0"
    rows = conn.execute(text(f"""
        SELECT l.semana, l.pro_codigo, i.id, o.id, o.nome, {vend}
        FROM com_promo_lote l
        JOIN ven_oferta_item i ON i.pro_codigo::text = l.pro_codigo
                              AND i.inicio >= l.semana AND i.inicio < l.semana + 7
        JOIN ven_oferta o ON o.id = i.oferta_id
        {venda}
        WHERE l.semana BETWEEN :ini AND :fim
        ORDER BY i.id"""), {"ini": semana_ini, "fim": semana_fim})
    saida = {}
    for semana, pro, _item, oferta_id, oferta_nome, qtd, valor in rows:
        k = (str(semana), pro)
        ant = saida.get(k, {"vendido_qtd": 0.0, "vendido_valor": 0.0})
        saida[k] = {"oferta_id": oferta_id, "oferta_nome": oferta_nome,
                    "vendido_qtd": ant["vendido_qtd"] + float(qtd or 0),
                    "vendido_valor": ant["vendido_valor"] + float(valor or 0)}
    return saida


@router.get("/promo/lote")
def lote_semanal(semana: Optional[str] = None):
    corrente = segunda(hoje_cuiaba())
    try:
        alvo = segunda(date.fromisoformat(semana)) if semana else corrente
    except ValueError:
        raise HTTPException(status_code=400, detail="semana deve ser uma data AAAA-MM-DD")
    conn = get_db_connection()
    try:
        if not _tabela_existe(conn, "com_promo_lote"):
            raise HTTPException(status_code=503, detail=_SEM_LOTE)
        if alvo == corrente:
            _garantir_lote(conn, alvo)
        ofertas = _ofertas_do_lote(conn, alvo, alvo)
        rows = conn.execute(text(f"""
            SELECT {', '.join(_COLS_LOTE)}, recusado_em, recusado_por, recusa_motivo
            FROM com_promo_lote WHERE semana = :s ORDER BY posicao"""), {"s": alvo}).mappings()
        itens = []
        for r in rows:
            d = {k: (float(v) if isinstance(v, Decimal) else str(v) if isinstance(v, (date, datetime)) else v)
                 for k, v in dict(r).items()}
            of = ofertas.get((str(alvo), d["pro_codigo"]))
            d.update(situacao="em_oferta" if of else "recusado" if d["recusado_em"] else "sugerido",
                     **(of or {"oferta_id": None, "oferta_nome": None, "vendido_qtd": 0, "vendido_valor": 0}))
            itens.append(d)
        return {"semana": str(alvo), "corrente": alvo == corrente, "itens": itens}
    finally:
        conn.close()


@router.get("/promo/lote/acompanhamento")
def acompanhamento_lote(semanas: int = 12):
    """Por semana (das últimas `semanas` com lote, mais recente primeiro): itens sugeridos,
    recusados (sem os que viraram oferta, como a situação do GET /promo/lote), em oferta,
    custo a vender sugerido e o vendido pelas ofertas."""
    semanas = max(1, min(52, semanas))
    conn = get_db_connection()
    try:
        if not _tabela_existe(conn, "com_promo_lote"):
            return []
        rows = list(conn.execute(text("""
            SELECT semana, pro_codigo, recusado_em, valor_vender FROM com_promo_lote
            WHERE semana IN (SELECT DISTINCT semana FROM com_promo_lote ORDER BY semana DESC LIMIT :n)"""),
            {"n": semanas}))
        if not rows:
            return []
        ofertas = _ofertas_do_lote(conn, min(r[0] for r in rows), max(r[0] for r in rows))
        por = {}
        for semana, pro, recusado_em, valor in rows:
            a = por.setdefault(str(semana), {"semana": str(semana), "sugeridos": 0, "recusados": 0, "em_oferta": 0,
                                             "valor_vender": 0.0, "vendido_qtd": 0.0, "vendido_valor": 0.0})
            a["sugeridos"] += 1
            a["valor_vender"] += float(valor or 0)
            of = ofertas.get((str(semana), pro))
            if of:
                a["em_oferta"] += 1
                a["vendido_qtd"] += of["vendido_qtd"]
                a["vendido_valor"] += of["vendido_valor"]
            elif recusado_em:
                a["recusados"] += 1
        return sorted(por.values(), key=lambda a: a["semana"], reverse=True)
    finally:
        conn.close()


def _marcar_recusa(req, motivo, usuario, recusado_em):
    conn = get_db_connection()
    try:
        if not _tabela_existe(conn, "com_promo_lote"):
            raise HTTPException(status_code=503, detail=_SEM_LOTE)
        res = conn.execute(text(f"""
            UPDATE com_promo_lote SET recusado_em = {recusado_em}, recusado_por = :usr, recusa_motivo = :motivo
            WHERE semana = :s AND pro_codigo = ANY(:itens)"""),
            {"usr": usuario, "motivo": motivo, "s": segunda(hoje_cuiaba()), "itens": list(req.itens)})
        conn.commit()
        return {"ok": True, "itens": res.rowcount}
    finally:
        conn.close()


@router.post("/promo/lote/recusar")
def recusar_lote(req: PromoLoteRecusaRequest):
    return _marcar_recusa(req, req.motivo, req.usuario, "NOW()")


@router.post("/promo/lote/desfazer-recusa")
def desfazer_recusa_lote(req: PromoLoteRecusaRequest):
    return _marcar_recusa(req, None, None, "NULL")
