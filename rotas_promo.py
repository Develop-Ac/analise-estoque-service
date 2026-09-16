# -*- coding: utf-8 -*-
"""Lista de promoção por valor parado: /promo/plan, /promo/export, /promo/acao/{pro_codigo}.

A regra (ação, público, preço) mora em promo_regra.py; aqui só se lê o com_fifo_completo,
aplica filtros/paginação/totais e grava a troca manual e as campanhas exportadas.
Nada é gravado no ERP — a carga da promoção continua no Celta.
"""
import io
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from estoque_rt import _get_stock_batches
from infra_db import get_db_connection
from modelos import PromoAcaoRequest, PromoPlanRequest
from promo_regra import ACOES, REGUA_PADRAO, VALIDADE_DIAS, avaliar

router = APIRouter()

ORDENACOES = {"valor_parado", "valor_excesso", "cobertura_meses", "dias_sem_venda", "caixa_potencial", "pro_descricao"}


def _tabela_existe(conn, nome):
    return conn.execute(text("SELECT to_regclass(:n)"), {"n": nome}).scalar() is not None


def _regua(conn):
    """Régua v3 do atacado (mesmo Postgres do vendas-service); seed embutido como plano B."""
    if not _tabela_existe(conn, "ven_regua_atacado"):
        return REGUA_PADRAO
    rows = conn.execute(text("SELECT classe, faixa, markup, desc_max FROM ven_regua_atacado WHERE ativo")).fetchall()
    return {(r[0], r[1]): (float(r[2]), float(r[3])) for r in rows} or REGUA_PADRAO


def _linhas(conn, req: PromoPlanRequest):
    """Todas as linhas candidatas (excesso > 0 ou sem custo) já avaliadas pela regra."""
    filters = ["f.data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)",
               "f.estoque_disponivel > 0",
               "(f.estoque_disponivel > CEIL(COALESCE(f.estoque_max_sugerido,0)) OR COALESCE(f.custo_unitario,0) <= 0)"]
    params = {}
    for campo, coluna, valores in (("sg", "sgr_descricao", req.subgroups), ("br", "mar_descricao", req.brands),
                                   ("cat", "categoria_saldo_atual", req.categories)):
        vals = [v.strip() for v in (valores or []) if v and v.strip()]
        if vals:
            keys = [f"{campo}_{i}" for i in range(len(vals))]
            params.update(zip(keys, vals))
            filters.append(f"f.{coluna} IN ({','.join(':' + k for k in keys)})")

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
                if tem_acao else "NULL AS acao, NULL AS preco_varejo_manual, NULL AS preco_atacado_manual, NULL AS observacao_manual"}
        FROM com_fifo_completo f
        {"LEFT JOIN ven_regua_item_excecao e ON e.pro_codigo::text = f.pro_codigo" if tem_exc else ""}
        {"LEFT JOIN com_promo_acao a ON a.pro_codigo = f.pro_codigo" if tem_acao else ""}
        WHERE {' AND '.join(filters)}
    """
    regua = _regua(conn)
    out = []
    for r in conn.execute(text(sql), params).mappings():
        # Decimal/date do Postgres → float/str: a regra faz conta e a resposta vira JSON
        d = {k: (float(v) if isinstance(v, Decimal) else str(v) if isinstance(v, (date, datetime)) else v)
             for k, v in dict(r).items()}
        d["acao_manual"] = d.pop("acao", None)
        out.append(avaliar(d, regua))

    if req.acoes:
        out = [x for x in out if x["acao"] in req.acoes]
    if req.publicos:
        out = [x for x in out if x["publico"] in req.publicos]
    if req.curvas:
        out = [x for x in out if (x.get("curva_abc") or "") in req.curvas]
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
        "meses_para_zerar": round(valor / custo_dem_mes, 1) if custo_dem_mes > 0 else None,
        "bonus_liquidacao": round(sum(x["excesso_qtd"] * (x.get("bonus_liquidacao_unit") or 0)
                                      for x in linhas if x["acao"] == "giro_caixa" and x["publico"] != "varejo"), 2),
        "por_acao": {k: {kk: (round(vv, 2) if isinstance(vv, float) else vv) for kk, vv in v.items()} for k, v in por_acao.items()},
    }


@router.post("/promo/plan")
def planejar_promocao(req: PromoPlanRequest):
    conn = get_db_connection()
    try:
        linhas = _linhas(conn, req)
        chave = req.sort if req.sort in ORDENACOES else "valor_parado"
        reverso = (req.sort_dir or "desc").lower() != "asc"
        linhas.sort(key=lambda x: ((x.get(chave) is None), x.get(chave) if x.get(chave) is not None else 0), reverse=reverso)
        if reverso:  # None sempre por último, mesmo em ordem decrescente
            linhas.sort(key=lambda x: x.get(chave) is None)
        total = len(linhas)
        size = max(1, min(req.page_size or 100, 1000))
        page = max(1, req.page or 1)
        pagina = linhas[(page - 1) * size: page * size]
        lotes = _get_stock_batches([x["pro_codigo"] for x in pagina]) if pagina else {}
        for x in pagina:
            b = lotes.get(x["pro_codigo"], [])
            x["lotes_estoque"] = [l.model_dump() if hasattr(l, "model_dump") else l.dict() for l in b]
            x["estoque_obsoleto"] = sum(l.qtd for l in b if l.dias_em_estoque > 240)
        return {"items": pagina, "total": total, "page": page, "page_size": size,
                "total_pages": (total + size - 1) // size, "totais": _totais(linhas)}
    except Exception as e:
        print(f"Erro promo plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


COLS_EXPORT = {
    "pro_codigo": "Código", "pro_descricao": "Descrição", "mar_descricao": "Marca", "sgr_descricao": "Subgrupo",
    "fornecedor1": "Fornecedor", "curva_abc": "Curva ABC", "categoria_saldo_atual": "Tempo em estoque",
    "estoque_disponivel": "Saldo", "estoque_max_sugerido": "Máximo sugerido", "excesso_qtd": "Excesso (un)",
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
