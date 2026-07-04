# -*- coding: utf-8 -*-
"""
BACKTEST da política (s,S) — valida o modelo contra a história real.

O que faz (prática recomendada de safety stock: validar o nível de serviço
realizado contra o alvo, em período de holdout):

  1. Congela a política em t0 = hoje − N meses: calcula mín/máx de cada produto
     usando SOMENTE dados anteriores a t0 (mesma matemática do worker:
     demanda 12m + venda perdida, Hampel, ABC, SBC, sazonalidade, newsvendor,
     lead time por fornecedor).
  2. Simula dia a dia [t0, hoje): a demanda real bate no estoque; quando a
     posição (estoque + em trânsito) cruza o mínimo, "compra" até o máximo,
     que chega após o lead time. Modelo de VENDA PERDIDA (sem backorder —
     igual ao balcão).
  3. Mede por curva ABC: fill rate realizado (% da demanda atendida) vs alvo,
     % de ciclos com ruptura, estoque médio simulado (unidades e R$).

Limitações (documentadas):
  - Item a item (sem o pooling dos grupos de marcas — na prática o realizado
    tende a ser MELHOR que o simulado, pois a marca substituta cobre a falta).
  - Política congelada em t0 (produção recalcula toda semana).
  - Só produtos com política estatística (num_vendas > 10 e giro nos 12m
    anteriores a t0); originais/sob encomenda ficam de fora.
  - Estoque inicial = máximo (S) — os primeiros dias são otimistas; os últimos
    N−1 meses dominam o resultado.

Uso:
  python backtest.py                # 12 meses, com venda perdida na demanda
  python backtest.py --meses 6
  python backtest.py --sem-vp       # demanda = só vendas faturadas
  python backtest.py --saida arq.xlsx
"""
import argparse
import heapq
import sys

import numpy as np
import pandas as pd

import main as m


def preparar_saidas(df_saidas, df_dev):
    """Mesma limpeza do run_job: desconta devoluções -> QUANTIDADE_AJUSTADA."""
    df_sai = df_saidas.copy()
    df_sai["PRO_CODIGO"] = df_sai["PRO_CODIGO"].astype(str).str.strip()
    df_sai["QUANTIDADE"] = pd.to_numeric(df_sai.get("QUANTIDADE"), errors="coerce")
    df_sai["DATA"] = pd.to_datetime(df_sai.get("DATA"), errors="coerce")
    if df_dev is not None and not df_dev.empty:
        dev = df_dev.copy()
        dev["PRO_CODIGO"] = dev["PRO_CODIGO"].astype(str).str.strip()
        dev["QTDE_DEVOLVIDA"] = pd.to_numeric(dev.get("QTDE_DEVOLVIDA"), errors="coerce")
        dev_agg = dev.groupby(["NFS", "PRO_CODIGO"], as_index=False)["QTDE_DEVOLVIDA"].sum()
        df_sai = df_sai.merge(dev_agg, on=["NFS", "PRO_CODIGO"], how="left")
        df_sai["QTDE_DEVOLVIDA"] = df_sai["QTDE_DEVOLVIDA"].fillna(0)
    else:
        df_sai["QTDE_DEVOLVIDA"] = 0
    df_sai["QUANTIDADE_AJUSTADA"] = df_sai["QUANTIDADE"] - df_sai["QTDE_DEVOLVIDA"]
    return df_sai[df_sai["QUANTIDADE_AJUSTADA"] > 0].copy()


def montar_politica(df_pre, df_vp_pre, df_saldo, t0, params_forn, mapa_compras):
    """
    Recalcula a política (s, S) como o worker teria feito EM t0, só com dados
    anteriores a t0. Retorna DataFrame por PRO_CODIGO com s, S, lt, alvo etc.
    """
    print("Calculando política em t0 (demanda, ABC, padrão, sazonalidade)...")
    rec = m.calcular_demanda_recente_e_variabilidade(df_pre, df_vp_pre, t0)
    if rec is None or rec.empty:
        return pd.DataFrame()
    rec["PRO_CODIGO"] = rec["PRO_CODIGO"].astype(str).str.strip()

    # nº de vendas vitalício e na janela (portão pouco-histórico, como no worker)
    nv = df_pre.groupby("PRO_CODIGO").size().rename("NUM_VENDAS")
    d12 = df_pre[df_pre["DATA"] >= t0 - pd.DateOffset(months=12)]
    nv12 = d12.groupby("PRO_CODIGO").size().rename("NUM_VENDAS_12M")
    dmax = df_pre.groupby("PRO_CODIGO")["DATA"].max().rename("DATA_MAX_VENDA")
    rec = (rec.set_index("PRO_CODIGO")
              .join(nv).join(nv12).join(dmax)
              .fillna({"NUM_VENDAS": 0, "NUM_VENDAS_12M": 0})
              .reset_index())

    # ABC pelo valor 12m (mesma regra 70/90/97)
    rec = rec.sort_values("VALOR_VENDIDO_12M", ascending=False).reset_index(drop=True)
    tot = rec["VALOR_VENDIDO_12M"].sum()
    pct = rec["VALOR_VENDIDO_12M"].cumsum() / tot * 100 if tot > 0 else 0
    rec["CURVA_ABC"] = pd.Series(pct).apply(
        lambda p: "A" if p <= 70 else "B" if p <= 90 else "C" if p <= 97 else "D")

    # cadastro (subgrupo, marca, fornecedor1)
    sp = df_saldo.copy()
    sp["PRO_CODIGO"] = sp["PRO_CODIGO"].astype(str).str.strip()
    cols = [c for c in ["PRO_CODIGO", "SGR_CODIGO", "MAR_DESCRICAO", "FORNECEDOR1"] if c in sp.columns]
    rec = rec.merge(sp[cols].drop_duplicates("PRO_CODIGO"), on="PRO_CODIGO", how="left")

    # sazonalidade calculada só com o passado (anos completos antes de t0)
    indices_saz = m.calcular_indices_sazonais(df_pre, sp, t0)

    def _saz(sgr):
        try:
            return indices_saz.get(int(sgr))
        except (TypeError, ValueError):
            return None

    def _forn(row):
        h = mapa_compras.get(str(row["PRO_CODIGO"]).strip())
        if h:
            return str(h[0][0]).strip().upper()
        f1 = str(row.get("FORNECEDOR1") or "").strip()
        return f1.upper() if f1 else None

    rec["FORN"] = rec.apply(_forn, axis=1)
    rec["PADRAO"] = rec.apply(lambda r: m._classificar_padrao(
        r["DEM_DIA_REAL"], r.get("ADI"), r.get("CV2_TAMANHO")), axis=1)

    print("Calculando mín/máx por produto (mesma matemática do worker)...")
    out = []
    for r in rec.itertuples(index=False):
        d = r._asdict()
        pf = params_forn.get(d.get("FORN") or "") or {}
        res = m.calcular_min_max(
            d["CURVA_ABC"], d["DEM_DIA_REAL"], d["SIGMA_DEMANDA_DIA"], d["PADRAO"],
            d.get("SGR_CODIGO"), d.get("DATA_MAX_VENDA"), d.get("CV2_TAMANHO"),
            d.get("MEAN_SIZE_MES"), t0,
            margem_unit=d.get("MARGEM_UNIT"), custo_unit=d.get("CUSTO_UNIT"),
            saz=_saz(d.get("SGR_CODIGO")),
            lead_time=pf.get("lead_time_dias"), revisao_dias=pf.get("tempo_revisao_dias"))
        alvo = res.get("NIVEL_SERVICO_CUSTO") or m.NS_POR_CURVA.get(d["CURVA_ABC"], 0.90)
        out.append({
            "PRO_CODIGO": d["PRO_CODIGO"], "CURVA": d["CURVA_ABC"], "PADRAO": d["PADRAO"],
            "MARCA": d.get("MAR_DESCRICAO"), "FORN": d.get("FORN"),
            "NUM_VENDAS": int(d.get("NUM_VENDAS") or 0),
            "NUM_VENDAS_12M": int(d.get("NUM_VENDAS_12M") or 0),
            "DEM_DIA": float(d.get("DEM_DIA_REAL") or 0.0),
            "CUSTO_UNIT": float(d.get("CUSTO_UNIT")) if pd.notna(d.get("CUSTO_UNIT")) else 0.0,
            "S_MIN": int(res["ESTOQUE_MIN_BASE"]), "S_MAX": int(res["ESTOQUE_MAX_BASE"]),
            "LT": float(res["LEAD_TIME_APLICADO"]) + float(res["PERIODO_REVISAO_APLICADO"]),
            "ALVO": float(alvo),
        })
    pol = pd.DataFrame(out)

    # escopo do backtest: política estatística de verdade
    orig = pol["MARCA"].apply(m.marca_eh_original) if "MARCA" in pol.columns else False
    pol = pol[(pol["NUM_VENDAS"] > 10) & (pol["NUM_VENDAS_12M"] > 0)
              & (pol["S_MAX"] > 0) & (pol["S_MIN"] > 0) & (~orig)]
    return pol.reset_index(drop=True)


def simular_produto(eventos, s, S, lt, horizonte_dias):
    """
    Simulação (s,S) por eventos, venda perdida (sem backorder).
    eventos: [(dia_idx, qtd)] ordenado. Retorna métricas do produto.
    """
    on_hand = float(S)
    on_order = 0.0
    chegadas = []  # heap (dia, qtd)
    t_prev = 0
    area = 0.0
    total = atendido = 0.0
    ciclos = ciclos_falta = 0
    falta_no_ciclo = False

    for dia, qtd in eventos:
        while chegadas and chegadas[0][0] <= dia:
            ad, aq = heapq.heappop(chegadas)
            area += on_hand * (ad - t_prev)
            t_prev = ad
            on_hand += aq
            on_order -= aq
            ciclos += 1
            if falta_no_ciclo:
                ciclos_falta += 1
            falta_no_ciclo = False
        area += on_hand * (dia - t_prev)
        t_prev = dia
        at = min(on_hand, qtd)
        atendido += at
        total += qtd
        if at < qtd:
            falta_no_ciclo = True
        on_hand -= at
        if on_hand + on_order <= s:
            q = S - (on_hand + on_order)
            heapq.heappush(chegadas, (dia + lt, q))
            on_order += q

    while chegadas:
        ad, aq = heapq.heappop(chegadas)
        ad = min(ad, horizonte_dias)
        area += on_hand * (ad - t_prev)
        t_prev = ad
        on_hand += aq
        ciclos += 1
        if falta_no_ciclo:
            ciclos_falta += 1
        falta_no_ciclo = False
    area += on_hand * max(horizonte_dias - t_prev, 0)

    return {
        "demanda": total, "atendido": atendido,
        "fill": (atendido / total) if total > 0 else np.nan,
        "estoque_medio": area / max(horizonte_dias, 1),
        "ciclos": ciclos, "ciclos_falta": ciclos_falta,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Backtest da política (s,S)")
    ap.add_argument("--meses", type=int, default=12, help="meses de simulação (holdout)")
    ap.add_argument("--sem-vp", action="store_true",
                    help="demanda simulada = só vendas (sem somar venda perdida)")
    ap.add_argument("--saida", default=None, help="arquivo Excel de saída")
    args = ap.parse_args(argv)

    hoje = pd.Timestamp.today().normalize()
    t0 = (hoje - pd.DateOffset(months=args.meses)).normalize()
    horizonte = max((hoje - t0).days, 1)
    print(f"=== BACKTEST (s,S): política congelada em {t0.date()}, simulando até {hoje.date()} "
          f"({horizonte} dias) ===")

    # carga completa (a política em t0 precisa de 12m antes de t0 + 5 anos p/ sazonal)
    df_saidas, df_ent, df_dev, df_saldo, df_vp = m.carregar_dados_do_banco(corte=None)
    df_sai = preparar_saidas(df_saidas, df_dev)

    df_pre = df_sai[df_sai["DATA"] < t0].copy()
    df_sim = df_sai[(df_sai["DATA"] >= t0) & (df_sai["DATA"] < hoje)].copy()

    vp = pd.DataFrame()
    if df_vp is not None and not df_vp.empty:
        vp = df_vp.rename(columns={c: str(c).upper() for c in df_vp.columns}).copy()
        vp["PRO_CODIGO"] = vp["PRO_CODIGO"].astype(str).str.strip()
        vp["DATA"] = pd.to_datetime(vp["DATA"], errors="coerce")
        vp["QUANTIDADE"] = pd.to_numeric(vp["QUANTIDADE"], errors="coerce").fillna(0)
    df_vp_pre = vp[vp["DATA"] < t0].copy() if not vp.empty else vp

    params_forn = m.carregar_parametros_fornecedor()
    mapa_compras = m.carregar_mapa_compras_fornecedor()

    pol = montar_politica(df_pre, df_vp_pre, df_saldo, t0, params_forn, mapa_compras)
    if pol.empty:
        print("Sem produtos elegíveis para o backtest.")
        return 1
    print(f"Produtos no backtest: {len(pol)}")

    # ---- demanda simulada por dia: vendas (+ venda perdida, capada por evento) ----
    df_sim["DIA"] = (df_sim["DATA"] - t0).dt.days
    dem = df_sim.groupby(["PRO_CODIGO", "DIA"])["QUANTIDADE_AJUSTADA"].sum()
    if not args.sem_vp and not vp.empty:
        vps = vp[(vp["DATA"] >= t0) & (vp["DATA"] < hoje) & (vp["QUANTIDADE"] > 0)].copy()
        if not vps.empty:
            vps["Q_CAP"] = vps["QUANTIDADE"].clip(upper=m.VP_CAP_TETO)
            vps["DIA"] = (vps["DATA"] - t0).dt.days
            dem = dem.add(vps.groupby(["PRO_CODIGO", "DIA"])["Q_CAP"].sum(), fill_value=0)

    eventos_por_pro = {}
    for (pro, dia), q in dem.items():
        eventos_por_pro.setdefault(pro, []).append((int(dia), float(q)))
    for pro in eventos_por_pro:
        eventos_por_pro[pro].sort()

    # ---- simulação ----
    print("Simulando...")
    res = []
    for r in pol.itertuples(index=False):
        ev = eventos_por_pro.get(r.PRO_CODIGO, [])
        sim = simular_produto(ev, r.S_MIN, r.S_MAX, r.LT, horizonte)
        res.append({**r._asdict(), **sim})
    df = pd.DataFrame(res)
    df["capital_medio"] = df["estoque_medio"] * df["CUSTO_UNIT"]

    # ---- resumo por curva ----
    linhas = []
    for curva, g in df.groupby("CURVA"):
        dem_t = g["demanda"].sum()
        at_t = g["atendido"].sum()
        cic = g["ciclos"].sum()
        cf = g["ciclos_falta"].sum()
        linhas.append({
            "curva": curva,
            "itens": len(g),
            "demanda (un)": round(dem_t),
            "fill rate realizado": round(at_t / dem_t, 4) if dem_t > 0 else None,
            "alvo médio (NS)": round(g["ALVO"].mean(), 4),
            "ciclos sem ruptura": round(1 - cf / cic, 4) if cic > 0 else None,
            "estoque médio (un)": round(g["estoque_medio"].sum()),
            "capital médio (R$)": round(g["capital_medio"].sum(), 2),
        })
    resumo = pd.DataFrame(linhas).sort_values("curva")

    print("\n================= RESULTADO POR CURVA =================")
    print(resumo.to_string(index=False))
    tot_dem = df["demanda"].sum()
    tot_at = df["atendido"].sum()
    print(f"\nGERAL: fill rate {tot_at / tot_dem:.2%} | capital médio simulado "
          f"R$ {df['capital_medio'].sum():,.0f} | {len(df)} itens")
    print("\nLeitura: fill rate REALIZADO abaixo do alvo da curva => aumentar nível de "
          "serviço/SS (ou revisar lead time); muito acima com capital alto => dá para "
          "reduzir SS/máximo. O alvo (NS) é probabilidade de não faltar no ciclo; o fill "
          "rate tende a ficar ACIMA dele quando o modelo está saudável.")

    # ---- piores itens (para ação) ----
    piores = df[(df["demanda"] > 0)].nsmallest(30, "fill")[
        ["PRO_CODIGO", "MARCA", "CURVA", "PADRAO", "FORN", "S_MIN", "S_MAX", "LT",
         "demanda", "atendido", "fill", "estoque_medio", "capital_medio"]]

    saida = args.saida or f"backtest_resultado_{hoje.date().isoformat()}.xlsx"
    with pd.ExcelWriter(saida, engine="openpyxl") as xw:
        resumo.to_excel(xw, sheet_name="resumo_por_curva", index=False)
        piores.to_excel(xw, sheet_name="piores_30_fill", index=False)
        df.drop(columns=["MARCA"], errors="ignore").to_excel(xw, sheet_name="por_produto", index=False)
    print(f"\nDetalhe salvo em: {saida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
