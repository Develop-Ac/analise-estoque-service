# -*- coding: utf-8 -*-
"""Sugestao de compra: memoria de calculo, montagem e PDF."""
import os
import math
import datetime
import concurrent.futures as _futures
from sqlalchemy import text
from infra_db import get_db_connection
from estoque_rt import _RT_EXECUTOR, get_all_realtime_stocks, get_realtime_stocks_bulk, _get_stock_batches
from fornecedor_info import get_compras_historico, get_fornecedor_parametros, get_fornecedor_principal_map, get_grupos_fornecedor, _expandir_fornecedores, SEM_HIST_COMPRA, _sug_norm, _sug_float
from config import FORN_EXPR


def _sug_posicao(it, stock_map, usar_rt):
    """Estoque (realtime se disponível, senão snapshot) + em trânsito de UM item."""
    cod = _sug_norm(it.get("pro_codigo"))
    est = stock_map.get(cod) if usar_rt else None
    estoque = float(est) if est is not None else _sug_float(it.get("estoque_snapshot"))
    transito = _sug_float(it.get("em_transito"))
    return cod, estoque, transito


# Z (nível de serviço) por curva — espelha Z_POR_CURVA do main.py.
_Z_POR_CURVA = {"A": 2.054, "B": 1.645, "C": 1.282, "D": 1.036}
# Nível de serviço (probabilidade) por curva — espelha NS_POR_CURVA do main.py.
_NS_POR_CURVA = {"A": 0.98, "B": 0.95, "C": 0.90, "D": 0.85}
_ALPHA_CROSTON = 0.1

# Cobertura por classe (dias de ciclo) — espelha REGRAS_DIAS do main.py.
_REGRAS_DIAS = {
    "default": {"A": (20, 60), "B": (30, 90), "C": (45, 120), "D": (0, 45)},
    154:       {"A": (45, 120), "B": (60, 180), "C": (90, 240), "D": (0, 120)},
}


def _dias_ciclo(curva, sgr_codigo):
    try:
        sgr = int(sgr_codigo)
    except (TypeError, ValueError):
        sgr = None
    regra = _REGRAS_DIAS.get(sgr, _REGRAS_DIAS["default"])
    dmin, dmax = regra.get(_sug_norm(curva).upper(), regra.get("C", (45, 120)))
    return max(dmax - dmin, 1), dmin, dmax


def montar_memoria_calculo(*, escopo, minimo, maximo, curva, classe, metodo,
                           demanda_dia, sigma_dia, z, lead_time, ss, fator_sazonal,
                           sgr_codigo, msize=None, cv2=None, membros=None,
                           ns_custo=None, z_custo=None, min_custo=None, max_custo=None,
                           ss_custo=None, margem_pct=None, custo_unit=None,
                           outlier_aparado=None, outlier_qtd=None, outlier_motivo=None,
                           periodo_revisao=None, fornecedor_lt=None):
    """
    Memória de cálculo do mín/máx: fórmula + valores REAIS que compuseram a
    quantidade. escopo='grupo'|'item'. `membros` (grupo) = contribuição por marca.
    `periodo_revisao` (dias) entra no período de proteção do mínimo (LT + revisão);
    `fornecedor_lt` = nome do fornecedor de quem veio o lead time (informativo).
    """
    ciclo, _dmin, _dmax = _dias_ciclo(curva, sgr_codigo)
    rev = _sug_float(periodo_revisao)
    t_prot = _sug_float(lead_time) + max(rev, 0.0)  # período de proteção do mínimo
    comp = [
        {"rotulo": "Demanda planejada", "valor": round(_sug_float(demanda_dia), 4), "unid": "un/dia"},
        {"rotulo": "Lead time (reposição)", "valor": int(_sug_float(lead_time)), "unid": "dias"},
        {"rotulo": "Estoque de segurança", "valor": int(_sug_float(ss)), "unid": "un"},
        {"rotulo": "Dias de ciclo (cobertura do máximo)", "valor": ciclo, "unid": "dias"},
    ]
    if rev > 0:
        comp.insert(2, {"rotulo": "Período de revisão do fornecedor", "valor": int(rev), "unid": "dias"})
    if fornecedor_lt:
        comp.append({"rotulo": "Lead time do fornecedor", "valor": str(fornecedor_lt)})
    if _sug_float(z):
        comp.append({"rotulo": "Nível de serviço (Z)", "valor": round(_sug_float(z), 3)})
    if _sug_float(sigma_dia):
        comp.append({"rotulo": "σ da demanda/dia", "valor": round(_sug_float(sigma_dia), 4)})
    fs = _sug_float(fator_sazonal)
    if fs and abs(fs - 1.0) >= 0.01:
        comp.append({"rotulo": "Fator sazonal (próximo período)", "valor": round(fs, 3), "unid": "x"})
    protecao_txt = "(lead time + revisão)" if rev > 0 else "lead time"
    mem = {
        "escopo": escopo,
        "minimo": int(_sug_float(minimo)),
        "maximo": int(_sug_float(maximo)),
        "curva": curva,
        "classe": classe,
        "metodo": metodo or "Normal (Z·σ·√LT)",
        "formula": (f"Mínimo (ponto de pedido) = demanda × {protecao_txt} + estoque de segurança.  "
                    "Máximo = Mínimo + demanda × dias de ciclo."),
        "componentes": comp,
    }

    # ----- Derivação do MÉTODO DE DEMANDA (passo a passo, com números reais) -----
    lt = t_prot  # proteção efetiva do mínimo (lead time + revisão)
    dem = _sug_float(demanda_dia)
    ss_v = _sug_float(ss)
    mmin = int(_sug_float(minimo))
    mmax = int(_sug_float(maximo))
    met = metodo or "Normal (Z·σ·√LT)"
    intermit = any(k in met for k in ("Croston", "Poisson", "Binomial"))
    prot_lbl = "lead time + revisão" if rev > 0 else "lead time"
    passos = []
    if intermit:
        ns = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        lam = dem * lt
        dist = "Binomial Negativa" if "Binomial" in met else "Poisson"
        passos = [
            f"Demanda intermitente/grumosa → {dist} composta (não usa a Normal).",
            f"λ (esperado no período de proteção) = demanda × {prot_lbl} = {round(dem, 4)} × {int(lt)} = {round(lam, 2)} un.",
            f"Ponto de pedido (mín) = quantil {int(ns * 100)}% da {dist} da demanda no período = {mmin} un.",
            f"Estoque de segurança = ponto de pedido − λ = {mmin} − {round(lam, 2)} = {int(ss_v)} un.",
            f"Máximo = quantil {int(ns * 100)}% no horizonte ({prot_lbl} + dias de ciclo) = {mmax} un.",
        ]
    else:
        z_v = _sug_float(z)
        sig = _sug_float(sigma_dia)
        # σ sazonalizado: o worker aplica σ × fator sazonal no SS (o σ acompanha a estação)
        sig_eff = sig * fs if (sig > 0 and fs and abs(fs - 1.0) >= 0.01) else sig
        if sig > 0 and z_v > 0:
            sig_txt = (f"(σ {round(sig, 4)} × sazonal {round(fs, 3)}) = {round(sig_eff, 4)}"
                       if sig_eff != sig else f"{round(sig, 4)}")
            passos.append(f"Estoque de segurança = Z × σ × √({prot_lbl}) = {round(z_v, 3)} × {sig_txt} × √{int(lt)} = {int(ss_v)} un.")
        else:
            passos.append(f"Estoque de segurança (Z·σ·√{prot_lbl}) = {int(ss_v)} un.")
        passos.append(f"Ponto de pedido (mín) = demanda × {prot_lbl} + SS = {round(dem, 4)} × {int(lt)} + {int(ss_v)} = {mmin} un.")
        passos.append(f"Máximo = mín + demanda × dias de ciclo = {mmin} + {round(dem, 4)} × {ciclo} = {mmax} un.")
    mem["metodo_calculo"] = {"tipo": met, "passos": passos}

    # ----- Dados para o GRÁFICO (igual ao manual) -----
    import math as _math
    if intermit:
        ns = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        lam = _sug_float(demanda_dia) * lt  # média direta (sem a antiga correção 1−α/2)
        kmax = max(mmin + 5, 8)

        def _cdf_arr(mean, var):
            """CDF[0..kmax] de Poisson (var≈mean) ou Binomial Negativa (var>mean).
            Espelha _quantil_demanda do main.py (var≥média; limiar Poisson 1.10)."""
            out = []; acc = 0.0
            if mean <= 0:
                return [1.0] * (kmax + 1)
            var = max(var, mean)
            if var <= mean * 1.10:
                pmf = _math.exp(-mean)
                for k in range(kmax + 1):
                    acc += pmf; out.append(min(acc, 1.0)); pmf = pmf * mean / (k + 1)
            else:
                r = mean * mean / (var - mean); pr = r / (r + mean); pmf = pr ** r
                for k in range(kmax + 1):
                    acc += pmf; out.append(min(acc, 1.0)); pmf = pmf * (k + r) / (k + 1) * (1 - pr)
            return out

        msize_v = _sug_float(msize)
        cv2_v = _sug_float(cv2)
        if msize_v > 0:
            # EXATO: mesma dispersão do modelo (disp = tam médio × (1+CV²); var = λ·disp)
            disp = max(msize_v * (1.0 + cv2_v), 1.0)
            var = lam * disp
            exato = True
        else:
            # Fallback (dados sem mean_size/cv2): ajusta var p/ o quantil NS = ponto de pedido
            lo, hi, var = lam, max(lam * 40, lam + 1.0), lam
            if mmin > 0 and lam > 0:
                for _ in range(40):
                    mid = (lo + hi) / 2.0
                    cdf = _cdf_arr(lam, mid)
                    q = next((k for k in range(len(cdf)) if cdf[k] >= ns), kmax)
                    if q >= mmin:
                        hi = mid; var = mid
                    else:
                        lo = mid
            exato = False
        cdf = _cdf_arr(lam, var)
        barras = []; prev = 0.0
        for k in range(kmax + 1):
            pmf = max(cdf[k] - prev, 0.0); prev = cdf[k]
            barras.append({"k": k, "pmf": round(pmf, 4), "cdf": round(cdf[k], 4)})
        mem["graf"] = {"tipo": "distribuicao", "dist": ("Binomial Negativa" if var > lam * 1.05 else "Poisson"),
                       "nivel_servico": ns, "ponto_pedido": mmin, "lambda": round(lam, 2),
                       "exato": exato, "barras": barras}
    else:
        mem["graf"] = {"tipo": "serra", "maximo": mmax, "minimo": mmin, "seguranca": int(ss_v),
                       "demanda_dia": round(dem, 4), "lead_time": int(lt), "ciclo": ciclo}

    # ----- Nível de serviço por CUSTO (razão crítica) — agora OFICIAL (NS_MODO=custo) -----
    # Payload legado da comparação curva×custo; a tela foi removida (o custo virou o mín/máx
    # oficial). Mantido só p/ compatibilidade de contrato; frontend atual ignora `custo`.
    if ns_custo is not None:
        nsc = _sug_float(ns_custo)
        cmin = int(_sug_float(min_custo)); cmax = int(_sug_float(max_custo))
        ns_curva = _NS_POR_CURVA.get(_sug_norm(curva).upper(), 0.90)
        mem["custo"] = {
            "nivel_servico": round(nsc, 4),
            "nivel_servico_curva": round(ns_curva, 4),
            "z": (round(_sug_float(z_custo), 3) if z_custo is not None else None),
            "minimo": cmin,
            "maximo": cmax,
            "seguranca": int(_sug_float(ss_custo)),
            "delta_min": cmin - mmin,
            "delta_max": cmax - mmax,
            "margem_pct": (round(_sug_float(margem_pct), 4) if margem_pct is not None else None),
            "custo_unit": (round(_sug_float(custo_unit), 2) if custo_unit is not None else None),
            "delta_capital": round((cmin - mmin) * _sug_float(custo_unit), 2) if custo_unit is not None else None,
            "formula": "p* = margem ÷ (margem + custo de manter);  limitado pela faixa da curva ABC.",
        }

    # ----- Auditoria do outlier de demanda aparado (mês fora da curva) -----
    if outlier_aparado:
        mem["outlier"] = {
            "aparado": True,
            "qtd": int(_sug_float(outlier_qtd)),
            "motivo": outlier_motivo or "Mês de venda fora da curva aparado no cálculo da demanda.",
        }

    if membros:
        mem["membros"] = membros
    return mem


def montar_sugestao_compra(items, stock_map, *, historico=None, consolidar_grupo=True,
                           usar_estoque_realtime=True, fornecedor=None, curva=None,
                           subgrupo=None, apenas_zerados=False, incluir_sem_historico=False,
                           params_forn=None, marca=None, grupos_forn=None, principal_forn=None,
                           pedidos_map=None):
    """
    Transforma as linhas de com_fifo_completo (1 por produto/marca) na sugestão de
    compra agrupada por fornecedor. FUNÇÃO PURA — usada pelo endpoint e pela validação.

    Cada item (dict) deve ter as chaves: pro_codigo, pro_descricao, mar_descricao,
    sgr_descricao, fornecedor1, curva_abc, classe_xyz, padrao_demanda, metodo_reposicao,
    ponto_pedido (mín individual), maximo (máx individual), estoque_snapshot, em_transito,
    demanda_media_dia_ajustada, valor_vendido_12m, sob_encomenda, grupo_chave,
    grupo_estoque_min, grupo_estoque_max, grupo_curva, grupo_padrao, grupo_metodo.

    Modo grupo (consolidar_grupo=True):
      - omite produtos "Sob Encomenda";
      - produtos com grupo_chave usam grupo_estoque_min/max e a POSIÇÃO CONSOLIDADA
        (soma do estoque+trânsito de todas as marcas do grupo) -> 1 linha por GRUPO;
      - produtos sem grupo_chave (avulsos, incluindo ORIGINAIS planejados) caem no
        cálculo individual, com fornecedor vindo do histórico de compra (concessionária).
    """
    import math
    from collections import defaultdict

    usar_rt = usar_estoque_realtime and bool(stock_map)
    historico = historico or {}
    params_forn = params_forn or {}
    grupos_forn = grupos_forn or {}        # {NOME_UPPER: [nomes do grupo]}
    principal_forn = principal_forn or {}  # {NOME_UPPER: NOME do principal}
    pedidos_map = pedidos_map or {}        # {pro_codigo(str): [{numero,status,qtd}]}

    def _pedidos_do(codigos):
        """
        Consolida os pedidos em aberto de um ou mais produtos (grupo) num só bloco:
        junta por número de pedido (somando a qtd pendente) e devolve
        (em_pedido, qtd_total_em_pedido, [ {numero, status, qtd} ordenado por qtd desc ]).
        """
        por_num = {}
        for c in codigos:
            for ped in pedidos_map.get(str(c), []):
                num = ped.get("numero")
                slot = por_num.get(num)
                if slot is None:
                    por_num[num] = {"numero": num, "status": ped.get("status"),
                                    "qtd": float(ped.get("qtd") or 0)}
                else:
                    slot["qtd"] += float(ped.get("qtd") or 0)
        lista = sorted(por_num.values(), key=lambda x: -x["qtd"])
        for p in lista:
            p["qtd"] = round(p["qtd"], 2)
        total = round(sum(p["qtd"] for p in lista), 2)
        return (bool(lista), total, lista)

    def _canon_forn(nome):
        """Fornecedor VINCULADO (grupo do compras) aparece como o PRINCIPAL —
        blocos de matriz/filiais são consolidados num só."""
        if not nome or nome == SEM_HIST_COMPRA:
            return nome
        return principal_forn.get(_sug_norm(nome).upper()) or nome

    def _forn_match(nome, termo_lower):
        """Busca por fornecedor respeita o grupo: o termo casa com o nome OU com
        qualquer fornecedor relacionado a ele."""
        if termo_lower in nome.lower():
            return True
        return any(termo_lower in m.lower()
                   for m in grupos_forn.get(_sug_norm(nome).upper(), []))

    curvas_filtro = [c.strip().upper() for c in curva.split(",")] if curva else None
    subs_filtro = [s.strip().lower() for s in subgrupo.split(",")] if subgrupo else None
    marcas_filtro = ([m.strip().upper() for m in marca.split(",") if m.strip()]
                     if marca else None)
    ordem_curva = {"A": 0, "B": 1, "C": 2, "D": 3}
    grupos = {}  # fornecedor -> [rec]

    # Índice DESCRIÇÃO -> itens por LINHA de marca, para mostrar no detalhe as
    # marcas do mesmo produto em OUTRAS linhas (não entram no cálculo do grupo,
    # mas o comprador precisa ver que existem — ex.: Bosch vs linha econômica).
    por_desc = {}
    for _it in items:
        if _it.get("sob_encomenda"):
            continue
        _d = _sug_norm(_it.get("pro_descricao")).upper()
        if _d:
            por_desc.setdefault(_d, []).append(_it)

    def _outras_linhas(descricao, linha_propria):
        try:
            lp = int(linha_propria) if linha_propria is not None else 2
        except (TypeError, ValueError):
            lp = 2
        out = []
        for o in por_desc.get(_sug_norm(descricao).upper(), []):
            try:
                lo = int(o.get("marca_linha")) if o.get("marca_linha") is not None else 2
            except (TypeError, ValueError):
                lo = 2
            if lo == lp:
                continue
            _, est_o, tr_o = _sug_posicao(o, stock_map, usar_rt)
            pos_o = est_o + tr_o
            max_o = _sug_float(o.get("maximo"))
            if max_o > 0 and pos_o > max_o:
                situ = "acima do máximo"
            elif max_o > 0 and pos_o <= _sug_float(o.get("ponto_pedido")):
                situ = "abaixo do mínimo"
            elif max_o > 0:
                situ = "ok"
            else:
                situ = "sem alvo"
            out.append({
                "pro_codigo": _sug_norm(o.get("pro_codigo")),
                "marca": o.get("mar_descricao"),
                "linha": lo,
                "estoque_atual": round(est_o, 2),
                "em_transito": round(tr_o, 2),
                "posicao": round(pos_o, 2),
                "maximo": int(max_o),
                "situacao": situ,
            })
        out.sort(key=lambda x: (x["linha"], -x["posicao"]))
        return out[:12]

    def _forns_hist(cod):
        """[(for_nome, qtd)] desc — de quem JÁ COMPRAMOS este produto."""
        return historico.get(_sug_norm(cod), [])

    def _sub_ok(sgr):
        return (not subs_filtro) or (_sug_norm(sgr).lower() in subs_filtro)

    def _passa_filtros_comuns(curva_item, estoque_total):
        if curvas_filtro and (curva_item or "").upper() not in curvas_filtro:
            return False
        if apenas_zerados and estoque_total > 0:
            return False
        return True

    def _registrar(bucket, rec):
        grupos.setdefault(bucket, []).append(rec)

    def _tratar_individual(it, permitir_ponto_zero=False):
        ponto = _sug_float(it.get("ponto_pedido"))
        maximo = _sug_float(it.get("maximo"))
        # Avulso puro exige ponto de pedido > 0; grupo de 1 item roteado p/ cá
        # (permitir_ponto_zero=True) aparece com máximo > 0 mesmo com mínimo 0,
        # igual ao gate leniente do caminho de grupo — senão o item sumiria.
        if maximo <= 0 or (ponto <= 0 and not permitir_ponto_zero):
            return
        cod, estoque, transito = _sug_posicao(it, stock_map, usar_rt)
        posicao = estoque + transito
        em_pedido, pedido_qtd, pedidos_lst = _pedidos_do([cod])
        # Compra-se quando o ESTOQUE REAL (sem o que está a caminho) chega ao ponto
        # de pedido. Itens cobertos por pedido em aberto NÃO somem: aparecem com a
        # qtd a comprar já descontada do que está em pedido (pode ir a 0) e a
        # marcação em_pedido, para o comprador ver o que está e o que não está.
        if estoque > ponto:
            return
        qtd = int(math.ceil(maximo - posicao))
        if qtd < 0:
            qtd = 0
        if qtd <= 0 and not em_pedido:
            return
        curva_item = _sug_norm(it.get("curva_abc"))
        if not _passa_filtros_comuns(curva_item, estoque):
            return
        if not _sub_ok(it.get("sgr_descricao")):
            return
        if marcas_filtro and _sug_norm(it.get("mar_descricao")).upper() not in marcas_filtro:
            return
        # Fornecedor vem do HISTÓRICO de compra (não do cadastro fornecedor1/2/3).
        h = _forns_hist(cod)
        all_forns = [n for n, _ in h]
        top = all_forns[0] if all_forns else SEM_HIST_COMPRA
        if fornecedor:
            # filtro casa se o termo está em QUALQUER fornecedor já comprado do
            # item — ou em algum RELACIONADO dele (grupo matriz/filiais)
            casado = next((n for n in all_forns if _forn_match(n, fornecedor.lower())), None)
            if not casado:
                return
            bucket = casado
        else:
            bucket = top
        if bucket == SEM_HIST_COMPRA and not incluir_sem_historico:
            return  # produto nunca comprado -> fora da lista
        bucket = _canon_forn(bucket)  # vinculados aparecem sob o PRINCIPAL
        memoria_item = montar_memoria_calculo(
            escopo="item", minimo=ponto, maximo=maximo,
            curva=curva_item, classe=it.get("classe_xyz"), metodo=it.get("metodo_reposicao"),
            demanda_dia=(it.get("demanda_planejamento_dia")
                         if it.get("demanda_planejamento_dia") is not None
                         else it.get("demanda_media_dia_ajustada")),
            sigma_dia=it.get("sigma_demanda_dia"),
            z=(it.get("nivel_servico_z") if it.get("nivel_servico_z") is not None
               else _Z_POR_CURVA.get(_sug_norm(curva_item).upper())),
            lead_time=(it.get("lead_time_dias") or 17),
            ss=it.get("estoque_seguranca"),
            fator_sazonal=it.get("fator_sazonal"),
            sgr_codigo=it.get("sgr_codigo"),
            msize=it.get("mean_size_mes"), cv2=it.get("cv2_tamanho"),
            outlier_aparado=it.get("teve_outlier_aparado"),
            outlier_qtd=it.get("outlier_qtd_aparada"), outlier_motivo=it.get("outlier_motivo"),
            periodo_revisao=it.get("periodo_revisao_dias"),
            fornecedor_lt=it.get("fornecedor_principal"),
        )
        custo_u = _sug_float(it.get("custo_unitario"))
        dem_item = _sug_float(it.get("demanda_planejamento_dia")
                              if it.get("demanda_planejamento_dia") is not None
                              else it.get("demanda_media_dia_ajustada"))
        _registrar(bucket, {
            "tipo": "individual",
            "memoria": memoria_item,
            "demanda_dia": round(dem_item, 4),
            "cobertura_dias": (round(posicao / dem_item, 0) if dem_item > 0 else None),
            "outras_linhas": _outras_linhas(it.get("pro_descricao"), it.get("marca_linha")),
            "grupo_chave": None,
            "pro_codigo": cod,
            "pro_descricao": it.get("pro_descricao"),
            "marca": it.get("mar_descricao"),
            "subgrupo": it.get("sgr_descricao"),
            "curva_abc": curva_item,
            "classe_xyz": it.get("classe_xyz"),
            "padrao_demanda": it.get("padrao_demanda"),
            "metodo_reposicao": it.get("metodo_reposicao"),
            "qtd_itens_grupo": 1,
            "marca_linha": (int(it.get("marca_linha")) if it.get("marca_linha") is not None else None),
            "marcas": [it.get("mar_descricao")] if it.get("mar_descricao") else [],
            "fornecedores": all_forns or [SEM_HIST_COMPRA],
            "estoque_atual": round(estoque, 2),
            "em_transito": round(transito, 2),
            "posicao": round(posicao, 2),
            "ponto_pedido": int(ponto),
            "maximo": int(maximo),
            "qtd_sugerida": qtd,
            "custo_unitario": round(custo_u, 4) if custo_u > 0 else None,
            "valor_estimado": round(qtd * custo_u, 2) if custo_u > 0 else None,
            "criticidade": "Zerado" if estoque <= 0 else "Abaixo do mínimo",
            "deficit": round(ponto - posicao, 2),
            "em_pedido": em_pedido,
            "pedido_qtd": pedido_qtd,
            "pedidos": pedidos_lst,
            "membros": [],
        })

    if not consolidar_grupo:
        for it in items:
            if it.get("sob_encomenda"):
                continue
            _tratar_individual(it)
    else:
        membros = defaultdict(list)
        avulsos = []
        for it in items:
            if it.get("sob_encomenda"):
                continue
            gk = it.get("grupo_chave")
            if gk is not None and _sug_norm(gk) != "":
                membros[_sug_norm(gk)].append(it)
            else:
                avulsos.append(it)

        for gk, mem in membros.items():
            # Regra do mín/máx por tamanho do grupo:
            #  - grupo_qtd_itens == 1 (grupo de UMA marca): não há pooling real, o
            #    consolidado é o próprio item -> usa o mín/máx INDIVIDUAL
            #    (estoque_min_sugerido/estoque_max_sugerido, que chegam como
            #    ponto_pedido/maximo) e a memória individual, roteando pelo
            #    caminho de item avulso (mantém valor e memória coerentes).
            #  - grupo_qtd_itens  > 1: usa o consolidado (pooled) do grupo.
            qtd_itens_grp = max((int(_sug_float(m.get("grupo_qtd_itens"))) for m in mem),
                                default=len(mem)) or len(mem)
            if qtd_itens_grp <= 1:
                for m in mem:
                    _tratar_individual(m, permitir_ponto_zero=True)
                continue
            # grupo_estoque_min/max são iguais para todos os membros (vêm do merge); usa o maior por segurança
            maximo = max((_sug_float(m.get("grupo_estoque_max")) for m in mem), default=0.0)
            ponto = max((_sug_float(m.get("grupo_estoque_min")) for m in mem), default=0.0)
            if maximo <= 0:
                continue
            gsgr = next((m.get("sgr_descricao") for m in mem if m.get("sgr_descricao")), None)
            if not _sub_ok(gsgr):
                continue

            estoque_total = transito_total = 0.0
            membros_det = []
            sup_qty = defaultdict(float)   # fornecedor -> qtd comprada (grupo todo)
            for m in mem:
                cod, est, tr = _sug_posicao(m, stock_map, usar_rt)
                estoque_total += est
                transito_total += tr
                h = _forns_hist(cod)
                for n, qq in h:
                    sup_qty[n] += qq
                membros_det.append({
                    "pro_codigo": cod,
                    "marca": m.get("mar_descricao"),
                    "classe_xyz": m.get("classe_xyz"),
                    # de quem MAIS COMPRAMOS essa marca (histórico); todos abaixo
                    "fornecedor": (h[0][0] if h else SEM_HIST_COMPRA),
                    "fornecedores_hist": [n for n, _ in h],
                    "estoque_atual": round(est, 2),
                    "em_transito": round(tr, 2),
                    "valor_vendido_12m": _sug_float(m.get("valor_vendido_12m")),
                    "demanda_media_dia_ajustada": _sug_float(m.get("demanda_media_dia_ajustada")),
                    # composição do cálculo (memória)
                    "demanda_real_dia": _sug_float(m.get("demanda_real_dia")),
                    "custo_unitario": _sug_float(m.get("custo_unitario")),
                    "min_ind": int(_sug_float(m.get("ponto_pedido"))),
                    "max_ind": int(_sug_float(m.get("maximo"))),
                })
            posicao = estoque_total + transito_total
            cods_grp = [d["pro_codigo"] for d in membros_det]
            em_pedido, pedido_qtd, pedidos_lst = _pedidos_do(cods_grp)
            # gatilho pelo ESTOQUE REAL do grupo (sem o em pedido); cobertos por
            # pedido não somem — ver comentário em _tratar_individual.
            if estoque_total > ponto:
                continue
            qtd = int(math.ceil(maximo - posicao))
            if qtd < 0:
                qtd = 0
            if qtd <= 0 and not em_pedido:
                continue

            # atributos do grupo (compartilhados entre membros)
            curva_item = next((_sug_norm(m.get("grupo_curva")) for m in mem if m.get("grupo_curva")), "")
            padrao = next((m.get("grupo_padrao") for m in mem if m.get("grupo_padrao")), None)
            metodo = next((m.get("grupo_metodo") for m in mem if m.get("grupo_metodo")), None)
            if not _passa_filtros_comuns(curva_item, estoque_total):
                continue
            if marcas_filtro and not any(
                    _sug_norm(m.get("mar_descricao")).upper() in marcas_filtro for m in mem):
                continue

            # ordena membros por relevância (vendas) p/ marca primária
            membros_det.sort(key=lambda x: (-x["valor_vendido_12m"], -x["demanda_media_dia_ajustada"],
                                            -x["estoque_atual"]))
            # fornecedores exibidos = quem mais nos vendeu de CADA marca (distinto)
            forns_ord, seen_f = [], set()
            marcas_ord, seen_m = [], set()
            for d in membros_det:
                f = d["fornecedor"]
                if f and f not in seen_f:
                    seen_f.add(f); forns_ord.append(f)
                mk = d.get("marca")
                if mk and mk not in seen_m:
                    seen_m.add(mk); marcas_ord.append(mk)
            # classe do grupo = a da marca principal (não há XYZ consolidado na análise)
            classe_grp = next((d["classe_xyz"] for d in membros_det if d.get("classe_xyz")), None)
            # bucket padrão = fornecedor de quem MAIS COMPRAMOS no grupo inteiro
            primario = max(sup_qty.items(), key=lambda kv: kv[1])[0] if sup_qty else SEM_HIST_COMPRA
            todos_forns_grupo = set(sup_qty.keys())  # p/ filtro: tudo já comprado no grupo

            # filtro/bucket: se filtrado, casa contra TUDO que já compramos do
            # grupo — inclusive os fornecedores RELACIONADOS (matriz/filiais)
            if fornecedor:
                casado = next((n for n in sorted(todos_forns_grupo) if _forn_match(n, fornecedor.lower())), None)
                if not casado:
                    continue
                bucket = casado
            else:
                bucket = primario
            if bucket == SEM_HIST_COMPRA and not incluir_sem_historico:
                continue  # grupo sem histórico de compra -> fora da lista
            bucket = _canon_forn(bucket)  # vinculados aparecem sob o PRINCIPAL

            sgr = gsgr
            # custo unitário do grupo = média ponderada pela demanda das marcas
            _c_num = sum(d["custo_unitario"] * max(d["demanda_media_dia_ajustada"], 0.0)
                         for d in membros_det if d["custo_unitario"] > 0)
            _c_den = sum(max(d["demanda_media_dia_ajustada"], 0.0)
                         for d in membros_det if d["custo_unitario"] > 0)
            _custos_pos = [d["custo_unitario"] for d in membros_det if d["custo_unitario"] > 0]
            custo_grp = (_c_num / _c_den) if _c_den > 0 else (
                sum(_custos_pos) / len(_custos_pos) if _custos_pos else 0.0)
            memoria_grp = montar_memoria_calculo(
                escopo="grupo", minimo=ponto, maximo=maximo,
                curva=curva_item, classe=classe_grp, metodo=metodo,
                demanda_dia=next((m.get("grupo_demanda_dia") for m in mem if m.get("grupo_demanda_dia") is not None), None),
                sigma_dia=None,
                z=_Z_POR_CURVA.get(_sug_norm(curva_item).upper()),
                lead_time=(next((m.get("grupo_lead_time_dias") for m in mem if m.get("grupo_lead_time_dias")), None)
                           or next((m.get("lead_time_dias") for m in mem if m.get("lead_time_dias")), 17)),
                ss=next((m.get("grupo_estoque_seguranca") for m in mem if m.get("grupo_estoque_seguranca") is not None), None),
                fator_sazonal=next((m.get("grupo_fator_sazonal") for m in mem if m.get("grupo_fator_sazonal") is not None), None),
                sgr_codigo=next((m.get("sgr_codigo") for m in mem if m.get("sgr_codigo") is not None), None),
                msize=next((m.get("grupo_mean_size") for m in mem if m.get("grupo_mean_size") is not None), None),
                cv2=next((m.get("grupo_cv2") for m in mem if m.get("grupo_cv2") is not None), None),
                periodo_revisao=next((m.get("periodo_revisao_dias") for m in mem if m.get("periodo_revisao_dias")), None),
                fornecedor_lt=primario if primario != SEM_HIST_COMPRA else None,
                membros=[{"marca": d["marca"], "pro_codigo": d["pro_codigo"],
                          "demanda_dia": round(d["demanda_real_dia"], 4),
                          "min_ind": d["min_ind"], "max_ind": d["max_ind"]}
                         for d in membros_det],
            )
            dem_grp = _sug_float(next((m.get("grupo_demanda_dia") for m in mem
                                       if m.get("grupo_demanda_dia") is not None), None))
            _registrar(bucket, {
                "tipo": "grupo",
                "memoria": memoria_grp,
                "demanda_dia": round(dem_grp, 4),
                "cobertura_dias": (round(posicao / dem_grp, 0) if dem_grp > 0 else None),
                "outras_linhas": _outras_linhas(mem[0].get("pro_descricao"),
                                                next((m.get("marca_linha") for m in mem
                                                      if m.get("marca_linha") is not None), None)),
                "grupo_chave": gk,
                "pro_codigo": membros_det[0]["pro_codigo"],  # representativo (maior venda)
                "pro_descricao": gk,
                "marca": marcas_ord[0] if marcas_ord else None,
                "subgrupo": sgr,
                "curva_abc": curva_item,
                "classe_xyz": classe_grp,
                "padrao_demanda": padrao,
                "metodo_reposicao": metodo,
                "qtd_itens_grupo": len(mem),
                "marca_linha": next((int(m.get("marca_linha")) for m in mem
                                     if m.get("marca_linha") is not None), None),
                "marcas": marcas_ord,
                "fornecedores": forns_ord,
                "estoque_atual": round(estoque_total, 2),
                "em_transito": round(transito_total, 2),
                "posicao": round(posicao, 2),
                "ponto_pedido": int(ponto),
                "maximo": int(maximo),
                "qtd_sugerida": qtd,
                "custo_unitario": round(custo_grp, 4) if custo_grp > 0 else None,
                "valor_estimado": round(qtd * custo_grp, 2) if custo_grp > 0 else None,
                "criticidade": "Zerado" if estoque_total <= 0 else "Abaixo do mínimo",
                "deficit": round(ponto - posicao, 2),
                "em_pedido": em_pedido,
                "pedido_qtd": pedido_qtd,
                "pedidos": pedidos_lst,
                "membros": membros_det,
            })

        for it in avulsos:
            _tratar_individual(it)

    fornecedores = []
    for f, its in grupos.items():
        its.sort(key=lambda x: (ordem_curva.get(x["curva_abc"], 9), -x["deficit"]))
        valor_total = round(sum(x["valor_estimado"] or 0.0 for x in its), 2)
        itens_sem_custo = sum(1 for x in its if not x.get("valor_estimado"))
        pf = params_forn.get(_sug_norm(f).upper()) or {}
        ped_min = pf.get("pedido_minimo_valor")
        ped_min_qtd = pf.get("pedido_minimo_qtd")
        qtd_total = sum(x["qtd_sugerida"] for x in its)
        fornecedores.append({
            "fornecedor": f,
            "qtd_itens": len(its),
            "qtd_total_sugerida": qtd_total,
            "valor_total_estimado": valor_total,
            "itens_sem_custo": itens_sem_custo,
            "lead_time_dias": pf.get("lead_time_dias"),
            "tempo_revisao_dias": pf.get("tempo_revisao_dias"),
            "pedido_minimo_valor": ped_min,
            "pedido_minimo_qtd": ped_min_qtd,
            # só alerta quando há custo para comparar (valor_total > 0)
            "abaixo_pedido_minimo": bool(ped_min and valor_total > 0 and valor_total < ped_min),
            # mínimo em QUANTIDADE: compara a soma das unidades sugeridas
            "abaixo_pedido_minimo_qtd": bool(ped_min_qtd and qtd_total > 0 and qtd_total < ped_min_qtd),
            "itens": its,
        })
    fornecedores.sort(key=lambda x: -x["qtd_itens"])

    return {
        "modo": "grupo" if consolidar_grupo else "individual",
        "total_itens": sum(g["qtd_itens"] for g in fornecedores),
        "total_fornecedores": len(fornecedores),
        "valor_total_geral": round(sum(g["valor_total_estimado"] for g in fornecedores), 2),
        "estoque_realtime": usar_rt,
        "fornecedores": fornecedores,
    }


# Status de pedido que ainda "seguram" quantidade a caminho e por isso CONTAM
# contra a necessidade de compra (levam em conta o que já está em pedido).
# TODOS contam a QUANTIDADE CHEIA do pedido: a mercadoria só entra no estoque do
# ERP quando o pedido vira 'Entregue(*)' — e esse fica FORA desta lista. Enquanto
# está Aguardando/Liberado/Faturado/Em Trânsito, a mercadoria ainda NÃO chegou
# aqui nem entrou no estoque, então conta como "vindo" e reduz o que falta comprar
# (não se desconta NF vínculo, que é conferência, não entrada de estoque).
# 'Cancelado'/'Vínculo sugerido' não contam.
# Itens com status_item='nao_atendido' (fornecedor não vai entregar) são excluídos.
PEDIDO_STATUS_EM_ABERTO = (
    'Aguardando analise', 'Liberado', 'Faturado', 'Faturado parcialmente',
    'Em Trânsito', 'Em Trânsito parcialmente',
)


def _sql_in_list(vals):
    """Monta a lista literal para um IN (...) de T-SQL/PG a partir de constantes."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)


def _carregar_pedidos_por_produto(conn):
    """
    Para cada produto (pro_codigo), os PEDIDOS em aberto em que ele aparece, com
    o status do pedido e a quantidade PEDIDA (cheia).

    Mesma regra de `em_transito`: statuses de PEDIDO_STATUS_EM_ABERTO e exclui os
    itens 'nao_atendido'. NÃO desconta NF vínculo — a mercadoria destes pedidos
    ainda não entrou no estoque (só entra em 'Entregue', fora da lista). Usado para
    exibir na tela "Comprar agora" o nº do pedido, o status e a qtd em pedido de
    cada item (sem sumir com o item).

    Retorna: { pro_codigo(str) -> [ {numero, status, qtd}, ... ] }
    """
    status_in = _sql_in_list(PEDIDO_STATUS_EM_ABERTO)
    sql = text(f"""
        SELECT p.pedido_cotacao AS numero, p.status AS status,
               i.pro_codigo::text AS pro_codigo, SUM(i.quantidade) AS qtd
        FROM com_pedido p
        JOIN com_pedido_itens i ON i.pedido_id = p.id
        WHERE p.status IN ({status_in})
          AND COALESCE(i.status_item, '') <> 'nao_atendido'
        GROUP BY p.id, p.pedido_cotacao, p.status, i.pro_codigo::text
        HAVING SUM(i.quantidade) > 0
    """)
    out = {}
    for r in conn.execute(sql).mappings().all():
        cod = str(r["pro_codigo"])
        out.setdefault(cod, []).append({
            "numero": r["numero"],
            "status": r["status"],
            "qtd": float(r["qtd"] or 0),
        })
    return out


def _carregar_itens_sugestao(conn):
    """Lê as linhas base da última análise + em trânsito (tolerante a colunas novas ausentes)."""
    existentes = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name='com_fifo_completo'"
    ))}

    def opt(c):
        return c if c in existentes else f"NULL AS {c}"

    _STATUS_EM_ABERTO_IN = _sql_in_list(PEDIDO_STATUS_EM_ABERTO)

    sql = text(f"""
        WITH base AS (
            SELECT pro_codigo, pro_descricao, mar_descricao, sgr_descricao, sgr_codigo, fornecedor1,
                   curva_abc, {opt('classe_xyz')}, {opt('padrao_demanda')}, {opt('metodo_reposicao')},
                   COALESCE(estoque_min_sugerido,0) AS ponto_pedido,
                   COALESCE(estoque_max_sugerido,0) AS maximo,
                   COALESCE(estoque_disponivel,0)   AS estoque_snapshot,
                   demanda_media_dia_ajustada,
                   {opt('valor_vendido_12m')},
                   {opt('demanda_planejamento_dia')}, {opt('demanda_real_dia')},
                   {opt('sigma_demanda_dia')}, {opt('nivel_servico_z')}, {opt('lead_time_dias')},
                   {opt('estoque_seguranca')}, {opt('fator_sazonal')},
                   {opt('mean_size_mes')}, {opt('cv2_tamanho')},
                   {opt('custo_unitario')}, {opt('periodo_revisao_dias')}, {opt('fornecedor_principal')},
                   {opt('marca_linha')},
                   {opt('sob_encomenda')}, {opt('eh_original')},
                   {opt('teve_outlier_aparado')}, {opt('outlier_qtd_aparada')}, {opt('outlier_motivo')},
                   {opt('grupo_chave')}, {opt('grupo_qtd_itens')},
                   {opt('grupo_estoque_min')}, {opt('grupo_estoque_max')},
                   {opt('grupo_curva')}, {opt('grupo_padrao')}, {opt('grupo_metodo')},
                   {opt('grupo_demanda_dia')}, {opt('grupo_estoque_seguranca')}, {opt('grupo_fator_sazonal')},
                   {opt('grupo_mean_size')}, {opt('grupo_cv2')}, {opt('grupo_lead_time_dias')}
            FROM com_fifo_completo
            WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
        ),
        pedido AS (
            -- Quantidade CHEIA em pedido aberto (mercadoria a caminho). Não se
            -- desconta NF vínculo: a mercadoria só entra no estoque em 'Entregue'
            -- (fora da lista de status). Exclui itens 'nao_atendido'.
            SELECT i.pro_codigo::text AS pro_codigo, SUM(i.quantidade) AS qtd_ped
            FROM com_pedido p
            JOIN com_pedido_itens i ON i.pedido_id = p.id
            WHERE p.status IN ({_STATUS_EM_ABERTO_IN})
              AND COALESCE(i.status_item, '') <> 'nao_atendido'
            GROUP BY i.pro_codigo::text
        )
        SELECT b.*,
               COALESCE(ped.qtd_ped, 0) AS em_transito
        FROM base b
        LEFT JOIN pedido ped ON ped.pro_codigo = b.pro_codigo::text
    """)
    return [dict(r) for r in conn.execute(sql).mappings().all()]


def _codigos_candidatos(items, historico, curva, subgrupo, fornecedor, consolidar_grupo,
                        marca=None):
    """
    Códigos de produto que passam nos filtros que NÃO dependem de estoque
    (curva, subgrupo, fornecedor-histórico, marca), respeitando os grupos.
    Usado para buscar o estoque realtime SÓ desses itens (em vez dos ~45k).
    No fim, EXPANDE para os produtos de mesma descrição (outras linhas de
    marca), para o detalhe "outras linhas" também mostrar estoque realtime.
    """
    curvas = [c.strip().upper() for c in curva.split(",")] if curva else None
    subs = [s.strip().lower() for s in subgrupo.split(",")] if subgrupo else None
    forn = fornecedor.lower() if fornecedor else None
    marcas = ([m.strip().upper() for m in marca.split(",") if m.strip()] if marca else None)
    historico = historico or {}

    def marca_ok(it):
        return (not marcas) or (_sug_norm(it.get("mar_descricao")).upper() in marcas)

    def _expandir_mesma_descricao(codes_sel):
        """Inclui os produtos que dividem a descrição com algum selecionado."""
        if not codes_sel:
            return codes_sel
        desc_de = {}
        por_desc = {}
        for it in items:
            if it.get("sob_encomenda"):
                continue
            c = _sug_norm(it.get("pro_codigo"))
            d = _sug_norm(it.get("pro_descricao")).upper()
            if c and d:
                desc_de[c] = d
                por_desc.setdefault(d, set()).add(c)
        extra = set()
        for c in codes_sel:
            d = desc_de.get(c)
            if d:
                extra |= por_desc[d]
        return codes_sel | extra

    # busca por fornecedor respeita o GRUPO (matriz/filiais): o termo casa com o
    # nome do histórico OU com qualquer fornecedor relacionado a ele
    _grupos_f = get_grupos_fornecedor() if forn else {}

    def _forn_nome_ok(n):
        if forn in n.lower():
            return True
        return any(forn in m.lower() for m in _grupos_f.get(_sug_norm(n).upper(), []))

    def forn_ok(cod):
        return (not forn) or any(_forn_nome_ok(n) for n, _ in historico.get(_sug_norm(cod), []))

    def sub_ok(sgr):
        return (not subs) or (_sug_norm(sgr).lower() in subs)

    codes = set()
    if not consolidar_grupo:
        for it in items:
            if it.get("sob_encomenda"):
                continue
            if curvas and _sug_norm(it.get("curva_abc")).upper() not in curvas:
                continue
            if not sub_ok(it.get("sgr_descricao")):
                continue
            if not forn_ok(it.get("pro_codigo")):
                continue
            if not marca_ok(it):
                continue
            codes.add(_sug_norm(it.get("pro_codigo")))
        return _expandir_mesma_descricao(codes)

    from collections import defaultdict
    membros = defaultdict(list)
    avulsos = []
    for it in items:
        if it.get("sob_encomenda"):
            continue
        gk = it.get("grupo_chave")
        if gk is not None and _sug_norm(gk) != "":
            membros[_sug_norm(gk)].append(it)
        else:
            avulsos.append(it)
    for gk, mem in membros.items():
        gcurva = next((_sug_norm(m.get("grupo_curva")).upper() for m in mem if m.get("grupo_curva")), "")
        gsgr = next((m.get("sgr_descricao") for m in mem if m.get("sgr_descricao")), None)
        if curvas and gcurva not in curvas:
            continue
        if not sub_ok(gsgr):
            continue
        if forn and not any(forn_ok(m.get("pro_codigo")) for m in mem):
            continue
        if marcas and not any(marca_ok(m) for m in mem):
            continue
        for m in mem:
            codes.add(_sug_norm(m.get("pro_codigo")))
    for it in avulsos:
        if curvas and _sug_norm(it.get("curva_abc")).upper() not in curvas:
            continue
        if not sub_ok(it.get("sgr_descricao")):
            continue
        if not forn_ok(it.get("pro_codigo")):
            continue
        if not marca_ok(it):
            continue
        codes.add(_sug_norm(it.get("pro_codigo")))
    return _expandir_mesma_descricao(codes)


def _gerar_sugestao_compra(fornecedor=None, curva=None, subgrupo=None, marca=None,
                           apenas_zerados=False, usar_estoque_realtime=True,
                           consolidar_grupo=True, incluir_sem_historico=False):
    """
    Monta o payload da sugestão de compra (mesma lógica do endpoint /compras/sugestao).
    Extraído para ser reutilizado pela rota de PDF.
    """
    conn = get_db_connection()
    try:
        items = _carregar_itens_sugestao(conn)
        pedidos_map = _carregar_pedidos_por_produto(conn)
    finally:
        conn.close()

    historico = {}
    try:
        historico = get_compras_historico()
    except Exception as e:
        print(f"AVISO: histórico de compra indisponível (usando vazio). {e}")

    # ESTRATÉGIA: aplica os filtros que NÃO dependem de estoque (curva/subgrupo/
    # fornecedor) e busca o estoque realtime SÓ dos itens filtrados. Sem filtro,
    # cai no "todos" (com timeout rígido + fallback pro snapshot).
    tem_filtro = bool(curva or subgrupo or fornecedor or marca)
    stock_map = {}
    if usar_estoque_realtime:
        if tem_filtro:
            codes = _codigos_candidatos(items, historico, curva, subgrupo, fornecedor,
                                        consolidar_grupo, marca=marca)
            try:
                fut = _RT_EXECUTOR.submit(get_realtime_stocks_bulk, codes)
                stock_map = fut.result(timeout=int(os.getenv("STOCK_RT_HARD_TIMEOUT_S") or 20))
            except Exception as e:
                print(f"AVISO: estoque realtime (lote) lento/indisponível ({type(e).__name__}), usando snapshot.")
                stock_map = {}
        else:
            try:
                fut = _RT_EXECUTOR.submit(get_all_realtime_stocks)
                stock_map = fut.result(timeout=int(os.getenv("STOCK_RT_HARD_TIMEOUT_S") or 20))
            except Exception as e:
                print(f"AVISO: estoque realtime lento/indisponível ({type(e).__name__}), usando snapshot.")
                stock_map = {}

    return montar_sugestao_compra(
        items, stock_map,
        historico=historico,
        consolidar_grupo=consolidar_grupo,
        usar_estoque_realtime=usar_estoque_realtime,
        fornecedor=fornecedor,
        curva=curva,
        subgrupo=subgrupo,
        apenas_zerados=apenas_zerados,
        incluir_sem_historico=incluir_sem_historico,
        params_forn=get_fornecedor_parametros(),
        marca=marca,
        grupos_forn=get_grupos_fornecedor(),
        principal_forn=get_fornecedor_principal_map(),
    )




def _render_sugestao_pdf(data, filtros=None):
    """
    Gera o PDF da sugestão de compra a partir do payload de montar_sugestao_compra.
    Uma seção (tabela) por fornecedor. Usa fpdf2 (pura Python, sem deps de sistema).
    """
    from fpdf import FPDF

    filtros = filtros or {}

    def _txt(s):
        # fpdf2 core fonts (Helvetica) são latin-1 — troca o que não couber.
        return str(s if s is not None else "").encode("latin-1", "replace").decode("latin-1")

    def _brl(v):
        if v is None:
            return "-"
        return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def _num(v):
        if v is None:
            return "-"
        return f"{float(v):,.0f}".replace(",", ".")

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.set_margins(10, 10, 10)
    pdf.alias_nb_pages()

    # Colunas: (título, largura mm, alinhamento, chave)
    cols = [
        ("Código", 20, "L", "pro_codigo"),
        ("Descrição", 95, "L", "pro_descricao"),
        ("Marca", 28, "L", "marca"),
        ("Curva", 14, "C", "curva_abc"),
        ("Estoque", 18, "R", "estoque_atual"),
        ("Trânsito", 18, "R", "em_transito"),
        ("Posição", 18, "R", "posicao"),
        ("P.Pedido", 18, "R", "ponto_pedido"),
        ("Máximo", 16, "R", "maximo"),
        ("Sugerido", 18, "R", "qtd_sugerida"),
        ("Vlr Estim.", 24, "R", "valor_estimado"),
    ]
    total_w = sum(c[1] for c in cols)

    def cabecalho_documento():
        pdf.set_font("Helvetica", "B", 15)
        pdf.cell(0, 8, _txt("Sugestão de Compra"), ln=1)
        pdf.set_font("Helvetica", "", 8)
        partes = []
        for rot, ch in (("Fornecedor", "fornecedor"), ("Curva", "curva"),
                        ("Subgrupo", "subgrupo"), ("Marca", "marca")):
            if filtros.get(ch):
                partes.append(f"{rot}: {filtros[ch]}")
        if filtros.get("apenas_zerados"):
            partes.append("Apenas zerados")
        partes.append("Modo: " + ("grupo" if data.get("modo") == "grupo" else "individual"))
        partes.append("Estoque em tempo real" if data.get("estoque_realtime") else "Estoque snapshot")
        pdf.set_text_color(90, 90, 90)
        pdf.cell(0, 5, _txt(" | ".join(partes)), ln=1)
        pdf.cell(0, 5, _txt(
            f"Fornecedores: {data.get('total_fornecedores', 0)}  •  "
            f"Itens: {data.get('total_itens', 0)}  •  "
            f"Valor total estimado: {_brl(data.get('valor_total_geral'))}"), ln=1)
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)

    def cabecalho_tabela():
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.set_fill_color(45, 55, 72)
        pdf.set_text_color(255, 255, 255)
        for titulo, w, _al, _ch in cols:
            pdf.cell(w, 7, _txt(titulo), border=0, align="C", fill=True)
        pdf.ln(7)
        pdf.set_text_color(0, 0, 0)

    pdf.add_page()
    cabecalho_documento()

    fornecedores = data.get("fornecedores") or []
    if not fornecedores:
        pdf.set_font("Helvetica", "I", 11)
        pdf.cell(0, 10, _txt("Nenhum item para comprar com os filtros informados."), ln=1)
        return bytes(pdf.output())

    for forn in fornecedores:
        # Espaço mínimo para o título + cabeçalho da tabela + 1 linha
        if pdf.get_y() + 30 > pdf.h - pdf.b_margin:
            pdf.add_page()

        # Título do fornecedor
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(230, 234, 240)
        info = f"{forn.get('fornecedor', '-')}   ({forn.get('qtd_itens', 0)} itens • {_brl(forn.get('valor_total_estimado'))}"
        alertas = []
        if forn.get("abaixo_pedido_minimo"):
            alertas.append(f"abaixo do mín. R$ {_brl(forn.get('pedido_minimo_valor'))}")
        if forn.get("abaixo_pedido_minimo_qtd"):
            alertas.append(f"abaixo do mín. {_num(forn.get('pedido_minimo_qtd'))} un")
        info += (" • " + "; ".join(alertas)) if alertas else ""
        info += ")"
        pdf.cell(total_w, 7, _txt(info), border=0, align="L", fill=True)
        pdf.ln(7)

        cabecalho_tabela()

        pdf.set_font("Helvetica", "", 7)
        fill = False
        for it in (forn.get("itens") or []):
            # Quebra de página no meio da tabela: repete o cabeçalho
            if pdf.get_y() + 6 > pdf.h - pdf.b_margin:
                pdf.add_page()
                cabecalho_tabela()
                pdf.set_font("Helvetica", "", 7)

            if fill:
                pdf.set_fill_color(244, 246, 249)
            for titulo, w, al, ch in cols:
                v = it.get(ch)
                if ch == "valor_estimado":
                    s = _brl(v)
                elif ch in ("estoque_atual", "em_transito", "posicao",
                            "ponto_pedido", "maximo", "qtd_sugerida"):
                    s = _num(v)
                else:
                    s = _txt(v)
                # Trunca descrição longa
                if ch == "pro_descricao" and pdf.get_string_width(s) > w - 2:
                    while s and pdf.get_string_width(s + "...") > w - 2:
                        s = s[:-1]
                    s = s + "..."
                pdf.cell(w, 5.5, _txt(s), border="B", align=al, fill=fill)
            pdf.ln(5.5)
            fill = not fill

        # Subtotal do fornecedor
        pdf.set_font("Helvetica", "B", 7.5)
        soma_sug = sum((x.get("qtd_sugerida") or 0) for x in (forn.get("itens") or []))
        w_ate_sug = sum(c[1] for c in cols[:-2])
        pdf.cell(w_ate_sug, 6, _txt("Total do fornecedor"), border=0, align="R")
        pdf.cell(cols[-2][1], 6, _txt(_num(soma_sug)), border=0, align="R")
        pdf.cell(cols[-1][1], 6, _txt(_brl(forn.get("valor_total_estimado"))), border=0, align="R")
        pdf.ln(10)

    return bytes(pdf.output())


