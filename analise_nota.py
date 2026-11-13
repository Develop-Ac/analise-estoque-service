# -*- coding: utf-8 -*-
"""Análise de compra por nota fiscal — cálculo puro, sem banco.

Lê o XML da NF-e de entrada (cabeçalho, itens com o desembolso de cada um e duplicatas)
e fecha o desembolso dos itens com o total da nota (vNF).
"""
import datetime
import math
import xml.etree.ElementTree as ET

_NS = "{http://www.portalfiscal.inf.br/nfe}"

# Componentes do desembolso por item: soma tudo que sai do caixa por aquele item.
_SOMA = ("vProd", "vFrete", "vSeg", "vOutro", "vIPI", "vICMSST", "vFCPST")


def _txt(el, tag):
    """Texto da primeira tag `tag` dentro de `el` (qualquer profundidade); None se ausente."""
    achado = next(el.iter(_NS + tag), None)
    return achado.text.strip() if achado is not None and achado.text else None


def _num(el, tag):
    v = _txt(el, tag)
    return float(v) if v else 0.0


def ler_xml(xml):
    """Cabeçalho, itens e duplicatas da NF-e. Aceita o XML com ou sem o envelope nfeProc.

    Levanta ValueError se o conteúdo não for uma NF-e completa (XML malformado ou resumo
    resNFe da distribuição, que não tem ide/emit/ICMSTot)."""
    try:
        raiz = ET.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    except ET.ParseError as e:
        raise ValueError("XML malformado") from e
    ide = next(raiz.iter(_NS + "ide"), None)
    emit = next(raiz.iter(_NS + "emit"), None)
    icms_tot = next(raiz.iter(_NS + "ICMSTot"), None)
    if ide is None or emit is None or icms_tot is None:
        raise ValueError("não é uma NF-e completa")
    dh_emi = _txt(ide, "dhEmi") or _txt(ide, "dEmi")
    cabecalho = {
        "emitente": _txt(emit, "xNome"),
        "cnpj": _txt(emit, "CNPJ") or _txt(emit, "CPF"),
        "numero": _txt(ide, "nNF"),
        "emissao": datetime.date.fromisoformat(dh_emi[:10]) if dh_emi else None,
        "valor_total": _num(icms_tot, "vNF"),
    }

    itens = []
    for det in raiz.iter(_NS + "det"):
        # Item de verdade tem nItem; há XML na conciliação com um <infNFe><det><prod/> vazio após o protNFe.
        if det.get("nItem") is None:
            continue
        prod = det.find(_NS + "prod")
        desembolso = sum(_num(det, t) for t in _SOMA) - _num(prod, "vDesc")
        itens.append({
            "n_item": int(det.get("nItem")),
            "cod_fornecedor": _txt(prod, "cProd"),
            "descricao": _txt(prod, "xProd"),
            "unidade": _txt(prod, "uCom"),
            "quantidade": _num(prod, "qCom"),
            "cfop": _txt(prod, "CFOP"),
            "desembolso": round(desembolso, 2),
        })

    # dVenc é opcional no leiaute; duplicata sem vencimento fica fora do prazo e do fluxo.
    duplicatas = [
        {
            "numero": _txt(dup, "nDup"),
            "vencimento": datetime.date.fromisoformat(_txt(dup, "dVenc")[:10]),
            "valor": _num(dup, "vDup"),
        }
        for dup in raiz.iter(_NS + "dup")
        if _txt(dup, "dVenc")
    ]
    return {"cabecalho": cabecalho, "itens": itens, "duplicatas": duplicatas}


def ratear_desembolso(itens, valor_total):
    """Fecha a soma dos desembolsos com o vNF.

    A diferença (desconto/acréscimo só no total, arredondamento) é distribuída proporcional
    ao desembolso de cada item; o resíduo de centavo vai no item de maior valor.
    Altera e devolve `itens`; a soma final em centavos é exatamente o vNF. Sem nenhum item com
    valor não há base de rateio: os itens ficam zerados (o total não vira valor de mercadoria).
    """
    if not itens:
        return itens
    centavos = [round(i["desembolso"] * 100) for i in itens]
    alvo = round(valor_total * 100)
    base = sum(centavos)
    if base and base != alvo:
        centavos = [c + (alvo - base) * c // base for c in centavos]
        maior = max(range(len(itens)), key=lambda k: centavos[k])
        centavos[maior] += alvo - sum(centavos)
    for item, c in zip(itens, centavos):
        item["desembolso"] = c / 100
    return itens


MOTIVO_SEM_PRODUTO = "produto não identificado"
MOTIVO_IMOBILIZADO = "imobilizado ou uso e consumo"
AVISO_SEM_FATOR = "sem fator de conversão: quantidade do XML"
# CFOP de entrada de imobilizado (x.551, x.406) e uso e consumo (x.556, x.407).
_CFOP_FORA = ("551", "556", "406", "407")


def norm_cprod(s):
    """Código do produto no fornecedor sem espaços, maiúsculo e sem zeros à esquerda
    (mesma regra do normCprod do compras-service)."""
    s = "".join(str(s or "").split()).upper()
    return (s.lstrip("0") or "0") if s else ""


def resolver_itens(itens_xml, itens_entrada, conciliacao, vinculos, prod_fornecedor, cadastro, lancada):
    """Produto (PRO_CODIGO) e quantidade na unidade de estoque de cada item do XML.

    Ordem, parando no primeiro que resolver: item da entrada na empresa 3 (só nota lançada,
    por n_item) → Verificação do Produto (n_item) → vínculo com pedido (cProd normalizado) →
    PRODUTOS_FORNECEDOR_NFE (cProd normalizado; mais de um produto → o primeiro do cadastro).
    `cadastro` = {pro_codigo(str): {COMERCIALIZAVEL, PRO_DESCRICAO}}; produto fora dele não é
    barrado (cadastro fora do ar não derruba os itens).
    Devolve (analisáveis [{n_item, pro_codigo, descricao, quantidade, desembolso, aviso?}],
    não analisados [{n_item, descricao, valor, motivo}]).
    """
    analisaveis, nao_analisados = [], []
    for it in itens_xml:
        n = it["n_item"]
        cprod = norm_cprod(it.get("cod_fornecedor"))
        candidatos = prod_fornecedor.get(cprod) or []
        entrada = itens_entrada.get(n) if lancada else None
        pro = None
        if entrada and entrada.get("PRO_CODIGO") is not None:
            pro = entrada["PRO_CODIGO"]
        elif conciliacao.get(n) is not None:
            pro = conciliacao[n]
        elif vinculos.get(cprod) is not None:
            pro = vinculos[cprod]
        elif candidatos:
            pf = next((c for c in candidatos if str(c["PRO_CODIGO"]) in cadastro), candidatos[0])
            pro = pf["PRO_CODIGO"]
        pro = str(pro).strip() if pro is not None else None

        cad = cadastro.get(pro) or {}
        cfop = str((entrada or {}).get("CFOP_NOTA") or "").strip()
        motivo = None
        if not pro:
            motivo = MOTIVO_SEM_PRODUTO
        elif (cad and str(cad.get("COMERCIALIZAVEL") or "").strip() != "S") or (lancada and cfop[-3:] in _CFOP_FORA):
            motivo = MOTIVO_IMOBILIZADO
        if motivo:
            nao_analisados.append({"n_item": n, "descricao": it.get("descricao"),
                                   "valor": it["desembolso"], "motivo": motivo})
            continue

        item = {"n_item": n, "pro_codigo": pro, "descricao": cad.get("PRO_DESCRICAO") or it.get("descricao"),
                "desembolso": it["desembolso"]}
        if entrada and entrada.get("PRO_CODIGO") is not None:
            item["quantidade"] = float(entrada.get("QUANTIDADE") or 0)
        else:
            # Fator do par (fornecedor, cProd) para o produto achado, venha ele de qualquer passo.
            fator = next((_f(c.get("FATOR_CONVERSAO")) for c in candidatos if str(c["PRO_CODIGO"]).strip() == pro), None)
            if fator:
                item["quantidade"] = float(it["quantidade"] or 0) * fator
            else:
                item["quantidade"] = float(it["quantidade"] or 0)
                item["aviso"] = AVISO_SEM_FATOR
        analisaveis.append(item)
    return analisaveis, nao_analisados


def _f(v):
    return float(v) if v is not None else None


def montar_unidades(itens, rodada, saldos, lancada, a_caminho=None):
    """Agrupa os itens analisáveis por unidade de análise e calcula estoque, cobertura e excesso.

    Unidade = o grupo (`grupo_chave` com mais de um membro na rodada) ou o produto sozinho.
    `rodada` traz todos os membros dos grupos; o saldo da unidade soma todos eles (saldo
    negativo de um produto conta zero). Nota lançada já está no saldo: antes = saldo − qtd.
    `a_caminho` = {pro_codigo: qtd} de outros pedidos em aberto (análise do pedido): somado
    aos membros, entra no estoque antes e depois da compra e sai em `a_caminho` na unidade;
    None (nota) deixa a conta e a saída sem esse campo.
    Devolve as unidades ordenadas pelo valor do excesso gerado (maior primeiro), sem custo de caixa.
    """
    unidades = {}
    for it in itens:
        pro = str(it["pro_codigo"])
        r = rodada.get(pro)
        em_grupo = bool(r and r.get("grupo_chave") and (r.get("grupo_qtd_itens") or 0) > 1)
        chave = r["grupo_chave"] if em_grupo else pro
        descricao = (r or {}).get("descricao") or it.get("descricao")
        u = unidades.setdefault(chave, {
            "tipo": "grupo" if em_grupo else "produto", "chave": chave, "rotulo": descricao,
            "produtos": [], "qtd": 0.0, "desembolso": 0.0, "sob_encomenda": False, "avisos": [],
            "maximo": _f(r.get("grupo_estoque_max" if em_grupo else "estoque_max_sugerido")) if r else None,
            "venda_dia": _f(r.get("grupo_demanda_dia" if em_grupo else "demanda_real_dia")) if r else None,
        })
        if pro not in {p["pro_codigo"] for p in u["produtos"]}:
            u["produtos"].append({"pro_codigo": pro, "descricao": descricao})
        u["qtd"] += float(it["quantidade"] or 0)
        u["desembolso"] += float(it["desembolso"] or 0)
        u["sob_encomenda"] = u["sob_encomenda"] or bool(r and r.get("sob_encomenda"))
        if it.get("aviso") and it["aviso"] not in u["avisos"]:
            u["avisos"].append(it["aviso"])

    saida = []
    for u in unidades.values():
        if u["tipo"] == "grupo":
            membros = [p for p, r in rodada.items() if r.get("grupo_chave") == u["chave"]]
        else:
            membros = [u["chave"]]
        agora = sum(max(float(saldos.get(p) or 0), 0.0) for p in membros)
        qtd = u["qtd"]
        caminho = sum(float(a_caminho.get(p) or 0) for p in membros) if a_caminho is not None else 0.0
        antes, depois = (max(agora - qtd, 0.0), agora) if lancada else (agora + caminho, agora + caminho + qtd)
        venda = u["venda_dia"] or 0.0
        maximo = u["maximo"]
        selos = []
        if maximo is None:
            existente = gerado = 0.0
            selos.append("sob_encomenda" if u["sob_encomenda"] else "produto_novo")
        else:
            existente = max(antes - maximo, 0.0)
            gerado = min(max(depois - maximo, 0.0) - existente, qtd)
        if venda <= 0:
            selos.append("sem_giro")

        def dias(estoque):
            return round(estoque / venda, 1) if venda > 0 else None

        custo_un = u["desembolso"] / qtd if qtd else 0.0
        unidade = {
            "tipo": u["tipo"], "chave": u["chave"], "rotulo": u["rotulo"], "produtos": u["produtos"],
            "qtd_nota": round(qtd, 4), "valor_nota": round(u["desembolso"], 2),
            "estoque_antes": round(antes, 4), "estoque_depois": round(depois, 4),
            "maximo": maximo, "venda_dia": round(venda, 4),
            "cobertura_depois": dias(depois), "sai_de": dias(antes), "sai_ate": dias(depois),
            "excesso_existente": round(existente, 4), "excesso_gerado_qtd": round(gerado, 4),
            "excesso_gerado_valor": round(gerado * custo_un, 2),
            "selos": selos, "avisos": u["avisos"],
        }
        if a_caminho is not None:
            unidade["a_caminho"] = round(caminho, 4)
        saida.append(unidade)
    saida.sort(key=lambda x: x["excesso_gerado_valor"], reverse=True)
    return saida


def prazo_medio(vencimentos, data_base):
    """Prazo médio de pagamento em dias, ponderado pelo valor; vencido conta 0; sem título = 0."""
    total = sum(float(v["valor"] or 0) for v in vencimentos)
    if not total:
        return 0.0
    dias = sum(max((v["vencimento"] - data_base).days, 0) * float(v["valor"] or 0) for v in vencimentos)
    return round(dias / total, 1)


AVISO_SEM_GIRO = "sem giro: simulado em 12 meses"


def aplicar_custo_caixa(unidades, prazo_medio_dias, taxa_mes, dias_teto):
    """Custo do dinheiro parado em estoque (juros simples) além do prazo do fornecedor.

    Dias parado da compra = (estoque antes + metade da nota) ÷ venda/dia; do excesso =
    (estoque depois − metade do excesso gerado) ÷ venda/dia; ambos limitados a `dias_teto`
    (unidade sem giro fica no teto). Custo = valor × taxa ao mês × dias além do prazo ÷ 30.
    Grava `custo_caixa_compra`/`custo_caixa_excesso` em cada unidade e devolve o bloco `custo_caixa`.
    """
    def custo(valor, dias):
        return valor * taxa_mes * max(dias - prazo_medio_dias, 0) / 30

    total_compra = total_excesso = 0.0
    for u in unidades:
        venda = u["venda_dia"] or 0.0
        if venda > 0:
            dias_compra = min((u["estoque_antes"] + u["qtd_nota"] / 2) / venda, dias_teto)
            dias_excesso = min((u["estoque_depois"] - u["excesso_gerado_qtd"] / 2) / venda, dias_teto)
        else:
            dias_compra = dias_excesso = dias_teto
            if AVISO_SEM_GIRO not in u["avisos"]:
                u["avisos"].append(AVISO_SEM_GIRO)
        c_compra = custo(u["valor_nota"], dias_compra)
        c_excesso = custo(u["excesso_gerado_valor"], dias_excesso)
        u["custo_caixa_compra"] = round(c_compra, 2)
        u["custo_caixa_excesso"] = round(c_excesso, 2)
        total_compra += c_compra
        total_excesso += c_excesso
    return {
        "taxa_mes": taxa_mes, "dias_teto": dias_teto, "prazo_medio_dias": prazo_medio_dias,
        "custo_compra": round(total_compra, 2), "custo_excesso": round(total_excesso, 2),
    }


def montar_resumo(valor_nota, unidades, nao_analisados):
    """Bloco `resumo`: quanto da nota foi analisado, excesso gerado, custos e valores à parte.

    O que fica fora de `unidades` (os `nao_analisados`) é a diferença entre o valor da nota
    e o valor analisado; percentuais são sobre o valor da nota.
    """
    def soma(campo, selo=None):
        return round(sum(u[campo] for u in unidades if selo is None or selo in u["selos"]), 2)

    def perc(v):
        return round(v / valor_nota * 100, 2) if valor_nota else 0.0

    analisado = soma("valor_nota")
    excesso = soma("excesso_gerado_valor")
    return {
        "valor_nota": round(valor_nota, 2), "valor_analisado": analisado,
        "perc_analisado": perc(analisado),
        "excesso_gerado_valor": excesso, "excesso_gerado_perc": perc(excesso),
        "custo_caixa_compra": soma("custo_caixa_compra"),
        "custo_caixa_excesso": soma("custo_caixa_excesso"),
        "valor_produto_novo": soma("valor_nota", "produto_novo"),
        "valor_sob_encomenda": soma("valor_nota", "sob_encomenda"),
    }


def montar_analise(valor_nota, analisaveis, nao_analisados, rodada, saldos, lancada,
                   vencimentos, data_base, base_prazo, comprometido_por_dia, hoje, taxa_mes, dias_teto):
    """Encadeia unidades → custo de caixa → resumo → fluxo e devolve os blocos da resposta
    (tudo menos `nota`). Sem item analisável: unidades vazias e resumo zerado."""
    unidades = montar_unidades(analisaveis, rodada, saldos, lancada)
    custo = aplicar_custo_caixa(unidades, prazo_medio(vencimentos, data_base), taxa_mes, dias_teto)
    custo["base_prazo"] = base_prazo
    return {
        "resumo": montar_resumo(valor_nota, unidades, nao_analisados),
        "unidades": unidades,
        "nao_analisados": nao_analisados,
        "custo_caixa": custo,
        "fluxo": montar_fluxo(vencimentos, comprometido_por_dia, hoje),
    }


def montar_nao_comprados(sugestao, pros_da_nota, fornecedor, aviso=None):
    """Bloco `nao_comprados`: o que a sugestão de compra manda comprar HOJE do fornecedor da nota
    (estoque no ponto de pedido ou abaixo) e não veio nesta nota.

    `sugestao` = resposta de /compras/sugestao filtrada pelo fornecedor; `pros_da_nota` = produtos
    da nota e os membros dos grupos deles — grupo que veio na nota, por qualquer marca, não entra.
    A quantidade a comprar aqui repõe só até o MÍNIMO (ponto de pedido), descontado o que está em
    trânsito — não até o máximo, como na tela Sugestão: é o necessário para não faltar, que é o
    que cabe comparar com o excesso da compra. Quem já alcança o mínimo fica fora.
    Zerados primeiro, depois o maior valor a comprar."""
    pros = {str(p).strip() for p in pros_da_nota}
    itens, minimo = [], None
    for f in sugestao.get("fornecedores") or []:
        minimo = minimo or f.get("pedido_minimo_valor")
        for it in f["itens"]:
            codigos = {str(it["pro_codigo"])} | {str(m["pro_codigo"]) for m in it.get("membros") or []}
            falta = float(it["ponto_pedido"] or 0) - max(float(it["estoque_atual"] or 0), 0.0) \
                - float(it["em_transito"] or 0)
            qtd = max(math.ceil(falta), 0)
            if codigos & pros or not qtd:
                continue
            custo = it.get("custo_unitario")
            itens.append({
                "tipo": it["tipo"], "pro_codigo": it["pro_codigo"], "rotulo": it["pro_descricao"],
                "marcas": it.get("marcas") or [], "curva": it.get("curva_abc"),
                "zerado": it["estoque_atual"] <= 0, "estoque": it["estoque_atual"],
                "em_transito": it["em_transito"], "minimo": it["ponto_pedido"], "maximo": it["maximo"],
                "cobertura_dias": it.get("cobertura_dias"),
                "comprar_qtd": qtd, "comprar_valor": round(qtd * custo, 2) if custo else None,
            })
    itens.sort(key=lambda x: (not x["zerado"], -(x["comprar_valor"] or 0)))
    return {
        "fornecedor": fornecedor, "pedido_minimo_valor": minimo, "itens": itens,
        "zerados": sum(1 for i in itens if i["zerado"]),
        "valor_total": round(sum(i["comprar_valor"] or 0 for i in itens), 2),
        "aviso": aviso,
    }


# Cuiabá é UTC−4 fixo (sem horário de verão); evita depender de tzdata na imagem slim.
_CUIABA = datetime.timezone(datetime.timedelta(hours=-4))
AVISO_SEM_VENCIMENTO = "nota sem vencimentos no XML"


def hoje_cuiaba():
    return datetime.datetime.now(_CUIABA).date()


def _a_vencer(vencimentos, hoje):
    return [v for v in vencimentos if not v.get("pago_em") and v["vencimento"] >= hoje]


def janela_semanas(vencimentos, hoje):
    """(segunda da semana de hoje, domingo da semana do último vencimento a vencer); sem nenhum, 8 semanas."""
    inicio = hoje - datetime.timedelta(days=hoje.weekday())
    datas = [v["vencimento"] for v in _a_vencer(vencimentos, hoje)]
    if not datas:
        return inicio, inicio + datetime.timedelta(weeks=8, days=-1)
    ultimo = max(datas)
    return inicio, ultimo + datetime.timedelta(days=6 - ultimo.weekday())


def montar_fluxo(vencimentos, comprometido_por_dia, hoje):
    """Bloco `fluxo`: vencimentos da nota e quadro semanal (segunda a domingo).

    `comprometido_por_dia` = {date: valor} do contas a pagar em aberto, já sem os títulos da
    própria nota. Data anterior a hoje não entra; `esta_nota` soma só títulos não pagos a vencer.
    """
    inicio, fim = janela_semanas(vencimentos, hoje)
    a_vencer = _a_vencer(vencimentos, hoje)
    semanas = []
    seg = inicio
    while seg <= fim:
        dom = seg + datetime.timedelta(days=6)
        ja = sum(float(v or 0) for d, v in comprometido_por_dia.items() if d >= hoje and seg <= d <= dom)
        nota = [float(v["valor"] or 0) for v in a_vencer if seg <= v["vencimento"] <= dom]
        semanas.append({
            "inicio": seg, "fim": dom, "ja_comprometido": round(ja, 2), "esta_nota": round(sum(nota), 2),
            "novo_total": round(ja + sum(nota), 2), "da_nota": bool(nota),
        })
        seg = dom + datetime.timedelta(days=1)
    return {
        "vencimentos": [
            {"numero": v.get("numero"), "vencimento": v["vencimento"], "valor": float(v["valor"] or 0),
             "pago_em": v.get("pago_em")}
            for v in vencimentos
        ],
        "semanas": semanas,
        "avisos": [] if vencimentos else [AVISO_SEM_VENCIMENTO],
    }
