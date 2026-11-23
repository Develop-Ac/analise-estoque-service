# -*- coding: utf-8 -*-
"""Fotos da análise de compra — cálculo puro, sem banco.

Cada fase (cotação, pedido, nota) guarda versões da sua análise. Aqui ficam as regras que não
dependem do banco: assinatura (decide se o documento mudou e merece versão nova), motivo da
versão, foto tardia e o recorte da foto de uma fase anterior para os produtos do documento aberto.
"""
import hashlib
import json

import analise_nota as an

# Status do pedido em que ele ainda não foi liberado ao fornecedor.
STATUS_PRE_LIBERACAO = ('Aguardando analise', 'Em analise')


def _num(v):
    """Decimal, int e float viram o mesmo número no hash."""
    return float(v) if v is not None else None


def _pro(v):
    return str(v).strip()


def _assinar(linhas, flag):
    """"<16 hex do sha1 do JSON da lista ordenada>:<flag>"; a ordem das linhas não muda o hash."""
    texto = json.dumps(sorted(linhas, key=json.dumps), sort_keys=True)
    return f"{hashlib.sha1(texto.encode('utf-8')).hexdigest()[:16]}:{1 if flag else 0}"


def assinatura_cotacao(itens):
    """Produto e quantidade de cada item da cotação; flag sempre 0."""
    return _assinar([[_pro(i["pro_codigo"]), _num(i.get("quantidade"))] for i in itens], False)


def assinatura_pedido(cab, itens):
    """Produto, quantidade, valor unitário, IPI e não atendido de cada item, mais frete e
    IPI no valor do cabeçalho. Flag 1 quando o pedido já saiu de análise (liberado em diante)."""
    linhas = [[_pro(i["pro_codigo"]), _num(i.get("quantidade")), _num(i.get("valor_unitario")),
               _num(i.get("ipi")), i.get("status_item") == "nao_atendido"] for i in itens]
    linhas.append(["frete", _num(cab.get("frete")), bool(cab.get("ipi_no_valor"))])
    return _assinar(linhas, cab.get("status") not in STATUS_PRE_LIBERACAO)


def assinatura_nota(analisaveis, lancada):
    """Produto e quantidade dos itens identificados da nota; flag 1 quando lançada na empresa 3."""
    return _assinar([[_pro(i["pro_codigo"]), _num(i.get("quantidade"))] for i in analisaveis], lancada)


def _flag(assinatura):
    return (assinatura or "").rsplit(":", 1)[-1] == "1"


def motivo(fase, anterior, nova):
    """Motivo da versão nova a partir da assinatura anterior (None = primeira versão).
    A virada da flag (liberação do pedido, lançamento da nota) vence a alteração do conteúdo."""
    if not anterior:
        return "criacao"
    if not _flag(anterior) and _flag(nova):
        return "lancamento" if fase == "nota" else "liberacao"
    return "identificacao" if fase == "nota" else "alteracao"


def tardia(versao, criado_doc, agora):
    """Primeira versão gravada mais de 1 dia depois da criação do documento: não há foto da época."""
    if versao != 1 or criado_doc is None:
        return False
    return (agora - criado_doc).total_seconds() > 86400


def produtos_do_conteudo(conteudo):
    """Produtos (pro_codigo) presentes na análise: das unidades e dos não analisados."""
    pros = {_pro(p["pro_codigo"]) for u in conteudo.get("unidades") or [] for p in u.get("produtos") or []}
    # Não analisado da nota pode não ter produto (sem de-para, imobilizado): fica de fora.
    pros |= {_pro(n["pro_codigo"]) for n in conteudo.get("nao_analisados") or [] if n.get("pro_codigo") is not None}
    return pros


def recortar(conteudo, produtos):
    """(conteúdo, recortada): a foto de uma fase anterior limitada aos produtos do documento aberto.

    Fica a unidade inteira que tem algum desses produtos; não analisados filtrados pelo produto.
    Resumo e totais do custo de caixa são refeitos só com o que ficou; o resto passa como está.
    """
    produtos = {_pro(p) for p in produtos}
    unidades = [u for u in conteudo.get("unidades") or []
                if any(_pro(p["pro_codigo"]) in produtos for p in u.get("produtos") or [])]
    nao_analisados = [n for n in conteudo.get("nao_analisados") or [] if n.get("pro_codigo") is not None and _pro(n["pro_codigo"]) in produtos]
    recortada = (len(unidades) != len(conteudo.get("unidades") or [])
                 or len(nao_analisados) != len(conteudo.get("nao_analisados") or []))
    if not recortada:
        return conteudo, False
    valor = sum(u.get("valor_nota") or 0 for u in unidades) + sum(n.get("valor") or 0 for n in nao_analisados)
    saida = {**conteudo, "unidades": unidades, "nao_analisados": nao_analisados,
             "resumo": an.montar_resumo(valor, unidades, nao_analisados)}
    if conteudo.get("custo_caixa") is not None:
        saida["custo_caixa"] = {
            **conteudo["custo_caixa"],
            "custo_compra": round(sum(u.get("custo_caixa_compra") or 0 for u in unidades), 2),
            "custo_excesso": round(sum(u.get("custo_caixa_excesso") or 0 for u in unidades), 2),
        }
    return saida, True


_CAMPOS_CONFRONTO = ("maximo", "venda_dia", "estoque_antes", "cobertura_depois",
                     "excesso_gerado_qtd", "excesso_gerado_valor")


def _lado(u):
    return {**{c: u.get(c) for c in _CAMPOS_CONFRONTO}, "qtd": u.get("qtd_nota")}


def confrontar(unidades_foto, unidades_hoje):
    """Unidade a unidade (casadas por `chave`), a análise da foto × a de hoje.

    `parcela_maximo_*` = quanto do excesso de hoje vem só da mudança do máximo: o excesso de hoje
    menos o que hoje daria com o máximo da época (em reais pelo custo unitário de hoje).
    `virou` = a conclusão (gerou excesso ou não) mudou; `causa` diz se foi o máximo (com o máximo
    da época hoje concluiria igual à foto) ou o estoque. Máximo nulo em um dos lados (produto novo,
    sob encomenda): só os números. Unidade que não estava na foto: `epoca` None.
    """
    foto = {u["chave"]: u for u in unidades_foto or []}
    saida = []
    for h in unidades_hoje or []:
        f = foto.get(h["chave"])
        parcela_qtd, parcela_valor, virou, causa = 0, 0, False, None
        if f is not None and f.get("maximo") is not None and h.get("maximo") is not None:
            gerado_h, gerado_f = h["excesso_gerado_qtd"] or 0, f.get("excesso_gerado_qtd") or 0
            com_max_epoca = an.excesso(h["estoque_antes"], h["estoque_depois"], h["qtd_nota"], f["maximo"])[1]
            parcela_qtd = round(gerado_h - com_max_epoca, 4)
            custo_un = h["valor_nota"] / h["qtd_nota"] if h.get("qtd_nota") else 0.0
            parcela_valor = round(parcela_qtd * custo_un, 2)
            virou = (gerado_f > 0) != (gerado_h > 0)
            if virou:
                causa = "maximo" if (com_max_epoca > 0) == (gerado_f > 0) else "estoque"
        saida.append({"chave": h["chave"], "epoca": _lado(f) if f is not None else None, "hoje": _lado(h),
                      "parcela_maximo_qtd": parcela_qtd, "parcela_maximo_valor": parcela_valor,
                      "virou": virou, "causa": causa})

    def soma(us):
        return round(sum(u.get("excesso_gerado_valor") or 0 for u in us), 2)

    return {"resumo": {"excesso_epoca": soma(unidades_foto or []), "excesso_hoje": soma(unidades_hoje or []),
                       "parcela_maximo": round(sum(u["parcela_maximo_valor"] for u in saida), 2)},
            "unidades": saida}


def _excesso_do(conteudo):
    return ((conteudo or {}).get("resumo") or {}).get("excesso_gerado_valor") or 0


def evolucao(fases, hoje):
    """Excesso gerado (R$) da primeira e da última versão de cada fase, já recortadas, e o de hoje.

    Cada fase traz `conteudo` (última versão) e, quando há mais de uma versão, `conteudo_primeira`;
    sem ele a primeira é a própria última.
    """
    saida = [{"fase": f["fase"], "documento": f.get("documento"), "numero": f.get("numero"),
              "primeira": _excesso_do(f.get("conteudo_primeira") or f.get("conteudo")),
              "ultima": _excesso_do(f.get("conteudo"))} for f in fases or []]
    if hoje:
        saida.append({"fase": "hoje", "primeira": _excesso_do(hoje), "ultima": _excesso_do(hoje)})
    return saida
