# -*- coding: utf-8 -*-
"""Vínculo MANUAL de similares (botão "Vincular" da tela) aplicado ao grupo_chave.

O grupo consolidado (grupo_chave) nasce de descrição + linha da marca. Quando o
comprador vincula produtos na tela (com_relacionamento_itens, origem='manual'),
as chaves desses produtos são UNIDAS: todos os itens que tinham qualquer uma
delas passam a compartilhar uma única chave (a menor em ordem alfabética, para
ser estável entre rodadas). Itens sem chave (originais, descrição vazia) não
entram — o original continua avulso, como no grupo automático.

Desvincular (origem='avulso') faz o contrário: o produto sai de QUALQUER grupo,
inclusive o da própria descrição, e é planejado sozinho até ser vinculado de novo.
"""
AVULSO = "avulso"


def group_id_avulso(pro_codigo):
    """group_id da linha de um produto desvinculado (a coluna é NOT NULL e única por produto)."""
    return f"{AVULSO}:{str(pro_codigo).strip()}"


def unificar_chaves(pro2chave, vinculos):
    """
    `pro2chave`: {pro_codigo: grupo_chave} dos itens que TÊM chave.
    `vinculos`: lista de listas de pro_codigo vinculados manualmente.
    Retorna {chave_antiga: chave_nova} só para as chaves que mudam.
    """
    pai = {}

    def raiz(k):
        while pai.get(k, k) != k:
            k = pai[k]
        return k

    for membros in vinculos:
        chaves = sorted({pro2chave[str(p).strip()] for p in membros
                         if str(p).strip() in pro2chave})
        for k in chaves[1:]:
            a, b = raiz(chaves[0]), raiz(k)
            if a != b:
                pai[max(a, b)] = min(a, b)

    return {k: raiz(k) for k in pai if raiz(k) != k}


def carregar_vinculos_manuais(engine):
    """Grupos de com_relacionamento_itens com pelo menos um membro de origem 'manual'.
    Sem a coluna `origem` (SQL ainda não aplicado) devolve [] e o grupo segue automático."""
    import pandas as pd
    try:
        df = pd.read_sql(
            """
            SELECT group_id, pro_codigo FROM com_relacionamento_itens
            WHERE group_id IN (SELECT group_id FROM com_relacionamento_itens WHERE origem = 'manual')
            """,
            engine,
        )
    except Exception as e:
        print(f"  - AVISO: vínculos manuais indisponíveis ({e}); grupos só por descrição.")
        return []
    df["pro_codigo"] = df["pro_codigo"].astype(str).str.strip()
    vinculos = [list(g["pro_codigo"]) for _, g in df.groupby("group_id")]
    if vinculos:
        print(f"  - Vínculos manuais: {len(vinculos)} grupos.")
    return vinculos


def carregar_avulsos(engine):
    """Produtos desvinculados na tela (origem 'avulso'): ficam fora de qualquer grupo."""
    import pandas as pd
    try:
        df = pd.read_sql(f"SELECT pro_codigo FROM com_relacionamento_itens WHERE origem = '{AVULSO}'", engine)
    except Exception as e:
        print(f"  - AVISO: desvinculados indisponíveis ({e}).")
        return set()
    avulsos = set(df["pro_codigo"].astype(str).str.strip())
    if avulsos:
        print(f"  - Desvinculados (avulsos): {len(avulsos)} produtos.")
    return avulsos
