# -*- coding: utf-8 -*-
"""Rotina de fotos da análise de compra: thread daemon que, a cada FOTO_INTERVALO_S, grava a foto
das cotações, pedidos e notas da janela que mudaram desde a última foto.

Cada passada compara o que é barato de ler (assinatura dos itens da cotação e do pedido, sinais
da nota) com a última foto de cada documento e só refaz a análise de quem mudou, no máximo
FOTO_LOTE_MAX análises por passada (a nota é pesada: saldo, entrada e contas a pagar). Falha numa
análise (ERP fora, por exemplo) não grava nada: a próxima passada tenta de novo, no fim da fila. Fica de fora
enquanto a análise semanal roda. Com duas instâncias do serviço, a trava consultiva de sessão
deixa uma passada por vez. Desligada com FOTO_ROTINA=0 (api.py).
"""
import os
import time

from fastapi import HTTPException
from sqlalchemy import text

import analise_foto as af
import analise_pedido as ap
import estado_job
import foto_compra as fc
import rotas_cotacao
import rotas_nota
import rotas_pedido
from infra_db import get_db_connection

FOTO_INTERVALO_S = int(os.getenv("FOTO_INTERVALO_S") or 600)
FOTO_JANELA_DIAS = int(os.getenv("FOTO_JANELA_DIAS") or 30)
FOTO_LOTE_MAX = int(os.getenv("FOTO_LOTE_MAX") or 20)

_TRAVA = "SELECT pg_try_advisory_lock(hashtext('com_analise_compra_foto'))"
_DESTRAVA = "SELECT pg_advisory_unlock(hashtext('com_analise_compra_foto'))"
_JANELA = "created_at >= NOW() - (:dias * INTERVAL '1 day')"

# (fase, documento) que falharam na última tentativa: vão para o fim da fila, para não tomarem
# sempre as vagas do lote. ponytail: em memória, zera ao reiniciar o serviço.
_FALHAS = set()


def _ultimas(conn):
    """{(fase, documento): (assinatura, sinais)} da última foto de cada documento."""
    return {(r["fase"], r["documento"]): (r["assinatura"], r["sinais"]) for r in conn.execute(text("""
        SELECT DISTINCT ON (fase, documento) fase, documento, assinatura, sinais
        FROM com_analise_compra_foto ORDER BY fase, documento, versao DESC""")).mappings().all()}


def _agrupar(linhas, chave):
    docs = {}
    for r in linhas:
        docs.setdefault(str(r[chave]), []).append(dict(r))
    return docs


def _candidatos(conn):
    """[(fase, documento, sinais)] dos documentos da janela sem foto ou mudados desde a última."""
    ult = _ultimas(conn)
    p = {"dias": FOTO_JANELA_DIAS}
    cotacoes = _agrupar(conn.execute(text(f"""
        SELECT c.pedido_cotacao, i.pro_codigo, i.quantidade
        FROM com_cotacao c JOIN com_cotacao_itens i ON i.pedido_cotacao = c.pedido_cotacao
        WHERE c.{_JANELA} AND NOT EXISTS (SELECT 1 FROM com_pedido p WHERE p.pedido_cotacao = c.pedido_cotacao
                                          AND p.status LIKE 'Entregue%')"""), p).mappings().all(), "pedido_cotacao")
    pedidos = _agrupar(conn.execute(text(f"""
        SELECT p.id, p.status, p.frete, p.ipi_no_valor, i.pro_codigo, i.quantidade, i.valor_unitario, i.ipi,
               i.status_item
        FROM com_pedido p JOIN com_pedido_itens i ON i.pedido_id = p.id
        WHERE COALESCE(p.origem, 'cotacao') = 'cotacao' AND p.{_JANELA} AND p.status = ANY(:status)"""),
        {**p, "status": list(ap.STATUS_ANALISAVEIS)}).mappings().all(), "id")
    chaves = [r[0] for r in conn.execute(text(f"""
        SELECT chave_nfe FROM com_nfe_conciliacao
        WHERE xml_completo IS NOT NULL AND xml_completo <> '' AND {_JANELA}"""), p).all()]
    conn.commit()   # nada de transação aberta enquanto as análises rodam

    def assinatura(fase, doc):
        return (ult.get((fase, doc)) or (None, None))[0]

    fila = [("cotacao", d, None) for d, its in cotacoes.items()
            if assinatura("cotacao", d) != af.assinatura_cotacao(its)]
    fila += [("pedido", d, None) for d, its in pedidos.items()
             if assinatura("pedido", d) != af.assinatura_pedido(its[0], its)]
    sinais = rotas_nota._sinais_notas(chaves) if chaves else {}
    fila += [("nota", c, s) for c, s in sinais.items()
             if (ult.get(("nota", c)) or (None, None))[1] != s and _nota_analisavel(c)]
    return fila


def _nota_analisavel(chave):
    """Mesma regra da tela (rotas_nota._nota_pg): resumo resNFe, base64/gzip inválido ou XML
    malformado ficam fora da fila."""
    try:
        rotas_nota._nota_pg(chave)
        return True
    except HTTPException:
        return False


_CALCULAR = {"cotacao": lambda d: rotas_cotacao.calcular(int(d)),
             "pedido": lambda d: rotas_pedido.calcular(d),
             "nota": lambda d: rotas_nota.calcular(d)}


def passada():
    """Uma volta da rotina. Devolve quantas análises fez."""
    if estado_job.em_execucao():
        return 0
    conn = get_db_connection()
    travou = False
    feitas = 0
    try:
        if not fc.tabela_ligada(conn):
            return 0
        travou = bool(conn.execute(text(_TRAVA)).scalar())
        conn.commit()
        if not travou:
            return 0
        fila = sorted(_candidatos(conn), key=lambda c: (c[0], c[1]) in _FALHAS)
        for fase, doc, sinais in fila:
            if feitas >= FOTO_LOTE_MAX:
                break
            feitas += 1
            try:
                calc = _CALCULAR[fase](doc)
                if calc.get("conteudo") is None:
                    continue
                fc.gravar_foto(conn, fase, doc, calc["conteudo"], calc["assinatura"], "rotina",
                               criado_doc=calc["criado_em"], rodada=calc["rodada"], sinais=sinais)
                _FALHAS.discard((fase, doc))
            except Exception as e:   # HTTPException do ERP inclusive: fica para a próxima passada
                _FALHAS.add((fase, doc))
                print(f"Rotina de fotos: {fase} {doc} não gravada: {getattr(e, 'detail', e)}")
                conn.rollback()
        return feitas
    finally:
        # A conexão volta ao pool: a trava de sessão tem de sair antes.
        try:
            conn.rollback()
            if travou:
                conn.execute(text(_DESTRAVA))
                conn.commit()
        finally:
            conn.close()


def laco():
    print(f"Rotina de fotos da análise de compra: a cada {FOTO_INTERVALO_S}s, janela {FOTO_JANELA_DIAS} dias, "
          f"até {FOTO_LOTE_MAX} análises por passada.")
    while True:
        try:
            passada()
        except Exception as e:
            print(f"Erro na rotina de fotos: {e}")
        time.sleep(FOTO_INTERVALO_S)
