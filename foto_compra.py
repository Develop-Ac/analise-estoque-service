# -*- coding: utf-8 -*-
"""Fotos da análise de compra no banco (com_analise_compra_foto / com_analise_compra_vista).

Cada fase (cotação, pedido, nota) de um documento guarda versões da sua análise; versão nova só
quando a assinatura muda (regras em analise_foto). `gravar_foto` é a única porta de gravação:
a rotina de fotos e a abertura da análise chamam a mesma função. As tabelas são criadas por SQL
manual; sem elas `tabela_ligada` é False e as análises abrem sem histórico.
"""
import datetime
import json

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text

import analise_foto as af
from analise_nota import _CUIABA
from infra_db import get_db_connection

AVISO_DESLIGADO = "histórico das análises ainda não está ligado"
AVISO_FALHA = "histórico das análises indisponível agora"
# Cotação/pedido já entregue: a situação de hoje mora na análise da nota.
SEM_HOJE = "a situação de hoje está na análise da nota"


def tabela_ligada(conn):
    return conn.execute(text("SELECT to_regclass('com_analise_compra_foto')")).scalar() is not None


def ultima_foto(conn, fase, documento):
    """{id, versao, assinatura, sinais} da versão mais recente, ou None."""
    r = conn.execute(text("""
        SELECT id, versao, assinatura, sinais FROM com_analise_compra_foto
        WHERE fase = :f AND documento = :d ORDER BY versao DESC LIMIT 1"""),
        {"f": fase, "d": str(documento)}).mappings().first()
    return dict(r) if r else None


def gravar_foto(conn, fase, documento, conteudo, assinatura, origem, usuario_id=None,
                criado_doc=None, rodada=None, sinais=None):
    """Grava versão nova se a assinatura mudou e devolve o id da foto vigente. A trava consultiva
    (por fase+documento, solta no commit) serializa rotina e abertura simultâneas; o ON CONFLICT
    cobre a corrida que sobrar."""
    documento = str(documento)
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:f || ':' || :d))"), {"f": fase, "d": documento})
    ult = ultima_foto(conn, fase, documento)
    if ult and ult["assinatura"] == assinatura:
        if sinais is not None and sinais != ult["sinais"]:
            conn.execute(text("UPDATE com_analise_compra_foto SET sinais = :s WHERE id = :id"),
                         {"s": sinais, "id": ult["id"]})
        conn.commit()
        return ult["id"]
    versao = (ult["versao"] if ult else 0) + 1
    agora = datetime.datetime.now(_CUIABA)
    if isinstance(criado_doc, datetime.date) and not isinstance(criado_doc, datetime.datetime):
        criado_doc = datetime.datetime.combine(criado_doc, datetime.time(), tzinfo=_CUIABA)
    elif isinstance(criado_doc, datetime.datetime) and criado_doc.tzinfo is None:
        # created_at sem fuso (Prisma/now() das tabelas de compras) está em UTC.
        criado_doc = criado_doc.replace(tzinfo=datetime.timezone.utc)
    novo = conn.execute(text("""
        INSERT INTO com_analise_compra_foto (fase, documento, versao, motivo, origem, usuario_id,
                                             assinatura, sinais, rodada, tardia, conteudo)
        VALUES (:fase, :doc, :versao, :motivo, :origem, :usuario, :assinatura, :sinais, :rodada, :tardia,
                CAST(:c AS jsonb))
        ON CONFLICT (fase, documento, versao) DO NOTHING
        RETURNING id"""),
        {"fase": fase, "doc": documento, "versao": versao,
         "motivo": af.motivo(fase, ult and ult["assinatura"], assinatura), "origem": origem,
         "usuario": usuario_id and str(usuario_id)[:100], "assinatura": assinatura, "sinais": sinais,
         "rodada": rodada, "tardia": af.tardia(versao, criado_doc, agora),
         "c": json.dumps(jsonable_encoder(conteudo))}).scalar()
    if novo is None:   # outra gravação levou essa versão
        novo = (ultima_foto(conn, fase, documento) or {}).get("id")
    conn.commit()
    return novo


def registrar_vista(conn, foto_id, usuario_id):
    """Marca que o usuário abriu essa versão (a primeira vez vale)."""
    conn.execute(text("""
        INSERT INTO com_analise_compra_vista (foto_id, usuario_id) VALUES (:f, :u)
        ON CONFLICT (foto_id, usuario_id) DO NOTHING"""), {"f": foto_id, "u": str(usuario_id)[:100]})
    conn.commit()


def _iso(v):
    return v.astimezone(_CUIABA).isoformat() if v is not None else None


def versoes(conn, fase, documento):
    """Versões da fase do documento, por ordem de versão, sem o conteúdo, com quem abriu e quem viu."""
    fotos = conn.execute(text("""
        SELECT f.id, f.versao, f.criado_em, f.motivo, f.origem, u.nome AS usuario, f.tardia, f.rodada
        FROM com_analise_compra_foto f
        LEFT JOIN sis_usuarios u ON u.id::text = f.usuario_id
        WHERE f.fase = :f AND f.documento = :d ORDER BY f.versao"""),
        {"f": fase, "d": str(documento)}).mappings().all()
    if not fotos:
        return []
    vistas = {}
    for v in conn.execute(text("""
            SELECT v.foto_id, COALESCE(u.nome, v.usuario_id) AS usuario, v.visto_em
            FROM com_analise_compra_vista v
            LEFT JOIN sis_usuarios u ON u.id::text = v.usuario_id
            WHERE v.foto_id = ANY(:ids) ORDER BY v.visto_em"""),
            {"ids": [f["id"] for f in fotos]}).mappings().all():
        vistas.setdefault(v["foto_id"], []).append({"usuario": v["usuario"], "em": _iso(v["visto_em"])})
    return [{"id": f["id"], "versao": f["versao"], "data": _iso(f["criado_em"]), "motivo": f["motivo"],
             "origem": f["origem"], "usuario": f["usuario"], "tardia": bool(f["tardia"]),
             "rodada": f["rodada"] and str(f["rodada"]), "vistas": vistas.get(f["id"], [])} for f in fotos]


def conteudo_da_foto(conn, foto_id):
    """(fase, documento, conteudo) da foto, ou None."""
    r = conn.execute(text("SELECT fase, documento, conteudo FROM com_analise_compra_foto WHERE id = :id"),
                     {"id": foto_id}).first()
    return (r[0], r[1], r[2]) if r else None


def cadeia(conn, fase, documento):
    """[(fase, documento, numero)] das fases que levaram ao documento aberto, ele por último.
    Só olha para trás: a cotação vê só ela, o pedido vê a cotação e ele, a nota vê as cotações e
    os pedidos vinculados (vínculo confirmado e não rejeitado). O pedido nunca enxerga a nota."""
    documento = str(documento)
    if fase == "cotacao":
        return [("cotacao", documento, int(documento) if documento.isdigit() else documento)]
    if fase == "pedido":
        cot = conn.execute(text("SELECT pedido_cotacao FROM com_pedido WHERE id = :id"),
                           {"id": documento}).scalar()
        antes = [("cotacao", str(cot), cot)] if cot is not None else []
        return antes + [("pedido", documento, cot)]
    linhas = conn.execute(text("""
        SELECT DISTINCT p.id, p.pedido_cotacao FROM com_pedido_nfe_vinculo v
        JOIN com_pedido p ON p.id = v.pedido_id
        WHERE v.chave_nfe = :c AND v.confirmado AND NOT v.rejeitado ORDER BY p.id"""),
        {"c": documento}).mappings().all()
    cotacoes = sorted({r["pedido_cotacao"] for r in linhas if r["pedido_cotacao"] is not None})
    return ([("cotacao", str(c), c) for c in cotacoes]
            + [("pedido", str(r["id"]), r["pedido_cotacao"]) for r in linhas]
            + [("nota", documento, None)])


def _conteudo(conn, foto_id):
    r = conteudo_da_foto(conn, foto_id)
    return r[2] if r else None


def _historico(conn, fase, documento, calc, usuario_id):
    """(fases, comparacoes, evolucao) do documento, gravando a foto de abertura e quem viu."""
    hoje = calc.get("conteudo")
    if hoje is not None:
        gravar_foto(conn, fase, documento, hoje, calc["assinatura"], "abertura", usuario_id=usuario_id,
                    criado_doc=calc.get("criado_em"), rodada=calc.get("rodada"))
    ult = ultima_foto(conn, fase, documento)
    if ult and usuario_id:
        registrar_vista(conn, ult["id"], usuario_id)
    fases = []
    for f, d, numero in cadeia(conn, fase, documento):
        vs = versoes(conn, f, d)
        if not vs:
            continue
        conteudo, recortada = _conteudo(conn, vs[-1]["id"]) or {}, False
        primeira = _conteudo(conn, vs[0]["id"]) if len(vs) > 1 else None
        if f != fase:
            conteudo, recortada = af.recortar(conteudo, calc["produtos"])
            primeira = primeira and af.recortar(primeira, calc["produtos"])[0]
        fases.append({"fase": f, "documento": d, "numero": calc.get("numero") if f == fase else numero,
                      "recortada": recortada, "versoes": vs, "conteudo": conteudo,
                      "conteudo_primeira": primeira})
    comparacoes = [] if hoje is None else [
        {"foto_id": x["versoes"][-1]["id"],
         **af.confrontar(x["conteudo"].get("unidades"), hoje.get("unidades"))} for x in fases]
    evolucao = af.evolucao(fases, hoje)
    for x in fases:
        x.pop("conteudo_primeira")
    return fases, comparacoes, evolucao


def responder(fase, documento, calc, usuario_id):
    """Resposta das rotas de análise: o cálculo de hoje mais o histórico das fases da cadeia.
    Falha no histórico não derruba a análise: devolve hoje com o histórico desligado."""
    documento = str(documento)
    saida = {"documento": {"fase": fase, "id": documento, "numero": calc.get("numero")},
             "hoje": calc.get("conteudo"), "sem_hoje": calc.get("sem_hoje"),
             "historico": {"ligado": False, "aviso": AVISO_DESLIGADO},
             "fases": [], "comparacoes": [], "evolucao": []}
    try:
        with get_db_connection() as conn:
            if not tabela_ligada(conn):
                return saida
            fases, comparacoes, evolucao = _historico(conn, fase, documento, calc, usuario_id)
    except Exception as e:
        print(f"Erro no histórico da análise {fase} {documento}: {e}")
        saida["historico"]["aviso"] = AVISO_FALHA
        return saida
    saida.update(historico={"ligado": True, "aviso": None}, fases=fases,
                 comparacoes=comparacoes, evolucao=evolucao)
    return saida


def responder_foto(fase, documento, foto_id, produtos=None):
    """{foto, conteudo, recortada} de uma versão guardada, sem recalcular. A foto tem de ser de
    uma fase da cadeia do documento aberto (senão 404); foto de fase anterior vem recortada para
    os produtos do documento (`produtos` None = os da última foto do próprio documento)."""
    documento = str(documento)
    with get_db_connection() as conn:
        achada = tabela_ligada(conn) and conteudo_da_foto(conn, foto_id)
        if not achada or (achada[0], achada[1]) not in {(f, d) for f, d, _ in cadeia(conn, fase, documento)}:
            raise HTTPException(status_code=404, detail="Foto não encontrada nesta análise.")
        f, d, conteudo = achada
        foto = next(v for v in versoes(conn, f, d) if v["id"] == foto_id)
        recortada = False
        if f != fase:
            if produtos is None:
                ult = ultima_foto(conn, fase, documento)
                produtos = af.produtos_do_conteudo((ult and _conteudo(conn, ult["id"])) or {})
            conteudo, recortada = af.recortar(conteudo, produtos)
    return {"foto": foto, "conteudo": conteudo, "recortada": recortada}
