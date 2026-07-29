# -*- coding: utf-8 -*-
"""
Reset do pacote FIFO no Mongo — força o BACKFILL na próxima execução do job.

QUANDO USAR
  Depois da correção de custo FIFO (2026-07-29): pacotes gravados antes dela não
  têm `custo` nas camadas congeladas, e esses produtos caem no fallback de última
  entrada em vez do custo real da camada. Só um backfill reconstrói tudo com custo.

O QUE FAZ
  Apaga a coleção `fifo_pack` e a `fifo_pack_meta`. Sem o doc `_id:"global"` do
  meta, o run_job entra em BACKFILL (carga completa desde 2005) e regrava o pacote.

  ATENÇÃO: apagar SÓ `fifo_pack` e deixar o meta é pior que não fazer nada — o job
  entraria em modo incremental com pacote vazio, leria apenas a janela de 12 meses
  e reconstruiria as camadas SEM o histórico anterior, sem acusar erro. Por isso
  este script apaga os dois juntos, ou nenhum.

USO
  python reset_pack_fifo.py            # dry-run: só mostra o estado atual
  python reset_pack_fifo.py --executar # apaga de verdade

  Precisa enxergar o Mongo. Rode de dentro da rede docker do serviço, ou aponte:
  MONGO_URL="mongodb://usuario:senha@host:27017/?tls=false" python reset_pack_fifo.py
"""
import os
import sys

import empacotamento as emp


def main():
    executar = "--executar" in sys.argv

    print(f"MONGO_URL : {emp.MONGO_URL}")
    print(f"MONGO_DB  : {emp.MONGO_DB}")
    print(f"coleções  : {emp.MONGO_COL} / {emp.MONGO_COL}_meta")
    print()

    try:
        import pymongo
        cli = pymongo.MongoClient(emp.MONGO_URL, serverSelectionTimeoutMS=8000)
        db = cli[emp.MONGO_DB]
        col = db[emp.MONGO_COL]
        col_meta = db[emp.MONGO_COL + "_meta"]
        n_docs = col.count_documents({})
        meta = col_meta.find_one({"_id": "global"}) or {}
    except Exception as e:
        print(f"ERRO: não consegui falar com o Mongo ({e})")
        print("Rode de dentro da rede docker do serviço ou exporte MONGO_URL.")
        return 1

    print("ESTADO ATUAL")
    print(f"  produtos no pacote : {n_docs}")
    print(f"  corte              : {meta.get('corte') or '(vazio — já entraria em backfill)'}")
    print(f"  atualizado_em      : {meta.get('atualizado_em') or '-'}")

    # quantas camadas já têm custo (indica se o pacote é pós-correção)
    if n_docs:
        com, sem = 0, 0
        for d in col.find({}, {"camadas": 1}).limit(2000):
            for c in d.get("camadas", []):
                if c.get("custo") is None:
                    sem += 1
                else:
                    com += 1
        tot = com + sem
        if tot:
            print(f"  camadas com custo  : {com}/{tot} ({com / tot:.1%}) — amostra de até 2000 produtos")
            if sem == 0:
                print("  -> pacote JÁ está pós-correção; o backfill provavelmente não é necessário.")

    if not executar:
        print("\nDRY-RUN. Nada foi apagado.")
        print("Para apagar de verdade: python reset_pack_fifo.py --executar")
        return 0

    print("\nAPAGANDO...")
    col_meta.delete_many({})       # o meta primeiro: é ele que decide o modo
    col.delete_many({})
    print(f"  {emp.MONGO_COL}_meta : limpo")
    print(f"  {emp.MONGO_COL}      : limpo")
    print("\nPronto. A PRÓXIMA execução do job será um BACKFILL (carga completa,")
    print("bem mais demorada que o normal). Dispare com:  python main.py run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
