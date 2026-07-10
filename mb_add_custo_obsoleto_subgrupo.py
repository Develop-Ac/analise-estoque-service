"""
Cria no Metabase o card "Custo Obsoleto por Subgrupo (top 15)" e o adiciona ao
dashboard 89 (Gestão de Estoque, coleção Estoque). Ranking do valor de custo do
saldo OBSOLETO (idade FIFO > 240 dias) por subgrupo, do maior para o menor —
para priorizar onde atacar o estoque parado.

Modelado no card 672 ("Venda Perdida 12m por Subgrupo"): mesmo banco (6, Intranet
análises), mesma variável opcional {{fornecedor}} mapeada ao parâmetro do
dashboard (fornparam), display horizontal (row).

Uso:  python mb_add_custo_obsoleto_subgrupo.py
A chave é lida do .env do cotacao-frontend (METABASE_API_KEY). Idempotente:
se já existir um card com o mesmo nome no dashboard, não duplica.
"""
import json
import os
import sys

import requests
import urllib3

urllib3.disable_warnings()

DASH_ID = 89
CARD_NAME = "Custo Obsoleto por Subgrupo (top 15)"
MODELO_CARD_ID = 672  # Venda Perdida por Subgrupo — copia coleção/param

SQL = """
SELECT sgr_descricao AS subgrupo,
       ROUND(SUM(COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0))::numeric, 2) AS valor_custo_obsoleto
FROM com_fifo_completo
WHERE data_processamento = (SELECT MAX(data_processamento) FROM com_fifo_completo)
  AND estoque_disponivel > 0
  AND categoria_saldo_atual = 'Obsoleto'
  [[AND UPPER(TRIM(COALESCE(NULLIF(fornecedor_principal,''), fornecedor1))) = ANY(string_to_array(UPPER({{fornecedor}}), '||'))]]
GROUP BY 1
HAVING SUM(COALESCE(estoque_disponivel,0) * COALESCE(custo_unitario,0)) > 0
ORDER BY 2 DESC
LIMIT 15
""".strip()


def carregar_credenciais():
    envp = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "cotacao-frontend", ".env",
    )
    key, site = None, "https://bi.acacessorios.local"
    with open(envp, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("METABASE_API_KEY="):
                key = line.split("=", 1)[1].strip()
            elif line.startswith("METABASE_SITE_URL="):
                site = line.split("=", 1)[1].strip().replace("http://", "https://")
    if not key:
        sys.exit("METABASE_API_KEY não encontrado no .env do cotacao-frontend")
    return key, site


def main():
    key, site = carregar_credenciais()
    S = requests.Session()
    S.verify = False
    S.headers.update({"x-api-key": key, "Content-Type": "application/json"})

    # 0) idempotência: card já está no dashboard?
    dash = S.get(f"{site}/api/dashboard/{DASH_ID}").json()
    for dc in dash.get("dashcards", []):
        if (dc.get("card") or {}).get("name") == CARD_NAME:
            print(f"Já existe (dashcard {dc['id']}, card {dc['card']['id']}). Nada a fazer.")
            return

    # 1) coleção + template-tag id do modelo
    modelo = S.get(f"{site}/api/card/{MODELO_CARD_ID}").json()
    collection_id = modelo.get("collection_id")
    tag_id = ((modelo.get("dataset_query", {}).get("native", {}) or {})
              .get("template-tags", {}).get("fornecedor", {}).get("id"))
    if not tag_id:
        tag_id = "2c30778b-ba20-4abb-9652-81bdec4652ea"

    # 2) cria o card
    card_payload = {
        "name": CARD_NAME,
        "display": "row",  # barras horizontais (ranking)
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "database": 6,
            "native": {
                "query": SQL,
                "template-tags": {
                    "fornecedor": {
                        "id": tag_id,
                        "name": "fornecedor",
                        "display-name": "Fornecedor",
                        "type": "text",
                    }
                },
            },
        },
        "visualization_settings": {
            "graph.dimensions": ["subgrupo"],
            "graph.metrics": ["valor_custo_obsoleto"],
            "column_settings": {
                '["name","valor_custo_obsoleto"]': {
                    "number_style": "currency",
                    "currency": "BRL",
                    "currency_style": "symbol",
                }
            },
        },
    }
    r = S.post(f"{site}/api/card", data=json.dumps(card_payload))
    if not r.ok:
        sys.exit(f"Falha ao criar card: HTTP {r.status_code} — {r.text[:500]}")
    novo = r.json()
    novo_id = novo["id"]
    print(f"Card criado: id={novo_id} '{CARD_NAME}'")

    # 3) adiciona ao dashboard (PUT com a lista completa de dashcards)
    # posição: linha abaixo de tudo, largura 12 col, altura 8.
    max_bottom = max((dc.get("row", 0) + dc.get("size_y", 0)
                      for dc in dash.get("dashcards", [])), default=0)

    def normaliza(dc):
        return {
            "id": dc["id"],
            "card_id": dc["card"]["id"],
            "row": dc["row"],
            "col": dc["col"],
            "size_x": dc["size_x"],
            "size_y": dc["size_y"],
            "series": dc.get("series", []),
            "parameter_mappings": dc.get("parameter_mappings", []),
            "visualization_settings": dc.get("visualization_settings", {}),
            "dashboard_tab_id": dc.get("dashboard_tab_id"),
        }

    dashcards = [normaliza(dc) for dc in dash.get("dashcards", []) if dc.get("card", {}).get("id")]
    dashcards.append({
        "id": -1,
        "card_id": novo_id,
        "row": max_bottom,
        "col": 0,
        "size_x": 12,
        "size_y": 8,
        "series": [],
        "parameter_mappings": [
            {"parameter_id": "fornparam", "card_id": novo_id,
             "target": ["variable", ["template-tag", "fornecedor"]]}
        ],
        "visualization_settings": {},
        "dashboard_tab_id": None,
    })

    r = S.put(f"{site}/api/dashboard/{DASH_ID}", data=json.dumps({"dashcards": dashcards}))
    if not r.ok:
        sys.exit(f"Card criado, mas falhou ao adicionar no dashboard: HTTP {r.status_code} — {r.text[:500]}")
    print(f"Adicionado ao dashboard {DASH_ID} em row={max_bottom}, col=0, 12x8. OK.")


if __name__ == "__main__":
    main()
