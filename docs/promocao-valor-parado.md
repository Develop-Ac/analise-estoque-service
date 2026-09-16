# Lista de promoção por valor parado — especificação

Cartão Trello "Estoque: Lista de promoção por valor parado, com ação sugerida para cada item (13)"
+ cartão atrelado "Estoque: Tela de análise de produto no padrão visual da intranet + auditoria impeccable (20)".
Análise de fundamentação (números, técnicas, regra): `Analises/Compras e Estoque/promocao-valor-parado/`.

Princípio: **nada é gravado no ERP**. A lista orienta compras e comercial; a carga da promoção
continua na tela de promoções do Celta (`PROMOCOES` / `PROMOCOES_ITENS`).

## 1. Dados por item (o que a lista precisa)

Fonte principal: `com_fifo_completo` da última `data_processamento`. Colunas já existentes:
`estoque_disponivel`, `estoque_max_sugerido`, `custo_unitario` (FIFO última entrada; `custo_fonte`),
`preco_venda_1` (tabela 1 = varejo, `PRODUTOS.PRECO_VENDA`), `preco_venda_2` (tabela 2 = atacado
especial, `PRECO2`), `curva_abc`, `demanda_real_dia`, `data_max_venda`, `categoria_saldo_atual`,
`sgr_codigo`, `group_id`.

Derivados na rota (`rotas_promo.py`):

| Campo | Regra |
|---|---|
| `valor_parado` | `estoque_disponivel × custo_unitario` (ordenação padrão, desc) |
| `excesso_qtd` / `valor_excesso` | `estoque_disponivel − CEIL(estoque_max_sugerido)` (≥ 0) × custo — mesma expressão do `/analise` (KPI "excesso") |
| `cobertura_meses` | `estoque_disponivel ÷ (demanda_real_dia × 30)`; nulo sem demanda |
| `dias_sem_venda` | hoje − `data_max_venda`; nulo = nunca vendeu |
| `publico` | pela venda de 12 m por canal (abaixo): `atacado` (≥ 80% das unidades), `varejo` (≤ 20%), `ambos` (misto ou sem venda) |
| `fora_regua` | existe em `ven_regua_item_excecao` (vendas-service, mesmo Postgres) |

**Canal por item (novo no batch, `main.py`):** duas colunas em `com_fifo_completo`,
`qtd_varejo_12m` e `qtd_atacado_12m`, preenchidas a cada run com uma consulta direta ao BI
(`BI.dbo.vw_analise_vendas`, mesma conexão SQL Server que já executa os OPENQUERY):

```sql
SELECT PRO_CODIGO,
       SUM(CASE WHEN UPPER(local_venda) LIKE '%ATAC%' THEN QUANTIDADE ELSE 0 END) AS qtd_atacado_12m,
       SUM(CASE WHEN UPPER(local_venda) LIKE '%ATAC%' THEN 0 ELSE QUANTIDADE END) AS qtd_varejo_12m
FROM BI.dbo.vw_analise_vendas
WHERE DT_EMISSAO >= '<yyyymmdd 12 meses atrás>' AND DT_CANCELAMENTO IS NULL
GROUP BY PRO_CODIGO
```

(`local_venda` ∈ BALCÃO, ATACADO, VAREJO-SERVIÇO; data no formato `yyyymmdd` — o locale do
SRVSQLWIN inverte `yyyy-mm-dd`). DDL das colunas segue o padrão idempotente do serviço em
`criar_tabela_postgres()`.

## 2. Ação sugerida (uma por item, avaliada nesta ordem)

A **idade média FIFO do saldo** (`tempo_medio_saldo_atual`, dias; limite `PROMO_IDADE_LIQUIDACAO_DIAS`
= 240 = faixa "Obsoleto") manda na liquidação, porque a promoção do ERP é **por item, não por
quantidade**: um desconto de liquidação atingiria também as unidades que entraram semana passada
(caso real: 47 un, 36 delas com 65 dias, cobertura 24,9 m → 40 % no balcão era irreal).

1. `revisar_cadastro` — `custo_unitario` nulo ou ≤ 0.
2. sem venda em 12 m (`dias_sem_venda` > 365 ou nulo):
   saldo velho (idade > 240) → `giro_caixa` / `sem_venda_12m`; saldo recente → `promocao` /
   `promocao_24` (30 %) — vira liquidação quando o saldo envelhecer (escada no tempo, run a run).
3. `cobertura_meses` > 24: saldo velho → `giro_caixa` / `cobertura_alta`; saldo recente →
   `vender_sem_repor` (compra grande recente: não desconta, não repõe).
4. `vender_sem_repor` — `curva_abc` ∈ {A, B} e `cobertura_meses` > 12.
5. `promocao` — os demais (degrau pela cobertura: ≤ 6 / ≤ 12 / > 12 m).

Sem idade informada, trata como velho (conservador só na ausência do dado).

Só entram na lista itens com `excesso_qtd > 0` (regra atual do `/promo/plan`), mais os
`revisar_cadastro` com saldo (para o comprador ver o buraco). Simulação de 13/09/2026: giro 8.443
itens / R$ 1.527 mil · vender sem repor 495 / R$ 521 mil · promoção 1.687 / R$ 426 mil (843 deles
sem venda em 12 m mas com saldo recente, R$ 218 mil, começam em 30 %).

**Regras de conteúdo (permanentes):** não existe ação "devolver ao fornecedor" (a AC não devolve);
nenhum rótulo, tooltip, exportação ou texto da tela cita filial, expansão ou qualquer plano futuro —
o operacional vê só ações operacionais.

**Troca manual:** tabela nova `com_promo_acao` (DDL manual em `sql/`, não rodar migration):

```sql
CREATE TABLE IF NOT EXISTS com_promo_acao (
  pro_codigo      VARCHAR(50) PRIMARY KEY,
  acao            VARCHAR(30) NOT NULL,   -- mesmos valores da ação sugerida
  preco_varejo    NUMERIC(15,4),          -- opcional: preço digitado pelo comprador
  preco_atacado   NUMERIC(15,4),
  observacao      VARCHAR(255),
  usuario         VARCHAR(100),
  atualizado_em   TIMESTAMP NOT NULL DEFAULT NOW()
);
```

`PUT /promo/acao/{pro_codigo}` grava/atualiza; `DELETE` volta à sugestão. A lista devolve
`acao_sugerida`, `acao` (manual se houver) e `acao_manual: bool`.

Campanhas exportadas (aba Carga ERP) — é por esta tabela que a bolsa e a comissão reconhecem venda
de promoção/liquidação:

```sql
CREATE TABLE IF NOT EXISTS com_promo_campanha (
  id              SERIAL PRIMARY KEY,
  pro_codigo      VARCHAR(50) NOT NULL,
  tipo            VARCHAR(20) NOT NULL,   -- promocao | liquidacao
  preco_varejo    NUMERIC(15,4),
  preco_atacado   NUMERIC(15,4),
  inicio          DATE NOT NULL,
  fim             DATE NOT NULL,
  usuario         VARCHAR(100),
  criado_em       TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_com_promo_campanha UNIQUE (pro_codigo, inicio)
);
CREATE INDEX IF NOT EXISTS ix_com_promo_campanha_vigencia ON com_promo_campanha (pro_codigo, inicio, fim);
```

> A pasta `sql/` deste repositório está no `.gitignore`; o arquivo
> `sql/2026-09-16_promocao_valor_parado.sql` existe só na máquina local — o DDL acima é a cópia
> versionada. Aplicar à mão no Postgres da intranet antes de subir esta versão.

## 3. Preço proposto por tabela

Parâmetros por env (defaults): `PROMO_PISO_VAREJO=1.30`, `PROMO_PISO_ATACADO_LIQ=1.30` (= piso da bolsa
1,25 + 0,05 de bônus do vendedor), `PROMO_VALIDADE_DIAS=30`. Ajuste do usuário 16/09: o balcão já dá 10%
sozinho, então a escada do varejo começa em 15% e termina em 50%.

| Situação | Varejo: desc. s/ `preco_venda_1` | Atacado esp.: s/ `preco_venda_2` | Piso atacado |
|---|---|---|---|
| promoção, cob ≤ 6 m | 15% | `preco_venda_2 × (1 − desc_max_faixa)` | `custo × mk_faixa × (1 − desc_max_faixa)` |
| promoção, 6–12 m | 20% | idem | idem |
| promoção, 12–24 m | 30% | idem | idem |
| vender sem repor | sem desconto (tabela 1) | sem desconto (tabela 2) | — |
| giro de caixa (liquidação), cob > 24 m | 40% | 15% | `custo × 1,30` |
| giro de caixa (liquidação), sem venda 12 m | 50% | 25% | `custo × 1,30` |

- `preco_promo_varejo = max(preco_venda_1 × (1 − d), custo × 1,30, preco_promo_atacado)` — balcão
  nunca abaixo do promocional do atacado (anti-arbitragem entre tabelas).
- `preco_promo_atacado = max(preco_venda_2 × (1 − d), piso)`; **se `piso ≥ preco_venda_2` → nulo**
  com motivo `tabela_abaixo_regua` (a tabela 2 real dos itens baratos está abaixo da régua v3;
  corrige-se pela Onda 1, não por promoção). Item `fora_regua` só recebe preço de atacado em
  `giro_caixa/sem_venda_12m`.
- Régua v3 (faixas pelos cortes exatos do ETL 10,01 / 38,50 / 69,61 / 124,37 / 249,86 / 299,87 /
  395,27 / 496,36 / 696,37 / 996,38; classe PB = `sgr_codigo 154`): ler de `ven_regua_atacado`
  (vendas-service, mesmo Postgres) — não duplicar a tabela no código.
- Sem `preco_venda_1`/`preco_venda_2` (325 / 899 itens) → preço nulo com motivo `sem_tabela`.
- A resposta traz `piso_varejo` e `piso_atacado` em R$ (a tela marca "no piso" quando o preço
  travou nele) e `idade_saldo_dias`.
- Público decide qual preço vai para a carga: `atacado` → só tabela 2; `varejo` → só tabela 1;
  `ambos` → as duas.

## 4. Rotas

- `POST /promo/plan` (manter contrato + novos campos): filtros existentes + `acoes: []`,
  `publicos: []`, `curvas: []`; paginação `page`/`page_size` (padrão 100, ordenação `valor_parado`
  desc; `sort` opcional) e bloco `totais` calculado no servidor sobre a seleção inteira:
  `valor_parado`, `valor_excesso`, `itens`, `meses_para_zerar` (Σ valor ÷ Σ demanda mensal × custo),
  `caixa_potencial` (Σ excesso × preço do público), `por_acao` (itens e valor).
  Remover a segunda definição duplicada da rota (a 2ª é morta).
- `POST /promo/export`: aba **Promoção** com os campos da tela + ação + público + preços + motivo;
  aba **Carga ERP** só dos itens com preço: `PRO_CODIGO`, `PROM_VALOR` (tabela 1 ou vazio),
  `PROM_VALOR2` (tabela 2 ou vazio), `DATA_INICIAL` (hoje), `DATA_FINAL` (hoje + 30).
- `PUT|DELETE /promo/acao/{pro_codigo}`.
Teste mínimo: `tests/test_promo_regra.py` com 5 itens sintéticos cobrindo cada ação e os pisos
(`assert` puro, sem banco): ação por precedência, preço varejo ≥ atacado, atacado nulo quando
piso ≥ tabela, vender sem repor sem desconto.

## 5. Tela (cotacao-frontend, `/estoque/analise`)

A lista deixa de ser modal e vira a **terceira aba** da página: `Painel | Análise | Promoção`
(`Tabs` do kit). `usePromotionLogic` continua como está; `PromotionModal.tsx` sai;
`PromotionFilters`/`PromotionTable` são reescritos com o kit (`FilterBar`, `MultiSelect`, `Select`,
`Card`, `Table/Th/Td/Tr/Pager`, `Badge`, `KpiCard`).

- Topo: 4 `KpiCard` — valor parado, valor em excesso (n itens), meses para zerar, caixa potencial.
- Filtros: subgrupo, marca, tempo em estoque, **ação**, **público**, curva; agrupar similares; Excel.
- Colunas: Produto (código + descrição + marca) · Curva (`Badge`) · Saldo · Excesso · Custo ·
  **Valor parado** · Cobertura (m) · Última venda · Público (`Badge`) · **Ação** (`Select` inline;
  manual = badge "manual") · Preço balcão · Preço atac. esp. (nulo → "—" com tooltip do motivo) ·
  detalhes (memória do cálculo com custo, tabelas, piso, degrau).
- Estados: sem resultado; erro em banner; carregando (`TableSkeleton`). A lista carrega ao abrir a
  aba e recarrega a cada filtro (sem botão "Gerar").
- Modal de detalhe organizado por pergunta (critique impeccable 16/09/2026, 18/40 antes do
  redesenho): "Por que esta ação" (frase com os números do item + para quem + troca de ação),
  "Preço proposto (aprovação da gerência)" (tabela Canal · Tabela · Proposta · Desconto · Piso em
  R$, badge "no piso"; caixa potencial em frase), "Estoque" (um fato por `Campo`, com detalhe),
  "Lotes em estoque"
  (`CardHead` com total acima de 240 dias). Datas `YYYY-MM-DD` formatadas sem `Date` (UTC recuaria
  um dia em Cuiabá).
- Destilado 16/09/2026 (noite, comando `distill`): o modal ficou com quatro blocos e nada além —
  (1) decisão: badge da ação + "Vender no …" + histórico por canal em uma linha, e uma frase curta com
  o número que decide; (2) preço proposto (Canal · Tabela · Proposta · Desconto; "no piso" vira badge
  na proposta, a coluna Piso saiu); (3) cinco `Campo`: Saldo (máximo), Leva para zerar, Última venda,
  Idade do saldo, Custo (valor parado); (4) lotes em `<details>` recolhido. Trocar a ação foi para o
  rodapé do modal. Tamanho `lg`. Saíram: nota da regra de piso, caixa potencial por item, valor em
  excesso, categoria de tempo, fonte do custo (tudo continua no Excel).
- Filtro de ação da aba: botão ativo pinta como primário (`Btn pressed`, `aria-pressed`) — antes o
  `bg-brand-50` perdia para o `bg-white` do kit e o ativo não aparecia.
- **Grupo de similares: a lista NÃO considera** (pendente de decisão). O excesso é medido contra o
  máximo do próprio item (`estoque_max_sugerido`); `group_id`, `grp_estoque_*` e `rateio_prop_grupo`
  existem em `com_fifo_completo` e permitem medir contra o grupo (excesso do grupo × rateio do item).
- Decisões do usuário 16/09/2026: o comprador **nunca decide o preço sozinho** — todo preço da
  lista é proposta e passa pela aprovação da gerência antes da carga no ERP (a tela diz isso na
  frase de abertura e no título da seção de preço). O **bônus do vendedor na liquidação sai só no
  Excel** (coluna "Bônus liquidação/un (R$)" da aba Promocao); não aparece na tela.

## 6. Cartão atrelado — tela no padrão visual + auditoria impeccable

- Ordenação por coluna em `GET /analise` (16/09/2026): `sort` ∈ produto | curva | estoque | capital |
  tendencia | sugestao e `sort_dir` asc|desc, lista fechada em `_SORT_COLS` (`rotas_analise.py`).
  Na visão agrupada o grupo inteiro é posicionado pelo seu extremo (window MAX/MIN por
  `group_id`) para os similares não se separarem; na individual a ordem é estrita. Sem `sort`
  vale a ordem histórica (curva do grupo, nome). No front, clique no cabeçalho: 1º desc (asc em
  Produto/ABC), 2º inverte, 3º volta ao padrão; `aria-sort` no `Th`.
- Visão individual: o mín–máx do grupo de similares aparece dentro do cartão Sugestão, abaixo do
  rótulo ("Grupo 5 – 20"), em vez de badge solto.

Escopo (só visual e acessibilidade; lógica, filtros e chamadas intocados):

1. `page.tsx` (3 mil linhas) para o kit: `PageContainer/PageHeader/Btn`, `Tabs`, `FilterBar` +
   `FilterInput/Select/MultiSelect` do kit (sai o `MultiSelect`/`Select` antigos), `Card` + `Table`
   com `Th/Td/Tr` (mantém redimensionar colunas), `Pager`, `Badge` para curva/tendência/status/
   categoria, chips de filtro em `Badge`, barra flutuante de seleção e menu de contexto com tokens
   (fundo card, borda, raio 12, sombra leve), sem cor solta (`purple-*`, `pink-*`, `amber-*`, hex).
2. Modais para o `Modal` do kit: `CalculationModal`, `HistoryModal`, `ConfirmModal`, `CotacaoModal`
   (o `MemoriaModal` é componente compartilhado — só se já não estiver no padrão).
3. Auditoria: `.claude/skills/impeccable/scripts/impeccable.cmd context --target "app/(private)/estoque/analise/page.tsx"`
   (uma vez na sessão, cwd = cotacao-frontend) → playbook `reference/audit.md` → relatório em
   `.impeccable/audit/<data>__app-private-estoque-analise.md` (5 dimensões, nota /20, P0–P3) →
   corrigir todos os P0 e P1 (esperado: menu de contexto sem teclado, cabeçalhos de tabela sem
   `scope`, contraste dos rótulos, cores fora dos tokens, larguras fixas) → nova rodada só de
   confirmação.
4. Aceite: zero tabela/badge/select inline; dark mode íntegro; nota da auditoria ≥ 14/20 após as
   correções; largura fluida até 1536 px; todas as funções da tela (filtros, KPI drill-down, vincular/
   desvincular, cotação, exportar, memória de cálculo) funcionando como antes.

## 7. Fora do escopo (dependências registradas)

- Bolsa e comissão do vendedor (vendas-service, cartão próprio): linha de **liquidação** não entra na
  bolsa (nem a favor nem contra) e paga bônus de 5% do custo por unidade; linha de **promoção** entra
  pela metade (`0,5 × (liquido − custo × piso)`); venda fora da promoção/liquidação = regra normal.
  Para a bolsa saber o tipo, a exportação/carga registra cada campanha em `com_promo_campanha`
  (`pro_codigo`, `tipo` promocao|liquidacao, `preco_varejo`, `preco_atacado`, `inicio`, `fim`) — a
  lista grava essa tabela ao exportar a aba Carga ERP; a bolsa cruza linha do BI com `PROMOCAO='S'`
  + item/período da tabela.
- Marcar na lista os itens da Onda 1 do atacado (não recebem preço de tabela 2 até dez/26) — hoje
  exclusão manual do comercial.
