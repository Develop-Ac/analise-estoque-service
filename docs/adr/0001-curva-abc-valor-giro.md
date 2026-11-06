# 0001 — Curva ABC por valor × giro

**Data:** 2026-11-06 · **Status:** aceita

## Contexto

A curva ABC era só o Pareto do valor vendido em 12 meses (70/90/97%). Com ~11 mil
itens vendidos, bastavam ~R$ 2.060 no ano para ser A: 805 dos 1.389 itens A
venderam menos de 12 unidades no ano (para-brisa, farol e retrovisor importados),
e itens baratos de giro mensal ficavam em C/D. A fila de compra filtrada por curva
A mandava repor peça que vende 1×/ano e esquecia o que sai todo mês.

Teunter, Babai & Syntetos (2010) mostram que classificar por um critério só (valor
ou volume) com nível de serviço fixo por classe fica longe do custo ótimo.

## Decisão

`curva_abc` = matriz de Flores & Whybark (1986) entre **valor** (Pareto 70/90/97,
gravado em `curva_valor`) e **giro** = meses com venda na janela (`classe_giro`:
A ≥ 6, B 3–5, C 0–2; parâmetros `GIRO_MESES_A/B`). Classes finais: AA/AB/BA = A;
AC/BB/CA = B; BC/CB = C; CC = D (valor D entra como C).

Giro por **meses com venda**, não por quantidade: quantidade mistura unidade, par,
jogo e metro, e uma venda única grande (200 parafusos, brinde) punha item em A.
Meses com venda é o critério FSN por % de períodos.

## Consequências

- Nível de serviço continua escolhido pelo custo (margem × custo de manter); a curva
  só muda a faixa piso/teto e a prioridade da sugestão. Muitos itens mudam de faixa
  na 1ª rodada (simulação nov/25–out/26: dos A antigos, 1.015 ficam A e 356 descem;
  433 B sobem para A) — o mín/máx desses itens muda e `teve_alteracao_analise` acende.
- Vale para `grupo_curva` (giro do grupo = meses em que alguma marca vendeu).
- `backtest.py` usa a mesma regra.
