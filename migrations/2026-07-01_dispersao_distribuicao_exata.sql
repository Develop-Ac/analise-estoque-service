-- ============================================================================
-- Migração: tamanho médio do lote + CV² (dispersão) — p/ a DISTRIBUIÇÃO EXATA
-- do gráfico da memória de cálculo (itens/grupos intermitentes).
-- Aplicar ANTES de rodar a análise nova. Idempotente. Exige RE-RUN para popular.
-- (O `python main.py run` também cria essas colunas automaticamente.)
-- ============================================================================
BEGIN;
ALTER TABLE com_fifo_completo ADD COLUMN IF NOT EXISTS mean_size_mes   DECIMAL(15,4);
ALTER TABLE com_fifo_completo ADD COLUMN IF NOT EXISTS cv2_tamanho     DECIMAL(10,4);
ALTER TABLE com_fifo_completo ADD COLUMN IF NOT EXISTS grupo_mean_size DECIMAL(15,4);
ALTER TABLE com_fifo_completo ADD COLUMN IF NOT EXISTS grupo_cv2       DECIMAL(10,4);
COMMIT;
