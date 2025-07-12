-- Check if columns exist first (optional)
UPDATE dsa."LicitacoesLicitacao"
SET search_ug = to_tsvector('portuguese', "NOME_UG_NORM")
WHERE search_ug IS DISTINCT FROM to_tsvector('portuguese', "NOME_UG_NORM");

UPDATE dsa."LicitacoesLicitacao"
SET search_orgao = to_tsvector('portuguese', "NOME_ORGAO_NORM")
WHERE search_orgao IS DISTINCT FROM to_tsvector('portuguese', "NOME_ORGAO_NORM");

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_gin_licitacao_search_ug ON dsa."LicitacoesLicitacao" USING GIN(search_ug);
CREATE INDEX IF NOT EXISTS idx_gin_licitacao_search_orgao ON dsa."LicitacoesLicitacao" USING GIN(search_orgao);