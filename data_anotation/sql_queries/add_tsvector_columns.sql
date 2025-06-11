UPDATE dsa."LicitacoesLicitacao"
SET search_ug = to_tsvector('portuguese', "NOME_UG_NORM");

UPDATE dsa."LicitacoesLicitacao"
SET search_orgao = to_tsvector('portuguese', "NOME_ORGAO_NORM");

-- Creates a GIN index on the 'search_ug' column
CREATE INDEX idx_gin_licitacao_search_ug ON dsa."LicitacoesLicitacao" USING GIN(search_ug);

-- Creates a GIN index on the 'search_orgao' column
CREATE INDEX idx_gin_licitacao_search_orgao ON dsa."LicitacoesLicitacao" USING GIN(search_orgao);