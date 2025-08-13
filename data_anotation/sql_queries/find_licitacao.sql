SELECT 
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    lpl.cnpj_participante,
    ceis.codigo_da_sancao as ceis_sancao,
    cnep."CODIGO_DA_SANCAO" as cnep_sancao,
    COUNT(lpl.numero_processo) AS numero_parcitipacoes
FROM 
    dsa."LicitacoesLicitacao" AS ll
INNER JOIN 
    dsa."LicitacoesParticipantesLicitacao" AS lpl ON ll.numero_do_processo = lpl.numero_processo
LEFT JOIN
    dsa."CEIS" as ceis ON CAST(ceis.cpf_ou_cnpj_do_sancionado AS text)  = lpl.cnpj_participante
LEFT JOIN
    dsa."CNEP" as cnep ON CAST(cnep."CPF_CNPJ" AS text)  = lpl.cnpj_participante
WHERE 
    ll.numero_licitacao = '4612022'
    AND ll.uf LIKE '%%SC%%'
    AND ll.municipio LIKE '%%FLORIANOPOLIS%%'
    AND (
		ll.search_ug @@ to_tsquery('portuguese', 'superintendencia & dnit')
		OR
		ll.search_orgao @@ to_tsquery('portuguese', 'superintendencia & dnit')
    )
GROUP BY
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    lpl.cnpj_participante,
    ceis.codigo_da_sancao,
    cnep."CODIGO_DA_SANCAO",
    ll.numero_do_processo -- Grouping to count participants per bid