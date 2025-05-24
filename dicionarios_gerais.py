#
# %%
import pandas as pd
from helper import to_snake_case
# Dicionário Python
dicionario_CNEP = {
    'COLUNA': ['CADASTRO', 'CÓDIGO DA SANÇÃO', 'TIPO DE PESSOA', 'CPF OU CNPJ DO SANCIONADO', 'NOME DO SANCIONADO',
               'NOME INFORMADO PELO ÓRGÃO SANCIONADOR', 'RAZÃO SOCIAL - CADASTRO RECEITA',
               'NOME FANTASIA - CADASTRO RECEITA', 'NÚMERO DO PROCESSO', 'CATEGORIA DA SANÇÃO', 'VALOR DA MULTA',
               'DATA INÍCIO SANÇÃO', 'DATA FINAL SANÇÃO', 'DATA PUBLICAÇÃO', 'PUBLICAÇÃO',
               'DETALHAMENTO DO MEIO DE PUBLICAÇÃO', 'DATA DO TRÂNSITO EM JULGADO', 'ABRANGÊNCIA DA SANÇÃO',
               'ÓRGÃO SANCIONADOR', 'UF ÓRGÃO SANCIONADOR', 'ESFERA ÓRGÃO SANCIONADOR', 'FUNDAMENTAÇÃO LEGAL',
               'DATA ORIGEM INFORMAÇÃO', 'ORIGEM INFORMAÇÕES', 'OBSERVAÇÕES'],
    'DESCRIÇÃO': ['Nome do Cadastro ao qual a penalidade está vinculada.',
                  'Código da sanção no Banco de Sanções.',
                  'Identifica se a penalidade foi aplicada a "pessoa física" ou "pessoa jurídica".',
                  'Número de cadastro do sancionado junto à Receita Federal: CPF para pessoas físicas e CNPJ para pessoas jurídicas.',
                  'Nome do Sancionado conforme consta no Portal da Transparência.',
                  'Conforme registrado pelo órgão cadastrador no Banco de Sanções ou conforme publicado no DOU.',
                  'Campo extraído da base CNPJ ou da base CPF da Receita Federal (resultado da busca pelo valor do campo "CPF ou CNPJ do Sancionado")',
                  'Idem ao anterior. Vale registrar que as informações de identificação do sancionado (nome informado, razão social e nome fantasia) são mantidas no CNEP para facilitar a pesquisa e dar transparência às sanções quando ocorre mudança de algum destes dados do sancionado.',
                  'Número do processo no âmbito do qual foi aplicada a sanção.',
                  'Categoria da penalidade aplicada, a partir das sanções previstas na Lei Anticorrupção (Lei nº 12.846/2013).',
                  'Valor da multa aplicada. Após o pagamento, com o respectivo registro no sistema, esta sanção é excluída do CNEP.',
                  'Considera-se a data da publicação da sanção, quando não houver menção expressa à data de início de vigência da penalidade.',
                  'Considera-se o prazo estabelecido para o término de vigência da penalidade.',
                  'Data da publicação da sanção em veículo oficial de informação.',
                  'Veículo oficial de informação no qual a sanção foi publicada.',
                  'Dados da publicação.',
                  'Campo opcional que indica a data em que a decisão judicial pela aplicação da sanção transitou em julgado, ou seja, quando não se pode mais recorrer dessa decisão.',
                  'Extensão dos efeitos da sanção aplicada, quando houver. A definição quanto à abrangência da sanção é de responsabilidade do usuário do cadastro.',
                  'Órgão que aplicou a sanção.',
                  'Unidade da Federação do órgão responsável pela aplicação da sanção.',
                  'Esfera do órgão sancionador que aplicou a sanção (Federal, Estadual ou Municipal).',
                  'Dispositivo legal que fundamenta a aplicação da sanção.',
                  'Data de registro da sanção no Banco de Sanções.',
                  'Órgão do usuário que cadastrou a sanção no Sistema Banco de Sanções ou órgão gestor do sistema que cadastrou a sanção no Banco de Sanções. Há, por exemplo, Governos Estaduais que estabelecem um órgão como o responsável por concentrar o registro das sanções aplicadas por todos as Secretarias daquele Governo.',
                  'Campo de texto livre, não obrigatório, que pode trazer observações sobre a sanção, tais como a origem do registro, informações complementares e o detalhamento da abrangência da sanção, conforme o caso.']
}

# Criação do DataFrame
CNEP_dicionario_colunas = pd.DataFrame(dicionario_CNEP)

# %%
'''
Para o CEIS
'''

# Dicionário Python com os dados fornecidos
dicionario_tabela_ceis = {
    'COLUNA': ['CADASTRO', 'CÓDIGO DA SANÇÃO', 'TIPO DE PESSOA', 'CPF OU CNPJ DO SANCIONADO',
               'NOME DO SANCIONADO', 'NOME INFORMADO PELO ÓRGÃO SANCIONADOR', 'RAZÃO SOCIAL - CADASTRO RECEITA',
               'NOME FANTASIA - CADASTRO RECEITA', 'NÚMERO DO PROCESSO', 'CATEGORIA DA SANÇÃO',
               'DATA INÍCIO SANÇÃO', 'DATA FINAL SANÇÃO', 'DATA PUBLICAÇÃO', 'PUBLICAÇÃO',
               'DETALHAMENTO DO MEIO DE PUBLICAÇÃO', 'DATA DO TRÂNSITO EM JULGADO', 'ABRANGÊNCIA DA SANÇÃO',
               'ÓRGÃO SANCIONADOR', 'UF ÓRGÃO SANCIONADOR', 'ESFERA ÓRGÃO SANCIONADOR', 'FUNDAMENTAÇÃO LEGAL',
               'DATA ORIGEM INFORMAÇÃO', 'ORIGEM INFORMAÇÕES', 'OBSERVAÇÕES'],
    'DESCRIÇÃO': ['Nome do Cadastro ao qual a penalidade está vinculada.',
                  'Código da sanção no Banco de Sanções.',
                  'Identifica se a penalidade foi aplicada a "pessoa física" ou "pessoa jurídica".',
                  'Número de cadastro do sancionado junto à Receita Federal: CPF para pessoas físicas e CNPJ para pessoas jurídicas.',
                  'Nome do Sancionado conforme consta no Portal da Transparência.',
                  'Conforme registrado pelo órgão cadastrador no Banco de Sanções ou conforme publicado no DOU.',
                  'Campo extraído da base CNPJ ou da base CPF da Receita Federal (resultado da busca pelo valor do campo "CPF ou CNPJ do Sancionado")',
                  'Idem ao anterior. Vale registrar que as informações de identificação do sancionado (nome informado, razão social e nome fantasia) são mantidas no CEIS para facilitar a pesquisa e dar transparência às sanções quando ocorre mudança de algum destes dados do sancionado.',
                  'Número do processo no âmbito do qual foi aplicada a sanção.',
                  'Categoria da penalidade aplicada, referente às sanções que impliquem em restrição ao direito de participar de licitações ou de celebrar contratos com a Administração Pública.',
                  'Considera-se a data da publicação da sanção, quando não houver menção expressa à data de início de vigência da penalidade.',
                  'Considera-se o prazo estabelecido para o término de vigência da penalidade. Especificamente quanto à declaração de inidoneidade com fundamento no art. 87, IV, da Lei nº 8.666/93, mesmo que conste um prazo de vigência, este é considerado prazo mínimo da penalidade. Portanto, a inidoneidade só é excluída do CEIS mediante inativação no Banco de Sanções (alteração da situação da sanção para "inativa", seja por reabilitação ou outra justificativa).',
                  'Data da publicação da sanção em veículo oficial de informação.',
                  'Veículo oficial de informação no qual a sanção foi publicada.',
                  'Dados da publicação.',
                  'Campo opcional que indica a data em que a decisão judicial pela aplicação da sanção transitou em julgado, ou seja, quando não se pode mais recorrer dessa decisão.',
                  'Extensão dos efeitos da sanção aplicada, quando houver. A definição quanto à abrangência da sanção é de responsabilidade do usuário do cadastro.',
                  'Órgão que aplicou a sanção.',
                  'Unidade da Federação do órgão responsável pela aplicação da sanção.',
                  'Esfera do órgão sancionador que aplicou a sanção (Federal, Estadual ou Municipal).',
                  'Dispositivo legal que fundamenta a aplicação da sanção.',
                  'Data de registro da sanção no Banco de Sanções.',
                  'Órgão do usuário que cadastrou a sanção no Sistema Banco de Sanções ou órgão gestor do sistema que cadastrou a sanção no Banco de Sanções. Há, por exemplo, Governos Estaduais que estabelecem um órgão como o responsável por concentrar o registro das sanções aplicadas por todos as Secretarias daquele Governo. Também é o caso do CNJ, que mantém o cadastro das sanções aplicadas por todos os órgãos judiciários nas ações de improbidade administrativa. Situação semelhante ocorre com o Ministério da Fazenda, que aparece como órgão de origem das sanções cadastradas via SICAF.',
                  'Campo de texto livre, não obrigatório, que pode trazer observações sobre a sanção, tais como a origem do registro, informações complementares e o detalhamento da abrangência da sanção, conforme o caso.']
}

# Criação do DataFrame
CEIS_dicionario_colunas = pd.DataFrame(dicionario_tabela_ceis)

# Exibição do DataFrame

# %%
'''
Dicionário Notas Fiscais
'''
# Dicionário Python com os dados da Nota Fiscal
dicionario_tabela_nf = {
    'COLUNA': ['CHAVE DE ACESSO', 'MODELO', 'SÉRIE', 'NÚMERO', 'NATUREZA DA OPERAÇÃO', 'DATA EMISSÃO',
               'EVENTO MAIS RECENTE', 'DATA/HORA EVENTO MAIS RECENTE', 'CPF/CNPJ Emitente',
               'RAZÃO SOCIAL EMITENTE', 'INSCRIÇÃO ESTADUAL EMITENTE', 'UF EMITENTE', 'MUNICÍPIO EMITENTE',
               'CNPJ DESTINATÁRIO', 'NOME DESTINATÁRIO', 'UF DESTINATÁRIO', 'INDICADOR IE DESTINATÁRIO',
               'DESTINO DA OPERAÇÃO', 'CONSUMIDOR FINAL', 'PRESENÇA DO COMPRADOR', 'VALOR NOTA FISCAL'],
    'DESCRIÇÃO': ['Chave de Acesso da Nota Fiscal',
                  'Modelo da Nota Fiscal',
                  'Série da Nota Fiscal',
                  'Número da Nota Fiscal',
                  'Termo que identifica a operação comercial que está sendo efetuada no tempo da emissão da Notas Fiscal',
                  'Data de emissão da Nota Fiscal',
                  'É possível encontrar as seguintes situações no Portal da Transparência:\n'
                  '• Autorizado o uso da NF-e: Situação inicial da Nota Fiscal autorizando o seu uso pelo emissor.\n'
                  '• Autorizado o uso da NF-e, autorização fora de prazo: Situação inicial da Nota Fiscal autorizando fora de prazo o seu uso pelo emissor.\n'
                  '• Cancelamento da NF-e: Este evento tem como objetivo cancelar uma NF-e autorizada.\n'
                  '• Carta de correção: Utilizada para a regularização de erro ocorrido na emissão do documento fiscal, desde que o erro não esteja relacionado com: I - as variáveis que determinam o valor do imposto tais como: base de cálculo, alíquota, diferença de preço, quantidade, valor da operação ou da prestação; II - a correção de dados cadastrais que implique mudança do remetente ou do destinatário; III - a data de emissão ou de saída.\n'
                  '• Manifestação do destinatário - Ciência da Operação: Recebimento pelo destinatário de informações relativas à existência de NF-e em que esteja envolvido, quando ainda não existem elementos suficientes para apresentar uma manifestação conclusiva.\n'
                  '• Manifestação do destinatário - Confirmação da Operação: Manifestação do destinatário confirmando que a operação descrita na NF-e ocorreu exatamente como informado nesta NF-e\n'
                  '• Manifestação do destinatário - Operação não realizada: Manifestação do destinatário reconhecendo sua participação na operação descrita na NF-e, mas declarando que a operação não ocorreu ou não se efetivou como informado nesta NF-e.\n'
                  '• Manifestação do destinatário - Desconhecimento da Operação: Manifestação do destinatário declarando que a operação descrita da NF-e não foi por ele solicitada.',
                  'Data do último evento da Nota Fiscal',
                  'CPF/CNPJ da Nota Fiscal',
                  'Razão social do Fornecedor (emitente da Nota Fiscal)',
                  'Inscrição estadual do Fornecedor (emitente da Nota Fiscal)',
                  'UF do Fornecedor (emitente da Nota Fiscal)',
                  'Município do Fornecedor (emitente da Nota Fiscal)',
                  'CNPJ do órgão destinatário',
                  'Nome do órgão destinatário',
                  'UF do órgão destinatário',
                  'Informa se o órgão destinatário é contribuinte de Imposto Estadual.',
                  'Informa se a operação é interestadual ou interna (ocorre dentro da mesma UF)',
                  'Informa se o órgão destinatário é o consumidor final',
                  'Informa se o comprador esteve presente',
                  'Valor da Nota Fiscal']
}

# Criação do DataFrame
dataframe_tabela_nf = pd.DataFrame(dicionario_tabela_nf)


import pandas as pd

# Dicionário Python com os dados de NotaFiscalEvento
dicionario_tabela_nf_evento = {
    'COLUNA': ['CHAVE DE ACESSO', 'MODELO', 'SÉRIE', 'NÚMERO', 'NATUREZA DA OPERAÇÃO', 'DATA EMISSÃO',
               'EVENTO', 'DATA/HORA EVENTO', 'DESCRIÇÃO EVENTO', 'MOTIVO EVENTO'],
    'DESCRIÇÃO': ['Chave de Acesso da Nota Fiscal',
                  'Modelo da Nota Fiscal',
                  'Série da Nota Fiscal',
                  'Número da Nota Fiscal',
                  'Termo que identifica a operação comercial que está sendo efetuada no tempo da emissão da Notas Fiscal',
                  'Data de emissão da Nota Fiscal',
                  'É possível encontrar as seguintes situações no Portal da Transparência:\n'
                  '• Autorizado o uso da NF-e: Situação inicial da Nota Fiscal autorizando o seu uso pelo emissor.\n'
                  '• Autorizado o uso da NF-e, autorização fora de prazo: Situação inicial da Nota Fiscal autorizando fora de prazo o seu uso pelo emissor.\n'
                  '• Cancelamento da NF-e: Este evento tem como objetivo cancelar uma NF-e autorizada.\n'
                  '• Carta de correção: Utilizada para a regularização de erro ocorrido na emissão do documento fiscal, desde que o erro não esteja relacionado com: I - as variáveis que determinam o valor do imposto tais como: base de cálculo, alíquota, diferença de preço, quantidade, valor da operação ou da prestação; II - a correção de dados cadastrais que implique mudança do remetente ou do destinatário; III - a data de emissão ou de saída.\n'
                  '• Manifestação do destinatário - Ciência da Operação: Recebimento pelo destinatário de informações relativas à existência de NF-e em que esteja envolvido, quando ainda não existem elementos suficientes para apresentar uma manifestação conclusiva.\n'
                  '• Manifestação do destinatário - Confirmação da Operação: Manifestação do destinatário confirmando que a operação descrita na NF-e ocorreu exatamente como informado nesta NF-e\n'
                  '• Manifestação do destinatário - Operação não realizada: Manifestação do destinatário reconhecendo sua participação na operação descrita na NF-e, mas declarando que a operação não ocorreu ou não se efetivou como informado nesta NF-e.\n'
                  '• Manifestação do destinatário - Desconhecimento da Operação: Manifestação do destinatário declarando que a operação descrita da NF-e não foi por ele solicitada.',
                  'Data do evento',
                  'Descrição do evento',
                  'Motivo do evento']
}

# Criação do DataFrame
dataframe_tabela_nf_evento = pd.DataFrame(dicionario_tabela_nf_evento)

import pandas as pd

# Dicionário Python com os dados de NotaFiscalItem
dicionario_tabela_nf_item = {
    'COLUNA': ['CHAVE DE ACESSO', 'MODELO', 'SÉRIE', 'NÚMERO', 'NATUREZA DA OPERAÇÃO', 'DATA EMISSÃO',
               'CPF/CNPJ Emitente', 'RAZÃO SOCIAL EMITENTE', 'INSCRIÇÃO ESTADUAL EMITENTE', 'UF EMITENTE',
               'MUNICÍPIO EMITENTE', 'CNPJ DESTINATÁRIO', 'NOME DESTINATÁRIO', 'UF DESTINATÁRIO',
               'INDICADOR IE DESTINATÁRIO', 'DESTINO DA OPERAÇÃO', 'CONSUMIDOR FINAL', 'PRESENÇA DO COMPRADOR',
               'NÚMERO PRODUTO', 'DESCRIÇÃO DO PRODUTO/SERVIÇO', 'CÓDIGO NCM/SH', 'NCM/SH (TIPO DE PRODUTO)',
               'CFOP', 'QUANTIDADE', 'UNIDADE', 'VALOR UNITÁRIO', 'VALOR TOTAL'],
    'DESCRIÇÃO': ['Chave de Acesso da Nota Fiscal',
                  'Modelo da Nota Fiscal',
                  'Série da Nota Fiscal',
                  'Número da Nota Fiscal',
                  'Termo que identifica a operação comercial que está sendo efetuada no tempo da emissão da Notas Fiscal',
                  'Data de emissão da Nota Fiscal',
                  'CPF/CNPJ da Nota Fiscal',
                  'Razão social do Fornecedor (emitente da Nota Fiscal)',
                  'Inscrição estadual do Fornecedor (emitente da Nota Fiscal)',
                  'UF do Fornecedor (emitente da Nota Fiscal)',
                  'Município do Fornecedor (emitente da Nota Fiscal)',
                  'CNPJ do órgão destinatário',
                  'Nome do órgão destinatário',
                  'UF do órgão destinatário',
                  'Informa se o órgão destinatário é contribuinte de Imposto Estadual.',
                  'Informa se a operação é interestadual ou interna (ocorre dentro da mesma UF)',
                  'Informa se o órgão destinatário é o consumidor final',
                  'Informa se o comprador esteve presente',
                  'Número do produto na Nota Fiscal (os produtos de uma mesma nota fiscal são sequenciados numericamente)',
                  'Descrição do produto/serviço',
                  'Código NCM (Nomenclatura Comum do Mercosul) / SH (Sistema Harmonizado). Código usado para identificar o tipo de produto.',
                  'Identificação do tipo de produto - NCM/SH (Nomenclatura Comum do Mercosul / Sistema Harmonizado)',
                  'Código Fiscal de Operações e Prestações. Esse código identifica uma determinada operação por categorias no momento da emissão da nota fiscal.',
                  'Quantidade de unidades do produto/serviço',
                  'Unidade de medida do produto/serviço',
                  'Valor unitário do item',
                  'Valor total do Item']
}

# Criação do DataFrame
dataframe_tabela_nf_item = pd.DataFrame(dicionario_tabela_nf_item)

# %%
'''
COLUNAS DO BANCO DE DADOS
'''

CNEP_SQL_COLUMNS = {
                             'CADASTRO':"CADASTRO",
                     'CÓDIGO DA SANÇÃO':"CODIGO_DA_SANCAO",
                       'TIPO DE PESSOA':"TIPO_DE_PESSOA",
            'CPF OU CNPJ DO SANCIONADO':"CPF_CNPJ",
                   'NOME DO SANCIONADO':"NOME_DO_SANCIONADO",
'NOME INFORMADO PELO ÓRGÃO SANCIONADOR':"NOME_INFORMADO_PELO_ORGAO_SANCIONADOR",
      'RAZÃO SOCIAL - CADASTRO RECEITA':"RAZAO_SOCIAL_CADASTRO_RECEITA",
     'NOME FANTASIA - CADASTRO RECEITA':"NOME_FANTASIA_CADASTRO_RECEITA",
                   'NÚMERO DO PROCESSO':"NUMERO_DO_PROCESSO",
                  'CATEGORIA DA SANÇÃO':"CATEGORIA_DA_SANCAO",
                       'VALOR DA MULTA':"VALOR_DA_MULTA",
                   'DATA INÍCIO SANÇÃO':"DATA_INICIO_SANCAO",
                    'DATA FINAL SANÇÃO':"DATA_FINAL_SANCAO",
                      'DATA PUBLICAÇÃO':"DATA_PUBLICACAO",
                           'PUBLICAÇÃO':"PUBLICACAO",
   'DETALHAMENTO DO MEIO DE PUBLICAÇÃO':"DETALHAMENTO_DO_MEIO_DE_PUBLICACAO",
          'DATA DO TRÂNSITO EM JULGADO':"DATA_DO_TRANSITO_EM_JULGADO",
                'ABRANGÊNCIA DA SANÇÃO':"ABRANGENCIA_DA_SANCAO",
                    'ÓRGÃO SANCIONADOR':"ORGAO_SANCIONADOR",
                 'UF ÓRGÃO SANCIONADOR':"UF_ORGAO_SANCIONADOR",
             'ESFERA ÓRGÃO SANCIONADOR':"ESFERA_ORGAO_SANCIONADOR",
                  'FUNDAMENTAÇÃO LEGAL':"FUNDAMENTACAO_LEGAL",
               'DATA ORIGEM INFORMAÇÃO':"DATA_ORIGEM_INFORMACAO",
                   'ORIGEM INFORMAÇÕES':"ORIGEM_INFORMACOES",
                          'OBSERVAÇÕES':"OBSERVACOES",

}

CEIS_SQL_COLUMNS = {
    'CADASTRO': 'cadastro',
    'CÓDIGO DA SANÇÃO': 'codigo_da_sancao',
    'TIPO DE PESSOA': 'tipo_de_pessoa',
    'CPF OU CNPJ DO SANCIONADO': 'cpf_ou_cnpj_do_sancionado',
    'NOME DO SANCIONADO': 'nome_do_sancionado',
    'NOME INFORMADO PELO ÓRGÃO SANCIONADOR': 'nome_informado_pelo_orgao_sancionador',
    'RAZÃO SOCIAL - CADASTRO RECEITA': 'razao_social_cadastro_receita',
    'NOME FANTASIA - CADASTRO RECEITA': 'nome_fantasia_cadastro_receita',
    'NÚMERO DO PROCESSO': 'numero_do_processo',
    'CATEGORIA DA SANÇÃO': 'categoria_da_sancao',
    'DATA INÍCIO SANÇÃO': 'data_inicio_sancao',
    'DATA FINAL SANÇÃO': 'data_final_sancao',
    'DATA PUBLICAÇÃO': 'data_publicacao',
    'PUBLICAÇÃO': 'publicacao',
    'DETALHAMENTO DO MEIO DE PUBLICAÇÃO': 'detalhamento_do_meio_de_publicacao',
    'DATA DO TRÂNSITO EM JULGADO': 'data_do_transito_em_julgado',
    'ABRANGÊNCIA DA SANÇÃO': 'abrangencia_da_sancao',
    'ÓRGÃO SANCIONADOR': 'orgao_sancionador',
    'UF ÓRGÃO SANCIONADOR': 'uf_orgao_sancionador',
    'ESFERA ÓRGÃO SANCIONADOR': 'esfera_orgao_sancionador',
    'FUNDAMENTAÇÃO LEGAL': 'fundamentacao_legal',
    'DATA ORIGEM INFORMAÇÃO': 'data_origem_informacao',
    'ORIGEM INFORMAÇÕES': 'origem_informacoes',
    'OBSERVAÇÕES': 'observacoes'
}

NFE_SQL_COLUMNS = {
    'CHAVE DE ACESSO': 'chave_de_acesso',
    'MODELO': 'modelo',
    'SÉRIE': 'serie',
    'NÚMERO': 'numero',
    'NATUREZA DA OPERAÇÃO': 'natureza_da_operacao',
    'DATA EMISSÃO': 'data_emissao',
    'CPF/CNPJ Emitente': 'cpf_cnpj_emitente',
    'RAZÃO SOCIAL EMITENTE': 'razao_social_emitente',
    'INSCRIÇÃO ESTADUAL EMITENTE': 'inscricao_estadual_emitente',
    'UF EMITENTE': 'uf_emitente',
    'MUNICÍPIO EMITENTE': 'municipio_emitente',
    'CNPJ DESTINATÁRIO': 'cnpj_destinatario',
    'NOME DESTINATÁRIO': 'nome_destinatario',
    'UF DESTINATÁRIO': 'uf_destinatario',
    'INDICADOR IE DESTINATÁRIO': 'indicador_ie_destinatario',
    'DESTINO DA OPERAÇÃO': 'destino_da_operacao',
    'CONSUMIDOR FINAL': 'consumidor_final',
    'PRESENÇA DO COMPRADOR': 'presenca_do_comprador',
    'NÚMERO PRODUTO': 'numero_produto',
    'DESCRIÇÃO DO PRODUTO/SERVIÇO': 'descricao_do_produto_servico',
    'CÓDIGO NCM/SH': 'codigo_ncm_sh',
    'NCM/SH (TIPO DE PRODUTO)': 'ncm_sh_tipo_de_produto',
    'CFOP': 'cfop',
    'QUANTIDADE': 'quantidade',
    'UNIDADE': 'unidade',
    'VALOR UNITÁRIO': 'valor_unitario',
    'VALOR TOTAL': 'valor_total'
}

# %%
'''
Dicionarios para Licitacoes
'''

# Dicionário Python para dicionario_tabela_contratos_compras
dicionario_tabela_contratos_compras = {
    'COLUNA': ['Número do Contrato', 'Objeto', 'Fundamento legal', 'Modalidade de compra',
               'Situação Contrato', 'Código do Órgão Superior', 'Nome Órgão Superior',
               'Código Órgão', 'Nome Órgão', 'Código UG', 'Nome UG',
               'Data Assinatura Contrato', 'Data Publicação DOU', 'Data Início da Vigência',
               'Data Fim da Vigência', 'Código Contratado', 'Nome Contratado',
               'Valor Inicial da Compra', 'Valor Final da Compra', 'Número da Licitação',
               'Código UG Licitação', 'Nome UG Licitação', 'Código Modalidade Compra Licitação',
               'Modalidade Compra Licitação'],
    'DESCRIÇÃO': ['Número que identifica o contrato no ComprasNet',
                  'Objeto do contrato',
                  'Indicação do embasamento legal do contrato',
                  'Concorrência; Concurso; Convite; Dispensa de Licitação; Inexigibilidade de Licitação; Pregão; Registro de Preço; Tomada de Preços.',
                  'Situação em que se encontra o contrato',
                  'Código do Órgão Superior responsável pela licitação\nÓRGÃO SUPERIOR - Unidade da Administração Direta que tenha entidades por ele supervisionadas.\nFonte: Manual do SIAFI',
                  'Nome do Órgão Superior',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Código da Unidade Gestora do contrato.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei nr. 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Data da assinatura do contrato',
                  'Data da publicação do contrato no DOU',
                  'Data de início da vigência do contrato',
                  'Data de fim da vigência do contrato',
                  'CNPJ do contratado',
                  'Nome do contratado',
                  'Valor inicial da compra',
                  'Valor final da compra após possíveis reajustes, acréscimos etc',
                  'Número que identifica a licitação no SIASG',
                  'Código da Unidade Gestora responsável pela licitação',
                  'Nome da Unidade Gestora responsável pela licitação',
                  'Código da modalidade',
                  'Modalidade da Licitação']
}

# Dicionário Python para dicionario_tabela_contratos_itemCompra
dicionario_tabela_contratos_itemCompra = {
    'COLUNA': ['Número Contrato', 'Código Órgão', 'Nome Órgão', 'Código UG', 'Nome UG',
               'Código Item Compra', 'Descrição Item Compra', 'Quantidade Item', 'Valor Item'],
    'DESCRIÇÃO': ['Número que identifica o contrato no ComprasNet',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Código da Unidade Gestora do contrato.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei nr. 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Código do item da compra no SIASG. O código do item é um número composto por 22 dígitos, formado através a seguinte lógica: 6 dígitos do código da Unidade Gestora + 2 dígitos da modalidade de compra + 5 dígitos do número da licitação no ano + 4 dígitos do ano da licitação + 5 dígitos do sequencial que identifica o item dentro da licitação.\n\nCódigos de modalidade de compra:\n01 - Convite\n02 - Tomada de Preços\n03 - Concorrência\n04 - Concorrência Internacional\n05 - Pregão\n06 - Dispensa de Licitação\n07 - Inexigibilidade de Licitação\n20 - Concurso\n22 - Tomada de Preços por Técnica e Preço\n33 - Concorrência por Técnica e Preço\n44 - Concorrência Internacional por Técnica e Preço\n-99 - Pregão - Registro de Preços',
                  'Descrição do item',
                  'Quantidade do item',
                  'Valor unitário do item']
}

# Dicionário Python para dicionario_tabela_contratos_termoAditivo
dicionario_tabela_contratos_termoAditivo = {
    'COLUNA': ['Número Contrato', 'Código do Órgão Superior', 'Nome Órgão Superior',
               'Código Órgão', 'Nome Órgão', 'Código UG', 'Nome UG',
               'Número Termo Aditivo', 'Data Publicação', 'Objeto'],
    'DESCRIÇÃO': ['Número que identifica o contrato no ComprasNet',
                  'Código do Órgão Superior responsável pela licitação\nÓRGÃO SUPERIOR - Unidade da Administração Direta que tenha entidades por ele supervisionadas.\nFonte: Manual do SIAFI',
                  'Nome do Órgão Superior',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Código da Unidade Gestora do contrato.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei nr. 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Número que identifica o termo aditivo no ComprasNet',
                  'Data da publicação do termo aditivo no DOU',
                  'Objeto do termo aditivo']
}

# Dicionário Python para dicionario_tabela_licitacoes_EmpenhosRelacionados
dicionario_tabela_licitacoes_EmpenhosRelacionados = {
    'COLUNA': ['Número Licitação', 'Código UG', 'Nome UG', 'Código Modalidade de Compra',
               'Modalidade compra', 'Número Processo', 'Código Empenho',
               'Data Emissão Empenho', 'Observação Empenho', 'Valor Empenho (R$)'],
    'DESCRIÇÃO': ['Número que identifica a licitação no SIASG',
                  'Código da Unidade Gestora responsável pela licitação.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei Nº 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Código da Modalidade de Compra',
                  'Modalidades de Compra:\n· Concorrência;\n· Concurso;\n· Convite;\n· Dispensa de Licitação;\n· Inexigibilidade de Licitação;\n· Pregão;\n· Registro de Preço;\n· Tomada de Preços.',
                  'Número do processo da licitação',
                  'Código do Empenho da Licitação',
                  'Data de Emissão do Empenho',
                  'Observação do Empenho',
                  'Valor do Empenho (R$)']
}

# Dicionário Python para dicionario_tabela_licitacoes_ItemLicitacao
dicionario_tabela_licitacoes_ItemLicitacao = {
    'COLUNA': ['Número Licitação', 'Código UG', 'Nome UG', 'Código Modalidade de Compra',
               'Modalidade compra', 'Número Processo', 'Código Órgão', 'Nome Órgão',
               'Código Item Compra', 'Descrição', 'Quantidade Item', 'Valor Item',
               'Código Vencedor', 'Nome Vencedor'],
    'DESCRIÇÃO': ['Número que identifica a licitação no SIASG',
                  'Código da Unidade Gestora responsável pela licitação.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei Nº 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Código da Modalidade de Compra',
                  'Modalidades de Compra:\n· Concorrência;\n· Concurso;\n· Convite;\n· Dispensa de Licitação;\n· Inexigibilidade de Licitação;\n· Pregão;\n· Registro de Preço;\n· Tomada de Preços.',
                  'Número do processo da licitação',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Código do item da compra no SIASG. O código do item é um número composto por 22 dígitos, formado através a seguinte lógica: 6 dígitos do código da Unidade Gestora + 2 dígitos da modalidade de compra + 5 dígitos do número da licitação no ano + 4 dígitos do ano da licitação + 5 dígitos do sequencial que identifica o item dentro da licitação.\n\nCódigos de modalidade de compra:\n01 - Convite\n02 - Tomada de Preços\n03 - Concorrência\n04 - Concorrência Internacional\n05 - Pregão\n06 - Dispensa de Licitação\n07 - Inexigibilidade de Licitação\n20 - Concurso\n22 - Tomada de Preços por Técnica e Preço\n33 - Concorrência por Técnica e Preço\n44 - Concorrência Internacional por Técnica e Preço\n-99 - Pregão - Registro de Preços',
                  'Descrição do item da compra no SIASG',
                  'Quantidade do item',
                  'Valor total do item',
                  'CNPJ do licitante vencedor',
                  'Nome do CNPJ vencedor']
}

# Dicionário Python para dicionario_tabela_licitacoes_licitacao
dicionario_tabela_licitacoes_licitacao = {
    'COLUNA': ['Número Licitação', 'Código UG', 'Nome UG', 'Código Modalidade de Compra',
               'Modalidade compra', 'Número do Processo', 'Objeto', 'Situação Licitação',
               'Código do Órgão Superior', 'Nome Órgão Superior', 'Código Órgão', 'Nome Órgão',
               'UF/Município', 'Data Resultado Compra', 'Data Abertura', 'Valor Licitação'],
    'DESCRIÇÃO': ['Número que identifica a licitação no SIASG',
                  'Código da Unidade Gestora responsável pela licitação.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei Nº 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Código da Modalidade de Compra',
                  'Modalidades de Compra:\n· Concorrência;\n· Concurso;\n· Convite;\n· Dispensa de Licitação;\n· Inexigibilidade de Licitação;\n· Pregão;\n· Registro de Preço;\n· Tomada de Preços.',
                  'Número do processo da licitação',
                  'Objeto da licitação, ou seja, aquilo que se quer comprar, alienar ou contratar',
                  'Situação em que se encontra o processo licitatório',
                  'Código do Órgão Superior responsável pela licitação\nÓRGÃO SUPERIOR - Unidade da Administração Direta que tenha entidades por ele supervisionadas.\nFonte: Manual do SIAFI',
                  'Nome do Órgão Superior',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Estado/Município onde ocorre a licitação',
                  'Data da publicação da Homologação no Diário Oficial da União',
                  'Data de abertura para envio das Propostas',
                  'Valor total licitado']
}

# Dicionário Python para dicionario_tabela_licitacoes_participantes_licitacao
dicionario_tabela_licitacoes_participantes_licitacao = {
    'COLUNA': ['Número Licitação', 'Código UG', 'Nome UG', 'Código Modalidade de Compra',
               'Modalidade compra', 'Número Processo', 'Código Órgão', 'Nome Órgão',
               'Código Item Compra', 'Descrição Item Compra', 'CNPJ Participante',
               'Nome Participante', 'Flag Vencedor'],
    'DESCRIÇÃO': ['Número que identifica a licitação no SIASG',
                  'Código da Unidade Gestora responsável pela licitação.\nUNIDADE GESTORA (UG) - Unidade Orçamentária ou Administrativa que realiza atos de gestão orçamentária, financeira e/ou patrimonial, cujo titular, em consequência, está sujeito a tomada de contas anual na conformidade do disposto nos artigos 81 e 82 do Decreto-lei Nº 200, de 25 de fevereiro de 1967.\nFonte: Manual do SIAFI',
                  'Nome da Unidade Gestora',
                  'Código da Modalidade de Compra',
                  'Modalidades de Compra:\n· Concorrência;\n· Concurso;\n· Convite;\n· Dispensa de Licitação;\n· Inexigibilidade de Licitação;\n· Pregão;\n· Registro de Preço;\n· Tomada de Preços.',
                  'Número do processo da licitação',
                  'Código do Órgão responsável pela licitação\nÓRGÃO SUBORDINADO - Entidade supervisionada por um Órgão da Administração Direta.\nFonte: Manual do SIAFI',
                  'Nome do Órgão',
                  'Código do item da compra no SIASG',
                  'Descrição do item da compra no SIASG',
                  'CNPJ do participante na licitação',
                  'Nome do CNPJ do participante',
                  'Indica se o participante é vencedor "SIM" ou "NÃO"']
}


contratos_compras_dicionario_colunas = pd.DataFrame(dicionario_tabela_contratos_compras)
contratos_itemCompra_dicionario_colunas = pd.DataFrame(dicionario_tabela_contratos_itemCompra)
contratos_termoAditivo_dicionario_colunas = pd.DataFrame(dicionario_tabela_contratos_termoAditivo)
licitacoes_EmpenhosRelacionados_dicionario_colunas = pd.DataFrame(dicionario_tabela_licitacoes_EmpenhosRelacionados)
licitacoes_ItemLicitacao_dicionario_colunas = pd.DataFrame(dicionario_tabela_licitacoes_ItemLicitacao)
licitacoes_licitacao_dicionario_colunas = pd.DataFrame(dicionario_tabela_licitacoes_licitacao)
licitacoes_participantes_licitacao_dicionario_colunas = pd.DataFrame(dicionario_tabela_licitacoes_participantes_licitacao)


contratos_compras_SQL_COLUMNS = {}
contratos_itemCompra_SQL_COLUMNS = {}
contratos_termoAditivo_SQL_COLUMNS = {}
licitacoes_EmpenhosRelacionados_SQL_COLUMNS = {}
licitacoes_ItemLicitacao_SQL_COLUMNS = {}
licitacoes_licitacao_SQL_COLUMNS = {}
licitacoes_participantes_licitacao_SQL_COLUMNS = {}

for each_column in dicionario_tabela_contratos_compras['COLUNA']:
    contratos_compras_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_contratos_itemCompra['COLUNA']:
    contratos_itemCompra_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_contratos_termoAditivo['COLUNA']:
    contratos_termoAditivo_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_licitacoes_EmpenhosRelacionados['COLUNA']:
    licitacoes_EmpenhosRelacionados_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_licitacoes_ItemLicitacao['COLUNA']:
    licitacoes_ItemLicitacao_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_licitacoes_licitacao['COLUNA']:
    licitacoes_licitacao_SQL_COLUMNS[each_column] = to_snake_case(each_column)

for each_column in dicionario_tabela_licitacoes_participantes_licitacao['COLUNA']:
    licitacoes_participantes_licitacao_SQL_COLUMNS[each_column] = to_snake_case(each_column)



# %%
