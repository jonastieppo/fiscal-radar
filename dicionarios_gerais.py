#
# %%
import pandas as pd

# Dicionário Python
dicionario_CNEP = {
    'COLUNA': ['CADASTRO', 'CÓDIGO DA SANÇÃO', 'TIPO DE PESSOA', 'CPF OU CNPJ DO SANCIONADO', 'NOME DO SANCIONADO',
               'NOME INFORMADO PELO ÓRGÃO SANCIONADOR', 'RAZÃO SOCIAL – CADASTRO RECEITA',
               'NOME FANTASIA – CADASTRO RECEITA', 'NÚMERO DO PROCESSO', 'CATEGORIA DA SANÇÃO', 'VALOR DA MULTA',
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
               'NOME DO SANCIONADO', 'NOME INFORMADO PELO ÓRGÃO SANCIONADOR', 'RAZÃO SOCIAL – CADASTRO RECEITA',
               'NOME FANTASIA – CADASTRO RECEITA', 'NÚMERO DO PROCESSO', 'CATEGORIA DA SANÇÃO',
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
