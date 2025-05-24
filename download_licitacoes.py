# %%
from helper import baixarportal_transparencia, extrair_zips
import numpy as np
import os
# Obtendo os dados de compras (contratações)

KILLSWITH = False # Criando essa variavel apenas para nao acidentalmente baixar todos os dados novamente

if KILLSWITH:


    base_url = r"https://portaldatransparencia.gov.br/download-de-dados/compras"

    for ano in np.linspace(2013,2025,12):
        for mes in np.linspace(1,12,12):
            try:
                baixarportal_transparencia(base_url,'contratos',int(ano),int(mes))
            except Exception as e:
                print(e)

    # Obtendo dados de licitacoes
    base_url = r"https://portaldatransparencia.gov.br/download-de-dados/licitacoes"

    for ano in np.linspace(2013,2025,12):
        for mes in np.linspace(1,12,12):
            try:
                baixarportal_transparencia(base_url,'licitacoes',int(ano),int(mes))
            except Exception as e:
                print(e)

# %%
'''
Extraindo os arquivos .zip
'''
if KILLSWITH:
    pasta_dados = os.path.join(os.getcwd(), 'dados','licitacoes')
    extrair_zips(pasta_dados)