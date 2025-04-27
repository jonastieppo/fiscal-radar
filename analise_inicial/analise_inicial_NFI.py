# %%
import os
import sys
sys.path.insert(0,os.path.join(os.getcwd(), '..'))

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dicionarios_gerais import dataframe_tabela_nf_item
from helper import selecionar_faixa

def init_exploration():
    data = pd.read_csv('dados/NOTA_FISCAL_ITEM_001P.csv', sep=';')

    data.info()
    data.describe()
    dataframe_tabela_nf_item
    '''
    Limpando as notas sem CNPJ/CPF
    '''
    data = data[data['CPF/CNPJ Emitente'].notna()]

    '''
    Gerando as colunas com a região do brasil de cada nota
    '''

    REGIOES_BRASIL = {
            'AC':'Norte',          # Acre
            'AP':'Norte',          # Amapá
            'AM':'Norte',          # Amazonas
            'PA':'Norte',          # Pará
            'RO':'Norte',          # Rondônia
            'RR':'Norte',          # Roraima
            'TO':'Norte',          # Tocantins
            'AL':'Nordeste',       # Alagoas
            'BA':'Nordeste',       # Bahia
            'CE':'Nordeste',       # Ceará
            'MA':'Nordeste',       # Maranhão
            'PB':'Nordeste',       # Paraíba
            'PE':'Nordeste',       # Pernambuco
            'PI':'Nordeste',       # Piauí
            'RN':'Nordeste',       # Rio Grande do Norte
            'SE':'Nordeste',       # Sergipe
            'DF': 'Centro-oeste',  # Distrito Federal
            'GO': 'Centro-oeste',  # Goiás
            'MT': 'Centro-oeste',  # Mato Grosso
            'MS': 'Centro-oeste',  # Mato Grosso do Sul
            'ES':'Sudeste',        # Espírito Santo
            'MG':'Sudeste',        # Minas Gerais
            'RJ':'Sudeste',        # Rio de Janeiro
            'SP':'Sudeste',        # São Paulo
            'PR': 'Sul',           # Paraná
            'RS': 'Sul',           # Rio Grande do Sul
            'SC': 'Sul'            # Santa Catarina
    }

    def converts_UF_to_REGIAO(df : pd.DataFrame)->pd.DataFrame:
        '''
        Converte a informação de UF da coluna 'UF DO EMITENTE' em região do país, criando uma nova coluna
        chamada 'REGIAO_PAIS'
        '''
        df['REGIAO_PAIS'] = df['UF EMITENTE'].map(REGIOES_BRASIL)

        return df


    data =  converts_UF_to_REGIAO(df=data)

    '''
    Box Plot de 1% da Amostra das notas (Até a mediana)
    '''
    fig,ax = plt.subplots(figsize=(8,4))
    ate_mediana = selecionar_faixa(df=data, coluna='VALOR TOTAL',q1=0, q2=0.5)


    sns.boxplot(ax = ax, data=ate_mediana, hue='REGIAO_PAIS', x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.legend(title='Região do País',
            bbox_to_anchor=(1,-0.2),
            ncol=5)

    fig,ax = plt.subplots(figsize=(8,5))

    sns.histplot(ax = ax, data=ate_mediana, x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.set_ylabel('Contagem')

    '''
    Box Plot de 1% da Amostra das notas (Até a mediana)
    '''
    fig,ax = plt.subplots(figsize=(8,4))
    ate_mediana = selecionar_faixa(df=data, coluna='VALOR TOTAL',q1=0.5, q2=0.8)


    sns.boxplot(ax = ax, data=ate_mediana, hue='REGIAO_PAIS', x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.legend(title='Região do País',
            bbox_to_anchor=(1,-0.2),
            ncol=5)

    fig,ax = plt.subplots(figsize=(8,5))

    sns.histplot(ax = ax, data=ate_mediana, x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.set_ylabel('Contagem')

    '''
    Box Plot de 1% da Amostra das notas (Até a mediana)
    '''
    fig,ax = plt.subplots(figsize=(8,4))
    ate_mediana = selecionar_faixa(df=data, coluna='VALOR TOTAL',q1=0.8, q2=0.9)


    sns.boxplot(ax = ax, data=ate_mediana, hue='REGIAO_PAIS', x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.legend(title='Região do País',
            bbox_to_anchor=(1,-0.2),
            ncol=5)

    fig,ax = plt.subplots(figsize=(8,5))

    sns.histplot(ax = ax, data=ate_mediana, x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.set_ylabel('Contagem')

    '''
    Box Plot de 1% da Amostra das notas (Até a mediana)
    '''
    fig,ax = plt.subplots(figsize=(8,4))
    ate_mediana = selecionar_faixa(df=data, coluna='VALOR TOTAL',q1=0.9, q2=0.99)


    sns.boxplot(ax = ax, data=ate_mediana, hue='REGIAO_PAIS', x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.legend(title='Região do País',
            bbox_to_anchor=(1,-0.2),
            ncol=5)

    fig,ax = plt.subplots(figsize=(8,5))

    sns.histplot(ax = ax, data=ate_mediana, x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.set_ylabel('Contagem')

    '''
    Box Plot de 1% da Amostra das notas (Até a mediana)
    '''
    fig,ax = plt.subplots(figsize=(8,4))
    ate_mediana = selecionar_faixa(df=data, coluna='VALOR TOTAL',q1=0.25, q2=0.75)


    sns.boxplot(ax = ax, data=ate_mediana, hue='REGIAO_PAIS', x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.legend(title='Região do País',
            bbox_to_anchor=(1,-0.2),
            ncol=5)

    fig,ax = plt.subplots(figsize=(8,5))

    sns.histplot(ax = ax, data=ate_mediana, x='VALOR TOTAL')
    ax.set_xlabel('Valor total (R$)')
    ax.set_ylabel('Contagem')
