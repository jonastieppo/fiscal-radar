# %%
'''
Exploração inicial
'''
import os
import sys
sys.path.insert(0,os.path.join(os.getcwd(), '..'))
from montagem_bases import CNEP
from helper import excluir_outliers, selecionar_faixa
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


def initial_exploration():

    # Informações gerais sobre o DataFrame
    print("Informações gerais do DataFrame CNEP:")
    print(CNEP.info())
    print("\n")

    # Estatísticas descritivas para colunas numéricas
    print("Estatísticas descritivas para colunas numéricas:")
    print(CNEP.describe())
    print("\n")

    # Verificando valores ausentes por coluna
    print("Valores ausentes por coluna:")
    print(CNEP.isnull().sum())
    print("\n")

    # Verificando valores duplicados
    print("Valores duplicados:")
    print(f"Número de linhas duplicadas: {CNEP.duplicated().sum()}")
    print("\n")

    # Análise de colunas categóricas
    categorical_cols = CNEP.select_dtypes(include=['object']).columns

    for col in categorical_cols:
        print(f"Análise da coluna: {col}")
        print(f"Número de valores únicos: {CNEP[col].nunique()}")
        if CNEP[col].nunique() < 50:  # Mostrar valores únicos se forem poucos
            print(f"Valores únicos: {CNEP[col].unique()}")
            print(f"Contagem de valores: \n{CNEP[col].value_counts()}")
        else:
            print(f"Exemplos de valores: {CNEP[col].sample(10).values}")  # Mostrar alguns exemplos
        print("\n")

    # %%
    # Histograma da coluna 'VALOR DA MULTA'
    fig, ax = plt.subplots()
    CNEP_SEM_ZERO = CNEP[CNEP['VALOR DA MULTA']>0]
    sns.boxplot(ax=ax, data=CNEP_SEM_ZERO, x='VALOR DA MULTA')
    sns.stripplot(ax=ax, data=CNEP_SEM_ZERO, x='VALOR DA MULTA')
    plt.title('Distribuição do Valor da Multa')
    plt.show()


    # %%
    CNEP_MEDIANOS = selecionar_faixa(CNEP_SEM_ZERO, 'VALOR DA MULTA', 0,0.5)

    # Histograma da coluna 'VALOR DA MULTA', sem outliers
    fig, ax = plt.subplots()
    sns.boxplot(ax=ax, data=CNEP_MEDIANOS, x='VALOR DA MULTA')
    sns.stripplot(ax=ax, data=CNEP_MEDIANOS, x='VALOR DA MULTA')
    plt.title('Distribuição do Valor da Multa (Sem outliers)')
    plt.show()

    fig, ax = plt.subplots()
    sns.histplot(ax=ax, data=CNEP_MEDIANOS, x='VALOR DA MULTA')
    plt.title('Distribuição do Valor da Multa, até a mediana')
    plt.show()

    # %%
    CNEP_MAIORES = selecionar_faixa(CNEP_SEM_ZERO, 'VALOR DA MULTA', 0.5,0.9)

    # Histograma da coluna 'VALOR DA MULTA', sem outliers
    fig, ax = plt.subplots()
    sns.boxplot(ax=ax, data=CNEP_MAIORES, x='VALOR DA MULTA')
    sns.stripplot(ax=ax, data=CNEP_MAIORES, x='VALOR DA MULTA')
    plt.title('Distribuição do Valor da Multa (Sem outliers)')
    plt.show()

    fig, ax = plt.subplots()
    sns.histplot(ax=ax, data=CNEP_MAIORES, x='VALOR DA MULTA')
    plt.title('Distribuição do Valor da Multa, entre a mediana e o valor superior')
    plt.show()

    # %%
    # Contagem de ocorrências para colunas categóricas (exemplos)
    if 'TIPO DE PESSOA' in CNEP.columns:
        plt.figure(figsize=(8, 5))
        sns.countplot(x='TIPO DE PESSOA', data=CNEP)
        plt.title('Contagem de Tipo de Pessoa')
        plt.show()

    if 'CATEGORIA DA SANÇÃO' in CNEP.columns:
        plt.figure(figsize=(10, 6))
        sns.countplot(y='CATEGORIA DA SANÇÃO', data=CNEP, order=CNEP['CATEGORIA DA SANÇÃO'].value_counts().index)
        plt.title('Contagem de Categoria da Sanção')
        plt.show()

    if 'ESFERA ÓRGÃO SANCIONADOR' in CNEP.columns:
        plt.figure(figsize=(8, 5))
        sns.countplot(x='ESFERA ÓRGÃO SANCIONADOR', data=CNEP)
        plt.title('Contagem de Esfera do Órgão Sancionador')
        plt.show()


    # Análise das Datas
    date_columns = ['DATA INÍCIO SANÇÃO', 'DATA FINAL SANÇÃO', 'DATA PUBLICAÇÃO', 'DATA DO TRÂNSITO EM JULGADO', 'DATA ORIGEM INFORMAÇÃO']

    for col in date_columns:
        CNEP[col] = pd.to_datetime(CNEP[col], errors='coerce')


    COLUNA_TEMPORAL_ANALISADA = 'DATA PUBLICAÇÃO'
    COLUNA_TEMPORAL_ALIAS = 'Data da publicação em veículo oficial'
    CNEP_DATA = CNEP.copy(deep=True) # fazendo um cópia, criando uma nova referencia
    CNEP_DATA = CNEP[CNEP[COLUNA_TEMPORAL_ANALISADA].notna()]
    CNEP_DATA[COLUNA_TEMPORAL_ALIAS] = CNEP_DATA[COLUNA_TEMPORAL_ANALISADA].dt.year
    plt.figure(figsize=(10, 6))
    sns.countplot(data=CNEP_DATA, x=COLUNA_TEMPORAL_ALIAS)
    plt.title('Distribuição de Sanções por Ano de Publicação')
    plt.xticks(rotation=90, ha="right")
    plt.ylabel('Contagem')
    plt.show()

    '''
    Será que existe correlação entre a data de publicação da BASE de dados CEIS e CNEP?
    '''
