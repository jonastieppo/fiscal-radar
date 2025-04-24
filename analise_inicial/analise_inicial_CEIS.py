# %%
'''
Exploração inicial
'''
import os
import sys
sys.path.insert(0,os.path.join(os.getcwd(), '..'))
from montagem_bases import CEIS
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def initial_exploration():
    # Informações gerais do DataFrame
    print("Informações gerais do DataFrame CEIS:")
    CEIS.info()
    print("\n")

    # Verificando valores únicos e contagem para colunas categóricas
    print("Valores únicos e contagem para colunas categóricas:")
    for col in CEIS.columns:
        if CEIS[col].dtype == 'object':
            print(f"\nColuna: {col}")
            print(f"Valores únicos: {CEIS[col].nunique()}")
            if CEIS[col].nunique() < 50:  # Mostrar valores se houver poucos
                print(f"Contagem de valores:\n{CEIS[col].value_counts()}")
            else:
                print(f"Exemplos de valores: {CEIS[col].unique()[:5]}") # Mostrar alguns exemplos se houver muitos

    print("\n")

    # Verificando valores faltantes
    print("Verificando valores faltantes:")
    print(CEIS.isnull().sum())
    print("\n")

    # Análise de duplicatas
    print("Análise de duplicatas:")
    print(f"Número de linhas duplicadas: {CEIS.duplicated().sum()}")
    print("\n")

    # Análise específica de algumas colunas
    # Exemplo: 'TIPO DE PESSOA'
    plt.figure(figsize=(8, 6))
    sns.countplot(data=CEIS, x='TIPO DE PESSOA')
    plt.title('Distribuição de Tipo de Pessoa')
    plt.xticks(rotation=45, ha="right")
    plt.ylabel('Contagem')
    plt.show()

    # Exemplo: 'CATEGORIA DA SANÇÃO'
    plt.figure(figsize=(10, 6))
    sns.countplot(data=CEIS, x='CATEGORIA DA SANÇÃO', order=CEIS['CATEGORIA DA SANÇÃO'].value_counts().index)
    plt.title('Distribuição de Categoria da Sanção')
    plt.xticks(rotation=90, ha="right", wrap=True)
    plt.ylabel('Contagem')
    plt.show()

    # Exemplo: 'ESFERA ÓRGÃO SANCIONADOR'
    plt.figure(figsize=(8, 6))
    sns.countplot(data=CEIS, x='ESFERA ÓRGÃO SANCIONADOR')
    plt.title('Distribuição de Esfera do Órgão Sancionador')
    plt.xticks(rotation=45, ha="right")
    plt.ylabel('Contagem')
    plt.show()

    # Análise das Datas
    date_columns = ['DATA INÍCIO SANÇÃO', 'DATA FINAL SANÇÃO', 'DATA PUBLICAÇÃO', 'DATA DO TRÂNSITO EM JULGADO', 'DATA ORIGEM INFORMAÇÃO']

    for col in date_columns:
        CEIS[col] = pd.to_datetime(CEIS[col], errors='coerce')


    COLUNA_TEMPORAL_ANALISADA = 'DATA PUBLICAÇÃO'
    COLUNA_TEMPORAL_ALIAS = 'Data da publicação em veículo oficial'
    CEIS_DATA = CEIS.copy(deep=True) # fazendo um cópia, criando uma nova referencia
    CEIS_DATA = CEIS[CEIS[COLUNA_TEMPORAL_ANALISADA].notna()]
    CEIS_DATA[COLUNA_TEMPORAL_ALIAS] = CEIS_DATA[COLUNA_TEMPORAL_ANALISADA].dt.year
    plt.figure(figsize=(10, 6))
    sns.countplot(data=CEIS_DATA, x=COLUNA_TEMPORAL_ALIAS)
    plt.title('Distribuição de Sanções por Ano de Publicação')
    plt.xticks(rotation=90, ha="right")
    plt.ylabel('Contagem')
    plt.show()
