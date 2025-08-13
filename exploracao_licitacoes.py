# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
# Configurações de visualização para os gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['figure.autolayout'] = True

def analisar_dados_licitacoes(caminho_arquivo):
    """
    Função para carregar e realizar uma análise exploratória básica 
    em um dataframe de licitações.

    Args:
        caminho_arquivo (str): O caminho para o arquivo CSV.
    """
    try:
        # Carrega o dataset a partir do arquivo CSV
        df = pd.read_csv(caminho_arquivo)

        # --- 1. VISÃO GERAL DOS DADOS ---
        print("--- 1. Visão Geral dos Dados ---")
        print("Formato do DataFrame (linhas, colunas):", df.shape)
        print("\nPrimeiras 5 linhas do DataFrame:")
        print(df.head())
        
        print("\n\nInformações sobre os tipos de dados e valores nulos:")
        # O método info() é ótimo para verificar os tipos de cada coluna e a contagem de não-nulos
        df.info()

        print("\n\nEstatísticas descritivas para colunas numéricas:")
        # O método describe() fornece estatísticas como média, desvio padrão, etc.
        print(df.describe())

        # --- 2. ANÁLISE DE SANÇÕES ---
        print("\n--- 2. Análise de Sanções ---")

        # Cria colunas booleanas para facilitar a análise de sanções
        # 'notna()' retorna True se o valor não for nulo (ou seja, existe sanção)
        df['tem_sancao_ceis'] = df['ceis_sancao'].notna()
        df['tem_sancao_cnep'] = df['cnep_sancao'].notna()
        df['tem_alguma_sancao'] = df['tem_sancao_ceis'] | df['tem_sancao_cnep']

        # Calcula a contagem de participações com e sem sanções
        contagem_sancoes = df['tem_alguma_sancao'].value_counts()
        print("\nContagem de participações com e sem sanções:")
        print(contagem_sancoes)

        # Calcula a porcentagem
        porcentagem_sancoes = df['tem_alguma_sancao'].value_counts(normalize=True) * 100
        print("\nPorcentagem de participações com e sem sanções:")
        print(porcentagem_sancoes)
        
        # --- 3. VISUALIZAÇÕES ---
        print("\n--- 3. Gerando Gráficos ---")
        
        # Gráfico 1: Distribuição das Modalidades de Compra
        plt.figure() # Cria uma nova figura para o gráfico
        ax1 = sns.countplot(y=df['modalidade_compra'], order=df['modalidade_compra'].value_counts().index, palette='viridis')
        ax1.set_title('Distribuição das Modalidades de Compra', fontsize=16)
        ax1.set_xlabel('Contagem', fontsize=12)
        ax1.set_ylabel('Modalidade', fontsize=12)
        ax1.bar_label(ax1.containers[0], fmt='%d', label_type='edge', padding=5) # Adiciona rótulos
        plt.show()

        # Gráfico 2: Proporção de Participações com e sem Sanção
        plt.figure()
        ax2 = sns.countplot(x=df['tem_alguma_sancao'], palette='pastel')
        ax2.set_title('Proporção de Participações com vs. sem Sanção', fontsize=16)
        ax2.set_xlabel('Possui alguma sanção?', fontsize=12)
        ax2.set_ylabel('Número de Participações', fontsize=12)
        ax2.set_xticklabels(['Não Sancionado', 'Sancionado'])
        ax2.bar_label(ax2.containers[0], fmt='%d', label_type='edge', padding=3) # Adiciona rótulos
        plt.show()

        # Gráfico 3: Top 10 Unidades Gestoras (UG) por número de participações
        plt.figure(figsize=(12, 8)) # Ajusta o tamanho para melhor visualização
        top_10_ug = df['nome_ug'].value_counts().nlargest(10)
        ax3 = sns.barplot(x=top_10_ug.values, y=top_10_ug.index, palette='mako')
        ax3.set_title('Top 10 Unidades Gestoras por Número de Participações em Licitações', fontsize=16)
        ax3.set_xlabel('Número de Participações', fontsize=12)
        ax3.set_ylabel('Unidade Gestora (UG)', fontsize=12)
        ax3.bar_label(ax3.containers[0], fmt='%d', label_type='edge', padding=5) # Adiciona rótulos
        plt.show()

        print("\nAnálise exploratória inicial concluída!")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_arquivo}' não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

# --- EXECUÇÃO DA ANÁLISE ---
# Substitua 'dataframe_licitacoes_anotado.csv' pelo caminho correto do seu arquivo se necessário.
nome_do_arquivo = os.path.join('data_anotation', 'dataframe_licitacoes_anotado.csv')
analisar_dados_licitacoes(nome_do_arquivo)
