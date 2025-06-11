# %%
import pickle
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# %%
# Carregando os modelos
with open('Random Forest.pkl', 'rb') as rf:
    random_forest = pickle.load(rf)

with open('Regressão Logística.pkl', 'rb') as rl:
    logistic_regression = pickle.load(rl)

with open('Gradient Boosting.pkl', 'rb') as gb:
    gradient_boosting = pickle.load(gb)

# Aplicando os modelos

# Garanta que o caminho para seus dados está correto
database_production = pd.read_csv(os.path.join('data_anotation', 'production_data.csv'))

# Passos de pré-processamento do script original
database_production['tem_sancao_ceis'] = database_production['ceis_sancao'].notna().astype(int)
database_production['tem_sancao_cnep'] = database_production['cnep_sancao'].notna().astype(int)
database_production['objeto'] = database_production['objeto'].fillna('')
database_production = database_production.drop(['ceis_sancao', 'cnep_sancao', 'nome_ug'], axis=1)

# Realizando as predições com os modelos carregados
number_of_fraud_random_forest = random_forest.predict(database_production)
number_of_fraud_logistic_regression = logistic_regression.predict(database_production)
number_of_fraud_gradient_boosting = gradient_boosting.predict(database_production)


# %%
# --- SEÇÃO DE PLOTAGEM (EM PORTUGUÊS) ---

# 1. Contar o número de fraudes encontradas por cada modelo
# Usamos np.sum() para contar as predições positivas (assumindo que fraude é codificado como 1)
num_rows = len(database_production)

counts = {
    'Random Forest': np.sum(number_of_fraud_random_forest)/num_rows*100,
    'Regressão Logística': np.sum(number_of_fraud_logistic_regression)/num_rows*100,
    'Gradient Boosting': np.sum(number_of_fraud_gradient_boosting)/num_rows*100
}

# 2. Criar um DataFrame para a plotagem
df_counts = pd.DataFrame(list(counts.items()), columns=['Modelo', 'Contagem de Fraudes'])

# 3. Gerar e exibir o gráfico de barras
plt.style.use('seaborn-v0_8-whitegrid') # Usando um estilo para o gráfico
fig, ax = plt.subplots(figsize=(10, 7))

# Criar o gráfico de barras
sns.barplot(x='Modelo', y='Contagem de Fraudes', data=df_counts, ax=ax, palette='viridis')

# Adicionar rótulos com a contagem exata no topo de cada barra
for index, row in df_counts.iterrows():
    ax.text(index, row['Contagem de Fraudes'], row['Contagem de Fraudes'], color='black', ha="center", va="bottom")

# Definir títulos e rótulos para maior clareza
ax.set_title('Comparação de Fraudes Detectadas por Diferentes Modelos', fontsize=16)
ax.set_xlabel('Modelo de Machine Learning', fontsize=12)
ax.set_ylabel(fr'% de Fraudes Detectadas (em relação a {num_rows} registros).', fontsize=12)

# Garantir que o layout fique bem ajustado e exibir o gráfico
plt.tight_layout()
plt.show()