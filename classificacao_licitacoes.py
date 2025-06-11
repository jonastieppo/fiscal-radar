# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_auc_score
import os
import pickle
# --- ETAPA 1: CARREGAMENTO E ANÁLISE DOS DADOS ---

# Carregar o dataset a partir do arquivo CSV fornecido

df = pd.read_csv(os.path.join('data_anotation','df_for_prediction.csv'))
print("Arquivo CSV carregado com sucesso!")
print("Dimensões do DataFrame:", df.shape)


print("\n--- Informações Iniciais do DataFrame ---")
df.info()

print("\n--- Primeiras 5 Linhas ---")
print(df.head())

print("\n--- Verificação de Valores Nulos ---")
print(df.isnull().sum())

# Análise da variável alvo 'is_fraud'
print("\n--- Distribuição da Variável Alvo (is_fraud) ---")
target_distribution = df['is_fraud'].value_counts(normalize=True) * 100
print(target_distribution)

# Visualização da distribuição da variável alvo
plt.figure(figsize=(8, 5))
sns.countplot(x='is_fraud', data=df)
plt.title('Distribuição de Licitações Fraudulentas vs. Não Fraudulentas')
plt.xlabel('É Fraude? (1 = Sim, 0 = Não)')
plt.ylabel('Contagem')
plt.show()

# --- ETAPA 2: PRÉ-PROCESSAMENTO E ENGENHARIA DE FEATURES ---

# A variável 'is_fraud' pode vir como booleana ou inteiro, vamos garantir que seja int.
df['is_fraud'] = df['is_fraud'].astype(int)

# 1. Criar features binárias para indicar a presença de sanções
# Se houver qualquer valor na coluna de sanção, consideramos como 1 (tem sanção), caso contrário 0.
df['tem_sancao_ceis'] = df['ceis_sancao'].notna().astype(int)
df['tem_sancao_cnep'] = df['cnep_sancao'].notna().astype(int)

# 2. Lidar com valores nulos na coluna 'objeto' (caso existam)
df['objeto'] = df['objeto'].fillna('')

# 3. Definir as features (X) e o alvo (y)
# Vamos dropar as colunas originais de sanção e também 'nome_ug' por enquanto,
# devido à sua alta cardinalidade (muitos nomes únicos), o que pode complicar o modelo inicial.
# Uma abordagem mais avançada poderia usar target encoding ou feature hashing para 'nome_ug'.
X = df.drop(['is_fraud', 'ceis_sancao', 'cnep_sancao', 'nome_ug'], axis=1)
y = df['is_fraud']

# 4. Dividir os dados em conjuntos de treino e teste
# Usamos stratify=y para manter a mesma proporção de fraudes nos conjuntos de treino e teste.
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# 5. Criar o pipeline de pré-processamento
# Diferentes colunas precisam de diferentes transformações. O ColumnTransformer é perfeito para isso.

# Para a coluna 'objeto', que é texto, usaremos TfidfVectorizer.
# Ele transforma o texto em uma matriz de features TF-IDF.
text_processor = TfidfVectorizer(max_features=5000, stop_words=None) # Usamos None para stop_words pois o padrão é 'english'

# Para a coluna 'modalidade_compra', que é categórica, usaremos OneHotEncoder.
# Ele cria novas colunas binárias para cada categoria.
categorical_processor = OneHotEncoder(handle_unknown='ignore')

# As colunas numéricas que criamos ou já existiam.
# Note que não vamos escalar aqui, pois os modelos de árvore não exigem,
# mas seria importante para a Regressão Logística. O Pipeline lida com isso.
numeric_features = ['numero_parcitipacoes', 'tem_sancao_ceis', 'tem_sancao_cnep']
categorical_features = ['modalidade_compra']
text_feature = 'objeto'

# Juntando tudo em um único pré-processador
preprocessor = ColumnTransformer(
    transformers=[
        ('text', text_processor, text_feature),
        ('cat', categorical_processor, categorical_features),
        ('num', 'passthrough', numeric_features)
    ],
    remainder='drop' # Ignora colunas não especificadas
)


# --- ETAPA 3: DEFINIÇÃO E TREINAMENTO DOS MODELOS ---

# Vamos definir os modelos que queremos treinar e comparar.
# Nota: O parâmetro class_weight='balanced' é útil para dados desbalanceados,
# o que é comum em detecção de fraude. Ele ajusta os pesos das classes de forma inversamente proporcional à sua frequência.
models = {
    "Regressão Logística": LogisticRegression(random_state=42, class_weight='balanced', max_iter=1000),
    "Random Forest": RandomForestClassifier(random_state=42, class_weight='balanced'),
    "Gradient Boosting": GradientBoostingClassifier(random_state=42)
}

# Dicionário para armazenar os resultados
results = {}

# Loop para treinar e avaliar cada modelo
for model_name, model in models.items():
    print(f"\n--- Treinando e Avaliando: {model_name} ---")

    # Criando o pipeline completo: pré-processamento -> modelo
    # Para a Regressão Logística, é uma boa prática escalar as features.
    if model_name == "Regressão Logística":
        # Atualiza o ColumnTransformer para escalar as features numéricas
        preprocessor_scaled = ColumnTransformer(
            transformers=[
                ('text', text_processor, text_feature),
                ('cat', categorical_processor, categorical_features),
                ('num', StandardScaler(), numeric_features) # Adiciona StandardScaler
            ],
            remainder='drop'
        )
        pipeline = Pipeline(steps=[('preprocessor', preprocessor_scaled),
                                   ('classifier', model)])
    else:
         pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                   ('classifier', model)])


    # Treinar o modelo
    pipeline.fit(X_train, y_train)

    # Fazer previsões no conjunto de teste
    y_pred = pipeline.predict(X_test)
    y_pred_proba = pipeline.predict_proba(X_test)[:, 1] # Probabilidades para a classe 1 (fraude)

    # Avaliar o modelo
    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    results[model_name] = {
        'Acurácia': accuracy,
        'ROC AUC': roc_auc,
        'Relatório de Classificação': report,
        'Matriz de Confusão': conf_matrix
    }

    # Imprimir os resultados
    print(f"Acurácia: {accuracy:.4f}")
    print(f"ROC AUC Score: {roc_auc:.4f}")
    print("Relatório de Classificação:")
    print(report)
    print("Matriz de Confusão:")
    # Plotar a matriz de confusão
    plt.figure(figsize=(6, 4))
    sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues',
                xticklabels=['Não Fraude', 'Fraude'],
                yticklabels=['Não Fraude', 'Fraude'])
    plt.title(f'Matriz de Confusão - {model_name}')
    plt.xlabel('Previsto')
    plt.ylabel('Verdadeiro')
    plt.show()

    # Salvo o Modelo em Pickle Para Aplicação Futura

    with open(f'{model_name}.pkl', 'wb') as model_file:
        pickle.dump(pipeline, model_file)


# --- ETAPA 4: COMPARAÇÃO FINAL DOS MODELOS ---
print("\n--- Resumo Comparativo dos Resultados ---")
results_df = pd.DataFrame(index=['Acurácia', 'ROC AUC'])
for model_name, metrics in results.items():
    results_df[model_name] = [metrics['Acurácia'], metrics['ROC AUC']]

print(results_df.T.sort_values(by='ROC AUC', ascending=False))

# %%
