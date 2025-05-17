# %%
import csv
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# %%
import psycopg2
import sqlalchemy
import pandas as pd
import dask.dataframe as dd
from helper import make_query, make_query_dask
# %%


DATABASE_URL = os.environ.get("DATABASE_URL")

# connecting to DB

engine = sqlalchemy.create_engine(DATABASE_URL)
connection = engine.connect()

# Filtrando todas as notas fiscais cujos cpfs/cnpjs estão na base de dados de corrupção

query = """
SELECT *
FROM dsa."dsa_nfe_items"
WHERE cpf_cnpj_emitente ~ '^[0-9]+$' -- Ensures it's a string of digits
AND (EXISTS (
    SELECT 1
    FROM dsa."CEIS"
    WHERE dsa."CEIS".cpf_ou_cnpj_do_sancionado = CAST(dsa."dsa_nfe_items".cpf_cnpj_emitente AS BIGINT)
)
or EXISTS (
    SELECT 1
    FROM dsa."CNEP"
    WHERE dsa."CNEP"."CPF_CNPJ" = CAST(dsa."dsa_nfe_items".cpf_cnpj_emitente AS BIGINT)
)
)
"""

df_sancionado = make_query(query, DATABASE_URL)
df_sancionado.to_csv(os.path.join(os.getcwd(), 'dados', 'nfe_cnpj_sancionados.csv'), sep=';', index=None, quotechar='"', quoting=csv.QUOTE_ALL)

query = """
SELECT *
FROM dsa."dsa_nfe_items"
TABLESAMPLE BERNOULLI(1)
WHERE cpf_cnpj_emitente ~ '^[0-9]+$' -- Ensures it's a string of digits
AND (EXISTS (
    SELECT 1
    FROM dsa."CEIS"
    WHERE dsa."CEIS".cpf_ou_cnpj_do_sancionado != CAST(dsa."dsa_nfe_items".cpf_cnpj_emitente AS BIGINT)
)
or EXISTS (
    SELECT 1
    FROM dsa."CNEP"
    WHERE dsa."CNEP"."CPF_CNPJ" != CAST(dsa."dsa_nfe_items".cpf_cnpj_emitente AS BIGINT)
)
)
"""
df_normal = make_query(query, DATABASE_URL)
df_normal.to_csv(os.path.join(os.getcwd(), 'dados', 'nfe_cnpj_normal.csv'), sep=';', index=None, quotechar='"', quoting=csv.QUOTE_ALL)

df_normal['sancionado'] = False
df_sancionado['sancionado'] = True

df_final = pd.concat([df_sancionado.sample(10000).copy(), df_normal.sample(10000).copy()])

del df_sancionado # deleting for memory sakes
del df_normal # deleting for memory sakes

df_final.to_csv(os.path.join(os.getcwd(), 'dados', 'conjunto_exploracao_inicial.csv'), sep=';', index=None, quotechar='"', quoting=csv.QUOTE_ALL)
df_final
# %%
df_final = pd.read_csv(os.path.join(os.getcwd(), 'dados', 'conjunto_exploracao_inicial.csv'), sep=';')

columns_to_drop = ['id',
'chave_de_acesso',
'modelo',
'serie','numero',
'data_emissao',
'cpf_cnpj_emitente',
'inscricao_estadual_emitente',
'cnpj_destinatario',
'numero_produto',
'valor_unitario',
'descricao_do_produto_servico',
'codigo_ncm_sh',
'ncm_sh_tipo_de_produto',
'cfop',
'quantidade',
'unidade',]

df_final = df_final.drop(columns=columns_to_drop)
df_final
# %%
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import make_pipeline

# 4. Criar e treinar o modelo

df_train = df_final.sample(frac=0.8, random_state=42)
df_test = df_final.drop(df_train.index)

modelo = make_pipeline(CountVectorizer(), MultinomialNB())
modelo.fit(df_train['nome_destinatario'], df_train['sancionado'])
# %%
# 5. Testar o modelo
y_pred =modelo.predict(df_test['nome_destinatario'])

checking = y_pred == df_test['sancionado']

rate =  checking.sum()/len(checking)

rate

# %%)
# %%
nome_exemplo = df_test['nome_destinatario'].iloc[0]

modelo.predict([nome_exemplo])
# %%
