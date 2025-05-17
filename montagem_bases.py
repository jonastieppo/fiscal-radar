# %%

# importando bibliotecas essenciais
import pandas as pd
import os
from helper import date_convertion_pandas
import dask.dataframe as dd
from dicionarios_gerais import dicionario_CNEP, dicionario_tabela_ceis, dicionario_tabela_nf_item
CNEP = pd.read_csv(os.path.join(os.getcwd(),'dados','20250418_CNEP.csv'),
            sep=';',
            encoding='cp1252',
            decimal=','
            )

CNEP.columns = dicionario_CNEP['COLUNA']

colunas_data = [
    'DATA INÍCIO SANÇÃO',
    'DATA FINAL SANÇÃO',
    'DATA PUBLICAÇÃO',
    'DATA DO TRÂNSITO EM JULGADO',
    'DATA ORIGEM INFORMAÇÃO'
]

CNEP = date_convertion_pandas(CNEP,colunas_data)

CEIS = pd.read_csv(os.path.join(os.getcwd(),'dados','20250418_CEIS.csv'),
            sep=';',
            encoding='cp1252',
            decimal=','
            )

CEIS.columns = dicionario_tabela_ceis['COLUNA']

colunas_data = [
    'DATA INÍCIO SANÇÃO',
    'DATA FINAL SANÇÃO',
    'DATA PUBLICAÇÃO',
    'DATA DO TRÂNSITO EM JULGADO',
    'DATA ORIGEM INFORMAÇÃO'
]

CEIS = date_convertion_pandas(CEIS, colunas_data)

NOTA_FISCAL_ITEM = dd.read_csv(
    os.path.join(os.getcwd(),'dados',"nota_fiscal_item.csv"),
    sep=";",
    blocksize=2.5e6,
    dtype={
        "CHAVE DE ACESSO": str,
        "MODELO": str,
        "SÉRIE": "int64",
        "NÚMERO": "int64",
        "NATUREZA DA OPERAÇÃO": str,
        "DATA EMISSÃO": str,
        "CPF/CNPJ Emitente": str,
        "RAZÃO SOCIAL EMITENTE": str,
        "INSCRIÇÃO ESTADUAL EMITENTE": "int64",
        "UF EMITENTE": str,
        "MUNICÍPIO EMITENTE": str,
        "CNPJ DESTINATÁRIO": "int64",
        "NOME DESTINATÁRIO": str,
        "UF DESTINATÁRIO": str,
        "INDICADOR IE DESTINATÁRIO": str,
        "DESTINO DA OPERAÇÃO": str,
        "CONSUMIDOR FINAL": str,
        "PRESENÇA DO COMPRADOR": str,
        "NÚMERO PRODUTO": "int64",
        "DESCRIÇÃO DO PRODUTO/SERVIÇO": str,
        "CÓDIGO NCM/SH": "int64",
        "NCM/SH (TIPO DE PRODUTO)": str,
        "CFOP": "int64",
        "QUANTIDADE": "float64",
        "UNIDADE": str,
        "VALOR UNITÁRIO": "float64",
        "VALOR TOTAL": "float64",
    },
)
NOTA_FISCAL_ITEM = NOTA_FISCAL_ITEM.drop(columns=['Unnamed: 0'])
NOTA_FISCAL_ITEM.columns = dicionario_tabela_nf_item['COLUNA']



NOTA_FISCAL_ITEM_001 = dd.read_csv(
    os.path.join(os.getcwd(),'dados',"NOTA_FISCAL_ITEM_001P.csv"),
    sep=";",
    blocksize=25e6,
    dtype={
        "CHAVE DE ACESSO": str,
        "MODELO": str,
        "SÉRIE": "int64",
        "NÚMERO": "int64",
        "NATUREZA DA OPERAÇÃO": str,
        "DATA EMISSÃO": str,
        "CPF/CNPJ Emitente": str,
        "RAZÃO SOCIAL EMITENTE": str,
        "INSCRIÇÃO ESTADUAL EMITENTE": "int64",
        "UF EMITENTE": str,
        "MUNICÍPIO EMITENTE": str,
        "CNPJ DESTINATÁRIO": "int64",
        "NOME DESTINATÁRIO": str,
        "UF DESTINATÁRIO": str,
        "INDICADOR IE DESTINATÁRIO": str,
        "DESTINO DA OPERAÇÃO": str,
        "CONSUMIDOR FINAL": str,
        "PRESENÇA DO COMPRADOR": str,
        "NÚMERO PRODUTO": "int64",
        "DESCRIÇÃO DO PRODUTO/SERVIÇO": str,
        "CÓDIGO NCM/SH": "int64",
        "NCM/SH (TIPO DE PRODUTO)": str,
        "CFOP": "int64",
        "QUANTIDADE": "float64",
        "UNIDADE": str,
        "VALOR UNITÁRIO": "float64",
        "VALOR TOTAL": "float64",
    },
)
NOTA_FISCAL_ITEM_001 = NOTA_FISCAL_ITEM_001.drop(columns=['Unnamed: 0'])

# %%
