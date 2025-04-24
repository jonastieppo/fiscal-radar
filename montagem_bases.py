# %%

# importando bibliotecas essenciais
import pandas as pd
import os
from helper import read_notas_fiscais
import dask.dataframe as dd

CNEP = pd.read_csv(os.path.join(os.getcwd(),'dados','20250418_CNEP.csv'),
            sep=';',
            encoding='cp1252',
            decimal=','
            )


CEIS = pd.read_csv(os.path.join(os.getcwd(),'dados','20250418_CEIS.csv'),
            sep=';',
            encoding='cp1252',
            decimal=','
            )


NOTA_FISCAL_ITEM = dd.read_csv(
    os.path.join(os.getcwd(),'dados',"nota_fiscal_item.csv"),
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
