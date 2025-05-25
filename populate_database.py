# %%
'''
Populando a base de dados os dados do CEIS
'''
from montagem_bases import CEIS, CNEP, NOTA_FISCAL_ITEM
import dask.dataframe as dd
from helper import csv_to_postgree_sql, get_table_name, insert_rows_in_database
from dicionarios_gerais import CEIS_SQL_COLUMNS, CNEP_SQL_COLUMNS, NFE_SQL_COLUMNS
from helper import parse_licitacoes_and_insert_in_database
from dotenv import load_dotenv
from dicionarios_gerais import contratos_compras_SQL_COLUMNS, \
contratos_itemCompra_SQL_COLUMNS, \
contratos_termoAditivo_SQL_COLUMNS, \
licitacoes_EmpenhosRelacionados_SQL_COLUMNS, \
licitacoes_ItemLicitacao_SQL_COLUMNS, \
licitacoes_licitacao_SQL_COLUMNS, \
licitacoes_participantes_licitacao_SQL_COLUMNS

import os
load_dotenv()

db_host = os.environ.get("db_host")
db_user = os.environ.get("db_user")
db_password = os.environ.get("db_password")
db_name = os.environ.get("db_name")

# ENABLE IF WANT TO INSERT INFO IN THE POSTGREE DATABASE
KILLSWITCH_CNEP = False 
KILLSWITCH_CEIS = False
KILLSWITCH_NFE = False
KILLSWITCH_LICITACOES = False



if KILLSWITCH_CNEP:

    insert_rows_in_database(
        df_to_insert=CNEP,
        table_name='CNEP',
        columns_dictionary=CNEP_SQL_COLUMNS,
        db_host=db_host,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name
    )


if KILLSWITCH_CEIS:

    insert_rows_in_database(
        df_to_insert=CEIS,
        table_name='CEIS',
        columns_dictionary=CEIS_SQL_COLUMNS,
        db_host=db_host,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name
    )
if KILLSWITCH_NFE:

    insert_rows_in_database(
        df_to_insert=NOTA_FISCAL_ITEM,
        table_name='dsa_nfe_items',
        columns_dictionary=NFE_SQL_COLUMNS,
        db_host=db_host,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name
    )


if KILLSWITCH_LICITACOES:
    parse_licitacoes_and_insert_in_database(
        root_folder=os.path.join(os.getcwd(),'dados','licitacoes'),
            db_host=db_host,
            db_user=db_user,
            db_password=db_password,
            db_name=db_name,
            schema = 'dsa',
            contratos_compras_SQL_COLUMNS = contratos_compras_SQL_COLUMNS,
            contratos_itemCompra_SQL_COLUMNS = contratos_itemCompra_SQL_COLUMNS,
            contratos_termoAditivo_SQL_COLUMNS = contratos_termoAditivo_SQL_COLUMNS,
            licitacoes_EmpenhosRelacionados_SQL_COLUMNS = licitacoes_EmpenhosRelacionados_SQL_COLUMNS,
            licitacoes_ItemLicitacao_SQL_COLUMNS = licitacoes_ItemLicitacao_SQL_COLUMNS,
            licitacoes_licitacao_SQL_COLUMNS = licitacoes_licitacao_SQL_COLUMNS,
            licitacoes_participantes_licitacao_SQL_COLUMNS = licitacoes_participantes_licitacao_SQL_COLUMNS,
        )

# %%

# %%
