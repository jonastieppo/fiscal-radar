# %%
'''
Populando a base de dados os dados do CEIS
'''
from montagem_bases import CEIS, CNEP, NOTA_FISCAL_ITEM
import dask.dataframe as dd
from helper import insert_rows_in_database
from dicionarios_gerais import CEIS_SQL_COLUMNS, CNEP_SQL_COLUMNS, NFE_SQL_COLUMNS
from dotenv import load_dotenv

import os
load_dotenv()

db_host = os.environ.get("db_host")
db_user = os.environ.get("db_user")
db_password = os.environ.get("db_password")
db_name = os.environ.get("db_name")

# %%
insert_rows_in_database(
    df_to_insert=CNEP,
    table_name='CNEP',
    columns_dictionary=CNEP_SQL_COLUMNS,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name
)

# %%

insert_rows_in_database(
    df_to_insert=CEIS,
    table_name='CEIS',
    columns_dictionary=CEIS_SQL_COLUMNS,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name
)
# %%

insert_rows_in_database(
    df_to_insert=NOTA_FISCAL_ITEM,
    table_name='dsa_nfe_items',
    columns_dictionary=NFE_SQL_COLUMNS,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name
)

# %%
