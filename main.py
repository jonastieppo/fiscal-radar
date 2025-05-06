import os
import sys
from dotenv import load_dotenv

load_dotenv()

# %%
import psycopg2
import sqlalchemy
import pandas as pd
import dask.dataframe as dd
from helper import make_query


DATABASE_URL = os.environ.get("DATABASE_URL")

# connecting to DB

engine = sqlalchemy.create_engine(DATABASE_URL)
connection = engine.connect()

# Making a simple query

query = """SELECT COUNT(*) FROM dsa.dsa_nfe_items"""

df = make_query(query, connection)

print(df)

# %%

# %%
from helper import insert_rows_in_database
from montagem_bases import NOTA_FISCAL_ITEM
from dicionarios_gerais import  NFE_SQL_COLUMNS


db_host = os.environ.get("db_host")
db_user = os.environ.get("db_user")
db_password = os.environ.get("db_password")
db_name = os.environ.get("db_name")

insert_rows_in_database(
    df_to_insert=NOTA_FISCAL_ITEM,
    table_name='dsa_nfe_items',
    columns_dictionary=NFE_SQL_COLUMNS,
)
