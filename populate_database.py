# %%
'''
Populando a base de dados os dados do CEIS
'''
from montagem_bases import CEIS, CNEP, NOTA_FISCAL_ITEM_001
import dask.dataframe as dd

from dicionarios_gerais import CEIS_SQL_COLUMNS, CNEP_SQL_COLUMNS, NFE_SQL_COLUMNS
import psycopg2
import pandas as pd
from dotenv import load_dotenv

import os
load_dotenv()

db_host = os.environ.get("db_host")
db_user = os.environ.get("db_user")
db_password = os.environ.get("db_password")
db_name = os.environ.get("db_name")

def insert_rows_in_database(df_to_insert : pd.DataFrame | dd.DataFrame, table_name : str, columns_dictionary, schema = 'dsa'):
    '''
    Function to insert rows in a existent dataframe
    '''
    df_column_names = df_to_insert.columns
    columns_in_table = [columns_dictionary[c] for c in df_column_names]

    # Establish the connection
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()

        if type(df_to_insert) == dd.DataFrame:

            for partition in df_to_insert.partitions:
                pdf = partition.compute()

                if pdf.empty:
                    continue  # Skip empty partitions

                data_to_insert = [tuple(x if pd.notna(x) else None for x in row) for row in pdf.values]

                insert_template = f'''
                INSERT INTO {schema}."{table_name}" ({', '.join(columns_in_table)})
                VALUES ({', '.join(['%s'] * len(columns_in_table))});
                '''

                # Commiting each partition
                try:
                    print(f"Attempting to insert {len(data_to_insert)} rows using execute_many...")
                    cur.executemany(insert_template, data_to_insert)
                    print("Execute many successful.")

                    # Commit the transaction
                    conn.commit()
                    print("Transaction committed successfully.")

                except (psycopg2.Error, Exception) as e:
                    print(f"Database error: {e}")
                    if conn:
                        conn.rollback() # Roll back the transaction on error
                        print("Transaction rolled back due to error.")


        elif type(df_to_insert) == pd.DataFrame:
            data_to_insert = [tuple(row) for row in df_to_insert.values]


            insert_template = f'''
            INSERT INTO {schema}."{table_name}" ({', '.join(columns_in_table)})
            VALUES ({', '.join(['%s'] * len(columns_in_table))});
            '''
            try:
                print(f"Attempting to insert {len(data_to_insert)} rows using execute_many...")
                cur.executemany(insert_template, data_to_insert)
                print("Execute many successful.")

                # Commit the transaction
                conn.commit()
                print("Transaction committed successfully.")

            except (psycopg2.Error, Exception) as e:
                print(f"Database error: {e}")
                if conn:
                    conn.rollback() # Roll back the transaction on error
                    print("Transaction rolled back due to error.")

    except (psycopg2.Error, Exception) as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback() # Roll back the transaction on error
            print("Transaction rolled back due to error.")

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")


# insert_rows_in_database(
#     df_to_insert=CEIS,
#     table_name='CEIS',
#     columns_dictionary=CEIS_SQL_COLUMNS
# )
# %%
from montagem_bases import NOTA_FISCAL_ITEM
from dicionarios_gerais import CEIS_SQL_COLUMNS, CNEP_SQL_COLUMNS, NFE_SQL_COLUMNS

insert_rows_in_database(
    df_to_insert=NOTA_FISCAL_ITEM,
    table_name='dsa_nfe_items',
    columns_dictionary=NFE_SQL_COLUMNS,
)
