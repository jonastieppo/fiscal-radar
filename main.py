# %%
from analise_inicial.analise_inicial_NFI import init_exploration as IE_NF
import pandas as pd
from montagem_bases import CNEP


data = pd.read_csv('dados/NOTA_FISCAL_ITEM_001P.csv', sep=';')
data.head()

# %%
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 # Needed for the database driver

# --- Database Connection Details ---
db_name = "fiscal-radr"
db_user = "postgres"
db_password = "123"
db_host = "localhost"
db_port = "5432"

# --- Your DataFrame ---
df = CNEP

# --- Target Schema and Table Names ---
schema_name = "test_"
table_name = "CNEP"
full_table_name = f"{schema_name}.{table_name}" # Combine for to_sql

# --- Create the schema first (to_sql won't create the schema itself) ---
conn_pg = None
cur_pg = None
try:
    conn_pg = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cur_pg = conn_pg.cursor()
    print(f"Creating schema '{schema_name}' if it does not exist...")
    cur_pg.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn_pg.commit()
    print("Schema creation command executed.")
except psycopg2.Error as e:
    print(f"Database error during schema creation: {e}")
    if conn_pg:
        conn_pg.rollback()
except Exception as e:
     print(f"An unexpected error occurred during schema creation: {e}")
finally:
    if cur_pg: cur_pg.close()
    if conn_pg: conn_pg.close()


# --- Use pandas .to_sql() with SQLAlchemy to create and populate the table ---
engine = None
try:
    # Create a SQLAlchemy engine
    # Ensure you have the 'sqlalchemy' library installed (pip install sqlalchemy)
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Use the to_sql method
    # if_exists='replace' will drop the table if it exists and recreate it
    # based on the DataFrame structure before inserting data.
    # If you only want to create it if it doesn't exist, you could check first,
    # or use if_exists='fail' and handle the error if it exists.
    # 'replace' is a straightforward way to ensure the table matches the DataFrame structure.
    print(f"\nCreating and populating table '{full_table_name}' using pandas .to_sql()...")
    df.to_sql(full_table_name, engine, if_exists='replace', index=False)

    print(f"Successfully created and populated table '{full_table_name}'.")

except Exception as e:
    print(f"Error during pandas .to_sql() operation: {e}")

finally:
    # Dispose of the engine connection pool
    if engine:
        engine.dispose()

# %%

# --- Database Connection Details ---
db_name = "fiscal-radr"
db_user = "postgres"
db_password = "123"
db_host = "localhost"
db_port = "5432"

# --- Your DataFrame ---
df = data

# --- Target Schema and Table Names ---
schema_name = "test_"
table_name = "table2"
# --- Map pandas dtypes to PostgreSQL data types ---
# This is a simplified mapping; you might need more specific types
dtype_mapping = {
    'int64': 'INTEGER',
    'float64': 'DOUBLE PRECISION',
    'object': 'VARCHAR(255)', # Or TEXT for potentially longer strings
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
    # Add more mappings as needed
}

conn = None
cur = None

try:
    # Connect to the database
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cur = conn.cursor()

    # 1. Create the schema (if it doesn't exist)
    print(f"Creating schema '{schema_name}' if it does not exist...")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    print("Schema creation command executed.")

    # 2. Generate the CREATE TABLE statement
    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        pg_dtype = dtype_mapping.get(str(dtype), 'TEXT') # Default to TEXT if dtype not in mapping
        columns_sql.append(f"{col_name} {pg_dtype}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {', '.join(columns_sql)}
        -- You might want to add primary keys or constraints here manually
    );
    """

    # 3. Execute the CREATE TABLE statement
    print(f"Creating table '{schema_name}.{table_name}' if it does not exist...")
    cur.execute(create_table_sql)
    conn.commit()
    print("Table creation command executed.")

    print("\nSchema and table creation process completed.")

    # After creating, you could then populate it using the methods from the previous example

except psycopg2.Error as e:
    print(f"Database error: {e}")
    if conn:
        conn.rollback() # Roll back changes on error
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    # Close the cursor and connection
    if cur:
        cur.close()
    if conn:
        conn.close()

# %%
