import csv
import os
import zipfile
import dask.dataframe as dd
import sqlalchemy
import pandas as pd
import psycopg2
import re
import tqdm
def extrair_zips(pasta):
    """
    Extrai o conteúdo de todos os arquivos .zip encontrados na pasta especificada.

    Args:
        pasta (str): O caminho para a pasta onde os arquivos .zip estão localizados.
    """
    if not os.path.isdir(pasta):
        print(f"Erro: A pasta '{pasta}' não existe.")
        return

    arquivos_zip = [f for f in os.listdir(pasta) if f.endswith(".zip")]

    if not arquivos_zip:
        print(f"Nenhum arquivo .zip encontrado na pasta '{pasta}'.")
        return

    print(f"Encontrados {len(arquivos_zip)} arquivos .zip na pasta '{pasta}'.")

    for arquivo_zip in arquivos_zip:
        caminho_arquivo_zip = os.path.join(pasta, arquivo_zip)
        nome_base = os.path.splitext(arquivo_zip)[0]
        pasta_destino = os.path.join(pasta, nome_base)

        try:
            with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
                print(f"Extraindo '{arquivo_zip}' para '{pasta_destino}'...")
                os.makedirs(pasta_destino, exist_ok=True)  # Cria a pasta se não existir
                zip_ref.extractall(pasta_destino)
            print(f"'{arquivo_zip}' extraído com sucesso!")
        except zipfile.BadZipFile:
            print(f"Erro: Arquivo '{arquivo_zip}' parece ser um arquivo zip inválido ou corrompido.")
        except Exception as e:
            print(f"Ocorreu um erro ao extrair '{arquivo_zip}': {e}")

# %%
import pandas as pd
import requests
import os
import time

def baixar_notas_fiscais(ano, mes):
    """
    Tenta baixar o arquivo de notas fiscais para o ano e mês especificados.

    Args:
        ano (int): O ano para baixar as notas fiscais.
        mes (int): O mês para baixar as notas fiscais (1 a 12).
    """
    mes_str = f"{mes:02d}"
    url = f"https://portaldatransparencia.gov.br/download-de-dados/notas-fiscais/{ano}{mes_str}"
    nome_arquivo = f"notas_fiscais_{ano}_{mes_str}.zip"

    try:
        print(f"Tentando baixar: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Lança uma exceção para status de erro HTTP

        with open(nome_arquivo, "wb") as arquivo:
            for chunk in response.iter_content(chunk_size=8192):
                arquivo.write(chunk)
        print(f"Download de {nome_arquivo} concluído com sucesso!")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar {url}: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao processar {url}: {e}")

def baixarportal_transparencia(base_url,nome_do_arquivo, ano, mes):
    """
    Tenta baixar o arquivo de notas fiscais para o ano e mês especificados.

    Args:
        ano (int): O ano para baixar as notas fiscais.
        mes (int): O mês para baixar as notas fiscais (1 a 12).
    """
    mes_str = f"{mes:02d}"
    url = f"{base_url}/{ano}{mes_str}"
    nome_arquivo = f"{nome_do_arquivo}_{ano}_{mes_str}.zip"

    try:
        print(f"Tentando baixar: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Lança uma exceção para status de erro HTTP

        with open(nome_arquivo, "wb") as arquivo:
            for chunk in response.iter_content(chunk_size=8192):
                arquivo.write(chunk)
        print(f"Download de {nome_arquivo} concluído com sucesso!")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar {url}: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao processar {url}: {e}")


def read_notas_fiscais(path : str, encoding='cp1252')->pd.DataFrame:

    return  pd.read_csv(path,
            sep=';',
            encoding=encoding,
            decimal=',',
            )

def adicionar_dataframe_csv(nome_arquivo, df, sep : str = ';'):
    """
    Adiciona o conteúdo de um DataFrame ao final de um arquivo CSV existente,
    encapsulando todos os valores com aspas duplas.

    Args:
        nome_arquivo (str): O nome do arquivo CSV a ser modificado.
        df (pd.DataFrame): O DataFrame contendo os dados a serem adicionados.
        sep (str, optional): O separador de colunas. O padrão é ';'.
    """
    try:
        with open(nome_arquivo, 'a', newline='', encoding='utf-8') as arquivo_csv:
            csv_writer = csv.writer(arquivo_csv, delimiter=sep, quoting=csv.QUOTE_ALL)
            for index, row in df.iterrows():
                csv_writer.writerow(row.astype(str).values.tolist())
        print(f"Dados do DataFrame adicionados com sucesso ao arquivo '{nome_arquivo}' com encapsulamento por \".")
    except Exception as e:
        print(f"Ocorreu um erro ao adicionar os dados do DataFrame ao arquivo '{nome_arquivo}': {e}")


def mount_nota_fiscal_item_dataframe(root_folder, max_dict=100):
    '''
    Scans each folder, and build an giant dataframe

    It will be really long. I will procede with care to note explode  my RAM memory kkkk

    '''
    counter=0

    # Primeiro remove o arquivo com os dados compilados
    if 'nota_fiscal_item.csv' in os.listdir():
        os.remove('nota_fiscal_item.csv')

    if 'nota_fiscal.csv' in os.listdir():
        os.remove('nota_fiscal.csv')

    def read_item_and_save_csv(item : os.DirEntry[str],
                               counter : int,
                               max_dict: int,
                               file_to_add : str
                               ):

        df = read_notas_fiscais(item.path)

        if counter>=max_dict:
            return

        if file_to_add in os.listdir():
            adicionar_dataframe_csv(file_to_add, df, sep=';')
            counter+=1
        else:
            df.to_csv(file_to_add,
                        index=None, sep=';',quoting=csv.QUOTE_ALL,
                        doublequote=True)
            counter+=1

    for item in os.scandir(root_folder):

        if item.is_dir():
            for item_2 in os.scandir(item.path):
                if item_2.is_file() and item_2.name.endswith('NotaFiscalItem.csv'):
                    read_item_and_save_csv(
                        item=item_2,
                        counter=counter,
                        max_dict=max_dict,file_to_add='nota_fiscal_item.csv'
                    )

def excluir_outliers(df, coluna):
    """
    Exclui outliers de uma coluna específica de um DataFrame usando o método do Intervalo Interquartil (IQR).

    Args:
        df (pd.DataFrame): O DataFrame do qual os outliers serão excluídos.
        coluna (str): O nome da coluna numérica para identificar e excluir outliers.

    Returns:
        pd.DataFrame: Um novo DataFrame sem os outliers da coluna especificada.
    """
    Q1 = df[coluna].quantile(0.25)
    Q3 = df[coluna].quantile(0.75)
    IQR = Q3 - Q1
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR
    df_sem_outliers = df[(df[coluna] >= limite_inferior) & (df[coluna] <= limite_superior)].copy(deep=True)
    return df_sem_outliers

def selecionar_faixa(df, coluna, q1, q2):
    """Seleciona um subconjunto de um DataFrame com base em uma faixa de valores em uma coluna específica.

    Esta função recebe um DataFrame e limites inferior e superior para filtrar os valores de uma coluna.
    Ela retorna um novo DataFrame contendo apenas as linhas onde os valores da coluna especificada
    estão dentro (ou iguais aos) dos limites fornecidos.

    Args:
        df (pd.DataFrame): O DataFrame a ser filtrado.
        coluna (str): O nome da coluna no DataFrame a ser usada para a seleção.
        q1 (float ou int): O limite inferior da faixa de valores desejada (inclusive).
        q2 (float ou int): O limite superior da faixa de valores desejada (inclusive).

    Returns:
        pd.DataFrame: Um novo DataFrame contendo apenas as linhas onde os valores da coluna
                      especificada estão dentro da faixa [q1, q2]. Uma cópia profunda
                      do subconjunto é retornada para evitar modificações no DataFrame original.

    Examples:
        >>> data = {'fruta': ['maçã', 'banana', 'uva', 'morango', 'abacaxi'],
        ...         'preco': [2.50, 1.80, 3.20, 4.00, 5.50]}
        >>> df = pd.DataFrame(data)
        >>> faixa_selecionada = selecionar_faixa(df, 'preco', 2.0, 4.5)
        >>> print(faixa_selecionada)
           fruta  preco
        0    maçã   2.5
        1  banana   1.8
        2     uva   3.2
        3 morango   4.0
    """
    limite_inferior = df[coluna].quantile(q1)
    limite_superior = df[coluna].quantile(q2)
    df_faixa = df[(df[coluna] >= limite_inferior) & (df[coluna] <= limite_superior)].copy(deep=True)
    return df_faixa

def date_convertion_pandas(df : pd.DataFrame | dd.DataFrame, date_columns : list[str]):
    '''
    Converts date columns to YYYY-MM-DD format
    '''

    df_copy = df.copy(deep=True)

    for col_name in date_columns:
        if col_name in df_copy.columns:
            print(f"Converting column '{col_name}' to datetime objects...")
            # Convert to datetime objects, coercing errors to NaT (Not a Time)
            df_copy[col_name] = pd.to_datetime(df_copy[col_name], format='%d/%m/%Y', errors='coerce')
            # Convert NaT to None (which becomes SQL NULL)
            df_copy[col_name] = df_copy[col_name].astype(object).where(df_copy[col_name].notnull(), None)
    # --- End Date Conversion ---

    if type(df_copy) == dd.DataFrame:
            df_copy = df_copy.compute()

    return df_copy

def date_convertion_dask(df : dd.DataFrame, date_columns : list[str], folder_export : str, final_arquive_name):
    '''
    Converts date columns to YYYY-MM-DD format
    '''

    df_copy = df.copy()

    for col_name in date_columns:
        if col_name in df_copy.columns:
            print(f"Converting column '{col_name}' to datetime objects...")
            # Convert to datetime objects, coercing errors to NaT (Not a Time)
            df_copy[col_name] = dd.to_datetime(df_copy[col_name], format='%d/%m/%Y', errors='coerce')
            # Convert NaT to None (which becomes SQL NULL)
            df_copy[col_name] = df_copy[col_name].astype(object).where(df_copy[col_name].notnull(), None)
    # --- End Date Conversion ---

    df_copy.to_csv(folder_export)

    # As dasks creates a folder with several files, we will parse every file in this folder:
    if final_arquive_name in os.listdir():
        os.remove(final_arquive_name)

    for f in os.scandir(folder_export):

        if f.name.endswith('.part'):
           df = pd.read_csv(
               f.path,
               sep=',',
               encoding='utf-8',
               decimal='.',
           )

           if final_arquive_name not in os.listdir():
                df.to_csv(final_arquive_name, sep=';',
                          quoting=csv.QUOTE_ALL,
                          doublequote=True,
                          index=None
                          )
           else:
               adicionar_dataframe_csv(
                   nome_arquivo=final_arquive_name,
                   df=df,
                   sep=';'
               )

def make_query(query: str, connection : sqlalchemy.Connection, chunksize = None)->pd.DataFrame | dd.DataFrame:

    try:
        df = pd.read_sql(query, connection, chunksize=chunksize)
        return df
    except Exception as e:
        print(e)


def make_query_dask(query: str, connection : str, index_col : str = None)->dd.DataFrame:

    try:
        df = dd.read_sql_query(query, connection, index_col=index_col)
        return df
    except Exception as e:
        print(e)

def insert_rows_in_database(df_to_insert : pd.DataFrame | dd.DataFrame, table_name : str,
                            columns_dictionary,
                            db_host : str,
                            db_user : str,
                            db_password : str,
                            db_name : str,
                            schema = 'dsa',
                            ):
    '''
    Function to insert rows in a existent dataframe
    '''
    df_column_names = df_to_insert.columns
    columns_in_table = [f'"{columns_dictionary[c]}"' for c in df_column_names]

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


# Helper function to convert column names to snake_case
def to_snake_case(name):
    name = name.lower()
    accents = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'â': 'a', 'ê': 'e', 'î': 'i', 'ô': 'o', 'û': 'u', 'ã': 'a', 'õ': 'o', 'ç': 'c'}
    for char_accent, char_simple in accents.items():
        name = name.replace(char_accent, char_simple)
    name = re.sub(r'\s*\(.*\)\s*$', '', name) # Remove content in parentheses at the end of the string
    name = name.replace(' ', '_').replace('-', '_').replace('/', '_').replace('.', '')
    name = re.sub(r'_+', '_', name) # Replace multiple underscores with single
    name = name.strip('_')
    return name

# Helper function to generate table name from dictionary key
def get_table_name(dict_key_name):
    name_part = dict_key_name.replace('dicionario_tabela_', '')
    # Convert snake_case or mixedCase to CamelCase
    # e.g., contratos_compras -> ContratosCompras
    # e.g., contratos_itemCompra -> ContratosItemCompra
    # e.g., licitacoes_EmpenhosRelacionados -> LicitacoesEmpenhosRelacionados
    
    # First, handle potential CamelCase parts already present
    name_part = re.sub(r'([a-z])([A-Z])', r'\1_\2', name_part)
    
    parts = name_part.split('_')
    return "".join(part.capitalize() for part in parts)

def find_date_columns(column_name: str) -> bool:
    '''
    Find, by re, if a certain column is from the type date.

    For example, if the column name contains the word
    Date/Data (portuguese) it is a Date column.
    '''
    # Regex to find 'data' or 'date' as whole words, case-insensitive
    # \b ensures word boundaries, so 'database' wouldn't match.
    pattern = r'\b(data|date)\b'
    if re.search(pattern, column_name, re.IGNORECASE):
        return True
    return False

def csv_to_postgree_sql(
                        csv_path: str,
                        table_name : str,
                        columns_dictionary : dict,
                        db_host : str,
                        db_user : str,
                        db_password : str,
                        db_name : str,
                        schema = 'dsa',                        
                        ):

    df = pd.read_csv(csv_path,
                sep=';',
                encoding='cp1252',
                decimal=',',
                )
    
    date_columns = [col for col in df.columns if find_date_columns(col)]
    df = date_convertion_pandas(df=df, date_columns=date_columns)

    #  assumes that the daframe columns has the same order as
    # the dictionary key-value pairs. 
    # I was checked, and for licitacao/compras it occurs 
    columns_in_table = [v for  _,v in columns_dictionary.items()]

    # Establish the connection
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        
        # insert the rows
    
        data_to_insert = [tuple(row) for row in df.values]

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

def parse_licitacoes_and_insert_in_database(
                                    root_folder,
                                    db_host : str,
                                    db_user : str,
                                    db_password : str,
                                    db_name : str,
                                    schema = 'dsa',
                                    **dicionaries_sql    
                                            ):
    '''
    Starting from a root folder, find all .csv and insert into the database
    '''
    contratos_compras_SQL_COLUMNS = dicionaries_sql.get('contratos_compras_SQL_COLUMNS')
    contratos_itemCompra_SQL_COLUMNS = dicionaries_sql.get('contratos_itemCompra_SQL_COLUMNS')
    contratos_termoAditivo_SQL_COLUMNS = dicionaries_sql.get('contratos_termoAditivo_SQL_COLUMNS')
    licitacoes_EmpenhosRelacionados_SQL_COLUMNS = dicionaries_sql.get('licitacoes_EmpenhosRelacionados_SQL_COLUMNS')
    licitacoes_ItemLicitacao_SQL_COLUMNS = dicionaries_sql.get('licitacoes_ItemLicitacao_SQL_COLUMNS')
    licitacoes_licitacao_SQL_COLUMNS = dicionaries_sql.get('licitacoes_licitacao_SQL_COLUMNS')
    licitacoes_participantes_licitacao_SQL_COLUMNS = dicionaries_sql.get('licitacoes_participantes_licitacao_SQL_COLUMNS')

    print('''
====================================================================
        INICIANDO O INSERT DOS DADOS DE COMPRAS/LITACOES
====================================================================
''')
    for folder, subfolder, files in tqdm.tqdm(os.walk(root_folder)):
        for file in tqdm.tqdm(files):
            if file.endswith('.csv'):
                csv_path = os.path.join(folder, file)
                print(f'''
.....................................
| Insert para: {file}               |
.....................................
            ''')

                if file.endswith('Compras.csv'):
                    columns_dictionary = contratos_compras_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_contratos_compras")

                elif file.endswith('ItemCompra.csv'):
                    columns_dictionary = contratos_itemCompra_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_contratos_itemCompra")

                elif file.endswith('TermoAditivo.csv'):
                    columns_dictionary = contratos_termoAditivo_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_contratos_termoAditivo")

                elif file.endswith('EmpenhosRelacionados.csv'):
                    columns_dictionary = licitacoes_EmpenhosRelacionados_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_licitacoes_EmpenhosRelacionados")

                elif file.endswith('ItemLicitacao.csv'):
                    columns_dictionary = licitacoes_ItemLicitacao_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_licitacoes_ItemLicitacao")

                elif file.endswith('_Licitação.csv'):
                    columns_dictionary = licitacoes_licitacao_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_licitacoes_licitacao")

                elif file.endswith('_ParticipantesLicitação.csv'):
                    columns_dictionary = licitacoes_participantes_licitacao_SQL_COLUMNS
                    table_name = get_table_name("dicionario_tabela_licitacoes_participantes_licitacao")

                else:
                    continue

                csv_to_postgree_sql(
                    csv_path=csv_path,
                    table_name=table_name,
                    columns_dictionary=columns_dictionary,
                    db_host=db_host,
                    db_user=db_user,
                    db_password=db_password,
                    db_name=db_name
                )

