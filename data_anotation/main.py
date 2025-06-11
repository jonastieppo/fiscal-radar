'''
A ideia é fazer download de publicações de diários oficiais, e minerar os pregões e licitações em ocorreram as fraudes/atos ilícitos
'''

# %%
import csv
import pickle
import os
import re
import time
import os
import requests
import pdfplumber
from openai import OpenAI
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd
from psycopg2.extras import execute_values
import json
import tqdm
import json
import nltk
nltk.download('stopwords')
from IPython.display import display, Markdown
load_dotenv(os.path.join(os.getcwd(), '..','.env'))

DEEP_SEEK_API_KEY = os.environ.get("DEEP_SEEK_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

class PostgresConnector:
    """
    A class to manage connections to a PostgreSQL database and execute queries.
    """
    def __init__(self, database_url: str):
        """
        Initializes the PostgresConnector with the database URL.

        Args:
            database_url (str): The SQLAlchemy database connection string.
        """
        self.database_url = database_url
        self.engine = sqlalchemy.create_engine(self.database_url)
        self.connection = None
        self.connect()

    def connect(self):
        """
        Establishes a connection to the database.
        Raises an exception if the connection fails.
        """
        if self.connection is None or self.connection.closed:
            try:
                self.connection = self.engine.connect()
                # print("Successfully connected to the database.") # Optional: for debugging
            except Exception as e:
                print(f"Failed to connect to the database: {e}")
                self.connection = None # Ensure connection is None if connect fails
                raise # Re-raise the exception to signal failure

    def disconnect(self):
        """Closes the database connection if it is open."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            # print("Database connection closed.") # Optional: for debugging
        self.connection = None

    def execute_select_query(self, query: str) -> pd.DataFrame | None:
        """
        Executes a SELECT SQL query and returns the result as a Pandas DataFrame.

        Args:
            query (str): The SELECT SQL query string to execute.

        Returns:
            pd.DataFrame | None: A DataFrame containing the query results,
                                 or None if an error occurs or connection is not active.
        """
        if self.connection is None or self.connection.closed:
            print("Error: Connection is not active. Call connect() or use a 'with' statement.")
            return None
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        
    def execute_update(self, update_query: str, params: dict = None) -> bool:
        """
        Executes an SQL query that modifies data (e.g., UPDATE, INSERT, DELETE).

        Args:
            update_query (str): The SQL query string to execute.
            params (dict, optional): A dictionary of parameters to bind to the query.
                                     Defaults to None.

        Returns:
            bool: True if the query was executed and committed successfully, False otherwise.
        """
        if self.connection is None or self.connection.closed:
            print("Error: Connection is not active. Call connect() or use a 'with' statement.")
            return False
        try:
            # For DML statements, it's good practice to use text() for safety and compatibility
            stmt = sqlalchemy.text(update_query)
            self.connection.execute(stmt, params if params else {})
            self.connection.commit()  # Commit the transaction
            # print(f"Query executed and committed successfully: {update_query}") # Optional
            return True
        except Exception as e:
            if self.connection and not self.connection.closed:
                self.connection.rollback()  # Rollback in case of error
            print(f"Error executing update query: {e}. Transaction rolled back.")
            return False

    def execute_update_several(self, query: str, update_data: list) -> bool:
        """
        Executes a batch update query using psycopg2.extras.execute_values.
        This is particularly useful for PostgreSQL's VALUES clause for bulk operations.

        Args:
            query (str): The SQL query string, with a %s placeholder for execute_values.
            update_data (list): A list of tuples (or lists of lists) containing the data
                                for the VALUES clause. Each tuple/list represents a row.
        Returns:
            bool: True if the operation was successful and committed, False otherwise.
        """
        if self.connection is None or self.connection.closed:
            print("Error: Connection is not active. Call connect() or use a 'with' statement.")
            return False

        # Access the underlying raw DBAPI (psycopg2) connection
        raw_dbapi_connection = self.connection.connection

        try:
            # The transaction is managed by the SQLAlchemy connection (self.connection)
            with raw_dbapi_connection.cursor() as cursor:
                execute_values(cursor, query, update_data)
            
            self.connection.commit()  # Commit the transaction via SQLAlchemy
            return True
        except Exception as e:
            print(f"Error executing batch update query with execute_values: {e}")
            if self.connection and not self.connection.closed:
                try:
                    self.connection.rollback()  # Rollback the transaction via SQLAlchemy
                except Exception as rb_exc:
                    print(f"Error during rollback: {rb_exc}")
            return False

    def __destroy__(self):
        self.disconnect()


class DataAnotation:
    def __init__(self) -> None:

        self.db_connection = PostgresConnector(DATABASE_URL)
        self.__connectLLM()

        pass

    def get_all_cgu_reports(self, only_licitacao=True):
        
        MAX_REPORT = 4211 # hard coded for now

        offset = 0
        while offset*15 < MAX_REPORT:
            self.download_cgu_report(offset, only_licitacao)
            offset+=1

        
    
    def download_cgu_report(self, offset, only_licitacao :  bool):
        
        if not only_licitacao:
            base_url = fr"https://eaud.cgu.gov.br/api/relatorios/pesquisa?colunaOrdenacao=dataPublicacao&direcaoOrdenacao=DESC&tamanhoPagina=15&offset={offset*15}&dataPublicacaoInicio=01%2F01%2F2013&dataPublicacaoFim=30%2F12%2F2023&grupoAtividade%5B%5D=2572&grupoAtividade%5B%5D=12517"
        else:
            base_url = fr"https://eaud.cgu.gov.br/api/relatorios/pesquisa?colunaOrdenacao=dataPublicacao&direcaoOrdenacao=DESC&tamanhoPagina=15&offset={offset*15}&dataPublicacaoInicio=01%2F01%2F2013&dataPublicacaoFim=30%2F12%2F2023&grupoAtividade%5B%5D=12517"

        response = requests.get(base_url, stream=True)
        json_data =  response.text

        try:
            python_dict = json.loads(json_data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

        def get_pdf(report_number):

            download_url = fr"https://eaud.cgu.gov.br/relatorios/download/{report_number}/"
            print(f"Downloading report {report_number}...")
            content = requests.get(download_url)
            report_name = f"report_{report_number}.pdf"
            with open(os.path.join(os.getcwd(), 'cgu_report', report_name), "wb") as f:
                for chunk in content.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                              
        [get_pdf(x['id']) for x in python_dict['data']]

    def read_results(self):
        self.df_sancioned = pd.read_csv('df_sancioned.csv')
        pass

    def __connectLLM(self):
        self.LLM_model = LLMPromptModel(api_key=DEEP_SEEK_API_KEY,
                                        base_url='https://api.deepseek.com',
                                        model_name='deepseek-chat'
                                          )
    def find_fraud_process(self):
        '''
        Método para encontrar as licitações referenciadas nos processos
        encontrados no diário oficial
        '''
        self.df_sancioned = self.parse_metadata()
        self.download_pdf_data(self.df_sancioned)
        # self.__connectLLM()
        self.extract_text_from_diario_oficial()
        self.df_sancioned.to_csv('df_sancioned.csv', index=False)

    def downloadDiarioOficial(self, ano, mes, dia, pagina, id, secao):

        if int(secao) == 1:
            jornal = 515
        if int(secao) == 2:
            jornal = 529
        if int(secao) == 3:
            jornal = 530

        base_url = fr"https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal={jornal}&pagina={pagina}&data={dia}/{mes}/{ano}&captchafield=firstAccess"
        response = requests.get(base_url, stream=True)

        with open(os.path.join(os.getcwd(), 'diario_oficial', f"processo_{id}.pdf"), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

    def download_pdf_data(self, df):

        for index, row in df.iterrows():
            
            # Only download if the file does not exists yet
            if not os.path.exists(os.path.join(os.getcwd(), 
                                'diario_oficial', 
                                f"processo_{index}.pdf")):
            
                self.downloadDiarioOficial(
                    ano=row['ano'],
                    mes=row['mes'],
                    dia=row['dia'],
                    pagina=row['pagina'],
                    id=index,
                    secao=row['secao']
                )


    def parse_metadata(self):
        """
        Gets the info from CNEP and CEIS database and parse
        the year, month, day, page and process number. This
        information will be used to download the correct pdf from
        Diario Oficial da Uniao
        """

        ceis = self.get_info_from_CEIS()
        cnep = self.get_info_from_CNEP()

        data = {
            "ano": [],
            "mes": [],
            "dia": [],
            "pagina": [],
            "secao": [],
            "num_processo": [],
            "cpf_cnpj": [],
            "nome_sancionado": [],
            "razao_social_cadastro_receita":[]
        }

        for index, row in ceis.iterrows():
            publicacao = row['publicacao']
            data_publicacao = row['data_publicacao']
            numero_do_processo = row['numero_do_processo']


            data["ano"].append(data_publicacao.year)
            data["mes"].append(f"{data_publicacao.month:02d}")
            data["dia"].append(f"{data_publicacao.day:02d}")

            regex_pattern = r"[Pp][aá]gina\s+(\d+)"
            match = re.search(regex_pattern, publicacao)
            page_number = match.group(1)

            data["pagina"].append(page_number)

            regex_pattern = r"Se[cç][aã]o\s+(\d+)"
            match = re.search(regex_pattern, publicacao)
            secao = match.group(1)
            data["secao"].append(int(secao))

            data["num_processo"].append(numero_do_processo)

            data["cpf_cnpj"].append(row['cpf_ou_cnpj_do_sancionado'])
            data["nome_sancionado"].append(row['nome_do_sancionado'])
            data["razao_social_cadastro_receita"].append(row['razao_social_cadastro_receita'])

        for index, row in cnep.iterrows():
            publicacao = row['PUBLICACAO']
            data_publicacao = row['DATA_PUBLICACAO']
            numero_do_processo = row['NUMERO_DO_PROCESSO']


            data["ano"].append(data_publicacao.year)
            data["mes"].append(f"{data_publicacao.month:02d}")
            data["dia"].append(f"{data_publicacao.day:02d}")

            regex_pattern = r"[Pp][aá]gina\s+(\d+)"
            match = re.search(regex_pattern, publicacao)
            page_number = match.group(1)

            data["pagina"].append(page_number)

            regex_pattern = r"Se[cç][aã]o\s+(\d+)"
            match = re.search(regex_pattern, publicacao)
            secao = match.group(1)
            data["secao"].append(int(secao))

            data["num_processo"].append(numero_do_processo)

            data["cpf_cnpj"].append(row['CPF_CNPJ'])
            data["nome_sancionado"].append(row['NOME_DO_SANCIONADO'])
            data["razao_social_cadastro_receita"].append(row['RAZAO_SOCIAL_CADASTRO_RECEITA'])



        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset='num_processo')

        df = df[df['secao'].isin([1,2,3])]
        return df



    def get_info_from_CEIS(self):

        query = """
SELECT publicacao, data_publicacao, numero_do_processo, cpf_ou_cnpj_do_sancionado, nome_do_sancionado, razao_social_cadastro_receita
FROM dsa."CEIS"
WHERE publicacao ~* 'Diário Oficial da União\s+Se[cç][aã]o\s+\d+\s+P[aá]gina\s+\d+'
and data_publicacao IS NOT NULL
"""
        return  self.db_connection.execute_select_query(query)


    def get_info_from_CNEP(self):

        query = """
SELECT "PUBLICACAO","DATA_PUBLICACAO","NUMERO_DO_PROCESSO", "CPF_CNPJ", "RAZAO_SOCIAL_CADASTRO_RECEITA", "NOME_DO_SANCIONADO"
FROM dsa."CNEP"
WHERE "PUBLICACAO" ~* 'Diário Oficial da União\s+Se[cç][aã]o\s+\d+\s+P[aá]gina\s+\d+'
and "DATA_PUBLICACAO" IS NOT NULL
"""

        return  self.db_connection.execute_select_query(query)
    
    def extract_text_from_diario_oficial(self):
        
        with open('./extracao_licitacao.prompt') as nf:
            init_prompt = nf.read()

        # Initialize the new column if it doesn't exist
        if "numero_licitacao_referenciada" not in self.df_sancioned.columns:
            self.df_sancioned["numero_licitacao_referenciada"] = pd.NA

        for index, row in tqdm.tqdm(self.df_sancioned.iterrows()):
            pdf_path = os.path.join(os.getcwd(),
                                'diario_oficial', 
                                f"processo_{index}.pdf")
            text = self.extract_text_from_pdf(pdf_path)

            final_prompt = fr"""
{init_prompt}

Agora, execute sua função para os seguintes dados:

======================================================

Dados de Procura: 
cpf_cnpj = {row['cpf_cnpj']}
nome_sancionado = {row['nome_sancionado']} 
razao_social_cadastro_receita = {row['razao_social_cadastro_receita']} 
num_processo = {row['num_processo']}

Texto:
{text}
"""         

            model_output = self.LLM_model.execute_prompt(final_prompt)
            try:
                json_object = self.LLM_model.parse_model_output(model_output)
                numero_licitacao = json_object.get("numero_licitacao_referenciada")
                print("Número Licitacao Encontrado:", numero_licitacao)
                self.df_sancioned.loc[index, "numero_licitacao_referenciada"] = numero_licitacao
                if numero_licitacao == None:
                    with open(f'prompt_degug_{index}.txt', 'w') as f:
                        f.write(final_prompt)

            except Exception as e:
                print(f"Ocorreu um erro para o índice {index}: {e}") # Good to include index in error


    def annotate_fraud_reports(self):

        system_prompt = '''
Analise o documento PDF fornecido emitido pela 'Controladoria-Geral da União (CGU)'. Seu objetivo é extrair informações específicas relacionadas ao processo de licitação auditado e à entidade auditada.

Por favor, identifique e extraia os seguintes detalhes. Se alguma informação não for encontrada no documento, o valor correspondente no JSON deve ser `null`. Não crie ou infira dados que não estejam explicitamente presentes no texto.

1.  **`numero_licitacao`**: Encontre o número oficial do processo de licitação (pregão, compra ou licitação). Este é frequentemente referido como 'Pregão Eletrônico n.º', 'Edital n.º' ou similar. O número da licitação deve ignorar quaisquer zeros à esquerda (por exemplo, 004155 deve se tornar 4155). Forneça este número e o ano, concatenados sem quaisquer símbolos (por exemplo, "Pregão n.º 004155/2023" deve se tornar "41552023"). Se não encontrado, retorne `null`.
3.  **`municipio`**: Identifique o nome do município onde a unidade auditada está localizada. Forneça o nome em letras maiúsculas (por exemplo, "FLORIANOPOLIS"). Se não encontrado, retorne `null`.
4.  **`nome_orgao`**: Extraia o nome do órgão auditado ('Órgão Auditado' ou 'Unidade Auditada'). Se o nome for muito longo, como 'Instituto Federal de Educação, Ciência e Tecnologia de [Nome do Estado]', tente fornecer uma versão mais curta comumente aceita, como 'Instituto Federal de [Nome do Estado]' (por exemplo, "Instituto Federal de Santa Catarina"). Se não encontrado, retorne `null`.
5.  **`numero_relatorio`**: Encontre o número de identificação do relatório de avaliação. Este é frequentemente referido como 'Relatório de Avaliação:', 'Nº do Relatório' ou similar. Forneça apenas os dígitos numéricos (por exemplo, "1350994"). Se não encontrado, retorne `null`.

A saída deve ser **apenas** o objeto JSON resultante, sem nenhum texto adicional, marcadores de código (como ```json) ou explicações.

Por exemplo, se o documento contivesse informações levando a:
Pregão Eletrônico n.º: 31007/2022
Unidade Auditada: Instituto Federal de Educação, Ciência e Tecnologia de Santa Catarina
Município/UF: Florianópolis/SC
Relatório de Avaliação: 1350994

A saída esperada seria estritamente:
{
"numero_licitacao" : "310072022",
"uf" : "SC",
"municipio" : "FLORIANOPOLIS",
"nome_orgao" : "Instituto Federal de Santa Catarina",
"numero_relatorio": "1350994"
}

Se, por outro lado, o nome do órgão e o número do relatório não fossem encontrados, a saída seria estritamente:
{
"numero_licitacao" : "310072022",
"uf" : "SC",
"municipio" : "FLORIANOPOLIS",
"nome_orgao" : null,
"numero_relatorio": null
}

Agora, por favor, processe o PDF que será fornecido e retorne as informações **apenas** no formato JSON.
'''
        
        self.responses_cgu = []
        self.responses_cgu_json = []

        for index,each_pdf in enumerate(os.listdir(os.path.join(os.getcwd(), 'cgu_report'))):
            
            file = os.path.join(os.getcwd(), 'cgu_report', each_pdf)
            report_geral_id = each_pdf.split('_')[-1].split('.')[0]

            text = self.extract_text_from_pdf(file)
            length = len(text)
            half = length // 2  # Integer division to get the midpoint

            first_half = text[:int(half)]  # From start to midpoint (probably the answerb exits in the first hald of the document)
                
            self.responses_cgu.append(self.LLM_model.execute_prompt_with_system(system_prompt,
                                                                    first_half))
            
            parsed = self.LLM_model.parse_model_output(self.responses_cgu[index])
            
            parsed = dict(parsed)
            parsed['report_geral_id'] = report_geral_id

            self.responses_cgu_json.append(parsed)

            with open('cgu_report.md', 'a', encoding='utf-8') as nf:
                print(self.responses_cgu_json[index], file=nf)

            #saving the found fraud numbers
            with open('fraud_numbers.pkl', 'wb') as f:
                pickle.dump(self.responses_cgu_json, f)

    def construct_database_for_prediction(self):

        df__no_fraud = self.__pick_random_licitacoes()
        df_fraud = pd.read_csv('dataframe_licitacoes_anotado.csv')

        df__no_fraud['is_fraud'] = 0
        df_fraud['is_fraud'] = 1

        df = pd.concat([df__no_fraud, df_fraud])

        df.to_csv('df_for_prediction.csv', index=False,
            quotechar='"', 
            quoting=csv.QUOTE_ALL         
                  )

    def __pick_random_licitacoes(self):

        query =  '''
SELECT 
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, 
    ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    ceis.codigo_da_sancao AS ceis_sancao,
    cnep."CODIGO_DA_SANCAO" AS cnep_sancao,
    COUNT(lpl.numero_processo) AS numero_parcitipacoes
FROM 
    (
SELECT *
FROM dsa."LicitacoesLicitacao" as llsub
TABLESAMPLE SYSTEM (1)
where llsub.numero_licitacao != '0'
and llsub.numero_licitacao != '-'
and llsub.numero_licitacao IS NOT NULL
and llsub.valor_licitacao != 0
LIMIT 200
	) as ll
INNER JOIN 
    dsa."LicitacoesParticipantesLicitacao" AS lpl 
    ON ll.numero_do_processo = lpl.numero_processo
LEFT JOIN
    dsa."CEIS" AS ceis 
    ON CAST(ceis.cpf_ou_cnpj_do_sancionado AS TEXT) = lpl.cnpj_participante
LEFT JOIN
    dsa."CNEP" AS cnep 
    ON CAST(cnep."CPF_CNPJ" AS TEXT) = lpl.cnpj_participante
GROUP BY
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, 
    ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    ceis.codigo_da_sancao,
    cnep."CODIGO_DA_SANCAO"
LIMIT 1000
'''

        df = self.db_connection.execute_select_query(query)

        return df

    def create_dataframe_licitacoes(self):
        '''

        '''

        # reading anotted data

        with open('fraud_numbers.pkl', 'rb') as f:
            responses = pickle.load(f)

        dfs = []
        for each_response in responses:
            
            if each_response == None:
                continue
            if each_response['numero_licitacao'] == None:
                continue
            try:
                df = self.__queryForLicitacoes(
                            numero_licitacao=each_response['numero_licitacao'],
                            uf=each_response['uf'],
                            municipio=each_response['municipio'],
                            nome_orgao=each_response['nome_orgao']
                            )
            except Exception as e:
                df = None
                print(e)

            if bool(type(df) != 'NoneType'):
                if bool(type(df) == pd.DataFrame) and len(df) > 0:
                    df.head()
                    dfs.append(df)

        self.dataframe_licitacoes_anotado = pd.concat(dfs)

        self.dataframe_licitacoes_anotado.to_csv(
            'dataframe_licitacoes_anotado.csv', 
            index=False, 
            quotechar='"', 
            quoting=csv.QUOTE_ALL
                                      )
        
    def __prepare_query(self, word : str):
        '''
        Receive an string, and separate the spaces by &
        '''
        word = word.upper()
        # no accent
        word = pd.Series(word).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').values[0]
        # no ponctuation
        word = word.replace(r'[^\w\s]', '')

        # no stop word
        stopwords = nltk.corpus.stopwords.words('portuguese')
        word = pd.Series(word).apply(lambda x: ' '.join([word for word in x.split() if word.lower() not in (stopwords)])).values[0]
        
        return word.replace(' ', ' & ')

    def __queryForLicitacoes(self, numero_licitacao, uf, municipio, nome_orgao):
        '''
        Method to make a query and create a dataframe with the results and
        features meaningfull for data exploration
        '''

        nome_orgao_query = self.__prepare_query(nome_orgao)


        query = f'''

SELECT 
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    ceis.codigo_da_sancao as ceis_sancao,
    cnep."CODIGO_DA_SANCAO" as cnep_sancao,
    COUNT(lpl.numero_processo) AS numero_parcitipacoes
FROM 
    dsa."LicitacoesLicitacao" AS ll
INNER JOIN 
    dsa."LicitacoesParticipantesLicitacao" AS lpl ON ll.numero_do_processo = lpl.numero_processo
LEFT JOIN
    dsa."CEIS" as ceis ON CAST(ceis.cpf_ou_cnpj_do_sancionado AS text)  = lpl.cnpj_participante
LEFT JOIN
    dsa."CNEP" as cnep ON CAST(cnep."CPF_CNPJ" AS text)  = lpl.cnpj_participante
WHERE 
    ll.numero_licitacao = '{numero_licitacao}'
    AND ll.uf LIKE '%%{uf}%%'
    AND ll.municipio LIKE '%%{municipio}%%'
    AND (
        ll.search_orgao @@ to_tsquery('portuguese', '{nome_orgao_query}')
        OR ll.search_ug @@ to_tsquery('portuguese', '{nome_orgao_query}')
    )
GROUP BY
    ll.numero_do_processo,
    ll.nome_ug, 
    ll.modalidade_compra, 
    ll.objeto, ll.uf, 
    ll.municipio, 
    ll.valor_licitacao,
    ceis.codigo_da_sancao,
    cnep."CODIGO_DA_SANCAO",
    ll.numero_do_processo -- Grouping to count participants per bid
'''
        
        with open('logger_licitacao.txt', 'a', encoding='utf-8') as nf:
                    
            print(f'''
    =================================================================
                        MAKING QUERY FOR
    numero: {numero_licitacao}
    uf: {uf}
    municipio: {municipio}
    nome_orgao: {nome_orgao}

    **query**

    {query}

    =================================================================
    ''',
    file=nf
    )

        return self.db_connection.execute_select_query(query)




    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extracts text content from a PDF file.

        Args:
            pdf_path (str): The absolute path to the PDF file.

        Returns:
            str: The extracted text content from the PDF.
        """
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() + "\n" # Add a newline for separation between pages
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
            return "" # Return empty string or raise the exception
        return text

class LLMPromptModel:
    def __init__(self, model_name: str = "deepseek-chat", api_key: str = None,
                 base_url: str = "https://api.deepseek.com",
                 ):
        """
        Initializes the LLMs model.
        base_url for ollama = https://localhost:11434
        gemma model downloaded = gemma3:latest
        """
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model_name = model_name

    def execute_prompt(self, prompt_text: str) -> str:
        """
        Executes a prompt against the specified Ollama model.
        Each call creates a new, stateless interaction with the model.

        Args:
            prompt_text (str): The prompt to send to the LLM.

        Returns:
            str: The content of the model's response. Returns an empty string on error.
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "user", "content": prompt_text},
                ],
                stream=False
            )

            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error executing prompt with model '{self.model_name}': {e}")
            return ""
        
    def execute_prompt_with_system(self, system_prompt, prompt_text: str)->str:

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_text} 
                ],
                stream=False
            )

            return response.choices[0].message.content.strip()
        
        except Exception as e:
            print(f"Error executing prompt with model '{self.model_name}': {e}")
            return ""

    def execute_several_prompts(self, system_prompt, user_prompts: list[str]) -> list[str]:
        """
        Executes a prompt against the specified Ollama model.
        Each call creates a new, stateless interaction with the model.

        Args:
            system_prompt (str): Configuration prompt to be sent the LLM.
            user_prompts (str): The prompt to send to the LLM.

        Returns:
            str: The content of the model's response. Returns an empty [string] on error.
        """
        try:
            user_p = [{"role": "user", "content": prompt} for prompt in user_prompts]
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    user_p
                ],
                stream=False
            )

            responses = [r.choices[0].message.content.strip() for r in response]

            return responses
        
        except Exception as e:
            print(f"Error executing prompt with model '{self.model_name}': {e}")
            return [""]
                
    def parse_model_output(self, output):
        # Remove the markdown code block markers
        json_str = output.strip().strip('```json').strip('```').strip()
        
        # Parse the JSON
        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None

class DataBaseSanitization():

        def __init__(self) -> None:

            self.db_connection = PostgresConnector(DATABASE_URL)
            
        def sanitize_db(self):
            
            number_of_rows = self.db_connection.execute_select_query(
                '''
            SELECT COUNT(*) FROM dsa."LicitacoesLicitacao"
            '''
            )

            number_of_offsets = number_of_rows['count'].values[0] //10000 + 1
            rest = number_of_rows['count'].values[0] % 10000
            

            for i in range(int(number_of_offsets)):
                df = self.__get_ug_orgao(offset=int(10000*i))
                df = self.__toUpperCase(df)
                df = self.__noAccent(df)
                df = self.__noPonctuation(df)
                df = self.__noStopWords(df)
                self.__update_ug_norm(df)
                self.__update_orgao_norm(df)


        def __update_ug_norm(self, df):
            
            update_data = list(df[['id', 'nome_ug']].itertuples(index=False, name=None))
            print(update_data)
            query = """
                UPDATE dsa."LicitacoesLicitacao" AS ll
                SET "NOME_UG_NORM" = v.nome_ug
                FROM (VALUES %s) AS v(id, nome_ug)
                WHERE ll.id = v.id;
            """
            self.db_connection.execute_update_several(query, update_data)

        def __update_orgao_norm(self, df):

            update_data = list(df[['id', 'nome_orgao']].itertuples(index=False, name=None))
            query = """
                UPDATE dsa."LicitacoesLicitacao" AS ll
                SET "NOME_ORGAO_NORM" = v.nome_orgao
                FROM (VALUES %s) AS v(id, nome_orgao)
                WHERE ll.id = v.id;
            """
            self.db_connection.execute_update_several(query, update_data)


        def __get_ug_orgao(self, offset : int):
            '''
            Get ug and orgao to sanitize
            '''
            return self.db_connection.execute_select_query(f'''
SELECT ll.id, ll.nome_ug, ll.nome_orgao
FROM dsa."LicitacoesLicitacao" AS ll
LIMIT 10000 OFFSET {offset}
''')
        
        def __toUpperCase(self, data : pd.DataFrame):
            '''
            transform in uppercase
            '''
            for column in data.columns:
                if data[column].dtype == 'object':
                    data[column] = data[column].str.upper()

            return data
        
        def __noAccent(self, data : pd.DataFrame):
            '''
            Remove accents
            '''
            for column in data.columns:
                if data[column].dtype == 'object':
                    data[column] = data[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

            return data
        
        
        def __noPonctuation(self, data : pd.DataFrame):
            '''
            Removes chars like -, ., ,, (, ).
            '''
            for column in data.columns:
                if data[column].dtype == 'object':
                    data[column] = data[column].str.replace(r'[^\w\s]', '', regex=True)

            return data
        
        def __noStopWords(self, data : pd.DataFrame):
            '''
            Removes stop words
            '''
            stopwords = nltk.corpus.stopwords.words('portuguese')

            for column in data.columns:
                if data[column].dtype == 'object':
                    data[column] = data[column].apply(lambda x: ' '.join([word for word in x.split() if word.lower() not in (stopwords)]))

            return data
        



D = DataAnotation()
# D.get_all_cgu_reports(only_licitacao=True)
# D.annotate_fraud_reports()
# D.create_dataframe_licitacoes()
D.construct_database_for_prediction()
# Class = DataBaseSanitization()

# %%





# %%

    


# %%
