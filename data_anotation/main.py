'''
A ideia é fazer download de publicações de diários oficiais, e minerar os pregões e licitações em ocorreram as fraudes/atos ilícitos
'''

# %%
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
import json
import tqdm

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

    def __destroy__(self):
        self.disconnect()


class DataAnotation:
    def __init__(self) -> None:

        self.db_connection = PostgresConnector(DATABASE_URL)


        pass

    def read_results(self):
        self.df_sancioned = pd.read_csv('df_sancioned.csv')
        pass


    def find_fraud_process(self):
        '''
        Método para encontrar as licitações referenciadas nos processos
        encontrados no diário oficial
        '''
        self.df_sancioned = self.parse_metadata()
        self.download_pdf_data(self.df_sancioned)
        self.LLM_model = LLMPromptModel(api_key=DEEP_SEEK_API_KEY,
                                        base_url='https://api.deepseek.com',
                                        model_name='deepseek-chat'
                                          )
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
        
    def execute_several_propmpts(self, prompt_texts: list[str]) -> list[str]:
        """
        Executes a list of prompts against the specified Ollama model.
        """
        try:
            messages = [{"role": "user", "content": prompt} for prompt in prompt_texts] 
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                stream=False
            )

            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error executing prompt with model '{self.model_name}': {e}")
            return ""
        
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


D = DataAnotation()
D.read_results()

# %%
