# Public Procurement and Fiscal Data Analysis Project

## 1. Overview
This project is designed to download, process, store, and analyze public procurement data, fiscal notes (NF-e), and information on sanctioned entities from Brazilian public data sources. The primary goal is to identify patterns, relationships, and potential irregularities, including linking sanctioned entities to specific procurement processes and fiscal transactions. It leverages a PostgreSQL database for data storage and various Python libraries for data manipulation, ETL, and machine learning/NLP tasks.

## 2. Features
### Data Acquisition:
- Downloads public procurement data (contracts, bidding processes) from the Brazilian Transparency Portal (portaldatransparencia.gov.br)
- Downloads information on sanctioned entities from CEIS (Cadastro de Empresas Inidôneas e Suspensas) and CNEP (Cadastro Nacional de Empresas Punidas)
- Downloads fiscal notes (NF-e items) data
- Downloads official gazette (Diário Oficial da União) publications in PDF format based on sanction records

### Data Storage:
- Creates a dedicated schema (dsa) and multiple tables in a PostgreSQL database
- Defines clear schemas for contracts, bidding items, sanctions, fiscal notes, etc.

### ETL (Extract, Transform, Load):
- Extracts data from downloaded ZIP files and CSVs
- Transforms data:
  - Converts column names to consistent snake_case format
  - Performs date conversions
  - Handles data type conversions for database insertion
- Loads processed data into PostgreSQL

### Data Analysis & Machine Learning:
- Identifies fiscal notes issued by/to sanctioned entities (CNPJs/CPFs)
- Initial exploration with Naive Bayes classifier to distinguish fiscal notes (main.py)
- Advanced Analysis (data_anotation/main.py):
  - Parses metadata from sanction records to identify official gazette publications
  - Extracts text from PDF official gazettes
  - Utilizes LLM (DeepSeek API) to identify procurement process numbers in sanction texts

### Modularity:
Structured into Python scripts handling specific tasks (schema creation, data download, processing, DB population)

## 3. Project Structure
Key Python scripts:
- `creating_schemas.py`: Creates DB schemas/tables using SQLAlchemy
- `dicionarios_gerais.py`: Column name mappings to snake_case
- `download_licitacoes.py`: Downloads procurement data from Transparency Portal
- `helper.py`: Utility functions (ZIP extraction, CSV handling, DB queries)
- `montagem_bases.py`: Processes raw data into Pandas/Dask DataFrames
- `populate_database.py`: Loads processed data into PostgreSQL
- `main.py`: Initial analysis linking fiscal notes to sanctions + Naive Bayes model
- `main2.py`: Advanced workflow (gazette PDF processing + LLM analysis)
- `extracao_licitacao.prompt`: LLM prompt template for procurement extraction

## 4. Setup and Installation
### 4.1. Prerequisites
- Python 3.x
- PostgreSQL server
- Internet access

### 4.2. Python Dependencies
```bash
pip install pandas dask sqlalchemy psycopg2-binary python-dotenv requests pdfplumber openai scikit-learn numpy tqdm
