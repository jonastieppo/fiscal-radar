# %%
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Numeric, Date, BigInteger, Boolean, text
import re
from sqlalchemy.dialects import postgresql
from dotenv import load_dotenv
import os
from helper import to_snake_case, get_table_name

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
print(DATABASE_URL)

engine = create_engine(DATABASE_URL)

schema_name='dsa'

try:
    print(f"Attempting to create schema '{schema_name}' if it does not exist...")
    with engine.connect() as connection:
        # Use text() to execute raw SQL
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        connection.commit() # Commit the transaction to finalize the schema creation
    print(f"Schema '{schema_name}' ensured to exist (created or already present).")

except Exception as e:
    print(f"An error occurred while trying to create schema '{schema_name}': {e}")


metadata = MetaData()
cnep_table = Table(
    "CNEP",
    metadata,
    Column("ID",
           autoincrement=True,
           unique=True,
           primary_key=True,
           nullable=False,
           type_=Integer
           ),
    Column("CADASTRO", Text, nullable=False),
    Column("CODIGO_DA_SANCAO", Integer, nullable=False), # int64 -> Integer
    Column("TIPO_DE_PESSOA", Text, nullable=True), # 1280/1290 -> nullable
    Column("CPF_CNPJ", BigInteger, nullable=False), # int64 -> BigInteger for large numbers
    Column("NOME_DO_SANCIONADO", Text, nullable=False),
    Column("NOME_INFORMADO_PELO_ORGAO_SANCIONADOR", Text, nullable=False),
    Column("RAZAO_SOCIAL_CADASTRO_RECEITA", Text, nullable=True), # 1260/1290 -> nullable
    Column("NOME_FANTASIA_CADASTRO_RECEITA", Text, nullable=True), # 817/1290 -> nullable
    Column("NUMERO_DO_PROCESSO", Text, nullable=False),
    Column("CATEGORIA_DA_SANCAO", Text, nullable=False),
    Column("VALOR_DA_MULTA", Numeric, nullable=False), # float64 -> Numeric for precision
    Column("DATA_INICIO_SANCAO", Date, nullable=False), # object likely holds date strings, map to Date
    Column("DATA_FINAL_SANCAO", Date, nullable=True), # 24/1290 -> nullable, map to Date
    Column("DATA_PUBLICACAO", Date, nullable=True), # 1230/1290 -> nullable, map to Date
    Column("PUBLICACAO", Text, nullable=False),
    Column("DETALHAMENTO_DO_MEIO_DE_PUBLICACAO", Text, nullable=True), # 280/1290 -> nullable
    Column("DATA_DO_TRANSITO_EM_JULGADO", Date, nullable=True), # 397/1290 -> nullable, map to Date
    Column("ABRANGENCIA_DA_SANCAO", Text, nullable=False),
    Column("ORGAO_SANCIONADOR", Text, nullable=False),
    Column("UF_ORGAO_SANCIONADOR", Text, nullable=True), # 562/1290 -> nullable
    Column("ESFERA_ORGAO_SANCIONADOR", Text, nullable=False),
    Column("FUNDAMENTACAO_LEGAL", Text, nullable=False),
    Column("DATA_ORIGEM_INFORMACAO", Date, nullable=False), # object likely holds date strings, map to Date
    Column("ORIGEM_INFORMACOES", Text, nullable=False),
    Column("OBSERVACOES", Text, nullable=True), # 291/1290 -> nullable)
    schema='dsa'
)

dsa_table = Table(
    "CEIS",  # Choose a suitable table name
    metadata,
    Column("id",
           autoincrement=True,
           unique=True,
           primary_key=True,
           nullable=False,
           type_=Integer
           ),
    Column("cadastro", Text, nullable=False), # 21125 non-null object -> Text
    Column("codigo_da_sancao", Integer, nullable=False), # 21125 non-null int64 -> Integer
    Column("tipo_de_pessoa", Text, nullable=True), # 21117 non-null object -> Text, nullable
    Column("cpf_ou_cnpj_do_sancionado", BigInteger, nullable=False), # 21125 non-null int64 -> BigInteger for potentially large numbers
    Column("nome_do_sancionado", Text, nullable=False), # 21125 non-null object -> Text
    Column("nome_informado_pelo_orgao_sancionador", Text, nullable=False), # 21125 non-null object -> Text
    Column("razao_social_cadastro_receita", Text, nullable=True), # 12508 non-null object -> Text, nullable
    Column("nome_fantasia_cadastro_receita", Text, nullable=True), # 8851 non-null object -> Text, nullable
    Column("numero_do_processo", Text, nullable=False), # 21125 non-null object -> Text
    Column("categoria_da_sancao", Text, nullable=False), # 21125 non-null object -> Text
    # Note: The dataframe output didn't explicitly show a 'VALOR_DA_MULTA'.
    # If it exists in the actual data and was just missed in the info() output,
    # you might need to add a Column('valor_da_multa', Numeric, nullable=...) here.
    Column("data_inicio_sancao", Date, nullable=False), # 21125 non-null object (likely date strings) -> Date
    Column("data_final_sancao", Date, nullable=True), # 19315 non-null object (likely date strings) -> Date, nullable
    Column("data_publicacao", Date, nullable=True), # 6891 non-null object (likely date strings) -> Date, nullable
    Column("publicacao", Text, nullable=False), # 21125 non-null object -> Text
    Column("detalhamento_do_meio_de_publicacao", Text, nullable=True), # 851 non-null object -> Text, nullable
    Column("data_do_transito_em_julgado", Date, nullable=True), # 10999 non-null object (likely date strings) -> Date, nullable
    Column("abrangencia_da_sancao", Text, nullable=False), # 21125 non-null object -> Text
    Column("orgao_sancionador", Text, nullable=False), # 21125 non-null object -> Text
    Column("uf_orgao_sancionador", Text, nullable=True), # 19858 non-null object -> Text, nullable
    Column("esfera_orgao_sancionador", Text, nullable=True), # 20423 non-null object -> Text, nullable (corrected based on count)
    Column("fundamentacao_legal", Text, nullable=False), # 21125 non-null object -> Text
    Column("data_origem_informacao", Date, nullable=False), # 21125 non-null object (likely date strings) -> Date
    Column("origem_informacoes", Text, nullable=True), # 21123 non-null object -> Text, nullable (corrected based on count)
    Column("observacoes", Text, nullable=True), # 9023 non-null object -> Text, nullable
    schema='dsa' # Specify the schema
)

dsa_nfe_items_table = Table(
    "dsa_nfe_items",  # Suitable table name indicating the data content
    metadata,
    Column("id",
           autoincrement=True,
           unique=True,
           primary_key=True,
           nullable=False,
           type_=Integer
           ),
    # Skipping 'Unnamed: 0' as it seems like an artifact index
    Column("chave_de_acesso", Text, nullable=False), # 211037 non-null object -> Text
    Column("modelo", Text, nullable=True), # 211037 non-null object -> Text
    Column("serie", Integer, nullable=True), # 211037 non-null int64 -> Integer
    Column("numero", BigInteger, nullable=True), # 211037 non-null int64 -> BigInteger for potentially large invoice numbers
    Column("natureza_da_operacao", Text, nullable=True), # 211036 non-null object -> Text, nullable
    Column("data_emissao", Date, nullable=True), # 211037 non-null object (likely date strings) -> Date
    Column("cpf_cnpj_emitente", Text, nullable=True), # 211037 non-null object -> Text (assuming it can be both CPF and CNPJ as string)
    Column("razao_social_emitente", Text, nullable=True), # 211037 non-null object -> Text
    Column("inscricao_estadual_emitente", BigInteger, nullable=True), # 211037 non-null int64 -> BigInteger
    Column("uf_emitente", Text, nullable=True), # 211037 non-null object -> Text
    Column("municipio_emitente", Text, nullable=True), # 211037 non-null object -> Text
    Column("cnpj_destinatario", Text, nullable=True), # 211037 non-null int64 -> BigInteger
    Column("nome_destinatario", Text, nullable=True), # 211037 non-null object -> Text
    Column("uf_destinatario", Text, nullable=True), # 211037 non-null object -> Text
    Column("indicador_ie_destinatario", Text, nullable=True), # 211037 non-null object -> Text
    Column("destino_da_operacao", Text, nullable=True), # 211037 non-null object -> Text
    Column("consumidor_final", Text, nullable=True), # 211037 non-null object -> Text
    Column("presenca_do_comprador", Text, nullable=True), # 211037 non-null object -> Text
    Column("numero_produto", Integer, nullable=True), # 211037 non-null int64 -> Integer
    Column("descricao_do_produto_servico", Text, nullable=True), # 211037 non-null object -> Text
    Column("codigo_ncm_sh", BigInteger, nullable=True), # 211037 non-null int64 -> BigInteger for NCM/SH codes
    Column("ncm_sh_tipo_de_produto", Text, nullable=True), # 199688 non-null object -> Text, nullable
    Column("cfop", Integer, nullable=True), # 211037 non-null int64 -> Integer
    Column("quantidade", Numeric, nullable=True), # 211037 non-null float64 -> Numeric for precision
    Column("unidade", Text, nullable=True), # 210740 non-null object -> Text, nullable
    Column("valor_unitario", Numeric, nullable=True), # 211037 non-null float64 -> Numeric for precision
    Column("valor_total", Numeric, nullable=True), # 211037 non-null float64 -> Numeric for precision
    schema='dsa' # Specify the schema
)


contratos_compras_table = Table(
    get_table_name('dicionario_tabela_contratos_compras'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número do Contrato"), Text, nullable=False),
    Column(to_snake_case("Objeto"), Text, nullable=True),
    Column(to_snake_case("Fundamento legal"), Text, nullable=True),
    Column(to_snake_case("Modalidade de compra"), Text, nullable=True),
    Column(to_snake_case("Situação Contrato"), Text, nullable=True),
    Column(to_snake_case("Código do Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Integer, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Integer, nullable=False),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Data Assinatura Contrato"), Date, nullable=True),
    Column(to_snake_case("Data Publicação DOU"), Date, nullable=True),
    Column(to_snake_case("Data Início da Vigência"), Date, nullable=True),
    Column(to_snake_case("Data Fim da Vigência"), Date, nullable=True),
    Column(to_snake_case("Código Contratado"), Text, nullable=True), 
    Column(to_snake_case("Nome Contratado"), Text, nullable=True),
    Column(to_snake_case("Valor Inicial da Compra"), Numeric, nullable=True),
    Column(to_snake_case("Valor Final da Compra"), Numeric, nullable=True),
    Column(to_snake_case("Número da Licitação"), Text, nullable=True),
    Column(to_snake_case("Código UG Licitação"), Text, nullable=True),
    Column(to_snake_case("Nome UG Licitação"), Text, nullable=True),
    Column(to_snake_case("Código Modalidade Compra Licitação"), Text, nullable=True),
    Column(to_snake_case("Modalidade Compra Licitação"), Text, nullable=True),
    schema='dsa'
)

contratos_item_compra_table = Table(
    get_table_name('dicionario_tabela_contratos_itemCompra'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Contrato"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Código Item Compra"), Text, nullable=True), # Structured 22-digit code
    Column(to_snake_case("Descrição Item Compra"), Text, nullable=True),
    Column(to_snake_case("Descrição Complementar Item Compra"), Text, nullable=True),
    Column(to_snake_case("Quantidade Item"), Text, nullable=True), 
    Column(to_snake_case("Valor Item"), Numeric, nullable=True),
    schema='dsa'
)

contratos_termo_aditivo_table = Table(
    get_table_name('dicionario_tabela_contratos_termoAditivo'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Contrato"), Text, nullable=True),
    Column(to_snake_case("Código do Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Número Termo Aditivo"), Text, nullable=True),
    Column(to_snake_case("Data Publicação"), Date, nullable=True),
    Column(to_snake_case("Objeto"), Text, nullable=True),
    schema='dsa'
)


licitacoes_empenhos_relacionados_table = Table(
    get_table_name('dicionario_tabela_licitacoes_EmpenhosRelacionados'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Licitação"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Código Modalidade de Compra"), Text, nullable=True),
    Column(to_snake_case("Modalidade compra"), Text, nullable=True),
    Column(to_snake_case("Número Processo"), Text, nullable=True),
    Column(to_snake_case("Código Empenho"), Text, nullable=True),
    Column(to_snake_case("Data Emissão Empenho"), Date, nullable=True),
    Column(to_snake_case("Observação Empenho"), Text, nullable=True),
    Column(to_snake_case("Valor Empenho (R$)"), Text, nullable=True),
    schema='dsa'
)


licitacoes_item_licitacao_table = Table(
    get_table_name('dicionario_tabela_licitacoes_ItemLicitacao'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Licitação"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Código Modalidade de Compra"), Text, nullable=True),
    Column(to_snake_case("Modalidade compra"), Text, nullable=True),
    Column(to_snake_case("Número Processo"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("Código Item Compra"), Text, nullable=True), # Structured 22-digit code
    Column(to_snake_case("Descrição"), Text, nullable=True),
    Column(to_snake_case("Quantidade Item"), Text, nullable=True), # Assuming integer quantity
    Column(to_snake_case("Valor Item"), Numeric, nullable=True),
    Column(to_snake_case("Código Vencedor"), Text, nullable=True), # CNPJ
    Column(to_snake_case("Nome Vencedor"), Text, nullable=True),
    schema='dsa'
)

licitacoes_licitacao_table = Table(
    get_table_name('dicionario_tabela_licitacoes_licitacao'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Licitação"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Código Modalidade de Compra"), Text, nullable=True),
    Column(to_snake_case("Modalidade compra"), Text, nullable=True),
    Column(to_snake_case("Número do Processo"), Text, nullable=True),
    Column(to_snake_case("Objeto"), Text, nullable=True),
    Column(to_snake_case("Situação Licitação"), Text, nullable=True),
    Column(to_snake_case("Código do Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão Superior"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("UF"), Text, nullable=True),
    Column(to_snake_case("Município"), Text, nullable=True),
    Column(to_snake_case("Data Resultado Compra"), Date, nullable=True),
    Column(to_snake_case("Data Abertura"), Date, nullable=True),
    Column(to_snake_case("Valor Licitação"), Numeric, nullable=True),
    schema='dsa'
)


licitacoes_participantes_licitacao_table = Table(
    get_table_name('dicionario_tabela_licitacoes_participantes_licitacao'),
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, nullable=False, unique=True),
    Column(to_snake_case("Número Licitação"), Text, nullable=True),
    Column(to_snake_case("Código UG"), Text, nullable=True),
    Column(to_snake_case("Nome UG"), Text, nullable=True),
    Column(to_snake_case("Código Modalidade de Compra"), Text, nullable=True),
    Column(to_snake_case("Modalidade compra"), Text, nullable=True),
    Column(to_snake_case("Número Processo"), Text, nullable=True),
    Column(to_snake_case("Código Órgão"), Text, nullable=True),
    Column(to_snake_case("Nome Órgão"), Text, nullable=True),
    Column(to_snake_case("Código Item Compra"), Text, nullable=True),
    Column(to_snake_case("Descrição Item Compra"), Text, nullable=True),
    Column(to_snake_case("CNPJ Participante"), Text, nullable=True), # CNPJ
    Column(to_snake_case("Nome Participante"), Text, nullable=True),
    Column(to_snake_case("Flag Vencedor"), Text, nullable=True), # "SIM" / "NÃO"
    schema='dsa'
)


metadata.create_all(engine)

# %%
