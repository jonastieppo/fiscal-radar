DROP TABLE IF EXISTS dsa.Empresas_Atividades_Secundarias;
DROP TABLE IF EXISTS dsa.Enderecos;
DROP TABLE IF EXISTS dsa.Contatos;
DROP TABLE IF EXISTS dsa.Empresas;
DROP TABLE IF EXISTS dsa.SituacoesEspeciais;
DROP TABLE IF EXISTS dsa.StatusCadastrais;
DROP TABLE IF EXISTS dsa.Atividades;
DROP TABLE IF EXISTS dsa.NaturezasJuridicas;
DROP TABLE IF EXISTS dsa.PortesEmpresa;


-- Tabela para armazenar as descrições dos portes de empresa
CREATE TABLE IF NOT EXISTS dsa.PortesEmpresa (
    id_porte_empresa VARCHAR(50) PRIMARY KEY,
    descricao VARCHAR(255) 
);

-- Tabela para armazenar as descrições da natureza jurídica
CREATE TABLE IF NOT EXISTS dsa.NaturezasJuridicas (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao VARCHAR(255) 
);

-- Tabela para armazenar os códigos e descrições das atividades (CNAE)
CREATE TABLE  IF NOT EXISTS dsa.Atividades (
    cnae VARCHAR(15) PRIMARY KEY,
    descricao VARCHAR(255) 
);

-- Tabela para armazenar os status cadastrais possíveis
CREATE TABLE  IF NOT EXISTS dsa.StatusCadastrais (
    status VARCHAR(50) PRIMARY KEY,
    descricao VARCHAR(255)
);

-- Tabela para armazenar os tipos de situação especial
CREATE TABLE IF NOT EXISTS dsa.SituacoesEspeciais (
    descricao VARCHAR(255) PRIMARY KEY
);

-- Tabela principal para as empresas
CREATE TABLE IF NOT EXISTS dsa.Empresas (
    cnpj VARCHAR(18) PRIMARY KEY,
    nome_empresarial VARCHAR(255) ,
    nome_fantasia VARCHAR(255),
    data_abertura DATE ,
    id_porte_empresa VARCHAR(50),
    codigo_natureza_juridica VARCHAR(10),
    cnae_principal VARCHAR(15),
    status_cadastral VARCHAR(50),
    data_status_cadastral DATE,
    motivo_status_cadastral VARCHAR(255),
    situacao_especial_descricao VARCHAR(255),
    data_situacao_especial DATE,
    
    -- Chaves Estrangeiras
    FOREIGN KEY (id_porte_empresa) REFERENCES dsa.PortesEmpresa (id_porte_empresa),
    FOREIGN KEY (codigo_natureza_juridica) REFERENCES dsa.NaturezasJuridicas (codigo),
    FOREIGN KEY (cnae_principal) REFERENCES dsa.Atividades (cnae),
    FOREIGN KEY (status_cadastral) REFERENCES dsa.StatusCadastrais (status),
    FOREIGN KEY (situacao_especial_descricao) REFERENCES dsa.SituacoesEspeciais (descricao)
);

-- Tabela para armazenar os dados de contato de cada empresa (relacionamento 1 para 1)
CREATE TABLE IF NOT EXISTS dsa.Contatos (
    id_contato SERIAL PRIMARY KEY,
    cnpj_empresa VARCHAR(18) UNIQUE, -- UNIQUE para garantir que cada empresa tenha apenas um contato
    email VARCHAR(255),
    telefone VARCHAR(100),
    
    FOREIGN KEY (cnpj_empresa) REFERENCES dsa.Empresas (cnpj)
);

-- Tabela para armazenar os dados de endereço de cada empresa (relacionamento 1 para 1)
CREATE TABLE IF NOT EXISTS dsa.Enderecos (
    id_endereco SERIAL PRIMARY KEY,
    cnpj_empresa VARCHAR(18) UNIQUE, -- UNIQUE para garantir que cada empresa tenha apenas um endereço
    logradouro VARCHAR(255) ,
    numero VARCHAR(50) ,
    complemento VARCHAR(255),
    cep VARCHAR(10),
    bairro_distrito VARCHAR(255),
    municipio VARCHAR(255),
    uf VARCHAR(2),
    
    FOREIGN KEY (cnpj_empresa) REFERENCES dsa.Empresas (cnpj)
);

-- Tabela de junção para a relação N-para-N (Empresas e Atividades Secundárias)
CREATE TABLE IF NOT EXISTS dsa.Empresas_Atividades_Secundarias (
    cnpj_empresa VARCHAR(18),
    cnae_secundaria VARCHAR(15),
    
    PRIMARY KEY (cnpj_empresa, cnae_secundaria), -- Chave primária composta
    FOREIGN KEY (cnpj_empresa) REFERENCES dsa.Empresas (cnpj),
    FOREIGN KEY (cnae_secundaria) REFERENCES dsa.Atividades (cnae)
);