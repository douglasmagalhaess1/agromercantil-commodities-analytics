-- Tabelas do pipeline Agromercantil Commodities Analytics

CREATE TABLE IF NOT EXISTS commodity (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(100) NOT NULL UNIQUE,
    unidade     VARCHAR(30) DEFAULT 'saca'
);

CREATE TABLE IF NOT EXISTS regiao (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(150) NOT NULL,
    uf          CHAR(2),
    macrorregiao VARCHAR(50),
    UNIQUE (nome, uf)
);

CREATE TABLE IF NOT EXISTS price_raw (
    id              SERIAL PRIMARY KEY,
    produto         VARCHAR(100) NOT NULL,
    regiao          VARCHAR(150),
    data_referencia DATE,
    preco           NUMERIC(12, 2) NOT NULL,
    unidade         VARCHAR(30) DEFAULT 'saca',
    data_coleta     TIMESTAMP DEFAULT NOW(),
    UNIQUE (produto, regiao, data_referencia, preco)
);

CREATE TABLE IF NOT EXISTS price_processed (
    id              SERIAL PRIMARY KEY,
    commodity_id    INTEGER REFERENCES commodity(id),
    region_id       INTEGER REFERENCES regiao(id),
    data_referencia DATE NOT NULL,
    preco           NUMERIC(12, 2) NOT NULL,
    volume          NUMERIC(14, 2),
    fonte           VARCHAR(50) DEFAULT 'CONAB',
    criado_em       TIMESTAMP DEFAULT NOW(),
    UNIQUE (commodity_id, region_id, data_referencia, preco)
);

CREATE TABLE IF NOT EXISTS price_curated (
    id              SERIAL PRIMARY KEY,
    produto         VARCHAR(100) NOT NULL,
    regiao          VARCHAR(150),
    preco_medio     NUMERIC(12, 2),
    preco_min       NUMERIC(12, 2),
    preco_max       NUMERIC(12, 2),
    qtd_registros   INTEGER,
    atualizado_em   TIMESTAMP DEFAULT NOW()
);
