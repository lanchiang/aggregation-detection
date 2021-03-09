-- \set AUTOCOMMIT = ON;

-- Database: aggregation detection

DO $$
BEGIN
  CREATE ROLE aggrdet WITH PASSWORD '123456' CREATEDB LOGIN;
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'not creating role my_role -- it already exists';
END
$$;

DROP DATABASE IF EXISTS aggrdet;

CREATE DATABASE aggrdet
    WITH
    OWNER = aggrdet
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\c aggrdet

-- fact tables

-- Table: dataset

CREATE TABLE dataset
(
    id SERIAL PRIMARY KEY,
    name text COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE dataset
    OWNER to aggrdet;

CREATE TABLE file
(
    id SERIAL PRIMARY KEY,
    dataset_id integer NOT NULL,
    file_name text NOT NULL,
    sheet_name text NOT NULL,
    number_format text NOT NULL,
    content text[][] NOT NULL,
    CONSTRAINT fk_dataset FOREIGN KEY(dataset_id) REFERENCES dataset(id)
)

TABLESPACE pg_default;

ALTER TABLE file OWNER TO aggrdet;

CREATE TABLE aggregation_type
(
    id SERIAL PRIMARY KEY,
    name text COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

INSERT INTO aggregation_type (name) VALUES ('Sum'), ('Subtract'), ('Average'), ('Percentage'), ('All');

ALTER TABLE aggregation_type OWNER TO aggrdet;

CREATE TYPE cell_index AS
(
    r_index integer,
    c_index integer
);

CREATE TABLE cell
(
    id SERIAL PRIMARY KEY,
    file_id integer NOT NULL,
    position cell_index NOT NULL,
    value text NOT NULL,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id)
)

TABLESPACE pg_default;

ALTER TABLE cell OWNER TO aggrdet;

CREATE TABLE aggregation
(
    id SERIAL PRIMARY KEY,
    file_id integer NOT NULL,
    aggregator cell_index NOT NULL,
    aggregatees cell_index[] NOT NULL,
    operator integer NOT NULL,
    error_level real NOT NULL,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id),
    CONSTRAINT fk_operator FOREIGN KEY(operator) REFERENCES aggregation_type(id)
)

TABLESPACE pg_default;

ALTER TABLE aggregation OWNER TO aggrdet;

-- dimensional tables

-- Table: experiment

CREATE TABLE experiment
(
    id SERIAL PRIMARY KEY,
    algorithm text NOT NULL,
    dataset_id integer NOT NULL,
    error_level decimal NOT NULL,
    error_strategy text COLLATE pg_catalog."default",
    extended_strategy boolean,
    timeout integer NOT NULL,
    precision real NOT NULL,
    recall real NOT NULL,
    f1 real NOT NULL,
    exec_time real NOT NULL, -- runtime for the whole dataset, might not be useful, nevertheless record it.
    CONSTRAINT fk_fileset_id FOREIGN KEY(dataset_id) REFERENCES dataset(id)
)

TABLESPACE pg_default;

ALTER TABLE experiment OWNER TO aggrdet;

-- Table: prediction

CREATE TABLE prediction
(
    id SERIAL PRIMARY KEY,
    experiment_id integer NOT NULL,
    file_id integer NOT NULL,
    tp_count integer not null,
    fn_count integer not null,
    fp_count integer not null,
    exec_time real NOT NULL,
    true_positives json not null,
    false_negatives json not null,
    false_positives json not null,
    CONSTRAINT fk_experiment_id FOREIGN KEY(experiment_id) REFERENCES experiment(id),
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id)
)

TABLESPACE pg_default;

ALTER TABLE prediction OWNER TO aggrdet;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aggrdet;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO aggrdet;

--` \set AUTOCOMMIT = OFF;