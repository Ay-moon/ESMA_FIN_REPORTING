--
-- PostgreSQL database dump
--

-- Dumped from database version 14.15
-- Dumped by pg_dump version 14.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: bi_master_securities; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA bi_master_securities;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: finance_load_log; Type: TABLE; Schema: bi_master_securities; Owner: -
--

CREATE TABLE bi_master_securities.finance_load_log (
    load_log_id bigint NOT NULL,
    process_name text NOT NULL,
    script_name text,
    source_system text DEFAULT 'BOURSORAMA'::text NOT NULL,
    target_table text DEFAULT 'bi_master_securities.stg_bourso_price_history'::text NOT NULL,
    file_name text,
    source_url text,
    produit_type text,
    snapshot_ts timestamp with time zone,
    load_start_ts timestamp with time zone DEFAULT now() NOT NULL,
    load_end_ts timestamp with time zone,
    status text DEFAULT 'STARTED'::text NOT NULL,
    rows_read bigint,
    rows_inserted bigint,
    rows_rejected bigint,
    error_message text,
    checksum text,
    params jsonb,
    extra jsonb
);


--
-- Name: finance_load_log_load_log_id_seq; Type: SEQUENCE; Schema: bi_master_securities; Owner: -
--

CREATE SEQUENCE bi_master_securities.finance_load_log_load_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finance_load_log_load_log_id_seq; Type: SEQUENCE OWNED BY; Schema: bi_master_securities; Owner: -
--

ALTER SEQUENCE bi_master_securities.finance_load_log_load_log_id_seq OWNED BY bi_master_securities.finance_load_log.load_log_id;


--
-- Name: stg_bourso_price_history; Type: TABLE; Schema: bi_master_securities; Owner: -
--

CREATE TABLE bi_master_securities.stg_bourso_price_history (
    stg_id bigint NOT NULL,
    isin text,
    libelle text,
    produit text,
    produit_type text NOT NULL,
    source_url text,
    date_extraction timestamp with time zone,
    dernier text,
    variation text,
    ouverture text,
    plus_haut text,
    plus_bas text,
    var_1janv text,
    volume text,
    achat text,
    vente text,
    sous_jacent text,
    ss_jacent text,
    borne_basse text,
    borne_haute text,
    barriere text,
    levier text,
    prix_exercice text,
    maturite text,
    delta text,
    emetteurs text,
    load_ts timestamp with time zone DEFAULT now() NOT NULL,
    file_name text NOT NULL
);


--
-- Name: stg_bourso_price_history_stg_id_seq; Type: SEQUENCE; Schema: bi_master_securities; Owner: -
--

CREATE SEQUENCE bi_master_securities.stg_bourso_price_history_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_bourso_price_history_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: bi_master_securities; Owner: -
--

ALTER SEQUENCE bi_master_securities.stg_bourso_price_history_stg_id_seq OWNED BY bi_master_securities.stg_bourso_price_history.stg_id;


--
-- Name: finance_load_log load_log_id; Type: DEFAULT; Schema: bi_master_securities; Owner: -
--

ALTER TABLE ONLY bi_master_securities.finance_load_log ALTER COLUMN load_log_id SET DEFAULT nextval('bi_master_securities.finance_load_log_load_log_id_seq'::regclass);


--
-- Name: stg_bourso_price_history stg_id; Type: DEFAULT; Schema: bi_master_securities; Owner: -
--

ALTER TABLE ONLY bi_master_securities.stg_bourso_price_history ALTER COLUMN stg_id SET DEFAULT nextval('bi_master_securities.stg_bourso_price_history_stg_id_seq'::regclass);


--
-- Name: finance_load_log finance_load_log_pkey; Type: CONSTRAINT; Schema: bi_master_securities; Owner: -
--

ALTER TABLE ONLY bi_master_securities.finance_load_log
    ADD CONSTRAINT finance_load_log_pkey PRIMARY KEY (load_log_id);


--
-- Name: stg_bourso_price_history stg_bourso_price_history_pkey; Type: CONSTRAINT; Schema: bi_master_securities; Owner: -
--

ALTER TABLE ONLY bi_master_securities.stg_bourso_price_history
    ADD CONSTRAINT stg_bourso_price_history_pkey PRIMARY KEY (stg_id);


--
-- Name: ix_finance_load_log_file; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_finance_load_log_file ON bi_master_securities.finance_load_log USING btree (file_name);


--
-- Name: ix_finance_load_log_prodtype; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_finance_load_log_prodtype ON bi_master_securities.finance_load_log USING btree (produit_type, snapshot_ts);


--
-- Name: ix_finance_load_log_status; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_finance_load_log_status ON bi_master_securities.finance_load_log USING btree (status, load_start_ts);


--
-- Name: ix_stg_bourso_isin; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_stg_bourso_isin ON bi_master_securities.stg_bourso_price_history USING btree (isin);


--
-- Name: ix_stg_bourso_loadts; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_stg_bourso_loadts ON bi_master_securities.stg_bourso_price_history USING btree (load_ts);


--
-- Name: ix_stg_bourso_prodtype_extr; Type: INDEX; Schema: bi_master_securities; Owner: -
--

CREATE INDEX ix_stg_bourso_prodtype_extr ON bi_master_securities.stg_bourso_price_history USING btree (produit_type, date_extraction);


--
-- PostgreSQL database dump complete
--

