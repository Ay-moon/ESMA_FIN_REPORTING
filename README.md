# ESMA ETL & Analytics Platform

Professional European Securities and Markets Authority (ESMA) regulatory reporting and financial analytics platform.

**Status:** Production (v1.0.0) | **Last Updated:** 2026-02-02 | **Python:** 3.13 | **SQL Server:** 2022+

## 🎯 Quick Start

```bash
# Setup
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
cp config/config.template.ini config/config.ini

# Run daily ETL
python src/python/ETL_FULIN_DTIN/ETL_ESMA_DAILY_RUN_AUTONOME.py

# Check database logs
# SELECT * FROM AUDIT_BI.[log].[ETL_Execution_Log] ORDER BY ExecutionStartTime DESC
```

## 🏗️ Architecture

**Core Components:**
- **Databases:** SQL Server (KHLWorldInvest, DWH_KHLWorldInvest, AUDIT_BI)
- **ETL Pipelines:** ESMA FIRDS, Boursorama, GLEIF LEI
- **Analytics:** PowerBI Dashboard (EU_FIN_OVERVIEW)
- **Logging:** Centralized AUDIT_BI database (replaces file logs)
- **Versioning:** DACPAC professional database projects

## 📊 Pipelines

1. **ESMA FIRDS** - Daily regulatory instrument data
2. **Boursorama** - Market data scraping & loading (PostgreSQL → SQL Server)
3. **GLEIF LEI** - Issuer reference data (monthly)

## 🗄️ Database

- **Versioning:** DACPAC projects (no raw SQL scripts)
- **Logging:** AUDIT_BI.[log].[ETL_Execution_Log] (all executions)
- **Deployment:** Automated with PowerShell scripts
- **Recovery (new SQL instance):** `src/sql/00_REBUILD_SQLSERVER_INSTANCE.sql` (sqlcmd bootstrap)

## 📖 Documentation

- [DACPAC Versioning](src/sql/DACPAC_VERSIONING_GUIDE.md)
- [ETL Standards](doc/STANDARDS.md)
- [Architecture](doc/Spec_ESMA/)

---

## Global Architecture (Logical View)

```
Sources
  │
  ├─ GLEIF (LEI reference data)
  ├─ Boursorama (market prices & instruments)
  │
Ingestion Layer
  │
  ├─ Web scraping / file download
  ├─ Controlled extraction & validation
  │
Processing Layer
  │
  ├─ Normalization & mapping
  ├─ Data quality checks
  ├─ Business‑ready structures
  │
Storage Layer
  │
  ├─ SQL Server (reference / regulatory staging)
  ├─ PostgreSQL (market data staging)
  │
Consumption
  │
  ├─ BI / Power BI
  ├─ Master Data / Issuer repositories
```

---

## GLEIF LEI – Autonomous ETL Pipeline

### Purpose

Autonomous ingestion of **GLEIF LEI Golden Copy** datasets to build a **trusted issuer reference layer**, aligned with regulatory and master‑data use cases.

### Data Scope

* LEI Golden Copy
* LEI Relationships
* Reporting Exceptions

### Workflow

```
01 → Download & Extract
02 → Normalize / Filter
03 → Load to SQL Server
```

### Key Features

* Automatic detection of latest GLEIF publications
* Controlled column mapping via configuration file
* Fail‑fast schema validation
* Full refresh load strategy (truncate & reload)
* Centralized execution logging

### Target Tables (SQL Server)

* `STG_LEI_CDF_GOLDEN`
* `STG_LEI_RELATION`
* `STG_LEI_REPORTING_EXCEPTION`

### Logging

All executions are logged in:

```
[log].[ESMA_Load_Log]
```

with timestamps, status and error details.

---

## Boursorama Market Data – Scraper & Loader Pipeline

### Purpose

Collection and structuring of **market prices and instrument attributes** from Boursorama, for analytical and BI usage.

### Data Scope

* Turbos
* Warrants
* Leveraged products
* Equities

### Workflow

```
01 → Web Scraping (HTML → CSV)
02 → SQL Loading & Archiving
```

### Scraper (05‑SCRAPER_BOURSORAMA.py)

* Parallel scraping with throttling
* Robust retry & pagination handling
* Table detection & normalization
* ISIN extraction & data cleansing
* CSV output generation

### Loader (06‑LOADER_BOURSORAMA_FILE.py)

* Automatic file discovery
* Type‑safe transformations
* Batch inserts into PostgreSQL
* Transactional control & rollback
* File archiving after successful load

### Target Table (PostgreSQL)

```
stg_bourso_price_history
```

---

## Configuration & Standards

### Configuration

All pipelines rely on a **shared configuration loader**:

* `config/config.ini` (local, ignored)
* `config/config.template.ini` (versioned)
* Environment variable override supported

### Logging Principles

* Centralized SQL logging
* START / SUCCESS / FAILED states
* Script‑level traceability
* Row counts and error messages

### Design Principles

* Autonomous execution
* Separation between raw, processed and loaded data
* Strong data quality & governance
* Production‑grade error handling
* Reusable standards across pipelines

---

## Typical Use Cases

* Issuer master data enrichment
* Regulatory & compliance reporting
* Market data analytics
* Power BI dashboards
* Data quality & lineage analysis

---

## Disclaimer

This repository contains **technical demonstrators and non‑confidential data pipelines only**.
No credentials, client data or proprietary datasets are included.
