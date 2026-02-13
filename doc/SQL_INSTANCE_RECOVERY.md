# Rebuild SQL Server instance (ESMA_FIN_REPORTING)

Ce guide sert quand l'instance SQL a été recréée et qu'il faut remettre les objets attendus par les scripts Python ETL.

## 1) Objets attendus côté Python / SQL

### Config utilisée
- Tous les scripts chargent `config/config.ini` (ou fallback `config/config.template.ini`) via `common.config_loader.load_config()`.  
- Les clés SQL utilisées sont principalement : `server`, `database_stg`, `database_dwh`, `user`, `password`, `driver`, `schema_stg`, `schema_log`.

### Dépendances SQL critiques repérées
- Table de logs : `log.ESMA_Load_Log` (écrite par les ETL Python et les procédures SQL).
- Procédure STG appelée par l'ETL : `stg.usp_Run_Daily_stg_Load`.
- Procédure DWH appelée par l'ETL : `mart.usp_Run_Daily_Mart_Load`.
- Tables STG chargées en BULK : `stg.ESMA_FULINS_WIDE`, `stg.ESMA_DLTINS_WIDE`.

## 2) Nouveau script de rebuild proposé

Script ajouté : `src/sql/00_REBUILD_SQLSERVER_INSTANCE.sql`

Ce script:
1. Crée les bases si absentes : `AUDIT_BI`, `KHLWorldInvest`, `DWH_KHLWorldInvest`.
2. Crée les schémas nécessaires (`log`, `stg`, `mart`) si absents.
3. Rejoue les scripts SQL versionnés déjà présents dans le dépôt via `:r` (mode `sqlcmd`).

## 3) Exécution

Depuis `src/sql`:

```bash
sqlcmd -S <SERVER> -E -i 00_REBUILD_SQLSERVER_INSTANCE.sql
```

ou en SQL auth:

```bash
sqlcmd -S <SERVER> -U <USER> -P <PASSWORD> -i 00_REBUILD_SQLSERVER_INSTANCE.sql
```

Optionnel: surcharge des noms de DB:

```bash
sqlcmd -S <SERVER> -E \
  -v STG_DB="KHLWorldInvest" DWH_DB="DWH_KHLWorldInvest" AUDIT_DB="AUDIT_BI" \
  -i 00_REBUILD_SQLSERVER_INSTANCE.sql
```

## 4) Vérifications post-install

```sql
SELECT DB_NAME();
SELECT TOP 20 * FROM AUDIT_BI.log.ESMA_Load_Log ORDER BY LogID DESC;
SELECT OBJECT_ID('KHLWorldInvest.stg.usp_Run_Daily_stg_Load') AS ProcStg;
SELECT OBJECT_ID('DWH_KHLWorldInvest.mart.usp_Run_Daily_Mart_Load') AS ProcMart;
```

## 5) Points d'attention repérés pendant l'analyse

- `common/audit_bi_logger.py` utilise par défaut une connexion `Trusted_Connection=yes` et des valeurs hardcodées (`AUDIT_BI`, serveur par défaut). En environnement multi-poste, il faut soit passer explicitement `sql_server/database`, soit l'aligner avec `config.ini`.
- `src/sql/AUDIT_BI_Views.sql` contient actuellement des `CREATE TABLE` (pas des `VIEW`) et semble dupliqué avec `AUDIT_BI_Tables.sql`. Ce fichier ne doit pas être utilisé pour l'initialisation.
