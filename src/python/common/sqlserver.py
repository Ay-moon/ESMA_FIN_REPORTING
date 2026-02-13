"""Shared SQL Server connection helpers for ETL scripts."""

from __future__ import annotations

import configparser
import base64
import json
import os
import subprocess
from datetime import date, datetime
from typing import Optional

import pyodbc


def _as_yes_no(value: str, *, default: str) -> str:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return "yes"
    if normalized in {"0", "false", "no", "n", "off"}:
        return "no"
    return default


def _config_get(cfg: configparser.ConfigParser, key: str, default: str = "") -> str:
    return cfg["SQLSERVER"].get(key, default).strip()


def _resolve_server(cfg: configparser.ConfigParser) -> str:
    server = _config_get(cfg, "server")
    if not server:
        raise KeyError("Missing SQLSERVER.server in config")
    port = _config_get(cfg, "port")
    if port and "," not in server and "\\" not in server:
        return f"{server},{port}"
    return server


def _resolve_database(cfg: configparser.ConfigParser, database: Optional[str]) -> str:
    if database:
        return database
    db = _config_get(cfg, "database_stg") or _config_get(cfg, "database")
    if not db:
        raise KeyError("Missing SQLSERVER.database_stg (or SQLSERVER.database) in config")
    return db


def build_connection_string(
    cfg: configparser.ConfigParser,
    *,
    database: Optional[str] = None,
    encrypt: Optional[str] = None,
    trust_server_certificate: Optional[str] = None,
) -> str:
    if "SQLSERVER" not in cfg:
        raise KeyError("Missing [SQLSERVER] section in config")

    driver = _config_get(cfg, "driver", "ODBC Driver 17 for SQL Server")
    user = _config_get(cfg, "user")
    password = _config_get(cfg, "password")
    if not user or not password:
        raise KeyError("Missing SQLSERVER.user and/or SQLSERVER.password in config")

    resolved_encrypt = _as_yes_no(
        encrypt if encrypt is not None else _config_get(cfg, "encrypt", "no"),
        default="no",
    )
    resolved_trust = _as_yes_no(
        trust_server_certificate
        if trust_server_certificate is not None
        else _config_get(cfg, "trust_server_certificate", "yes"),
        default="yes",
    )

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={_resolve_server(cfg)}",
        f"DATABASE={_resolve_database(cfg, database)}",
        f"UID={user}",
        f"PWD={password}",
        f"Encrypt={resolved_encrypt}",
        f"TrustServerCertificate={resolved_trust}",
    ]

    timeout = _config_get(cfg, "connection_timeout")
    if timeout:
        parts.append(f"Connection Timeout={timeout}")

    return ";".join(parts) + ";"


def connect(
    cfg: configparser.ConfigParser,
    *,
    database: Optional[str] = None,
    autocommit: bool = True,
) -> pyodbc.Connection:
    conn_str = build_connection_string(cfg, database=database)
    try:
        return pyodbc.connect(conn_str, autocommit=autocommit)
    except pyodbc.Error as exc:
        msg = str(exc).lower()
        tls_error = (
            "chiffrement non pris en charge" in msg
            or "fournisseur ssl" in msg
            or "ssl provider" in msg
            or "tls" in msg
            or "seccreatecredentials" in msg
        )
        if not tls_error:
            raise
        return _SqlClientConnection(cfg, database=_resolve_database(cfg, database))


def _sql_quote(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "N'" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    if isinstance(value, date):
        return "N'" + value.strftime("%Y-%m-%d") + "'"
    s = str(value).replace("'", "''")
    return f"N'{s}'"


def _bind_params(sql: str, params) -> str:
    if params is None:
        return sql
    if not isinstance(params, (list, tuple)):
        params = [params]
    parts = sql.split("?")
    expected = len(parts) - 1
    if expected != len(params):
        raise ValueError(f"Parameter mismatch: expected {expected}, got {len(params)}")
    out = parts[0]
    for idx, value in enumerate(params):
        out += _sql_quote(value) + parts[idx + 1]
    return out


def _run_sql_via_powershell(
    *,
    server: str,
    user: str,
    password: str,
    database: str,
    sql: str,
    timeout_sec: int = 180,
):
    script = r"""
$ErrorActionPreference = 'Stop'
$server = $env:ESMA_SQL_SERVER
$user = $env:ESMA_SQL_USER
$password = $env:ESMA_SQL_PASSWORD
$database = $env:ESMA_SQL_DATABASE
$sqlText = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($env:ESMA_SQL_B64))

$builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
$builder['Data Source'] = $server
$builder['Initial Catalog'] = $database
$builder['User ID'] = $user
$builder['Password'] = $password
$builder['Encrypt'] = $false
$builder['TrustServerCertificate'] = $true
$builder['Connect Timeout'] = 30

$conn = New-Object System.Data.SqlClient.SqlConnection $builder.ConnectionString
$conn.Open()
try {
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sqlText
    $cmd.CommandTimeout = 180

    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
    $ds = New-Object System.Data.DataSet
    $null = $adapter.Fill($ds)

    if ($ds.Tables.Count -eq 0) {
        '{"columns":[],"rows":[]}'
        exit 0
    }

    $table = $ds.Tables[0]
    $columns = @()
    foreach ($c in $table.Columns) { $columns += $c.ColumnName }

    $rows = @()
    foreach ($row in $table.Rows) {
        $vals = @()
        foreach ($c in $table.Columns) {
            $v = $row[$c]
            if ($null -eq $v -or $v -is [System.DBNull]) {
                $vals += $null
            } elseif ($v -is [DateTime]) {
                $vals += $v.ToString('yyyy-MM-dd HH:mm:ss')
            } else {
                $vals += $v
            }
        }
        $rows += ,$vals
    }

    $obj = @{ columns = $columns; rows = $rows }
    $obj | ConvertTo-Json -Compress -Depth 6
}
finally {
    $conn.Close()
}
"""
    env = os.environ.copy()
    env["ESMA_SQL_SERVER"] = server
    env["ESMA_SQL_USER"] = user
    env["ESMA_SQL_PASSWORD"] = password
    env["ESMA_SQL_DATABASE"] = database
    env["ESMA_SQL_B64"] = base64.b64encode(sql.encode("utf-8")).decode("ascii")

    proc = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        env=env,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(f"SqlClient execution failed: {stderr or stdout}")
    raw = (proc.stdout or "").strip()
    if not raw:
        return []
    payload = raw.splitlines()[-1].strip()
    data = json.loads(payload)
    rows = data.get("rows", [])
    return [tuple(row) for row in rows]


class _SqlClientCursor:
    def __init__(self, conn: "_SqlClientConnection"):
        self._conn = conn
        self._rows = []
        self._idx = 0

    def execute(self, sql: str, params=None):
        final_sql = _bind_params(sql, params)
        self._rows = _run_sql_via_powershell(
            server=self._conn.server,
            user=self._conn.user,
            password=self._conn.password,
            database=self._conn.database,
            sql=final_sql,
        )
        self._idx = 0
        return self

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def close(self):
        return None


class _SqlClientConnection:
    def __init__(self, cfg: configparser.ConfigParser, database: str):
        self.server = _resolve_server(cfg)
        self.user = _config_get(cfg, "user")
        self.password = _config_get(cfg, "password")
        self.database = database

    def cursor(self):
        return _SqlClientCursor(self)

    def close(self):
        return None
