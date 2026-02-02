"""
ETL Centralized Logging Module
Database-backed logging for all ETL processes

Replaces file-based logging with AUDIT_BI.[log] tables
Provides structured, queryable logging with performance tracking
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any
from uuid import uuid4
import pyodbc


class AuditBILogger:
    """
    Logs ETL execution to AUDIT_BI.[log].[ETL_Execution_Log]
    
    Usage:
        logger = AuditBILogger(
            etl_name='06-LOADER_BOURSORAMA_FILE',
            sql_server='PERSO-AJE-DELL\\MSSQLSERVER01',
            database='AUDIT_BI'
        )
        
        execution_id = logger.start_execution(
            source_db='PostgreSQL',
            target_db='KHLWorldInvest',
            target_schema='bi_master_securities'
        )
        
        # Process files
        logger.log_file(
            file_name='action_2026-02-02_082947.csv',
            records_read=50,
            records_inserted=50,
            status='OK'
        )
        
        logger.end_execution(
            status='SUCCESS',
            total_files=80,
            total_read=185600,
            total_inserted=185600
        )
    """
    
    def __init__(self, 
                 etl_name: str,
                 sql_server: str = 'PERSO-AJE-DELL\\MSSQLSERVER01',
                 database: str = 'AUDIT_BI',
                 schema: str = 'log'):
        """Initialize ETL logger"""
        self.etl_name = etl_name
        self.sql_server = sql_server
        self.database = database
        self.schema = schema
        self.connection = None
        self.execution_id = None
        self.execution_log_id = None
        
        # Also setup console logger as fallback
        self.console_logger = self._setup_console_logger()
        
    def _setup_console_logger(self) -> logging.Logger:
        """Setup console logging as fallback"""
        logger = logging.getLogger(f'ETL_{self.etl_name}')
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _get_connection(self):
        """Get connection to AUDIT_BI database"""
        if not self.connection:
            try:
                self.connection = pyodbc.connect(
                    f'Driver={{ODBC Driver 17 for SQL Server}};'
                    f'Server={self.sql_server};'
                    f'Database={self.database};'
                    f'Trusted_Connection=yes;'
                )
            except pyodbc.Error as e:
                self.console_logger.error(f"Failed to connect to {self.database}: {e}")
                raise
        return self.connection
    
    def start_execution(self,
                       source_db: Optional[str] = None,
                       target_db: Optional[str] = None,
                       target_schema: Optional[str] = None) -> str:
        """
        Start ETL execution and create log record
        
        Returns: execution_id (UUID)
        """
        self.execution_id = str(uuid4())
        now = datetime.now(timezone.utc)
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Insert into ESMA_Load_Log table
            sql = f"""
            INSERT INTO [{self.schema}].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(sql, (
                self.etl_name,
                now,
                now,
                f"Execution started: {source_db} → {target_db}.{target_schema}",
                self.etl_name,
                'START'
            ))
            
            # Get inserted ID
            cursor.execute("SELECT @@IDENTITY")
            self.execution_log_id = cursor.fetchone()[0]
            conn.commit()
            
            self.console_logger.info(
                f"✅ Started execution {self.execution_id} "
                f"({source_db} → {target_db}.{target_schema})"
            )
            
        except pyodbc.Error as e:
            self.console_logger.error(f"Failed to start execution: {e}")
            # Don't raise - allow execution to continue
            return self.execution_id
        
        return self.execution_id
    
    def log_file(self,
                file_name: str,
                records_read: int,
                records_inserted: int,
                records_failed: int = 0,
                status: str = 'OK',
                error_message: Optional[str] = None,
                processing_time: Optional[float] = None):
        """Log individual file processing"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            now = datetime.now(timezone.utc)
            
            # Insert into ESMA_Load_Log table
            sql = f"""
            INSERT INTO [{self.schema}].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, StartTime, EndTime, Message, FileName, Element, Complement)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            end_time = now
            duration_msg = f" ({processing_time:.2f}s)" if processing_time else ""
            message = f"{status}: Read={records_read}, Inserted={records_inserted}, Failed={records_failed}{duration_msg}"
            if error_message:
                message += f" | Error: {error_message[:500]}"
            
            cursor.execute(sql, (
                self.etl_name,
                now,  # LaunchTimestamp
                now,  # StartTime
                end_time,  # EndTime
                message,
                file_name,
                status,
                f"Records: {records_read}/{records_inserted}/{records_failed}"
            ))
            conn.commit()
            
            # Log to console
            status_symbol = "✓" if status == "OK" else "✗"
            self.console_logger.info(
                f"{status_symbol} {file_name}: {status} - read={records_read}, "
                f"inserted={records_inserted}, failed={records_failed}"
            )
            
        except pyodbc.Error as e:
            self.console_logger.warning(f"Could not log file {file_name} to database: {e}")
    
    def end_execution(self,
                     status: str,
                     total_files: int,
                     total_read: int,
                     total_inserted: int,
                     total_failed: int = 0,
                     error_message: Optional[str] = None):
        """Complete ETL execution"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            now = datetime.now(timezone.utc)
            success_rate = (total_inserted / total_read * 100) if total_read > 0 else 0
            
            message = (
                f"Execution {status}: {total_files} files, "
                f"{total_inserted}/{total_read} records ({success_rate:.1f}% success)"
            )
            if error_message:
                message += f" | Error: {error_message[:500]}"
            
            # Insert final log entry
            sql = f"""
            INSERT INTO [{self.schema}].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, EndTime, Message, FileName, Element, Complement)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(sql, (
                self.etl_name,
                now,  # LaunchTimestamp
                now,  # EndTime
                message,
                f"FINAL_{self.etl_name}",
                'END',
                f"Files={total_files}, Read={total_read}, Inserted={total_inserted}, Failed={total_failed}"
            ))
            conn.commit()
            
            # Log to console
            self.console_logger.info(message)
            
        except pyodbc.Error as e:
            self.console_logger.warning(f"Could not end execution: {e}")
        finally:
            if self.connection:
                self.connection.close()
                self.connection = None
    
    def query_recent_executions(self, hours: int = 24, limit: int = 10) -> list:
        """Query recent executions for this ETL"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            sql = f"""
            SELECT TOP {limit} 
                ExecutionID, ExecutionStartTime, Status, 
                TotalRecordsRead, TotalRecordsInserted
            FROM [{self.schema}].[ETL_Execution_Log]
            WHERE ETL_Name = ?
            AND ExecutionStartTime >= DATEADD(HOUR, -{hours}, GETUTCDATE())
            ORDER BY ExecutionStartTime DESC
            """
            
            cursor.execute(sql, (self.etl_name,))
            return cursor.fetchall()
            
        except pyodbc.Error as e:
            self.console_logger.error(f"Failed to query executions: {e}")
            return []


# Backward compatibility: Create standard logger that uses AuditBILogger
def create_etl_logger(etl_name: str) -> AuditBILogger:
    """Factory function to create ETL logger"""
    return AuditBILogger(etl_name)
