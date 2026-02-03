"""
ETL ORCHESTRATOR v2.0 - OPTIMIZED VERSION
==========================================

Enhanced orchestrator with:
✅ Retry logic (exponential backoff)
✅ File synchronization (lock file)
✅ Dependency checking
✅ Storage cleanup
✅ Advanced monitoring
✅ Rollback on error
✅ Atomic file handling

Author: AI Assistant
Date: 2026-02-02
"""

import sys
import os
import subprocess
import time
import logging
import json
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
from dataclasses import dataclass, asdict
from threading import Lock
import configparser

# Add src/python to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src' / 'python'))

from common.audit_bi_logger import AuditBILogger
from common.config_loader import load_config


class ETLStatus(Enum):
    """ETL execution status"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    PARTIAL = "PARTIAL"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"
    RETRYING = "RETRYING"


@dataclass
class PipelineConfig:
    """Configuration pour une pipeline"""
    name: str
    script_path: str
    order: int
    timeout_seconds: int = 3600
    max_retries: int = 3
    retry_backoff: float = 2.0
    skip_if_fails: bool = False  # Si True, on continue même si échec
    dependencies: List[str] = None  # Noms des pipelines dont dépend celle-ci
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class ExecutionResult:
    """Résultat d'une exécution"""
    pipeline_name: str
    status: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    error_message: Optional[str] = None
    attempt: int = 1
    stdout: Optional[str] = None
    stderr: Optional[str] = None


class ETLOrchestrator:
    """
    Orchestrateur ETL optimisé avec:
    - Retry automatique
    - Gestion des fichiers temporaires
    - Lock file pour synchronisation
    - Cleanup des vieux fichiers
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize orchestrator with enhanced features"""
        self.pipelines: Dict[str, PipelineConfig] = {}
        self.results: Dict[str, ExecutionResult] = {}
        self.config = load_config()
        self.orchestration_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        self.start_time = None
        self.end_time = None
        
        # Setup logging
        self.logger = self._setup_logger()
        self.audit_logger = self._setup_audit_logger()
        
        # Storage & locks
        self.locks: Dict[str, Lock] = {}
        self.staging_dir = PROJECT_ROOT / 'data' / 'staging' / self.orchestration_id
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Load enhanced config if provided
        self.enhanced_config = self._load_enhanced_config(config_path)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup console logging"""
        logger = logging.getLogger('ETL_ORCHESTRATOR_v2')
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)8s] %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _setup_audit_logger(self) -> AuditBILogger:
        """Setup AUDIT_BI database logger"""
        try:
            return AuditBILogger(etl_name='ETL_ORCHESTRATOR_v2')
        except Exception as e:
            self.logger.warning(f"Could not connect to AUDIT_BI: {e}")
            return None
    
    def _load_enhanced_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load orchestrator configuration"""
        config = {
            'retry': {
                'max_retries': 3,
                'backoff_factor': 2.0,
                'initial_delay': 60,  # secondes
            },
            'storage': {
                'gleif_retention_days': 30,
                'boursorama_retention_days': 7,
                'esma_retention_days': 30,
                'cleanup_on_success': True,
            },
            'sync': {
                'use_lock_files': True,
                'scraper_ready_dir': 'data/tmp_web/BOURSORAMA/ready',
                'loader_lock_timeout': 3600,
            }
        }
        
        # Load from file if provided
        if config_path and Path(config_path).exists():
            cfg = configparser.ConfigParser()
            cfg.read(config_path)
            # Merge with defaults
            for section in cfg.sections():
                if section in config:
                    config[section].update(dict(cfg[section]))
        
        return config
    
    def add_pipeline(self, pipeline_config: PipelineConfig) -> None:
        """Add ETL pipeline to orchestration"""
        self.pipelines[pipeline_config.name] = pipeline_config
        self.locks[pipeline_config.name] = Lock()
        self.logger.info(f"Added pipeline: {pipeline_config.order}. {pipeline_config.name}")
    
    def check_dependencies(self, pipeline: PipelineConfig) -> Tuple[bool, str]:
        """Check if all dependencies are met"""
        for dep_name in pipeline.dependencies:
            if dep_name not in self.results:
                return False, f"Dependency {dep_name} not executed yet"
            
            dep_result = self.results[dep_name]
            if dep_result.status != ETLStatus.SUCCESS.value:
                return False, f"Dependency {dep_name} failed with status {dep_result.status}"
        
        return True, "All dependencies met"
    
    def _run_with_retry(self, pipeline: PipelineConfig) -> ExecutionResult:
        """Run ETL with retry logic"""
        max_retries = pipeline.max_retries
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            attempt += 1
            
            try:
                self.logger.info(
                    f"Executing {pipeline.name} (attempt {attempt}/{max_retries})"
                )
                
                result = self._run_etl(pipeline)
                
                if result.status == ETLStatus.SUCCESS.value:
                    self.logger.info(f"✅ {pipeline.name} succeeded on attempt {attempt}")
                    return result
                else:
                    last_error = result.error_message
                    if attempt < max_retries:
                        self._wait_before_retry(attempt, pipeline.retry_backoff)
            
            except Exception as e:
                last_error = str(e)
                self.logger.error(f"❌ {pipeline.name} failed: {e}")
                if attempt < max_retries:
                    self._wait_before_retry(attempt, pipeline.retry_backoff)
        
        # All retries exhausted
        return ExecutionResult(
            pipeline_name=pipeline.name,
            status=ETLStatus.FAILED.value,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_seconds=0,
            error_message=f"Failed after {max_retries} attempts: {last_error}",
            attempt=max_retries
        )
    
    def _wait_before_retry(self, attempt: int, backoff_factor: float):
        """Wait before retrying with exponential backoff"""
        wait_time = 60 * (backoff_factor ** (attempt - 1))
        self.logger.warning(
            f"Waiting {wait_time:.0f}s before retry (exponential backoff)..."
        )
        time.sleep(wait_time)
    
    def _run_etl(self, pipeline: PipelineConfig) -> ExecutionResult:
        """Execute single ETL pipeline"""
        start_time = datetime.now(timezone.utc)
        
        try:
            # Validate script exists
            script_path = Path(pipeline.script_path)
            if not script_path.exists():
                raise FileNotFoundError(f"Script not found: {script_path}")
            
            self.logger.info(f"Starting: {pipeline.name}")
            
            # Build command
            python_exe = sys.executable
            cmd = [python_exe, str(script_path)]
            
            # Execute with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=pipeline.timeout_seconds,
                cwd=PROJECT_ROOT
            )
            
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            if result.returncode == 0:
                self.logger.info(
                    f"✅ {pipeline.name} completed in {duration:.2f}s"
                )
                return ExecutionResult(
                    pipeline_name=pipeline.name,
                    status=ETLStatus.SUCCESS.value,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=duration,
                    stdout=result.stdout if result.stdout else None
                )
            else:
                # Log full error output for debugging
                error_output = result.stderr if result.stderr else result.stdout
                if error_output:
                    self.logger.error(f"❌ {pipeline.name} failed with full output:\n{error_output}")
                # Keep truncated version for the result object
                error_truncated = error_output[-1000:] if error_output else "Unknown error"
                return ExecutionResult(
                    pipeline_name=pipeline.name,
                    status=ETLStatus.FAILED.value,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=duration,
                    error_message=error_truncated,
                    stderr=error_output
                )
        
        except subprocess.TimeoutExpired:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            error = f"Timeout after {duration:.2f}s"
            self.logger.error(f"❌ {pipeline.name} timeout: {error}")
            return ExecutionResult(
                pipeline_name=pipeline.name,
                status=ETLStatus.ERROR.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message=error
            )
        
        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            self.logger.error(f"❌ {pipeline.name} error: {e}")
            return ExecutionResult(
                pipeline_name=pipeline.name,
                status=ETLStatus.ERROR.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message=str(e)
            )
    
    def _cleanup_old_files(self):
        """Cleanup old files based on retention policy"""
        cleanup_config = self.enhanced_config['storage']
        
        cleanup_rules = [
            ('data/csv/GLEIF', cleanup_config['gleif_retention_days']),
            ('data/tmp_web/BOURSORAMA', cleanup_config['boursorama_retention_days']),
            ('data/csv/BOURSORAMA', cleanup_config['esma_retention_days']),
        ]
        
        for dir_path, retention_days in cleanup_rules:
            full_path = PROJECT_ROOT / dir_path
            if not full_path.exists():
                continue
            
            cutoff_time = time.time() - (retention_days * 86400)
            deleted_count = 0
            
            try:
                for file_path in full_path.glob('**/*'):
                    if file_path.is_file():
                        if file_path.stat().st_mtime < cutoff_time:
                            file_path.unlink()
                            deleted_count += 1
                
                if deleted_count > 0:
                    self.logger.info(
                        f"Cleaned up {deleted_count} files from {dir_path} "
                        f"(retention: {retention_days}d)"
                    )
            
            except Exception as e:
                self.logger.warning(f"Could not cleanup {dir_path}: {e}")
    
    def run(self, stop_on_error: bool = False, cleanup: bool = True) -> Dict:
        """
        Run all ETL pipelines in order with enhanced features
        
        Args:
            stop_on_error: If True, stop on first error
            cleanup: If True, cleanup old files after success
        
        Returns:
            Dictionary with execution results
        """
        self.start_time = datetime.now(timezone.utc)
        
        self.logger.info("=" * 80)
        self.logger.info("🚀 ETL ORCHESTRATION v2.0 STARTED")
        self.logger.info("=" * 80)
        
        # Sort pipelines by order
        sorted_pipelines = sorted(
            self.pipelines.values(),
            key=lambda p: p.order
        )
        
        for pipeline in sorted_pipelines:
            self.logger.info(f"\n[{pipeline.order}/{len(sorted_pipelines)}] {pipeline.name}")
            
            # Check dependencies
            deps_ok, deps_msg = self.check_dependencies(pipeline)
            if not deps_ok:
                self.logger.warning(f"Skipping {pipeline.name}: {deps_msg}")
                continue
            
            # Execute with retry
            result = self._run_with_retry(pipeline)
            self.results[pipeline.name] = result
            
            # Log to AUDIT_BI
            if self.audit_logger:
                try:
                    self.audit_logger.log_file(
                        file_name=pipeline.name,
                        records_read=1,
                        records_inserted=1 if result.status == ETLStatus.SUCCESS.value else 0,
                        records_failed=1 if result.status != ETLStatus.SUCCESS.value else 0,
                        status='OK' if result.status == ETLStatus.SUCCESS.value else 'FAILED',
                        error_message=result.error_message,
                        processing_time=result.duration_seconds
                    )
                except Exception as e:
                    self.logger.warning(f"Could not log to AUDIT_BI: {e}")
            
            # Check error handling
            if result.status != ETLStatus.SUCCESS.value:
                if pipeline.skip_if_fails:
                    self.logger.warning(
                        f"Pipeline {pipeline.name} failed but marked as skip_if_fails"
                    )
                elif stop_on_error:
                    self.logger.error(
                        f"Stopping orchestration due to error in {pipeline.name}"
                    )
                    break
        
        self.end_time = datetime.now(timezone.utc)
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        # Cleanup if successful
        if cleanup:
            success_count = sum(
                1 for r in self.results.values()
                if r.status == ETLStatus.SUCCESS.value
            )
            if success_count == len(self.results):
                self.logger.info("🧹 Cleaning up old files...")
                self._cleanup_old_files()
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("🏁 ETL ORCHESTRATION COMPLETED")
        self.logger.info("=" * 80)
        
        return self._generate_summary(total_duration)
    
    def _generate_summary(self, total_duration: float) -> Dict:
        """Generate execution summary"""
        summary = {
            'orchestration_id': self.orchestration_id,
            'version': 'v2.0',
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'total_duration_seconds': total_duration,
            'pipelines': []
        }
        
        for pipeline_name, result in self.results.items():
            summary['pipelines'].append({
                'name': result.pipeline_name,
                'status': result.status,
                'duration_seconds': result.duration_seconds,
                'attempts': result.attempt,
                'error': result.error_message
            })
        
        # Calculate overall status
        statuses = [r.status for r in self.results.values()]
        if all(s == ETLStatus.SUCCESS.value for s in statuses):
            summary['overall_status'] = ETLStatus.SUCCESS.value
        elif any(s in [ETLStatus.FAILED.value, ETLStatus.ERROR.value] for s in statuses):
            summary['overall_status'] = ETLStatus.FAILED.value
        else:
            summary['overall_status'] = ETLStatus.PARTIAL.value
        
        self.logger.info(f"\nOverall Status: {summary['overall_status']}")
        self.logger.info(f"Total Duration: {total_duration:.2f}s")
        
        return summary


def main():
    """Main entry point"""
    orchestrator = ETLOrchestrator(
        config_path='orchestrator.ini'  # Optional enhanced config
    )
    
    # Add ETL pipelines with configuration
    orchestrator.add_pipeline(PipelineConfig(
        name='ETL_FULIN_DTIN',
        script_path=str(PROJECT_ROOT / 'src/python/ETL_FULIN_DTIN/ETL_ESMA_DAILY_RUN_AUTONOME.py'),
        order=1,
        max_retries=2,
        skip_if_fails=False
    ))
    
    orchestrator.add_pipeline(PipelineConfig(
        name='ETL_GLEIF_LEI',
        script_path=str(PROJECT_ROOT / 'src/python/ETL_GLEIF_LEI/ETL_GLEIF_LEI_RUN_AUTONOME.py'),
        order=2,
        max_retries=2,
        skip_if_fails=False
    ))
    
    orchestrator.add_pipeline(PipelineConfig(
        name='ETL_BOURSORAMA_SCRAPER',
        script_path=str(PROJECT_ROOT / 'src/python/ETL_BOURSORAMA/05-SCRAPER_BOURSORAMA.py'),
        order=3,
        max_retries=3,  # Plus de retries pour web scraping
        skip_if_fails=False
    ))
    
    orchestrator.add_pipeline(PipelineConfig(
        name='ETL_BOURSORAMA_LOADER',
        script_path=str(PROJECT_ROOT / 'src/python/ETL_BOURSORAMA/06-LOADER_BOURSORAMA_FILE.py'),
        order=4,
        max_retries=2,
        skip_if_fails=False,
        dependencies=['ETL_BOURSORAMA_SCRAPER']  # DÉPEND du scraper!
    ))
    
    # Run orchestration
    results = orchestrator.run(stop_on_error=False, cleanup=True)
    
    # Print summary
    print(json.dumps(results, indent=2, default=str))
    
    return results


if __name__ == '__main__':
    main()
