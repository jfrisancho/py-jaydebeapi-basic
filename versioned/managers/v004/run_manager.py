# managers/run.py

import uuid
from datetime import datetime
from typing import Optional

from db import Database
from string_helper import StringHelper
from .random import RandomManager
from .path import PathManager
from .coverage import CoverageManager
from .validation import ValidationManager

class RunManager:
    """Main execution orchestration for analysis runs."""
    
    def __init__(self, db: Database):
        self.db = db
        self.random_manager = RandomManager(db)
        self.path_manager = PathManager(db)
        self.coverage_manager = CoverageManager(db)
        self.validation_manager = ValidationManager(db)
    
    def execute_random_run(self, config: 'RandomRunConfig') -> str:
        """Execute a random sampling run."""
        run_id = str(uuid.uuid4())
        
        # Create run record
        self._create_run_record(run_id, config)
        
        try:
            # Update run status
            self._update_run_status(run_id, 'RUNNING')
            
            # Initialize coverage scope
            coverage_scope = self.coverage_manager.initialize_coverage_scope(
                fab_no=config.fab_no,
                phase_no=config.phase_no,
                model_no=config.model_no,
                e2e_group_no=config.e2e_group_no
            )
            
            # Execute random sampling
            sampling_results = self.random_manager.execute_sampling(
                run_id=run_id,
                config=config,
                coverage_scope=coverage_scope
            )
            
            # Update run status after sampling
            self._update_run_status(run_id, 'SAMPLING_COMPLETED')
            
            # Validate all found paths
            validation_results = self.validation_manager.validate_run_paths(run_id)
            
            # Generate final summary
            summary = self._generate_run_summary(run_id, sampling_results, validation_results)
            self._store_run_summary(summary)
            
            # Update final run status
            final_status = 'COMPLETED' if summary.completion_status.value == 'COMPLETED' else 'PARTIAL'
            self._update_run_status(run_id, final_status, datetime.now())
            
            return run_id
            
        except Exception as e:
            self._update_run_status(run_id, 'FAILED', datetime.now())
            raise RuntimeError(f'Run {run_id} failed: {e}')
    
    def _create_run_record(self, run_id: str, config: 'RandomRunConfig'):
        """Create initial run record in database."""
        sql = '''
            INSERT INTO tb_runs (
                id, date, approach, method, coverage_target,
                fab_no, phase_no, model_no, toolset,
                tag, status, execution_mode, run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            StringHelper.date_to_sqldate(datetime.now().date()),
            'RANDOM',
            'SIMPLE',
            config.coverage_target,
            config.fab_no,
            config.phase_no,
            config.model_no,
            config.toolset,
            config.tag,
            'INITIALIZED',
            'DEFAULT',
            StringHelper.datetime_to_sqltimestamp(datetime.now())
        ]
        
        self.db.update(sql, params)
    
    def _update_run_status(self, run_id: str, status: str, ended_at: Optional[datetime] = None):
        """Update run status and optionally end time."""
        if ended_at:
            sql = 'UPDATE tb_runs SET status = ?, ended_at = ? WHERE id = ?'
            params = [status, StringHelper.datetime_to_sqltimestamp(ended_at), run_id]
        else:
            sql = 'UPDATE tb_runs SET status = ? WHERE id = ?'
            params = [status, run_id]
        
        self.db.update(sql, params)
    
    def _generate_run_summary(self, run_id: str, sampling_results: dict, validation_results: dict) -> 'RunSummary':
        """Generate comprehensive run summary."""
        from dataclasses import dataclass
        
        # Fetch basic metrics
        path_metrics = self._fetch_run_path_metrics(run_id)
        coverage_metrics = self.coverage_manager.fetch_coverage_metrics(run_id)
        error_metrics = self._fetch_run_error_metrics(run_id)
        
        # Calculate derived metrics
        success_rate = None
        if sampling_results.get('total_attempts', 0) > 0:
            success_rate = path_metrics['paths_found'] / sampling_results['total_attempts']
        
        coverage_efficiency = None
        if coverage_metrics.get('target_coverage') and coverage_metrics.get('target_coverage') > 0:
            coverage_efficiency = coverage_metrics.get('achieved_coverage', 0) / coverage_metrics['target_coverage']
        
        # Determine completion status
        completion_status = self._determine_completion_status(
            coverage_metrics.get('achieved_coverage', 0),
            coverage_metrics.get('target_coverage', 0),
            error_metrics.get('critical_errors', 0)
        )
        
        return RunSummary(
            run_id=run_id,
            total_attempts=sampling_results.get('total_attempts', 0),
            total_paths_found=path_metrics.get('paths_found', 0),
            unique_paths=path_metrics.get('unique_paths', 0),
            target_coverage=coverage_metrics.get('target_coverage'),
            achieved_coverage=coverage_metrics.get('achieved_coverage'),
            coverage_efficiency=coverage_efficiency,
            total_errors=error_metrics.get('total_errors', 0),
            total_review_flags=error_metrics.get('total_flags', 0),
            critical_errors=error_metrics.get('critical_errors', 0),
            total_nodes=path_metrics.get('total_nodes', 0),
            total_links=path_metrics.get('total_links', 0),
            avg_path_nodes=path_metrics.get('avg_nodes'),
            avg_path_links=path_metrics.get('avg_links'),
            avg_path_length=path_metrics.get('avg_length'),
            success_rate=success_rate,
            completion_status=completion_status,
            execution_time_mm=self._calculate_execution_time(run_id),
            started_at=self._fetch_run_start_time(run_id),
            ended_at=datetime.now(),
            summarized_at=datetime.now()
        )
    
    def _fetch_run_path_metrics(self, run_id: str) -> dict:
        """Fetch path-related metrics for a run."""
        sql = '''
            SELECT 
                COUNT(*) as paths_found,
                COUNT(DISTINCT path_definition_id) as unique_paths,
                SUM(pd.node_count) as total_nodes,
                SUM(pd.link_count) as total_links,
                AVG(CAST(pd.node_count AS FLOAT)) as avg_nodes,
                AVG(CAST(pd.link_count AS FLOAT)) as avg_links,
                AVG(pd.total_length_mm) as avg_length
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        '''
        
        result = self.db.query(sql, [run_id])
        if result:
            row = result[0]
            return {
                'paths_found': row[0] or 0,
                'unique_paths': row[1] or 0,
                'total_nodes': row[2] or 0,
                'total_links': row[3] or 0,
                'avg_nodes': row[4],
                'avg_links': row[5],
                'avg_length': row[6]
            }
        return {}
    
    def _fetch_run_error_metrics(self, run_id: str) -> dict:
        """Fetch error and flag metrics for a run."""
        error_sql = '''
            SELECT 
                COUNT(*) as total_errors,
                SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_errors
            FROM tb_validation_errors
            WHERE run_id = ?
        '''
        
        flag_sql = '''
            SELECT COUNT(*) as total_flags
            FROM tb_review_flags
            WHERE run_id = ?
        '''
        
        error_result = self.db.query(error_sql, [run_id])
        flag_result = self.db.query(flag_sql, [run_id])
        
        metrics = {}
        if error_result:
            metrics['total_errors'] = error_result[0][0] or 0
            metrics['critical_errors'] = error_result[0][1] or 0
        
        if flag_result:
            metrics['total_flags'] = flag_result[0][0] or 0
        
        return metrics
    
    def _determine_completion_status(self, achieved_coverage: float, target_coverage: float, critical_errors: int) -> 'RunStatus':
        """Determine the completion status of a run."""
        from enum import Enum
        
        class RunStatus(Enum):
            COMPLETED = 'COMPLETED'
            PARTIAL = 'PARTIAL'
            FAILED = 'FAILED'
        
        if critical_errors > 0:
            return RunStatus.FAILED
        
        if target_coverage and achieved_coverage:
            if achieved_coverage >= target_coverage * 0.95:  # 95% of target
                return RunStatus.COMPLETED
            elif achieved_coverage >= target_coverage * 0.75:  # 75% of target
                return RunStatus.PARTIAL
            else:
                return RunStatus.FAILED
        
        return RunStatus.COMPLETED
    
    def _calculate_execution_time(self, run_id: str) -> Optional[float]:
        """Calculate execution time in minutes."""
        sql = 'SELECT run_at, ended_at FROM tb_runs WHERE id = ?'
        result = self.db.query(sql, [run_id])
        
        if result and result[0][1]:  # ended_at exists
            start_time = result[0][0]
            end_time = result[0][1]
            # Convert to datetime objects and calculate difference
            # This assumes the database returns datetime objects
            if hasattr(start_time, 'timestamp') and hasattr(end_time, 'timestamp'):
                return (end_time.timestamp() - start_time.timestamp()) / 60.0
        
        return None
    
    def _fetch_run_start_time(self, run_id: str) -> datetime:
        """Fetch run start time."""
        sql = 'SELECT run_at FROM tb_runs WHERE id = ?'
        result = self.db.query(sql, [run_id])
        
        if result:
            return result[0][0]
        return datetime.now()
    
    def _store_run_summary(self, summary: 'RunSummary'):
        """Store run summary in database."""
        sql = '''
            INSERT INTO tb_run_summaries (
                run_id, total_attempts, total_paths_found, unique_paths,
                total_scenario_tests, scenario_success_rate, total_errors,
                total_reviews, critical_errors, target_coverage, achieved_coverage,
                coverage_efficiency, total_nodes, total_links, avg_path_nodes,
                avg_path_links, avg_path_length, success_rate, completion_status,
                execution_time_seconds, started_at, ended_at, summarized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            summary.run_id,
            summary.total_attempts,
            summary.total_paths_found,
            summary.unique_paths,
            summary.total_scenario_tests,
            summary.scenario_success_rate,
            summary.total_errors,
            summary.total_review_flags,
            summary.critical_errors,
            summary.target_coverage,
            summary.achieved_coverage,
            summary.coverage_efficiency,
            summary.total_nodes,
            summary.total_links,
            summary.avg_path_nodes,
            summary.avg_path_links,
            summary.avg_path_length,
            summary.success_rate,
            summary.completion_status.value,
            summary.execution_time_mm * 60 if summary.execution_time_mm else None,  # Convert to seconds
            StringHelper.datetime_to_sqltimestamp(summary.started_at),
            StringHelper.datetime_to_sqltimestamp(summary.ended_at) if summary.ended_at else None,
            StringHelper.datetime_to_sqltimestamp(summary.summarized_at)
        ]
        
        self.db.update(sql, params)
    
    def fetch_run_status(self, run_id: str) -> Optional[dict]:
        """Fetch current run status and basic metrics."""
        sql = '''
            SELECT id, status, approach, method, coverage_target,
                   fab_no, phase_no, model_no, toolset, tag,
                   run_at, ended_at
            FROM tb_runs
            WHERE id = ?
        '''
        
        result = self.db.query(sql, [run_id])
        if result:
            row = result[0]
            return {
                'run_id': row[0],
                'status': row[1],
                'approach': row[2],
                'method': row[3],
                'coverage_target': row[4],
                'fab_no': row[5],
                'phase_no': row[6],
                'model_no': row[7],
                'toolset': row[8],
                'tag': row[9],
                'started_at': row[10],
                'ended_at': row[11]
            }
        return None