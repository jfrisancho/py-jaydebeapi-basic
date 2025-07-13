# managers/run.py

import uuid
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass
from enum import Enum

from db import Database
from string_helper import StringHelper
from sample_models import (
    RunStatus, RandomRunConfig, ScenarioRunConfig, RunConfig,
    Approach, Method, ExecutionMode, RunSummary
)


class RunManager:
    """Main execution orchestration for analysis runs."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def create_run(self, config: RunConfig) -> str:
        """Create a new run record and return the run_id."""
        run_data = {
            'id': config.run_id,
            'approach': config.approach.value,
            'method': config.method.value,
            'tag': config.tag,
            'status': RunStatus.INITIALIZED.value,
            'run_at': StringHelper.datetime_to_sqltimestamp(config.started_at)
        }
        
        # Add approach-specific fields
        if config.approach == Approach.RANDOM and config.random_config:
            rc = config.random_config
            run_data.update({
                'coverage_target': rc.coverage_target,
                'fab_no': rc.fab_no,
                'phase_no': rc.phase_no,
                'model_no': rc.model_no,
                'toolset': rc.toolset,
                'e2e_group_nos': str(rc.e2e_group_no) if rc.e2e_group_no else None
            })
        elif config.approach == Approach.SCENARIO and config.scenario_config:
            sc = config.scenario_config
            run_data.update({
                'scenario_code': sc.scenario_code,
                'scenario_file': sc.scenario_file,
                'coverage_target': 0.0
            })
        
        # Build insert statement
        columns = list(run_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_runs ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(run_data.values()))
        return config.run_id
    
    def update_run_status(self, run_id: str, status: RunStatus, 
                         total_coverage: Optional[float] = None,
                         total_nodes: Optional[int] = None,
                         total_links: Optional[int] = None) -> None:
        """Update run status and metrics."""
        updates = {'status': status.value}
        
        if status in [RunStatus.COMPLETED, RunStatus.PARTIAL, RunStatus.FAILED]:
            updates['ended_at'] = StringHelper.datetime_to_sqltimestamp(datetime.now())
        
        if total_coverage is not None:
            updates['total_coverage'] = total_coverage
        if total_nodes is not None:
            updates['total_nodes'] = total_nodes
        if total_links is not None:
            updates['total_links'] = total_links
        
        set_clause, params = StringHelper.build_update_set_clause(updates)
        sql = f'UPDATE tb_runs {set_clause} WHERE id = ?'
        params.append(run_id)
        
        self.db.update(sql, params)
    
    def fetch_run(self, run_id: str) -> Optional[dict]:
        """Fetch run details by ID."""
        sql = 'SELECT * FROM tb_runs WHERE id = ?'
        rows = self.db.query(sql, [run_id])
        return dict(zip([
            'id', 'approach', 'method', 'coverage_target', 'fab_no', 'phase_no',
            'model_no', 'toolset', 'e2e_group_nos', 'scenario_code', 'scenario_file',
            'tag', 'status', 'total_coverage', 'total_nodes', 'total_links',
            'run_at', 'ended_at'
        ], rows[0])) if rows else None
    
    def fetch_active_runs(self) -> list[dict]:
        """Fetch all active (non-completed) runs."""
        sql = '''
            SELECT id, approach, method, tag, status, coverage_target, 
                   total_coverage, run_at, ended_at
            FROM tb_runs 
            WHERE status NOT IN ('COMPLETED', 'FAILED')
            ORDER BY run_at DESC
        '''
        rows = self.db.query(sql)
        return [dict(zip([
            'id', 'approach', 'method', 'tag', 'status', 'coverage_target',
            'total_coverage', 'run_at', 'ended_at'
        ], row)) for row in rows]
    
    def fetch_recent_runs(self, limit: int = 10) -> list[dict]:
        """Fetch recent runs with summary info."""
        sql = '''
            SELECT r.id, r.approach, r.method, r.tag, r.status, 
                   r.coverage_target, r.total_coverage, r.run_at, r.ended_at,
                   s.total_attempts, s.total_paths_found, s.unique_paths,
                   s.success_rate, s.execution_time_seconds
            FROM tb_runs r
            LEFT JOIN tb_run_summaries s ON r.id = s.run_id
            ORDER BY r.run_at DESC
            LIMIT ?
        '''
        rows = self.db.query(sql, [limit])
        return [dict(zip([
            'id', 'approach', 'method', 'tag', 'status', 'coverage_target',
            'total_coverage', 'run_at', 'ended_at', 'total_attempts',
            'total_paths_found', 'unique_paths', 'success_rate', 'execution_time_seconds'
        ], row)) for row in rows]
    
    def store_run_summary(self, run_id: str, summary: RunSummary) -> None:
        """Store aggregated run metrics."""
        summary_data = {
            'run_id': run_id,
            'total_attempts': summary.total_attempts,
            'total_paths_found': summary.total_paths_found,
            'unique_paths': summary.unique_paths,
            'total_scenario_tests': summary.total_scenario_tests,
            'scenario_success_rate': summary.scenario_success_rate,
            'total_errors': summary.total_errors,
            'total_reviews': summary.total_review_flags,
            'critical_errors': summary.critical_errors,
            'target_coverage': summary.target_coverage,
            'achieved_coverage': summary.achieved_coverage,
            'coverage_efficiency': summary.coverage_efficiency,
            'total_nodes': summary.total_nodes,
            'total_links': summary.total_links,
            'avg_path_nodes': summary.avg_path_nodes,
            'avg_path_links': summary.avg_path_links,
            'avg_path_length': summary.avg_path_length,
            'success_rate': summary.success_rate,
            'completion_status': summary.completion_status.value,
            'execution_time_seconds': summary.execution_time_mm * 60 if summary.execution_time_mm else None,
            'started_at': StringHelper.datetime_to_sqltimestamp(summary.started_at),
            'ended_at': StringHelper.datetime_to_sqltimestamp(summary.ended_at) if summary.ended_at else None
        }
        
        # Remove None values
        summary_data = {k: v for k, v in summary_data.items() if v is not None}
        
        columns = list(summary_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'''
            INSERT INTO tb_run_summaries ({", ".join(columns)}) 
            VALUES ({placeholders})
        '''
        
        self.db.update(sql, list(summary_data.values()))
    
    def calculate_execution_time(self, run_id: str) -> Optional[float]:
        """Calculate execution time in minutes for a run."""
        run = self.fetch_run(run_id)
        if not run or not run['ended_at']:
            return None
        
        start_time = datetime.fromisoformat(run['run_at'].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(run['ended_at'].replace('Z', '+00:00'))
        
        return (end_time - start_time).total_seconds() / 60.0
    
    def cleanup_old_runs(self, days_old: int = 30) -> int:
        """Clean up runs older than specified days. Returns count of deleted runs."""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        sql = '''
            DELETE FROM tb_runs 
            WHERE run_at < ? AND status IN ('COMPLETED', 'FAILED')
        '''
        return self.db.update(sql, [StringHelper.datetime_to_sqltimestamp(cutoff_date)])
    
    @staticmethod
    def generate_run_id() -> str:
        """Generate a unique run ID."""
        return str(uuid.uuid4())
    
    @staticmethod
    def create_run_config(approach: Approach, method: Method, 
                         coverage_target: Optional[float] = None,
                         fab: Optional[str] = None,
                         model: Optional[str] = None,
                         phase: Optional[str] = None,
                         toolset: Optional[str] = None,
                         scenario_code: Optional[str] = None,
                         scenario_file: Optional[str] = None,
                         is_inter_toolset: bool = False,
                         execution_mode: ExecutionMode = ExecutionMode.DEFAULT,
                         verbose_mode: bool = False) -> RunConfig:
        """Factory method to create RunConfig with proper setup."""
        run_id = RunManager.generate_run_id()
        started_at = datetime.now()
        
        random_config = None
        scenario_config = None
        
        if approach == Approach.RANDOM:
            random_config = RandomRunConfig(
                coverage_target=coverage_target,
                fab=fab,
                model=model,
                phase=phase,
                toolset=toolset,
                is_inter_toolset=is_inter_toolset
            )
        elif approach == Approach.SCENARIO:
            scenario_config = ScenarioRunConfig(
                scenario_code=scenario_code,
                scenario_file=scenario_file
            )
        
        # Generate tag
        tag_parts = [started_at.strftime('%Y%m%d'), approach.value, method.value]
        
        if random_config and random_config.tag:
            tag_parts.append(random_config.tag)
        elif scenario_config and scenario_config.tag:
            tag_parts.append(scenario_config.tag)
        
        tag = '_'.join(tag_parts)
        
        return RunConfig(
            run_id=run_id,
            approach=approach,
            method=method,
            started_at=started_at,
            tag=tag,
            random_config=random_config,
            scenario_config=scenario_config,
            execution_mode=execution_mode,
            verbose_mode=verbose_mode
        )