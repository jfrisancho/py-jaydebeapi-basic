# managers/run.py

import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import asdict

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

    def execute_random_run(self, config: 'RandomRunConfig') -> 'RunSummary':
        """Execute a complete random sampling run."""
        run_id = str(uuid.uuid4())
        started_at = datetime.now()
        
        # Generate tag and create run config
        tag = self._generate_run_tag(config, started_at)
        run_config = self._create_run_config(run_id, 'RANDOM', 'SIMPLE', started_at, tag, config)
        
        try:
            # Initialize run in database
            self._store_run(run_config)
            
            # Initialize coverage scope
            coverage_scope = self.coverage_manager.initialize_scope(
                fab_no=config.fab_no,
                phase_no=config.phase_no,
                model_no=config.model_no,
                e2e_group_no=config.e2e_group_no
            )
            
            # Execute sampling phase
            sampling_results = self.random_manager.execute_sampling(
                run_id=run_id,
                config=config,
                coverage_scope=coverage_scope
            )
            
            # Update run status
            self._update_run_status(run_id, 'SAMPLING_COMPLETED')
            
            # Execute validation phase
            validation_results = self.validation_manager.validate_run_paths(run_id)
            
            # Calculate final coverage
            final_coverage = self.coverage_manager.calculate_coverage(run_id, coverage_scope)
            
            # Generate run summary
            summary = self._generate_run_summary(
                run_id=run_id,
                sampling_results=sampling_results,
                validation_results=validation_results,
                coverage_results=final_coverage,
                started_at=started_at
            )
            
            # Store summary
            self._store_run_summary(summary)
            
            # Update run status to completed
            self._update_run_status(run_id, 'COMPLETED', datetime.now())
            
            return summary
            
        except Exception as e:
            self._update_run_status(run_id, 'FAILED', datetime.now())
            raise RuntimeError(f'Run {run_id} failed: {str(e)}')

    def _generate_run_tag(self, config: 'RandomRunConfig', started_at: datetime) -> str:
        """Generate run tag based on configuration."""
        tag_parts = [started_at.strftime('%Y%m%d'), 'RANDOM', 'SIMPLE']
        
        if config.coverage_target:
            tag_parts.append(f'{config.coverage_target*100:.0f}P')
        
        if config.fab:
            tag_parts.append(config.fab)
        
        if config.phase:
            tag_parts.append(config.phase)
        
        if config.toolset:
            tag_parts.append(config.toolset)
        
        return '_'.join(tag_parts)

    def _create_run_config(self, run_id: str, approach: str, method: str, 
                          started_at: datetime, tag: str, config: 'RandomRunConfig') -> Dict[str, Any]:
        """Create run configuration dictionary."""
        return {
            'run_id': run_id,
            'approach': approach,
            'method': method,
            'started_at': started_at,
            'tag': tag,
            'coverage_target': config.coverage_target,
            'fab_no': config.fab_no,
            'phase_no': config.phase_no,
            'model_no': config.model_no,
            'toolset': config.toolset,
            'execution_mode': 'DEFAULT',
            'status': 'RUNNING'
        }

    def _store_run(self, run_config: Dict[str, Any]):
        """Store run configuration in database."""
        sql = '''
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target, fab_no, phase_no, 
            model_no, toolset, total_coverage, total_nodes, total_links,
            tag, status, execution_mode, run_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_config['run_id'],
            run_config['started_at'].date(),
            run_config['approach'],
            run_config['method'],
            run_config['coverage_target'],
            run_config['fab_no'],
            run_config['phase_no'],
            run_config['model_no'],
            run_config['toolset'],
            0.0,  # total_coverage - will be updated later
            0,    # total_nodes - will be updated later
            0,    # total_links - will be updated later
            run_config['tag'],
            run_config['status'],
            run_config['execution_mode'],
            run_config['started_at']
        ]
        
        self.db.update(sql, params)

    def _update_run_status(self, run_id: str, status: str, ended_at: Optional[datetime] = None):
        """Update run status and optionally end time."""
        if ended_at:
            sql = 'UPDATE tb_runs SET status = ?, ended_at = ? WHERE id = ?'
            params = [status, ended_at, run_id]
        else:
            sql = 'UPDATE tb_runs SET status = ? WHERE id = ?'
            params = [status, run_id]
        
        self.db.update(sql, params)

    def _generate_run_summary(self, run_id: str, sampling_results: Dict[str, Any], 
                             validation_results: Dict[str, Any], coverage_results: Dict[str, Any],
                             started_at: datetime) -> 'RunSummary':
        """Generate comprehensive run summary."""
        ended_at = datetime.now()
        execution_time_mm = (ended_at - started_at).total_seconds() / 60.0
        
        # Calculate success rate
        total_attempts = sampling_results.get('total_attempts', 0)
        total_paths_found = sampling_results.get('total_paths_found', 0)
        success_rate = (total_paths_found / total_attempts * 100) if total_attempts > 0 else 0.0
        
        # Calculate coverage efficiency
        target_coverage = sampling_results.get('target_coverage', 0)
        achieved_coverage = coverage_results.get('achieved_coverage', 0)
        coverage_efficiency = (achieved_coverage / target_coverage * 100) if target_coverage > 0 else 0.0
        
        return {
            'run_id': run_id,
            'total_attempts': total_attempts,
            'total_paths_found': total_paths_found,
            'unique_paths': sampling_results.get('unique_paths', 0),
            'total_scenario_tests': 0,
            'scenario_success_rate': None,
            'total_errors': validation_results.get('total_errors', 0),
            'total_review_flags': validation_results.get('total_review_flags', 0),
            'critical_errors': validation_results.get('critical_errors', 0),
            'target_coverage': target_coverage,
            'achieved_coverage': achieved_coverage,
            'coverage_efficiency': coverage_efficiency,
            'total_nodes': coverage_results.get('total_nodes', 0),
            'total_links': coverage_results.get('total_links', 0),
            'avg_path_nodes': sampling_results.get('avg_path_nodes'),
            'avg_path_links': sampling_results.get('avg_path_links'),
            'avg_path_length': sampling_results.get('avg_path_length'),
            'success_rate': success_rate,
            'completion_status': 'COMPLETED',
            'execution_time_mm': execution_time_mm,
            'started_at': started_at,
            'ended_at': ended_at,
            'summarized_at': datetime.now()
        }

    def _store_run_summary(self, summary: Dict[str, Any]):
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
            summary['run_id'],
            summary['total_attempts'],
            summary['total_paths_found'],
            summary['unique_paths'],
            summary['total_scenario_tests'],
            summary['scenario_success_rate'],
            summary['total_errors'],
            summary['total_review_flags'],
            summary['critical_errors'],
            summary['target_coverage'],
            summary['achieved_coverage'],
            summary['coverage_efficiency'],
            summary['total_nodes'],
            summary['total_links'],
            summary['avg_path_nodes'],
            summary['avg_path_links'],
            summary['avg_path_length'],
            summary['success_rate'],
            summary['completion_status'],
            summary['execution_time_mm'] * 60 if summary['execution_time_mm'] else None,
            summary['started_at'],
            summary['ended_at'],
            summary['summarized_at']
        ]
        
        self.db.update(sql, params)

    def fetch_run_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Fetch run by ID."""
        sql = 'SELECT * FROM tb_runs WHERE id = ?'
        rows = self.db.query(sql, [run_id])
        return rows[0] if rows else None

    def fetch_runs_by_filters(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch runs by various filters."""
        base_sql = 'SELECT * FROM tb_runs'
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY run_at DESC'
        
        return self.db.query(sql, params)

    def fetch_run_summary(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Fetch run summary by run ID."""
        sql = 'SELECT * FROM tb_run_summaries WHERE run_id = ?'
        rows = self.db.query(sql, [run_id])
        return rows[0] if rows else None