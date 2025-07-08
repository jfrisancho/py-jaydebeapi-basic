# managers/run.py

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from db import Database
from string_helper import StringHelper
from .random import RandomManager
from .path import PathManager
from .coverage import CoverageManager
from .validation import ValidationManager


class RunManager:
    """
    Main execution orchestration for path analysis runs.
    Coordinates random sampling, path finding, coverage tracking, and validation.
    """
    
    def __init__(self, db: Database):
        self.db = db
        self.random_manager = RandomManager(db)
        self.path_manager = PathManager(db)
        self.coverage_manager = CoverageManager(db)
        self.validation_manager = ValidationManager(db)
    
    def execute_random_run(self, config: 'RandomRunConfig') -> 'RunSummary':
        """
        Execute a complete random sampling run with the given configuration.
        
        Args:
            config: RandomRunConfig with coverage target and optional filters
            
        Returns:
            RunSummary with execution results and metrics
        """
        # Generate run ID and tag
        run_id = str(uuid.uuid4())
        started_at = datetime.now()
        tag = config.tag or f'{started_at.strftime("%Y%m%d")}_RANDOM_SIMPLE_{config.coverage_tag}'
        
        # Create run configuration
        run_config = RunConfig(
            run_id=run_id,
            approach=Approach.RANDOM,
            method=Method.SIMPLE,
            started_at=started_at,
            tag=tag,
            random_config=config
        )
        
        try:
            # Initialize run record
            self._create_run_record(run_config)
            
            # Set up coverage scope and tracking
            scope = self._build_coverage_scope(config)
            self.coverage_manager.initialize_coverage(run_id, scope)
            
            # Execute random sampling
            sampling_summary = self._execute_sampling(run_config, scope)
            
            # Update run status
            self._update_run_status(run_id, RunStatus.SAMPLING_COMPLETED)
            
            # Validate all found paths
            validation_summary = self._execute_validation(run_id)
            
            # Generate final summary
            summary = self._generate_run_summary(run_config, sampling_summary, validation_summary)
            
            # Store summary and update run status
            self._store_run_summary(summary)
            self._update_run_status(run_id, RunStatus.COMPLETED)
            
            return summary
            
        except Exception as e:
            self._update_run_status(run_id, RunStatus.FAILED)
            raise RuntimeError(f'Run {run_id} failed: {str(e)}')
    
    def _create_run_record(self, config: 'RunConfig') -> None:
        """Create initial run record in database."""
        
        # Extract random config values
        random_config = config.random_config
        coverage_target = random_config.coverage_target if random_config else None
        fab_no = random_config.fab_no if random_config else None
        phase_no = random_config.phase_no if random_config else None
        model_no = random_config.model_no if random_config else None
        toolset = random_config.toolset if random_config else None
        
        sql = '''
            INSERT INTO tb_runs (
                id, date, approach, method, coverage_target,
                fab_no, phase_no, model_no, toolset,
                total_coverage, total_nodes, total_links,
                tag, status, execution_mode, run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            config.run_id,
            config.started_at.date(),
            config.approach.value,
            config.method.value,
            coverage_target,
            fab_no,
            phase_no,
            model_no,
            toolset,
            0.0,  # total_coverage - will be updated
            0,    # total_nodes - will be updated
            0,    # total_links - will be updated
            config.tag,
            RunStatus.INITIALIZED.value,
            config.execution_mode.value,
            config.started_at
        ]
        
        self.db.update(sql, params)
    
    def _build_coverage_scope(self, config: 'RandomRunConfig') -> 'CoverageScope':
        """
        Build coverage scope based on configuration filters.
        
        Args:
            config: RandomRunConfig with optional filters
            
        Returns:
            CoverageScope with total nodes/links and mappings
        """
        # Build filter conditions
        filters = {}
        
        if config.fab_no is not None:
            filters['fab_no'] = ('=', config.fab_no)
        if config.phase_no is not None:
            filters['phase_no'] = ('=', config.phase_no)
        if config.model_no is not None:
            filters['model_no'] = ('=', config.model_no)
        if config.e2e_group_no is not None:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        
        # Get total nodes and links in scope
        node_where, node_params = StringHelper.build_where_clause(filters)
        link_where, link_params = StringHelper.build_where_clause(filters)
        
        # Count nodes in scope
        node_sql = f'SELECT COUNT(*) FROM nw_nodes{node_where}'
        total_nodes = self.db.query(node_sql, node_params)[0][0]
        
        # Count links in scope  
        link_sql = f'SELECT COUNT(*) FROM nw_links{link_where}'
        total_links = self.db.query(link_sql, link_params)[0][0]
        
        # Create scope object
        scope = CoverageScope(
            toolset=config.toolset,
            fab_no=config.fab_no,
            phase_no=config.phase_no,
            model_no=config.model_no,
            e2e_group_no=config.e2e_group_no,
            total_nodes=total_nodes,
            total_links=total_links
        )
        
        return scope
    
    def _execute_sampling(self, config: 'RunConfig', scope: 'CoverageScope') -> 'RandomRunSummary':
        """
        Execute random sampling until coverage target is reached.
        
        Args:
            config: RunConfig for the current run
            scope: CoverageScope defining the sampling universe
            
        Returns:
            RandomRunSummary with sampling results
        """
        return self.random_manager.execute_random_sampling(config, scope)
    
    def _execute_validation(self, run_id: str) -> Dict[str, Any]:
        """
        Execute validation on all paths found during sampling.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with validation summary metrics
        """
        return self.validation_manager.validate_run_paths(run_id)
    
    def _generate_run_summary(self, config: 'RunConfig', 
                             sampling_summary: 'RandomRunSummary',
                             validation_summary: Dict[str, Any]) -> 'RunSummary':
        """
        Generate comprehensive run summary from sampling and validation results.
        
        Args:
            config: RunConfig for the run
            sampling_summary: Results from random sampling
            validation_summary: Results from validation
            
        Returns:
            RunSummary with all metrics
        """
        ended_at = datetime.now()
        execution_time = (ended_at - config.started_at).total_seconds() / 60.0  # minutes
        
        return RunSummary(
            run_id=config.run_id,
            total_attempts=sampling_summary.total_attempts,
            total_paths_found=sampling_summary.total_paths_found,
            unique_paths=sampling_summary.unique_paths,
            total_errors=validation_summary.get('total_errors', 0),
            total_review_flags=validation_summary.get('total_review_flags', 0),
            critical_errors=validation_summary.get('critical_errors', 0),
            target_coverage=sampling_summary.target_coverage,
            achieved_coverage=sampling_summary.achieved_coverage,
            coverage_efficiency=sampling_summary.coverage_efficiency,
            total_nodes=sampling_summary.total_nodes,
            total_links=sampling_summary.total_links,
            avg_path_nodes=sampling_summary.avg_path_nodes,
            avg_path_links=sampling_summary.avg_path_links,
            avg_path_length=sampling_summary.avg_path_length,
            success_rate=sampling_summary.success_rate,
            completion_status=RunStatus.COMPLETED,
            execution_time_mm=execution_time,
            started_at=config.started_at,
            ended_at=ended_at,
            summarized_at=datetime.now()
        )
    
    def _store_run_summary(self, summary: 'RunSummary') -> None:
        """Store run summary in database."""
        
        sql = '''
            INSERT INTO tb_run_summaries (
                run_id, total_attempts, total_paths_found, unique_paths,
                total_errors, total_reviews, critical_errors,
                target_coverage, achieved_coverage, coverage_efficiency,
                total_nodes, total_links, avg_path_nodes, avg_path_links, avg_path_length,
                success_rate, completion_status, execution_time_seconds,
                started_at, ended_at, summarized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            summary.run_id,
            summary.total_attempts,
            summary.total_paths_found,
            summary.unique_paths,
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
            summary.started_at,
            summary.ended_at,
            summary.summarized_at
        ]
        
        self.db.update(sql, params)
    
    def _update_run_status(self, run_id: str, status: 'RunStatus') -> None:
        """Update run status in database."""
        
        sql = 'UPDATE tb_runs SET status = ?'
        params = [status.value]
        
        if status in [RunStatus.COMPLETED, RunStatus.FAILED]:
            sql += ', ended_at = ?'
            params.append(datetime.now())
        
        sql += ' WHERE id = ?'
        params.append(run_id)
        
        self.db.update(sql, params)
    
    def fetch_run_summary(self, run_id: str) -> Optional['RunSummary']:
        """
        Fetch run summary by ID.
        
        Args:
            run_id: Run identifier
            
        Returns:
            RunSummary if found, None otherwise
        """
        sql = '''
            SELECT run_id, total_attempts, total_paths_found, unique_paths,
                   total_errors, total_reviews, critical_errors,
                   target_coverage, achieved_coverage, coverage_efficiency,
                   total_nodes, total_links, avg_path_nodes, avg_path_links, avg_path_length,
                   success_rate, completion_status, execution_time_seconds,
                   started_at, ended_at, summarized_at
            FROM tb_run_summaries 
            WHERE run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows:
            return None
        
        row = rows[0]
        return RunSummary(
            run_id=row[0],
            total_attempts=row[1],
            total_paths_found=row[2],
            unique_paths=row[3],
            total_errors=row[4],
            total_review_flags=row[5],
            critical_errors=row[6],
            target_coverage=row[7],
            achieved_coverage=row[8],
            coverage_efficiency=row[9],
            total_nodes=row[10],
            total_links=row[11],
            avg_path_nodes=row[12],
            avg_path_links=row[13],
            avg_path_length=row[14],
            success_rate=row[15],
            completion_status=RunStatus(row[16]),
            execution_time_mm=row[17] / 60.0 if row[17] else None,  # Convert from seconds
            started_at=row[18],
            ended_at=row[19],
            summarized_at=row[20]
        )
    
    def fetch_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent runs with basic information.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of run dictionaries
        """
        sql = '''
            SELECT id, date, approach, method, coverage_target,
                   fab_no, phase_no, model_no, toolset,
                   total_coverage, tag, status, run_at, ended_at
            FROM tb_runs 
            ORDER BY run_at DESC 
            LIMIT ?
        '''
        
        rows = self.db.query(sql, [limit])
        
        runs = []
        for row in rows:
            runs.append({
                'run_id': row[0],
                'date': row[1],
                'approach': row[2],
                'method': row[3],
                'coverage_target': row[4],
                'fab_no': row[5],
                'phase_no': row[6],
                'model_no': row[7],
                'toolset': row[8],
                'total_coverage': row[9],
                'tag': row[10],
                'status': row[11],
                'started_at': row[12],
                'ended_at': row[13]
            })
        
        return runs