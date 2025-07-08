# managers/run.py

import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field

from db import Database
from string_helper import StringHelper
from .random import RandomManager
from .path import PathManager
from .coverage import CoverageManager
from .validation import ValidationManager

class RunManager:
    """Main execution orchestration for random sampling runs."""
    
    def __init__(self, db: Database):
        self.db = db
        self.random_manager = RandomManager(db)
        self.path_manager = PathManager(db)
        self.coverage_manager = CoverageManager(db)
        self.validation_manager = ValidationManager(db)
    
    def execute_random_run(self, config: 'RandomRunConfig') -> 'RunSummary':
        """Execute a complete random sampling run."""
        run_id = self._generate_run_id()
        
        try:
            # Initialize run record
            self._store_run_start(run_id, config)
            
            # Initialize coverage tracking
            coverage_scope = self.coverage_manager.initialize_coverage_scope(config)
            
            # Initialize sampling universe
            sampling_universe = self.random_manager.initialize_sampling_universe(config)
            
            # Execute sampling phase
            sampling_results = self._execute_sampling_phase(
                run_id, config, coverage_scope, sampling_universe
            )
            
            # Update run status after sampling
            self._update_run_status(run_id, RunStatus.SAMPLING_COMPLETED)
            
            # Execute validation phase
            validation_results = self._execute_validation_phase(
                run_id, sampling_results.path_definitions
            )
            
            # Generate final summary
            summary = self._generate_run_summary(
                run_id, config, sampling_results, validation_results
            )
            
            # Store summary and finalize run
            self._store_run_summary(summary)
            self._update_run_status(run_id, RunStatus.COMPLETED, summary.ended_at)
            
            return summary
            
        except Exception as e:
            self._update_run_status(run_id, RunStatus.FAILED)
            raise RuntimeError(f'Run {run_id} failed: {str(e)}')
    
    def _execute_sampling_phase(self, run_id: str, config: 'RandomRunConfig', 
                               coverage_scope: 'CoverageScope', 
                               sampling_universe: List[Dict]) -> 'SamplingResults':
        """Execute the random sampling phase until coverage target is met."""
        
        attempts = 0
        paths_found = 0
        unique_path_hashes = set()
        attempt_records = []
        
        while not self.coverage_manager.is_target_achieved(coverage_scope, config.coverage_target):
            attempts += 1
            
            # Select random POC pair with bias mitigation
            poc_pair = self.random_manager.select_random_poc_pair(
                sampling_universe, config.bias_reduction
            )
            
            if not poc_pair:
                break
                
            # Store attempt record
            attempt_id = self._store_attempt(run_id, poc_pair, attempts)
            attempt_records.append(attempt_id)
            
            # Find path between POCs
            path_result = self.random_manager.find_path_between_pocs(
                poc_pair['start_poc'], poc_pair['end_poc']
            )
            
            if path_result:
                # Check for path uniqueness
                path_hash = StringHelper.generate_path_hash(
                    path_result.nodes, path_result.links
                )
                
                if path_hash not in unique_path_hashes:
                    unique_path_hashes.add(path_hash)
                    
                    # Store path definition
                    path_def_id = self.path_manager.store_path_definition(
                        path_result, path_hash, config
                    )
                    
                    # Update coverage
                    self.coverage_manager.update_coverage(
                        coverage_scope, path_result.nodes, path_result.links
                    )
                    
                    paths_found += 1
                    
                    # Update attempt with success
                    self._update_attempt_success(attempt_id, path_def_id, path_result.total_cost)
                else:
                    # Mark as duplicate
                    self._update_attempt_notes(attempt_id, 'Duplicate path')
            else:
                # Check if POCs should be reviewed
                self._check_unused_pocs_for_review(run_id, poc_pair)
        
        return SamplingResults(
            attempts=attempts,
            paths_found=paths_found,
            unique_paths=len(unique_path_hashes),
            attempt_records=attempt_records,
            path_definitions=list(unique_path_hashes)
        )
    
    def _execute_validation_phase(self, run_id: str, 
                                path_definitions: List[str]) -> 'ValidationResults':
        """Execute comprehensive validation on all found paths."""
        
        total_errors = 0
        total_review_flags = 0
        critical_errors = 0
        
        for path_hash in path_definitions:
            # Get path details
            path_def = self.path_manager.fetch_path_definition_by_hash(path_hash)
            
            if path_def:
                # Run connectivity validation
                connectivity_errors = self.validation_manager.validate_connectivity(
                    run_id, path_def
                )
                
                # Run utility consistency validation
                utility_errors = self.validation_manager.validate_utility_consistency(
                    run_id, path_def
                )
                
                # Count errors by severity
                all_errors = connectivity_errors + utility_errors
                total_errors += len(all_errors)
                critical_errors += len([e for e in all_errors if e.severity == 'CRITICAL'])
                
                # Generate review flags for critical issues
                review_flags = self.validation_manager.generate_review_flags(
                    run_id, path_def, all_errors
                )
                total_review_flags += len(review_flags)
        
        return ValidationResults(
            total_errors=total_errors,
            total_review_flags=total_review_flags,
            critical_errors=critical_errors
        )
    
    def _generate_run_summary(self, run_id: str, config: 'RandomRunConfig',
                            sampling_results: 'SamplingResults',
                            validation_results: 'ValidationResults') -> 'RunSummary':
        """Generate comprehensive run summary."""
        
        ended_at = datetime.now()
        execution_time = (ended_at - config.started_at).total_seconds() / 60.0
        
        # Get coverage metrics
        coverage_metrics = self.coverage_manager.calculate_final_coverage(config)
        
        # Calculate path statistics
        path_stats = self.path_manager.calculate_path_statistics(run_id)
        
        success_rate = (sampling_results.paths_found / sampling_results.attempts * 100.0 
                       if sampling_results.attempts > 0 else 0.0)
        
        coverage_efficiency = (coverage_metrics.achieved_coverage / config.coverage_target 
                             if config.coverage_target > 0 else 0.0)
        
        return RunSummary(
            run_id=run_id,
            total_attempts=sampling_results.attempts,
            total_paths_found=sampling_results.paths_found,
            unique_paths=sampling_results.unique_paths,
            target_coverage=config.coverage_target,
            achieved_coverage=coverage_metrics.achieved_coverage,
            coverage_efficiency=coverage_efficiency,
            total_errors=validation_results.total_errors,
            total_review_flags=validation_results.total_review_flags,
            critical_errors=validation_results.critical_errors,
            total_nodes=coverage_metrics.total_nodes,
            total_links=coverage_metrics.total_links,
            avg_path_nodes=path_stats.avg_nodes,
            avg_path_links=path_stats.avg_links,
            avg_path_length=path_stats.avg_length,
            success_rate=success_rate,
            completion_status=RunStatus.COMPLETED,
            execution_time_mm=execution_time,
            started_at=config.started_at,
            ended_at=ended_at
        )
    
    def _generate_run_id(self) -> str:
        """Generate unique run identifier."""
        return str(uuid.uuid4())
    
    def _store_run_start(self, run_id: str, config: 'RandomRunConfig'):
        """Store initial run record."""
        sql = '''
            INSERT INTO tb_runs (
                id, date, approach, method, coverage_target, 
                fab_no, phase_no, model_no, toolset,
                total_coverage, total_nodes, total_links,
                tag, status, execution_mode, run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            config.started_at.date(),
            'RANDOM',
            'SIMPLE',
            config.coverage_target,
            config.fab_no,
            config.phase_no,
            config.model_no,
            config.toolset,
            0.0,  # Initial coverage
            0,    # Initial nodes
            0,    # Initial links
            config.tag,
            RunStatus.INITIALIZED.value,
            config.execution_mode.value if hasattr(config, 'execution_mode') else 'DEFAULT',
            config.started_at
        ]
        
        self.db.update(sql, params)
    
    def _update_run_status(self, run_id: str, status: 'RunStatus', ended_at: datetime = None):
        """Update run status and end time."""
        if ended_at:
            sql = 'UPDATE tb_runs SET status = ?, ended_at = ? WHERE id = ?'
            params = [status.value, ended_at, run_id]
        else:
            sql = 'UPDATE tb_runs SET status = ? WHERE id = ?'
            params = [status.value, run_id]
        
        self.db.update(sql, params)
    
    def _store_attempt(self, run_id: str, poc_pair: Dict, attempt_num: int) -> int:
        """Store attempt record and return attempt ID."""
        sql = '''
            INSERT INTO tb_attempt_paths (
                run_id, start_node_id, end_node_id, picked_at, notes
            ) VALUES (?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            poc_pair['start_poc']['node_id'],
            poc_pair['end_poc']['node_id'],
            datetime.now(),
            f'Attempt #{attempt_num}'
        ]
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            self.db._conn.commit()
            
            # Get the auto-generated ID
            cur.execute('SELECT LAST_INSERT_ID()')
            return cur.fetchone()[0]
    
    def _update_attempt_success(self, attempt_id: int, path_def_id: int, cost: float):
        """Update attempt record with successful path finding."""
        sql = '''
            UPDATE tb_attempt_paths 
            SET path_definition_id = ?, cost = ?, tested_at = ?
            WHERE id = ?
        '''
        
        params = [path_def_id, cost, datetime.now(), attempt_id]
        self.db.update(sql, params)
    
    def _update_attempt_notes(self, attempt_id: int, notes: str):
        """Update attempt record with notes."""
        sql = 'UPDATE tb_attempt_paths SET notes = ? WHERE id = ?'
        self.db.update(sql, [notes, attempt_id])
    
    def _check_unused_pocs_for_review(self, run_id: str, poc_pair: Dict):
        """Check if unused POCs should be flagged for review."""
        start_poc = poc_pair['start_poc']
        end_poc = poc_pair['end_poc']
        
        # Check if both POCs are marked as used
        if start_poc.get('is_used') and end_poc.get('is_used'):
            # Flag for manual review - used POCs should have connectivity
            self.validation_manager.create_review_flag(
                run_id=run_id,
                flag_type='CONNECTIVITY_ISSUE',
                severity='MEDIUM',
                reason='Used POCs without connectivity path',
                object_type='POC',
                object_id=start_poc['id'],
                notes=f'No path found between used POCs {start_poc["id"]} and {end_poc["id"]}'
            )
    
    def _store_run_summary(self, summary: 'RunSummary'):
        """Store final run summary."""
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
            summary.execution_time_mm * 60 if summary.execution_time_mm else None,
            summary.started_at,
            summary.ended_at,
            summary.summarized_at
        ]
        
        self.db.update(sql, params)


@dataclass
class SamplingResults:
    """Results from the sampling phase."""
    attempts: int
    paths_found: int
    unique_paths: int
    attempt_records: List[int]
    path_definitions: List[str]


@dataclass
class ValidationResults:
    """Results from the validation phase."""
    total_errors: int
    total_review_flags: int
    critical_errors: int