# managers/path.py

import hashlib
import json
from typing import Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

from db import Database
from string_helper import StringHelper
from sample_models import PathResult


@dataclass
class PathDefinition:
    """Defines a path request before execution."""
    definition_hash: str
    source_type: str  # RANDOM, SCENARIO
    scope: str  # CONNECTIVITY, FLOW, MATERIAL
    s_node_id: Optional[int] = None
    e_node_id: Optional[int] = None
    filter_fab_no: Optional[int] = None
    filter_model_no: Optional[int] = None
    filter_phase_no: Optional[int] = None
    filter_toolset_no: Optional[int] = None
    filter_e2e_group_nos: Optional[str] = None
    filter_category_nos: Optional[str] = None
    filter_utility_nos: Optional[str] = None
    filter_references: Optional[str] = None
    target_data_codes: Optional[str] = None
    forbidden_node_ids: Optional[str] = None


@dataclass
class PathExecution:
    """Represents an executed path with metrics and data."""
    run_id: str
    path_definition_id: int
    path_hash: str
    node_count: int
    link_count: int
    total_length_mm: float
    coverage: float
    cost: float
    data_codes_scope: str
    utilities_scope: str
    references_scope: str
    path_context: str
    validation_passed: bool
    validation_errors: Optional[str] = None


@dataclass
class AttemptPath:
    """Records an attempt to find a path."""
    run_id: str
    path_definition_id: int
    status: str  # FOUND, NOT_FOUND, ERROR
    notes: Optional[str] = None


class PathManager:
    """Manages path definitions, executions, and retrieval operations."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_definition(self, definition: PathDefinition) -> int:
        """Store a path definition and return its ID."""
        # Check if definition already exists
        existing_id = self.fetch_path_definition_id_by_hash(definition.definition_hash)
        if existing_id:
            return existing_id
        
        definition_data = {
            'definition_hash': definition.definition_hash,
            'source_type': definition.source_type,
            'scope': definition.scope,
            's_node_id': definition.s_node_id,
            'e_node_id': definition.e_node_id,
            'filter_fab_no': definition.filter_fab_no,
            'filter_model_no': definition.filter_model_no,
            'filter_phase_no': definition.filter_phase_no,
            'filter_toolset_no': definition.filter_toolset_no,
            'filter_e2e_group_nos': definition.filter_e2e_group_nos,
            'filter_category_nos': definition.filter_category_nos,
            'filter_utilitie_nos': definition.filter_utility_nos,
            'filter_references': definition.filter_references,
            'target_data_codes': definition.target_data_codes,
            'forbidden_node_ids': definition.forbidden_node_ids
        }
        
        # Remove None values
        definition_data = {k: v for k, v in definition_data.items() if v is not None}
        
        columns = list(definition_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_path_definitions ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(definition_data.values()))
        
        # Get the inserted ID
        return self.fetch_path_definition_id_by_hash(definition.definition_hash)
    
    def store_path_execution(self, execution: PathExecution) -> None:
        """Store a path execution record."""
        execution_data = {
            'run_id': execution.run_id,
            'path_definition_id': execution.path_definition_id,
            'path_hash': execution.path_hash,
            'node_count': execution.node_count,
            'link_count': execution.link_count,
            'total_length_mm': execution.total_length_mm,
            'coverage': execution.coverage,
            'cost': execution.cost,
            'data_codes_scope': execution.data_codes_scope,
            'utilities_scope': execution.utilities_scope,
            'references_scope': execution.references_scope,
            'path_context': execution.path_context,
            'validation_passed': 1 if execution.validation_passed else 0,
            'validation_errors': execution.validation_errors
        }
        
        # Remove None values
        execution_data = {k: v for k, v in execution_data.items() if v is not None}
        
        columns = list(execution_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_path_executions ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(execution_data.values()))
    
    def store_attempt_path(self, attempt: AttemptPath) -> None:
        """Store a path attempt record."""
        attempt_data = {
            'run_id': attempt.run_id,
            'path_definition_id': attempt.path_definition_id,
            'status': attempt.status,
            'notes': attempt.notes
        }
        
        # Remove None values
        attempt_data = {k: v for k, v in attempt_data.items() if v is not None}
        
        columns = list(attempt_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_attempt_paths ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(attempt_data.values()))
    
    def fetch_path_definition_id_by_hash(self, definition_hash: str) -> Optional[int]:
        """Fetch path definition ID by hash."""
        sql = 'SELECT id FROM tb_path_definitions WHERE definition_hash = ?'
        rows = self.db.query(sql, [definition_hash])
        return rows[0][0] if rows else None
    
    def fetch_path_definition(self, definition_id: int) -> Optional[PathDefinition]:
        """Fetch path definition by ID."""
        sql = '''
            SELECT definition_hash, source_type, scope, s_node_id, e_node_id,
                   filter_fab_no, filter_model_no, filter_phase_no, filter_toolset_no,
                   filter_e2e_group_nos, filter_category_nos, filter_utilitie_nos,
                   filter_references, target_data_codes, forbidden_node_ids
            FROM tb_path_definitions 
            WHERE id = ?
        '''
        rows = self.db.query(sql, [definition_id])
        
        if not rows:
            return None
        
        row = rows[0]
        return PathDefinition(
            definition_hash=row[0],
            source_type=row[1],
            scope=row[2],
            s_node_id=row[3],
            e_node_id=row[4],
            filter_fab_no=row[5],
            filter_model_no=row[6],
            filter_phase_no=row[7],
            filter_toolset_no=row[8],
            filter_e2e_group_nos=row[9],
            filter_category_nos=row[10],
            filter_utility_nos=row[11],
            filter_references=row[12],
            target_data_codes=row[13],
            forbidden_node_ids=row[14]
        )
    
    def fetch_path_executions_by_run(self, run_id: str) -> list[PathExecution]:
        """Fetch all path executions for a run."""
        sql = '''
            SELECT run_id, path_definition_id, path_hash, node_count, link_count,
                   total_length_mm, coverage, cost, data_codes_scope, utilities_scope,
                   references_scope, path_context, validation_passed, validation_errors
            FROM tb_path_executions 
            WHERE run_id = ?
            ORDER BY executed_at
        '''
        rows = self.db.query(sql, [run_id])
        
        return [PathExecution(
            run_id=row[0],
            path_definition_id=row[1],
            path_hash=row[2],
            node_count=row[3],
            link_count=row[4],
            total_length_mm=row[5],
            coverage=row[6],
            cost=row[7],
            data_codes_scope=row[8],
            utilities_scope=row[9],
            references_scope=row[10],
            path_context=row[11],
            validation_passed=bool(row[12]),
            validation_errors=row[13]
        ) for row in rows]
    
    def fetch_unique_paths_by_run(self, run_id: str) -> list[dict]:
        """Fetch unique paths with aggregated metrics for a run."""
        sql = '''
            SELECT path_hash, COUNT(*) as execution_count,
                   AVG(node_count) as avg_node_count,
                   AVG(link_count) as avg_link_count,
                   AVG(total_length_mm) as avg_length,
                   AVG(coverage) as avg_coverage,
                   MIN(executed_at) as first_executed,
                   MAX(executed_at) as last_executed
            FROM tb_path_executions 
            WHERE run_id = ?
            GROUP BY path_hash
            ORDER BY first_executed
        '''
        rows = self.db.query(sql, [run_id])
        
        return [dict(zip([
            'path_hash', 'execution_count', 'avg_node_count', 'avg_link_count',
            'avg_length', 'avg_coverage', 'first_executed', 'last_executed'
        ], row)) for row in rows]
    
    def fetch_attempt_paths_by_run(self, run_id: str) -> list[AttemptPath]:
        """Fetch all path attempts for a run."""
        sql = '''
            SELECT run_id, path_definition_id, status, notes
            FROM tb_attempt_paths 
            WHERE run_id = ?
            ORDER BY picked_at
        '''
        rows = self.db.query(sql, [run_id])
        
        return [AttemptPath(
            run_id=row[0],
            path_definition_id=row[1],
            status=row[2],
            notes=row[3]
        ) for row in rows]
    
    def fetch_path_execution_metrics(self, run_id: str) -> dict[str, Any]:
        """Fetch aggregated path execution metrics for a run."""
        sql = '''
            SELECT 
                COUNT(*) as total_executions,
                COUNT(DISTINCT path_hash) as unique_paths,
                AVG(node_count) as avg_node_count,
                AVG(link_count) as avg_link_count,
                AVG(total_length_mm) as avg_length,
                AVG(coverage) as avg_coverage,
                AVG(cost) as avg_cost,
                SUM(CASE WHEN validation_passed = 1 THEN 1 ELSE 0 END) as validated_paths,
                MIN(executed_at) as first_execution,
                MAX(executed_at) as last_execution
            FROM tb_path_executions 
            WHERE run_id = ?
        '''
        rows = self.db.query(sql, [run_id])
        
        if not rows or not rows[0][0]:
            return {}
        
        row = rows[0]
        return {
            'total_executions': row[0],
            'unique_paths': row[1],
            'avg_node_count': row[2],
            'avg_link_count': row[3],
            'avg_length': row[4],
            'avg_coverage': row[5],
            'avg_cost': row[6],
            'validated_paths': row[7],
            'validation_rate': row[7] / row[0] if row[0] > 0 else 0,
            'first_execution': row[8],
            'last_execution': row[9]
        }
    
    def fetch_attempt_path_metrics(self, run_id: str) -> dict[str, Any]:
        """Fetch aggregated attempt path metrics for a run."""
        sql = '''
            SELECT 
                COUNT(*) as total_attempts,
                SUM(CASE WHEN status = 'FOUND' THEN 1 ELSE 0 END) as found_paths,
                SUM(CASE WHEN status = 'NOT_FOUND' THEN 1 ELSE 0 END) as not_found_paths,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_paths
            FROM tb_attempt_paths 
            WHERE run_id = ?
        '''
        rows = self.db.query(sql, [run_id])
        
        if not rows or not rows[0][0]:
            return {}
        
        row = rows[0]
        total_attempts = row[0]
        
        return {
            'total_attempts': total_attempts,
            'found_paths': row[1],
            'not_found_paths': row[2],
            'error_paths': row[3],
            'success_rate': row[1] / total_attempts if total_attempts > 0 else 0,
            'error_rate': row[3] / total_attempts if total_attempts > 0 else 0
        }
    
    @staticmethod
    def generate_path_definition_hash(s_node_id: Optional[int], e_node_id: Optional[int],
                                    filters: dict[str, Any]) -> str:
        """Generate a hash for path definition deduplication."""
        # Create a consistent string representation
        key_parts = [
            f's_node:{s_node_id}' if s_node_id else 's_node:None',
            f'e_node:{e_node_id}' if e_node_id else 'e_node:None'
        ]
        
        # Add filters in sorted order for consistency
        for key in sorted(filters.keys()):
            value = filters[key]
            key_parts.append(f'{key}:{value}')
        
        path_key = '|'.join(key_parts)
        return hashlib.md5(path_key.encode()).hexdigest()
    
    @staticmethod
    def generate_path_execution_hash(nodes: list[int], links: list[int]) -> str:
        """Generate a hash for path execution deduplication."""
        return StringHelper.generate_path_hash(nodes, links)
    
    @staticmethod
    def create_path_definition(source_type: str, scope: str,
                             s_node_id: Optional[int] = None,
                             e_node_id: Optional[int] = None,
                             **filters) -> PathDefinition:
        """Factory method to create PathDefinition with proper hash."""
        definition_hash = PathManager.generate_path_definition_hash(
            s_node_id, e_node_id, filters
        )
        
        return PathDefinition(
            definition_hash=definition_hash,
            source_type=source_type,
            scope=scope,
            s_node_id=s_node_id,
            e_node_id=e_node_id,
            filter_fab_no=filters.get('fab_no'),
            filter_model_no=filters.get('model_no'),
            filter_phase_no=filters.get('phase_no'),
            filter_toolset_no=filters.get('toolset_no'),
            filter_e2e_group_nos=filters.get('e2e_group_nos'),
            filter_category_nos=filters.get('category_nos'),
            filter_utility_nos=filters.get('utility_nos'),
            filter_references=filters.get('references'),
            target_data_codes=filters.get('target_data_codes'),
            forbidden_node_ids=filters.get('forbidden_node_ids')
        )
    
    @staticmethod
    def create_path_execution_from_result(run_id: str, path_definition_id: int,
                                        path_result: PathResult,
                                        validation_passed: bool,
                                        validation_errors: Optional[str] = None) -> PathExecution:
        """Factory method to create PathExecution from PathResult."""
        path_hash = PathManager.generate_path_execution_hash(
            path_result.nodes, path_result.links
        )
        
        # Convert lists to JSON strings for storage
        data_codes_scope = json.dumps(path_result.data_codes)
        utilities_scope = json.dumps(path_result.utility_nos)
        references_scope = json.dumps(path_result.references)
        path_context = json.dumps({
            'nodes': path_result.nodes,
            'links': path_result.links,
            'start_equipment_id': path_result.start_equipment_id,
            'end_equipment_id': path_result.end_equipment_id,
            'start_poc_id': path_result.start_poc_id,
            'end_poc_id': path_result.end_poc_id
        })
        
        # Calculate coverage based on unique nodes and links
        coverage = len(set(path_result.nodes)) + len(set(path_result.links))
        
        return PathExecution(
            run_id=run_id,
            path_definition_id=path_definition_id,
            path_hash=path_hash,
            node_count=len(path_result.nodes),
            link_count=len(path_result.links),
            total_length_mm=path_result.total_length_mm,
            coverage=float(coverage),
            cost=path_result.total_cost,
            data_codes_scope=data_codes_scope,
            utilities_scope=utilities_scope,
            references_scope=references_scope,
            path_context=path_context,
            validation_passed=validation_passed,
            validation_errors=validation_errors
        )