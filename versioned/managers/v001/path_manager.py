# managers/path.py

import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from db import Database
from string_helper import StringHelper


class PathManager:
    """
    Path storage and retrieval manager.
    Handles path definitions, deduplication, and metadata management.
    """
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_definition(self, run_id: str, path_result: 'PathResult', path_hash: str) -> int:
        """
        Store a path definition with all associated metadata.
        
        Args:
            run_id: Run identifier
            path_result: PathResult containing path information
            path_hash: Unique hash for the path
            
        Returns:
            Path definition ID
        """
        # Extract scope information from path
        data_codes_scope = json.dumps(sorted(path_result.data_codes))
        utilities_scope = json.dumps(sorted(path_result.utility_nos))
        references_scope = json.dumps(sorted(path_result.references))
        
        # Create path context (nodes and links sequence)
        path_context = json.dumps({
            'nodes': path_result.nodes,
            'links': path_result.links,
            'start_poc_id': path_result.start_poc_id,
            'end_poc_id': path_result.end_poc_id,
            'start_equipment_id': path_result.start_equipment_id,
            'end_equipment_id': path_result.end_equipment_id
        })
        
        # Calculate coverage for this specific path
        coverage = self._calculate_path_coverage(path_result.nodes, path_result.links)
        
        # Determine source type and target information
        source_type = 'RANDOM'  # For now, all paths are from random sampling
        scope = 'CONNECTIVITY'  # Default scope
        
        # Get target information from run configuration
        target_info = self._fetch_run_target_info(run_id)
        
        sql = '''
            INSERT INTO tb_path_definitions (
                path_hash, source_type, scope,
                target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                target_data_codes, target_utilities, target_references,
                node_count, link_count, total_length_mm, coverage,
                data_codes_scope, utilities_scope, references_scope, path_context,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            path_hash,
            source_type,
            scope,
            target_info.get('fab_no'),
            target_info.get('model_no'),
            target_info.get('phase_no'),
            target_info.get('e2e_group_no'),
            ','.join(map(str, path_result.data_codes)) if path_result.data_codes else None,
            ','.join(map(str, path_result.utility_nos)) if path_result.utility_nos else None,
            ','.join(path_result.references) if path_result.references else None,
            len(path_result.nodes),
            len(path_result.links),
            path_result.total_length_mm,
            coverage,
            data_codes_scope,
            utilities_scope,
            references_scope,
            path_context,
            datetime.now()
        ]
        
        self.db.update(sql, params)
        
        # Get the inserted path definition ID
        path_definition_id = self._fetch_last_insert_id()
        
        # Update attempt record with path definition ID
        self._update_attempt_with_path(run_id, path_result, path_definition_id)
        
        return path_definition_id
    
    def fetch_path_definition(self, path_definition_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a path definition by ID.
        
        Args:
            path_definition_id: Path definition identifier
            
        Returns:
            Dictionary with path definition data or None
        """
        sql = '''
            SELECT id, path_hash, source_type, scope,
                   target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                   target_data_codes, target_utilities, target_references,
                   node_count, link_count, total_length_mm, coverage,
                   data_codes_scope, utilities_scope, references_scope, path_context,
                   created_at
            FROM tb_path_definitions
            WHERE id = ?
        '''
        
        rows = self.db.query(sql, [path_definition_id])
        if not rows:
            return None
        
        row = rows[0]
        
        # Parse JSON fields
        try:
            data_codes_scope = json.loads(row[14]) if row[14] else []
            utilities_scope = json.loads(row[15]) if row[15] else []
            references_scope = json.loads(row[16]) if row[16] else []
            path_context = json.loads(row[17]) if row[17] else {}
        except json.JSONDecodeError:
            data_codes_scope = []
            utilities_scope = []
            references_scope = []
            path_context = {}
        
        return {
            'id': row[0],
            'path_hash': row[1],
            'source_type': row[2],
            'scope': row[3],
            'target_fab_no': row[4],
            'target_model_no': row[5],
            'target_phase_no': row[6],
            'target_toolset_no': row[7],
            'target_data_codes': row[8],
            'target_utilities': row[9],
            'target_references': row[10],
            'node_count': row[11],
            'link_count': row[12],
            'total_length_mm': row[13],
            'coverage': row[14],
            'data_codes_scope': data_codes_scope,
            'utilities_scope': utilities_scope,
            'references_scope': references_scope,
            'path_context': path_context,
            'created_at': row[19]
        }
    
    def fetch_run_paths(self, run_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch all path definitions for a specific run.
        
        Args:
            run_id: Run identifier
            limit: Optional limit on number of paths to return
            
        Returns:
            List of path definition dictionaries
        """
        sql = '''
            SELECT pd.id, pd.path_hash, pd.source_type, pd.scope,
                   pd.target_fab_no, pd.target_model_no, pd.target_phase_no, pd.target_toolset_no,
                   pd.node_count, pd.link_count, pd.total_length_mm, pd.coverage,
                   pd.created_at,
                   ap.start_node_id, ap.end_node_id, ap.cost, ap.picked_at, ap.tested_at
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
            ORDER BY pd.created_at DESC
        '''
        
        params = [run_id]
        if limit:
            sql += ' LIMIT ?'
            params.append(limit)
        
        rows = self.db.query(sql, params)
        
        paths = []
        for row in rows:
            paths.append({
                'path_definition_id': row[0],
                'path_hash': row[1],
                'source_type': row[2],
                'scope': row[3],
                'target_fab_no': row[4],
                'target_model_no': row[5],
                'target_phase_no': row[6],
                'target_toolset_no': row[7],
                'node_count': row[8],
                'link_count': row[9],
                'total_length_mm': row[10],
                'coverage': row[11],
                'created_at': row[12],
                'start_node_id': row[13],
                'end_node_id': row[14],
                'cost': row[15],
                'picked_at': row[16],
                'tested_at': row[17]
            })
        
        return paths
    
    def fetch_path_by_hash(self, path_hash: str) -> Optional[Dict[str, Any]]:
        """
        Fetch path definition by hash.
        
        Args:
            path_hash: Path hash identifier
            
        Returns:
            Dictionary with path definition data or None
        """
        sql = '''
            SELECT id, path_hash, source_type, scope,
                   target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                   target_data_codes, target_utilities, target_references,
                   node_count, link_count, total_length_mm, coverage,
                   data_codes_scope, utilities_scope, references_scope, path_context,
                   created_at
            FROM tb_path_definitions
            WHERE path_hash = ?
        '''
        
        rows = self.db.query(sql, [path_hash])
        if not rows:
            return None
        
        return self._parse_path_definition_row(rows[0])
    
    def fetch_paths_by_criteria(self, filters: Dict[str, Tuple[str, Any]], 
                               limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch path definitions by various criteria.
        
        Args:
            filters: Dictionary of filter criteria (column -> (operator, value))
            limit: Optional limit on results
            
        Returns:
            List of matching path definitions
        """
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id, path_hash, source_type, scope,
                   target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                   target_data_codes, target_utilities, target_references,
                   node_count, link_count, total_length_mm, coverage,
                   data_codes_scope, utilities_scope, references_scope, path_context,
                   created_at
            FROM tb_path_definitions
            {where_clause}
            ORDER BY created_at DESC
        '''
        
        if limit:
            sql += ' LIMIT ?'
            params.append(limit)
        
        rows = self.db.query(sql, params)
        
        return [self._parse_path_definition_row(row) for row in rows]
    
    def fetch_path_statistics(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch path statistics for analysis.
        
        Args:
            run_id: Optional run ID to filter statistics
            
        Returns:
            Dictionary with path statistics
        """
        filters = {}
        if run_id:
            # Join with attempt_paths to filter by run
            sql = '''
                SELECT 
                    COUNT(*) as total_paths,
                    COUNT(DISTINCT pd.path_hash) as unique_paths,
                    AVG(pd.node_count) as avg_nodes,
                    AVG(pd.link_count) as avg_links,
                    AVG(pd.total_length_mm) as avg_length,
                    MIN(pd.total_length_mm) as min_length,
                    MAX(pd.total_length_mm) as max_length,
                    AVG(pd.coverage) as avg_coverage
                FROM tb_path_definitions pd
                JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
                WHERE ap.run_id = ?
            '''
            params = [run_id]
        else:
            sql = '''
                SELECT 
                    COUNT(*) as total_paths,
                    COUNT(DISTINCT path_hash) as unique_paths,
                    AVG(node_count) as avg_nodes,
                    AVG(link_count) as avg_links,
                    AVG(total_length_mm) as avg_length,
                    MIN(total_length_mm) as min_length,
                    MAX(total_length_mm) as max_length,
                    AVG(coverage) as avg_coverage
                FROM tb_path_definitions
            '''
            params = []
        
        rows = self.db.query(sql, params)
        if not rows:
            return {}
        
        row = rows[0]
        return {
            'total_paths': row[0] or 0,
            'unique_paths': row[1] or 0,
            'avg_nodes': float(row[2]) if row[2] else 0.0,
            'avg_links': float(row[3]) if row[3] else 0.0,
            'avg_length': float(row[4]) if row[4] else 0.0,
            'min_length': float(row[5]) if row[5] else 0.0,
            'max_length': float(row[6]) if row[6] else 0.0,
            'avg_coverage': float(row[7]) if row[7] else 0.0
        }
    
    def delete_run_paths(self, run_id: str) -> int:
        """
        Delete all paths associated with a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Number of deleted path definitions
        """
        # First get path definition IDs for this run
        sql = '''
            SELECT DISTINCT pd.id
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
        '''
        
        path_ids = [row[0] for row in self.db.query(sql, [run_id])]
        
        if not path_ids:
            return 0
        
        # Delete path definitions (cascading deletes will handle related records)
        placeholders = ','.join(['?'] * len(path_ids))
        delete_sql = f'DELETE FROM tb_path_definitions WHERE id IN ({placeholders})'
        
        return self.db.update(delete_sql, path_ids)
    
    def update_path_scope(self, path_definition_id: int, new_scope: str) -> bool:
        """
        Update the scope of a path definition.
        
        Args:
            path_definition_id: Path definition identifier
            new_scope: New scope value
            
        Returns:
            True if updated successfully
        """
        sql = 'UPDATE tb_path_definitions SET scope = ? WHERE id = ?'
        affected_rows = self.db.update(sql, [new_scope, path_definition_id])
        return affected_rows > 0
    
    def fetch_duplicate_paths(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch paths that have duplicate hashes.
        
        Args:
            limit: Optional limit on results
            
        Returns:
            List of duplicate path groups
        """
        sql = '''
            SELECT path_hash, COUNT(*) as count, 
                   GROUP_CONCAT(id) as path_ids,
                   MIN(created_at) as first_created,
                   MAX(created_at) as last_created
            FROM tb_path_definitions
            GROUP BY path_hash
            HAVING COUNT(*) > 1
            ORDER BY count DESC, first_created
        '''
        
        params = []
        if limit:
            sql += ' LIMIT ?'
            params.append(limit)
        
        rows = self.db.query(sql, params)
        
        duplicates = []
        for row in rows:
            duplicates.append({
                'path_hash': row[0],
                'count': row[1],
                'path_ids': [int(x) for x in row[2].split(',')],
                'first_created': row[3],
                'last_created': row[4]
            })
        
        return duplicates
    
    def _calculate_path_coverage(self, nodes: List[int], links: List[int]) -> float:
        """
        Calculate coverage percentage for a specific path.
        This is a simplified calculation - actual implementation may vary.
        """
        # For now, use a simple metric based on path length
        # In practice, this might be more sophisticated
        total_elements = len(nodes) + len(links)
        return min(total_elements / 1000.0, 1.0)  # Normalize to 0-1 range
    
    def _fetch_run_target_info(self, run_id: str) -> Dict[str, Any]:
        """Fetch target information from run configuration."""
        sql = '''
            SELECT fab_no, model_no, phase_no, e2e_group_no, toolset
            FROM tb_runs
            WHERE id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows:
            return {}
        
        row = rows[0]
        return {
            'fab_no': row[0],
            'model_no': row[1],
            'phase_no': row[2],
            'e2e_group_no': row[3],
            'toolset': row[4]
        }
    
    def _fetch_last_insert_id(self) -> int:
        """Fetch the last inserted row ID."""
        # This is database-specific. For H2/HSQLDB, you might use IDENTITY() or similar
        sql = 'SELECT IDENTITY()'
        result = self.db.query(sql)
        return result[0][0] if result else None
    
    def _update_attempt_with_path(self, run_id: str, path_result: 'PathResult', path_definition_id: int) -> None:
        """Update attempt record with found path information."""
        sql = '''
            UPDATE tb_attempt_paths 
            SET path_definition_id = ?, cost = ?, tested_at = ?
            WHERE run_id = ? AND start_node_id = ? AND end_node_id = ?
            AND path_definition_id IS NULL
        '''
        
        params = [
            path_definition_id,
            path_result.total_cost,
            datetime.now(),
            run_id,
            path_result.start_node_id,
            path_result.end_node_id
        ]
        
        self.db.update(sql, params)
    
    def _parse_path_definition_row(self, row: Tuple) -> Dict[str, Any]:
        """Parse a path definition database row into a dictionary."""
        try:
            data_codes_scope = json.loads(row[14]) if row[14] else []
            utilities_scope = json.loads(row[15]) if row[15] else []
            references_scope = json.loads(row[16]) if row[16] else []
            path_context = json.loads(row[17]) if row[17] else {}
        except (json.JSONDecodeError, IndexError):
            data_codes_scope = []
            utilities_scope = []
            references_scope = []
            path_context = {}
        
        return {
            'id': row[0],
            'path_hash': row[1],
            'source_type': row[2],
            'scope': row[3],
            'target_fab_no': row[4],
            'target_model_no': row[5],
            'target_phase_no': row[6],
            'target_toolset_no': row[7],
            'target_data_codes': row[8],
            'target_utilities': row[9],
            'target_references': row[10],
            'node_count': row[11],
            'link_count': row[12],
            'total_length_mm': row[13],
            'coverage': row[14],
            'data_codes_scope': data_codes_scope,
            'utilities_scope': utilities_scope,
            'references_scope': references_scope,
            'path_context': path_context,
            'created_at': row[18]
        }
    
    def add_path_tag(self, run_id: str, path_definition_id: int, 
                     tag_type: str, tag_code: str, tag: str,
                     source: str = 'SYSTEM', confidence: float = 1.0,
                     notes: Optional[str] = None) -> None:
        """
        Add a tag to a path definition.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition identifier
            tag_type: Type of tag (QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO)
            tag_code: Tag code
            tag: Tag value
            source: Tag source (SYSTEM, USER, VALIDATION)
            confidence: Confidence score for auto-generated tags
            notes: Optional notes
        """
        sql = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, tag_type, tag_code, tag,
                source, confidence, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            path_definition_id,
            tag_type,
            tag_code,
            tag,
            source,
            confidence,
            datetime.now(),
            notes
        ]
        
        self.db.update(sql, params)
    
    def fetch_path_tags(self, path_definition_id: int) -> List[Dict[str, Any]]:
        """
        Fetch all tags for a path definition.
        
        Args:
            path_definition_id: Path definition identifier
            
        Returns:
            List of tag dictionaries
        """
        sql = '''
            SELECT id, run_id, tag_type, tag_code, tag, source, confidence,
                   created_at, created_by, notes
            FROM tb_path_tags
            WHERE path_definition_id = ?
            ORDER BY created_at DESC
        '''
        
        rows = self.db.query(sql, [path_definition_id])
        
        tags = []
        for row in rows:
            tags.append({
                'id': row[0],
                'run_id': row[1],
                'tag_type': row[2],
                'tag_code': row[3],
                'tag': row[4],
                'source': row[5],
                'confidence': row[6],
                'created_at': row[7],
                'created_by': row[8],
                'notes': row[9]
            })
        
        return tags