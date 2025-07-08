# managers/path.py

from datetime import datetime
from typing import Optional, list, dict

from db import Database
from string_helper import StringHelper

class PathManager:
    """Path storage and retrieval manager."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_definition(self, path_result: 'PathResult', run_id: str) -> int:
        """Store or retrieve existing path definition."""
        # Generate path hash for deduplication
        path_hash = StringHelper.generate_path_hash(path_result.nodes, path_result.links)
        
        # Check if path already exists
        existing_id = self._fetch_existing_path_definition(path_hash)
        if existing_id:
            return existing_id
        
        # Create new path definition
        return self._create_path_definition(path_result, path_hash, run_id)
    
    def store_attempt_path(self, run_id: str, path_definition_id: int, poc_pair: dict, path_result: 'PathResult'):
        """Store an attempt path record."""
        sql = '''
            INSERT INTO tb_attempt_paths (
                run_id, path_definition_id, start_node_id, end_node_id,
                cost, picked_at, tested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            path_definition_id,
            path_result.start_node_id,
            path_result.end_node_id,
            path_result.total_cost,
            StringHelper.datetime_to_sqltimestamp(datetime.now()),
            StringHelper.datetime_to_sqltimestamp(datetime.now())
        ]
        
        self.db.update(sql, params)
    
    def fetch_run_paths(self, run_id: str) -> list[dict]:
        """Fetch all paths for a given run."""
        sql = '''
            SELECT 
                ap.id,
                ap.path_definition_id,
                ap.start_node_id,
                ap.end_node_id,
                ap.cost,
                pd.path_hash,
                pd.node_count,
                pd.link_count,
                pd.total_length_mm,
                pd.coverage,
                pd.path_context
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
            ORDER BY ap.picked_at
        '''
        
        results = self.db.query(sql, [run_id])
        
        paths = []
        for row in results:
            paths.append({
                'attempt_id': row[0],
                'path_definition_id': row[1],
                'start_node_id': row[2],
                'end_node_id': row[3],
                'cost': row[4],
                'path_hash': row[5],
                'node_count': row[6],
                'link_count': row[7],
                'total_length_mm': row[8],
                'coverage': row[9],
                'path_context': row[10]
            })
        
        return paths
    
    def fetch_path_definition(self, path_definition_id: int) -> Optional[dict]:
        """Fetch complete path definition by ID."""
        sql = '''
            SELECT 
                id, path_hash, source_type, scope,
                target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                target_data_codes, target_utilities, target_references,
                forbidden_node_ids, node_count, link_count, total_length_mm,
                coverage, data_codes_scope, utilities_scope, references_scope,
                path_context, created_at
            FROM tb_path_definitions
            WHERE id = ?
        '''
        
        result = self.db.query(sql, [path_definition_id])
        if result:
            row = result[0]
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
                'forbidden_node_ids': row[11],
                'node_count': row[12],
                'link_count': row[13],
                'total_length_mm': row[14],
                'coverage': row[15],
                'data_codes_scope': row[16],
                'utilities_scope': row[17],
                'references_scope': row[18],
                'path_context': row[19],
                'created_at': row[20]
            }
        return None
    
    def fetch_paths_by_hash(self, path_hash: str) -> list[dict]:
        """Fetch all attempt paths that share the same path hash."""
        sql = '''
            SELECT 
                ap.run_id,
                ap.start_node_id,
                ap.end_node_id,
                ap.picked_at,
                ap.cost
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE pd.path_hash = ?
            ORDER BY ap.picked_at
        '''
        
        results = self.db.query(sql, [path_hash])
        
        paths = []
        for row in results:
            paths.append({
                'run_id': row[0],
                'start_node_id': row[1],
                'end_node_id': row[2],
                'picked_at': row[3],
                'cost': row[4]
            })
        
        return paths
    
    def update_path_definition_scope(self, path_definition_id: int, scope_data: dict):
        """Update path definition with additional scope information."""
        filters = {'id': ('=', path_definition_id)}
        where_clause, where_params = StringHelper.build_where_clause(filters)
        
        set_clause, set_params = StringHelper.build_update_set_clause(scope_data)
        
        if set_clause:
            sql = f'UPDATE tb_path_definitions {set_clause} {where_clause}'
            params = set_params + where_params
            self.db.update(sql, params)
    
    def fetch_unique_paths_count(self, run_id: str) -> int:
        """Get count of unique paths for a run."""
        sql = '''
            SELECT COUNT(DISTINCT pd.path_hash)
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        '''
        
        result = self.db.query(sql, [run_id])
        return result[0][0] if result else 0
    
    def fetch_path_statistics(self, run_id: Optional[str] = None) -> dict:
        """Fetch comprehensive path statistics."""
        filters = {}
        if run_id:
            filters['ap.run_id'] = ('=', run_id)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT 
                COUNT(*) as total_attempts,
                COUNT(DISTINCT pd.path_hash) as unique_paths,
                AVG(CAST(pd.node_count AS FLOAT)) as avg_nodes,
                AVG(CAST(pd.link_count AS FLOAT)) as avg_links,
                AVG(pd.total_length_mm) as avg_length,
                MIN(pd.total_length_mm) as min_length,
                MAX(pd.total_length_mm) as max_length,
                SUM(pd.node_count) as total_nodes_traversed,
                SUM(pd.link_count) as total_links_traversed
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            {where_clause}
        '''
        
        result = self.db.query(sql, params)
        if result:
            row = result[0]
            return {
                'total_attempts': row[0] or 0,
                'unique_paths': row[1] or 0,
                'avg_nodes': row[2],
                'avg_links': row[3],
                'avg_length': row[4],
                'min_length': row[5],
                'max_length': row[6],
                'total_nodes_traversed': row[7] or 0,
                'total_links_traversed': row[8] or 0
            }
        return {}
    
    def fetch_paths_by_criteria(self, criteria: dict) -> list[dict]:
        """Fetch paths matching specific criteria."""
        filters = {}
        
        # Build filters from criteria
        if 'min_length' in criteria:
            filters['pd.total_length_mm'] = ('>=', criteria['min_length'])
        if 'max_length' in criteria:
            filters['pd.total_length_mm'] = ('<=', criteria['max_length'])
        if 'min_nodes' in criteria:
            filters['pd.node_count'] = ('>=', criteria['min_nodes'])
        if 'max_nodes' in criteria:
            filters['pd.node_count'] = ('<=', criteria['max_nodes'])
        if 'target_fab_no' in criteria:
            filters['pd.target_fab_no'] = ('=', criteria['target_fab_no'])
        if 'source_type' in criteria:
            filters['pd.source_type'] = ('=', criteria['source_type'])
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT DISTINCT
                pd.id,
                pd.path_hash,
                pd.node_count,
                pd.link_count,
                pd.total_length_mm,
                pd.coverage,
                pd.target_fab_no,
                pd.target_phase_no,
                pd.created_at
            FROM tb_path_definitions pd
            {where_clause}
            ORDER BY pd.created_at DESC
        '''
        
        results = self.db.query(sql, params)
        
        paths = []
        for row in results:
            paths.append({
                'id': row[0],
                'path_hash': row[1],
                'node_count': row[2],
                'link_count': row[3],
                'total_length_mm': row[4],
                'coverage': row[5],
                'target_fab_no': row[6],
                'target_phase_no': row[7],
                'created_at': row[8]
            })
        
        return paths
    
    def store_path_tags(self, run_id: str, path_definition_id: int, tags: list[dict]):
        """Store tags for a path."""
        for tag in tags:
            sql = '''
                INSERT INTO tb_path_tags (
                    run_id, path_definition_id, path_hash, tag_type,
                    tag_code, tag, source, confidence, created_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            params = [
                run_id,
                path_definition_id,
                tag.get('path_hash'),
                tag['tag_type'],
                tag['tag_code'],
                tag.get('tag'),
                tag.get('source', 'SYSTEM'),
                tag.get('confidence', 1.0),
                StringHelper.datetime_to_sqltimestamp(datetime.now()),
                tag.get('notes')
            ]
            
            self.db.update(sql, params)
    
    def fetch_path_tags(self, path_definition_id: int, tag_type: Optional[str] = None) -> list[dict]:
        """Fetch tags for a path definition."""
        filters = {'path_definition_id': ('=', path_definition_id)}
        
        if tag_type:
            filters['tag_type'] = ('=', tag_type)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT tag_type, tag_code, tag, source, confidence, created_at, notes
            FROM tb_path_tags
            {where_clause}
            ORDER BY created_at
        '''
        
        results = self.db.query(sql, params)
        
        tags = []
        for row in results:
            tags.append({
                'tag_type': row[0],
                'tag_code': row[1],
                'tag': row[2],
                'source': row[3],
                'confidence': row[4],
                'created_at': row[5],
                'notes': row[6]
            })
        
        return tags
    
    def _fetch_existing_path_definition(self, path_hash: str) -> Optional[int]:
        """Check if path definition already exists."""
        sql = 'SELECT id FROM tb_path_definitions WHERE path_hash = ?'
        result = self.db.query(sql, [path_hash])
        return result[0][0] if result else None
    
    def _create_path_definition(self, path_result: 'PathResult', path_hash: str, run_id: str) -> int:
        """Create new path definition record."""
        # Create path context string
        path_context = self._build_path_context(path_result.nodes, path_result.links)
        
        # Convert lists to strings for storage
        data_codes_str = ','.join(map(str, path_result.data_codes)) if path_result.data_codes else ''
        utilities_str = ','.join(map(str, path_result.utility_nos)) if path_result.utility_nos else ''
        references_str = ','.join(path_result.references) if path_result.references else ''
        
        # Calculate coverage (simplified - actual implementation would be more complex)
        coverage = len(path_result.nodes) / 1000.0  # Placeholder calculation
        
        sql = '''
            INSERT INTO tb_path_definitions (
                path_hash, source_type, scope, node_count, link_count,
                total_length_mm, coverage, data_codes_scope, utilities_scope,
                references_scope, path_context, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            path_hash,
            'RANDOM',
            'CONNECTIVITY',
            len(path_result.nodes),
            len(path_result.links),
            path_result.total_length_mm,
            coverage,
            data_codes_str,
            utilities_str,
            references_str,
            path_context,
            StringHelper.datetime_to_sqltimestamp(datetime.now())
        ]
        
        self.db.update(sql, params)
        
        # Get the inserted ID
        id_result = self.db.query('SELECT id FROM tb_path_definitions WHERE path_hash = ?', [path_hash])
        return id_result[0][0] if id_result else None
    
    def _build_path_context(self, nodes: list[int], links: list[int]) -> str:
        """Build path context string for storage."""
        node_str = ','.join(map(str, nodes))
        link_str = ','.join(map(str, links))
        return f'NODES:[{node_str}];LINKS:[{link_str}]'
    
    def parse_path_context(self, path_context: str) -> dict:
        """Parse path context string back to nodes and links."""
        if not path_context:
            return {'nodes': [], 'links': []}
        
        try:
            parts = path_context.split(';')
            nodes = []
            links = []
            
            for part in parts:
                if part.startswith('NODES:[') and part.endswith(']'):
                    node_str = part[7:-1]  # Remove 'NODES:[' and ']'
                    if node_str:
                        nodes = [int(x) for x in node_str.split(',')]
                elif part.startswith('LINKS:[') and part.endswith(']'):
                    link_str = part[7:-1]  # Remove 'LINKS:[' and ']'
                    if link_str:
                        links = [int(x) for x in link_str.split(',')]
            
            return {'nodes': nodes, 'links': links}
        except (ValueError, IndexError):
            return {'nodes': [], 'links': []}
    
    def delete_run_paths(self, run_id: str) -> int:
        """Delete all paths associated with a run."""
        # First delete attempt paths
        deleted_attempts = self.db.update('DELETE FROM tb_attempt_paths WHERE run_id = ?', [run_id])
        
        # Delete orphaned path definitions (not referenced by other runs)
        cleanup_sql = '''
            DELETE FROM tb_path_definitions 
            WHERE id NOT IN (
                SELECT DISTINCT path_definition_id 
                FROM tb_attempt_paths 
                WHERE path_definition_id IS NOT NULL
            )
        '''
        self.db.update(cleanup_sql)
        
        return deleted_attempts