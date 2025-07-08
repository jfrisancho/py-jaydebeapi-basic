# managers/path.py

from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from db import Database
from string_helper import StringHelper


class PathManager:
    """Path storage and retrieval operations."""
    
    def __init__(self, db: Database):
        self.db = db

    def store_path_definition(self, path_data: Dict[str, Any]) -> int:
        """Store a path definition and return its ID."""
        path_hash = StringHelper.generate_path_hash(
            path_data.get('nodes', []), 
            path_data.get('links', [])
        )
        
        # Check if path already exists
        existing_id = self.fetch_path_definition_by_hash(path_hash)
        if existing_id:
            return existing_id
        
        sql = '''
        INSERT INTO tb_path_definitions (
            path_hash, source_type, scope, target_fab_no, target_model_no,
            target_phase_no, target_toolset_no, target_data_codes,
            target_utilities, target_references, forbidden_node_ids,
            node_count, link_count, total_length_mm, coverage,
            data_codes_scope, utilities_scope, references_scope,
            path_context, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            path_hash,
            path_data.get('source_type', 'RANDOM'),
            path_data.get('scope', 'CONNECTIVITY'),
            path_data.get('target_fab_no'),
            path_data.get('target_model_no'),
            path_data.get('target_phase_no'),
            path_data.get('target_toolset_no'),
            self._list_to_string(path_data.get('target_data_codes', [])),
            self._list_to_string(path_data.get('target_utilities', [])),
            self._list_to_string(path_data.get('target_references', [])),
            self._list_to_string(path_data.get('forbidden_node_ids', [])),
            len(path_data.get('nodes', [])),
            len(path_data.get('links', [])),
            path_data.get('total_length_mm', 0.0),
            path_data.get('coverage', 0.0),
            self._list_to_string(path_data.get('data_codes_scope', [])),
            self._list_to_string(path_data.get('utilities_scope', [])),
            self._list_to_string(path_data.get('references_scope', [])),
            self._build_path_context(path_data.get('nodes', []), path_data.get('links', [])),
            datetime.now()
        ]
        
        self.db.update(sql, params)
        
        # Return the inserted ID
        return self.fetch_path_definition_by_hash(path_hash)

    def store_attempt_path(self, run_id: str, path_definition_id: int, 
                          start_node_id: int, end_node_id: int, 
                          cost: Optional[float] = None, notes: Optional[str] = None) -> int:
        """Store an attempt path record."""
        sql = '''
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id,
            cost, picked_at, tested_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id, path_definition_id, start_node_id, end_node_id,
            cost, datetime.now(), datetime.now(), notes
        ]
        
        return self.db.update(sql, params)

    def store_path_tag(self, run_id: str, path_definition_id: int, tag_type: str,
                      tag_code: str, tag: Optional[str] = None, source: str = 'SYSTEM',
                      confidence: float = 1.0, notes: Optional[str] = None) -> int:
        """Store a path tag."""
        sql = '''
        INSERT INTO tb_path_tags (
            run_id, path_definition_id, path_hash, tag_type, tag_code,
            tag, source, confidence, created_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        # Get path hash for the definition
        path_hash = self._fetch_path_hash_by_id(path_definition_id)
        
        params = [
            run_id, path_definition_id, path_hash, tag_type, tag_code,
            tag, source, confidence, datetime.now(), notes
        ]
        
        return self.db.update(sql, params)

    def fetch_path_definition_by_hash(self, path_hash: str) -> Optional[int]:
        """Fetch path definition ID by hash."""
        sql = 'SELECT id FROM tb_path_definitions WHERE path_hash = ?'
        rows = self.db.query(sql, [path_hash])
        return rows[0][0] if rows else None

    def fetch_path_definition_by_id(self, path_id: int) -> Optional[Dict[str, Any]]:
        """Fetch complete path definition by ID."""
        sql = 'SELECT * FROM tb_path_definitions WHERE id = ?'
        rows = self.db.query(sql, [path_id])
        
        if not rows:
            return None
        
        row = rows[0]
        return {
            'id': row[0],
            'path_hash': row[1],
            'source_type': row[2],
            'scope': row[3],
            'target_fab_no': row[4],
            'target_model_no': row[5],
            'target_phase_no': row[6],
            'target_toolset_no': row[7],
            'target_data_codes': self._string_to_list(row[8]),
            'target_utilities': self._string_to_list(row[9]),
            'target_references': self._string_to_list(row[10]),
            'forbidden_node_ids': self._string_to_list(row[11]),
            'node_count': row[12],
            'link_count': row[13],
            'total_length_mm': row[14],
            'coverage': row[15],
            'data_codes_scope': self._string_to_list(row[16]),
            'utilities_scope': self._string_to_list(row[17]),
            'references_scope': self._string_to_list(row[18]),
            'path_context': row[19],
            'created_at': row[20]
        }

    def fetch_run_paths(self, run_id: str) -> List[Dict[str, Any]]:
        """Fetch all paths for a specific run."""
        sql = '''
        SELECT ap.*, pd.path_hash, pd.node_count, pd.link_count, 
               pd.total_length_mm, pd.scope
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        WHERE ap.run_id = ?
        ORDER BY ap.picked_at
        '''
        
        rows = self.db.query(sql, [run_id])
        
        return [
            {
                'attempt_id': row[0],
                'run_id': row[1],
                'path_definition_id': row[2],
                'start_node_id': row[3],
                'end_node_id': row[4],
                'cost': row[5],
                'picked_at': row[6],
                'tested_at': row[7],
                'notes': row[8],
                'path_hash': row[9],
                'node_count': row[10],
                'link_count': row[11],
                'total_length_mm': row[12],
                'scope': row[13]
            }
            for row in rows
        ]

    def fetch_unique_paths_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Fetch unique path definitions for a run."""
        sql = '''
        SELECT DISTINCT pd.*
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        ORDER BY pd.created_at
        '''
        
        rows = self.db.query(sql, [run_id])
        
        return [self._row_to_path_definition(row) for row in rows]

    def fetch_paths_by_filters(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch path definitions by various filters."""
        base_sql = 'SELECT * FROM tb_path_definitions'
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY created_at DESC'
        
        rows = self.db.query(sql, params)
        return [self._row_to_path_definition(row) for row in rows]

    def fetch_path_tags(self, run_id: Optional[str] = None, 
                       path_definition_id: Optional[int] = None,
                       tag_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch path tags with optional filters."""
        filters = {}
        
        if run_id:
            filters['run_id'] = ('=', run_id)
        if path_definition_id:
            filters['path_definition_id'] = ('=', path_definition_id)
        if tag_type:
            filters['tag_type'] = ('=', tag_type)
        
        base_sql = '''
        SELECT id, run_id, path_definition_id, path_hash, tag_type,
               tag_code, tag, source, confidence, created_at, created_by, notes
        FROM tb_path_tags
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY created_at DESC'
        
        rows = self.db.query(sql, params)
        
        return [
            {
                'id': row[0],
                'run_id': row[1],
                'path_definition_id': row[2],
                'path_hash': row[3],
                'tag_type': row[4],
                'tag_code': row[5],
                'tag': row[6],
                'source': row[7],
                'confidence': row[8],
                'created_at': row[9],
                'created_by': row[10],
                'notes': row[11]
            }
            for row in rows
        ]

    def fetch_path_statistics(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Fetch path statistics for analysis."""
        filters = {}
        if run_id:
            filters['ap.run_id'] = ('=', run_id)
        
        base_sql = '''
        SELECT 
            COUNT(*) as total_attempts,
            COUNT(DISTINCT pd.path_hash) as unique_paths,
            AVG(pd.node_count) as avg_node_count,
            AVG(pd.link_count) as avg_link_count,
            AVG(pd.total_length_mm) as avg_length,
            MIN(pd.total_length_mm) as min_length,
            MAX(pd.total_length_mm) as max_length,
            SUM(pd.node_count) as total_nodes_covered,
            SUM(pd.link_count) as total_links_covered
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause
        
        rows = self.db.query(sql, params)
        
        if not rows:
            return {}
        
        row = rows[0]
        return {
            'total_attempts': row[0] or 0,
            'unique_paths': row[1] or 0,
            'avg_node_count': float(row[2]) if row[2] else 0.0,
            'avg_link_count': float(row[3]) if row[3] else 0.0,
            'avg_length': float(row[4]) if row[4] else 0.0,
            'min_length': float(row[5]) if row[5] else 0.0,
            'max_length': float(row[6]) if row[6] else 0.0,
            'total_nodes_covered': row[7] or 0,
            'total_links_covered': row[8] or 0
        }

    def fetch_paths_by_scope(self, scope: str) -> List[Dict[str, Any]]:
        """Fetch paths by scope (CONNECTIVITY, FLOW, MATERIAL)."""
        sql = '''
        SELECT * FROM tb_path_definitions 
        WHERE scope = ? 
        ORDER BY created_at DESC
        '''
        
        rows = self.db.query(sql, [scope])
        return [self._row_to_path_definition(row) for row in rows]

    def update_path_coverage(self, path_definition_id: int, coverage: float) -> int:
        """Update coverage value for a path definition."""
        sql = 'UPDATE tb_path_definitions SET coverage = ? WHERE id = ?'
        return self.db.update(sql, [coverage, path_definition_id])

    def delete_path_definition(self, path_definition_id: int) -> int:
        """Delete a path definition and related records."""
        # Delete related attempt paths first
        delete_attempts_sql = 'DELETE FROM tb_attempt_paths WHERE path_definition_id = ?'
        self.db.update(delete_attempts_sql, [path_definition_id])
        
        # Delete related tags
        delete_tags_sql = 'DELETE FROM tb_path_tags WHERE path_definition_id = ?'
        self.db.update(delete_tags_sql, [path_definition_id])
        
        # Delete path definition
        delete_path_sql = 'DELETE FROM tb_path_definitions WHERE id = ?'
        return self.db.update(delete_path_sql, [path_definition_id])

    def fetch_paths_by_utility(self, utility_nos: List[int]) -> List[Dict[str, Any]]:
        """Fetch paths that involve specific utilities."""
        if not utility_nos:
            return []
        
        utility_conditions = []
        params = []
        
        for utility_no in utility_nos:
            utility_conditions.append('utilities_scope LIKE ?')
            params.append(f'%{utility_no}%')
        
        sql = f'''
        SELECT * FROM tb_path_definitions 
        WHERE ({' OR '.join(utility_conditions)})
        ORDER BY created_at DESC
        '''
        
        rows = self.db.query(sql, params)
        return [self._row_to_path_definition(row) for row in rows]

    def fetch_paths_by_data_codes(self, data_codes: List[int]) -> List[Dict[str, Any]]:
        """Fetch paths that involve specific data codes."""
        if not data_codes:
            return []
        
        data_code_conditions = []
        params = []
        
        for data_code in data_codes:
            data_code_conditions.append('data_codes_scope LIKE ?')
            params.append(f'%{data_code}%')
        
        sql = f'''
        SELECT * FROM tb_path_definitions 
        WHERE ({' OR '.join(data_code_conditions)})
        ORDER BY created_at DESC
        '''
        
        rows = self.db.query(sql, params)
        return [self._row_to_path_definition(row) for row in rows]

    def fetch_path_context_details(self, path_definition_id: int) -> Dict[str, Any]:
        """Parse and return detailed path context information."""
        path_def = self.fetch_path_definition_by_id(path_definition_id)
        
        if not path_def or not path_def.get('path_context'):
            return {}
        
        context = path_def['path_context']
        
        # Parse path context format: "nodes:1,2,3;links:4,5,6"
        nodes = []
        links = []
        
        try:
            parts = context.split(';')
            for part in parts:
                if part.startswith('nodes:'):
                    node_str = part[6:]  # Remove 'nodes:' prefix
                    if node_str:
                        nodes = [int(x) for x in node_str.split(',') if x]
                elif part.startswith('links:'):
                    link_str = part[6:]  # Remove 'links:' prefix
                    if link_str:
                        links = [int(x) for x in link_str.split(',') if x]
        except (ValueError, IndexError):
            pass
        
        return {
            'path_definition_id': path_definition_id,
            'nodes': nodes,
            'links': links,
            'node_count': len(nodes),
            'link_count': len(links)
        }

    def _list_to_string(self, items: List) -> str:
        """Convert list to comma-separated string."""
        if not items:
            return ''
        return ','.join(str(item) for item in items)

    def _string_to_list(self, value: Optional[str]) -> List[str]:
        """Convert comma-separated string to list."""
        if not value:
            return []
        return [item.strip() for item in value.split(',') if item.strip()]

    def _build_path_context(self, nodes: List[int], links: List[int]) -> str:
        """Build path context string from nodes and links."""
        nodes_str = ','.join(map(str, nodes)) if nodes else ''
        links_str = ','.join(map(str, links)) if links else ''
        return f'nodes:{nodes_str};links:{links_str}'

    def _fetch_path_hash_by_id(self, path_definition_id: int) -> Optional[str]:
        """Fetch path hash by definition ID."""
        sql = 'SELECT path_hash FROM tb_path_definitions WHERE id = ?'
        rows = self.db.query(sql, [path_definition_id])
        return rows[0][0] if rows else None

    def _row_to_path_definition(self, row) -> Dict[str, Any]:
        """Convert database row to path definition dictionary."""
        return {
            'id': row[0],
            'path_hash': row[1],
            'source_type': row[2],
            'scope': row[3],
            'target_fab_no': row[4],
            'target_model_no': row[5],
            'target_phase_no': row[6],
            'target_toolset_no': row[7],
            'target_data_codes': self._string_to_list(row[8]),
            'target_utilities': self._string_to_list(row[9]),
            'target_references': self._string_to_list(row[10]),
            'forbidden_node_ids': self._string_to_list(row[11]),
            'node_count': row[12],
            'link_count': row[13],
            'total_length_mm': row[14],
            'coverage': row[15],
            'data_codes_scope': self._string_to_list(row[16]),
            'utilities_scope': self._string_to_list(row[17]),
            'references_scope': self._string_to_list(row[18]),
            'path_context': row[19],
            'created_at': row[20]
        }