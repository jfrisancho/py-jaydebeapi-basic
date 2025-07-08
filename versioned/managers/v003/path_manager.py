# managers/path.py

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from db import Database
from string_helper import StringHelper


class PathManager:
    """Path storage and retrieval manager."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_definition(self, path_result: 'PathResult', path_hash: str, 
                             config: 'RandomRunConfig') -> int:
        """Store a path definition and return the path definition ID."""
        
        sql = '''
            INSERT INTO tb_path_definitions (
                path_hash, source_type, scope, target_fab_no, target_model_no,
                target_phase_no, target_toolset_no, target_data_codes,
                target_utilities, target_references, node_count, link_count,
                total_length_mm, coverage, data_codes_scope, utilities_scope,
                references_scope, path_context, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        # Convert lists to comma-separated strings
        data_codes_str = ','.join(map(str, path_result.data_codes)) if path_result.data_codes else ''
        utilities_str = ','.join(map(str, path_result.utility_nos)) if path_result.utility_nos else ''
        references_str = ','.join(path_result.references) if path_result.references else ''
        toolset_nos_str = ','.join(map(str, path_result.toolset_nos)) if path_result.toolset_nos else ''
        
        # Create path context (nodes and links sequence)
        path_context = self._create_path_context(path_result.nodes, path_result.links)
        
        # Calculate coverage (simplified - actual coverage calculated by CoverageManager)
        coverage = len(path_result.nodes) + len(path_result.links)
        
        params = [
            path_hash,
            'RANDOM',  # source_type
            'CONNECTIVITY',  # scope
            config.fab_no,
            config.model_no,
            config.phase_no,
            config.e2e_group_no,
            data_codes_str,
            utilities_str,
            references_str,
            len(path_result.nodes),
            len(path_result.links),
            path_result.total_length_mm,
            coverage,
            data_codes_str,  # data_codes_scope (same as target for random)
            utilities_str,   # utilities_scope
            references_str,  # references_scope
            path_context,
            datetime.now()
        ]
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            self.db._conn.commit()
            
            # Get the auto-generated ID
            cur.execute('SELECT LAST_INSERT_ID()')
            return cur.fetchone()[0]
    
    def fetch_path_definition_by_hash(self, path_hash: str) -> Optional[Dict]:
        """Fetch path definition by hash."""
        
        sql = '''
            SELECT id, path_hash, source_type, scope, target_fab_no, target_model_no,
                   target_phase_no, target_toolset_no, target_data_codes,
                   target_utilities, target_references, node_count, link_count,
                   total_length_mm, coverage, data_codes_scope, utilities_scope,
                   references_scope, path_context, created_at
            FROM tb_path_definitions
            WHERE path_hash = ?
        '''
        
        rows = self.db.query(sql, [path_hash])
        
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
            'target_data_codes': row[8],
            'target_utilities': row[9],
            'target_references': row[10],
            'node_count': row[11],
            'link_count': row[12],
            'total_length_mm': row[13],
            'coverage': row[14],
            'data_codes_scope': row[15],
            'utilities_scope': row[16],
            'references_scope': row[17],
            'path_context': row[18],
            'created_at': row[19]
        }
    
    def fetch_path_definition_by_id(self, path_def_id: int) -> Optional[Dict]:
        """Fetch path definition by ID."""
        
        sql = '''
            SELECT id, path_hash, source_type, scope, target_fab_no, target_model_no,
                   target_phase_no, target_toolset_no, target_data_codes,
                   target_utilities, target_references, node_count, link_count,
                   total_length_mm, coverage, data_codes_scope, utilities_scope,
                   references_scope, path_context, created_at
            FROM tb_path_definitions
            WHERE id = ?
        '''
        
        rows = self.db.query(sql, [path_def_id])
        
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
            'target_data_codes': row[8],
            'target_utilities': row[9],
            'target_references': row[10],
            'node_count': row[11],
            'link_count': row[12],
            'total_length_mm': row[13],
            'coverage': row[14],
            'data_codes_scope': row[15],
            'utilities_scope': row[16],
            'references_scope': row[17],
            'path_context': row[18],
            'created_at': row[19]
        }
    
    def fetch_paths_for_run(self, run_id: str) -> List[Dict]:
        """Fetch all paths for a specific run."""
        
        sql = '''
            SELECT DISTINCT pd.id, pd.path_hash, pd.source_type, pd.scope, 
                   pd.target_fab_no, pd.target_model_no, pd.target_phase_no,
                   pd.target_toolset_no, pd.target_data_codes, pd.target_utilities,
                   pd.target_references, pd.node_count, pd.link_count,
                   pd.total_length_mm, pd.coverage, pd.data_codes_scope,
                   pd.utilities_scope, pd.references_scope, pd.path_context,
                   pd.created_at
            FROM tb_path_definitions pd
            INNER JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
            ORDER BY pd.created_at
        '''
        
        rows = self.db.query(sql, [run_id])
        
        return [
            {
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
                'data_codes_scope': row[15],
                'utilities_scope': row[16],
                'references_scope': row[17],
                'path_context': row[18],
                'created_at': row[19]
            }
            for row in rows
        ]
    
    def calculate_path_statistics(self, run_id: str) -> 'PathStatistics':
        """Calculate aggregated path statistics for a run."""
        
        sql = '''
            SELECT 
                COUNT(*) as path_count,
                AVG(CAST(pd.node_count AS FLOAT)) as avg_nodes,
                AVG(CAST(pd.link_count AS FLOAT)) as avg_links,
                AVG(pd.total_length_mm) as avg_length,
                MIN(pd.total_length_mm) as min_length,
                MAX(pd.total_length_mm) as max_length,
                SUM(pd.node_count) as total_nodes,
                SUM(pd.link_count) as total_links
            FROM tb_path_definitions pd
            INNER JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        
        if not rows or not rows[0][0]:
            return PathStatistics()
        
        row = rows[0]
        return PathStatistics(
            path_count=row[0],
            avg_nodes=row[1],
            avg_links=row[2],
            avg_length=row[3],
            min_length=row[4],
            max_length=row[5],
            total_nodes=row[6],
            total_links=row[7]
        )
    
    def fetch_path_nodes_and_links(self, path_context: str) -> Dict[str, List[int]]:
        """Extract nodes and links from path context string."""
        
        # Parse path context string to extract nodes and links
        # Format expected: "nodes:1,2,3|links:10,11,12"
        
        nodes = []
        links = []
        
        if path_context:
            parts = path_context.split('|')
            
            for part in parts:
                if part.startswith('nodes:'):
                    node_str = part.replace('nodes:', '')
                    if node_str:
                        nodes = [int(x.strip()) for x in node_str.split(',') if x.strip()]
                elif part.startswith('links:'):
                    link_str = part.replace('links:', '')
                    if link_str:
                        links = [int(x.strip()) for x in link_str.split(',') if x.strip()]
        
        return {'nodes': nodes, 'links': links}
    
    def fetch_path_utilities_and_references(self, path_def: Dict) -> Dict[str, List]:
        """Extract utilities and references from path definition."""
        
        utilities = []
        references = []
        data_codes = []
        
        # Parse utilities
        if path_def.get('utilities_scope'):
            utility_str = path_def['utilities_scope']
            if utility_str:
                utilities = [int(x.strip()) for x in utility_str.split(',') if x.strip().isdigit()]
        
        # Parse references
        if path_def.get('references_scope'):
            ref_str = path_def['references_scope']
            if ref_str:
                references = [x.strip() for x in ref_str.split(',') if x.strip()]
        
        # Parse data codes
        if path_def.get('data_codes_scope'):
            data_str = path_def['data_codes_scope']
            if data_str:
                data_codes = [int(x.strip()) for x in data_str.split(',') if x.strip().isdigit()]
        
        return {
            'utilities': utilities,
            'references': references,
            'data_codes': data_codes
        }
    
    def store_path_tags(self, run_id: str, path_def_id: int, tags: List[Dict]):
        """Store path tags for categorization and analysis."""
        
        if not tags:
            return
        
        sql = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, path_hash, tag_type, tag_code,
                tag, source, confidence, created_at, created_by, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for tag in tags:
            params = [
                run_id,
                path_def_id,
                tag.get('path_hash'),
                tag.get('tag_type'),
                tag.get('tag_code'),
                tag.get('tag'),
                tag.get('source', 'SYSTEM'),
                tag.get('confidence', 1.0),
                datetime.now(),
                tag.get('created_by', 'SYSTEM'),
                tag.get('notes')
            ]
            
            self.db.update(sql, params)
    
    def fetch_path_tags(self, run_id: str, path_def_id: Optional[int] = None) -> List[Dict]:
        """Fetch path tags for analysis."""
        
        filters = {'run_id': ('=', run_id)}
        
        if path_def_id:
            filters['path_definition_id'] = ('=', path_def_id)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id, run_id, path_definition_id, path_hash, tag_type,
                   tag_code, tag, source, confidence, created_at, created_by, notes
            FROM tb_path_tags
            {where_clause}
            ORDER BY created_at
        '''
        
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
    
    def update_path_coverage(self, path_def_id: int, coverage: float):
        """Update path definition coverage value."""
        
        sql = 'UPDATE tb_path_definitions SET coverage = ? WHERE id = ?'
        self.db.update(sql, [coverage, path_def_id])
    
    def fetch_paths_by_criteria(self, criteria: Dict) -> List[Dict]:
        """Fetch paths matching specific criteria."""
        
        filters = {}
        
        if criteria.get('source_type'):
            filters['source_type'] = ('=', criteria['source_type'])
        
        if criteria.get('scope'):
            filters['scope'] = ('=', criteria['scope'])
        
        if criteria.get('target_fab_no'):
            filters['target_fab_no'] = ('=', criteria['target_fab_no'])
        
        if criteria.get('target_model_no'):
            filters['target_model_no'] = ('=', criteria['target_model_no'])
        
        if criteria.get('target_phase_no'):
            filters['target_phase_no'] = ('=', criteria['target_phase_no'])
        
        if criteria.get('min_node_count'):
            filters['node_count'] = ('>=', criteria['min_node_count'])
        
        if criteria.get('max_node_count'):
            filters['node_count'] = ('<=', criteria['max_node_count'])
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id, path_hash, source_type, scope, target_fab_no, target_model_no,
                   target_phase_no, target_toolset_no, target_data_codes,
                   target_utilities, target_references, node_count, link_count,
                   total_length_mm, coverage, data_codes_scope, utilities_scope,
                   references_scope, path_context, created_at
            FROM tb_path_definitions
            {where_clause}
            ORDER BY created_at DESC
        '''
        
        rows = self.db.query(sql, params)
        
        return [
            {
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
                'data_codes_scope': row[15],
                'utilities_scope': row[16],
                'references_scope': row[17],
                'path_context': row[18],
                'created_at': row[19]
            }
            for row in rows
        ]
    
    def _create_path_context(self, nodes: List[int], links: List[int]) -> str:
        """Create path context string from nodes and links."""
        
        nodes_str = ','.join(map(str, nodes)) if nodes else ''
        links_str = ','.join(map(str, links)) if links else ''
        
        return f'nodes:{nodes_str}|links:{links_str}'


@dataclass
class PathStatistics:
    """Path statistics for a run."""
    path_count: int = 0
    avg_nodes: Optional[float] = None
    avg_links: Optional[float] = None
    avg_length: Optional[float] = None
    min_length: Optional[float] = None
    max_length: Optional[float] = None
    total_nodes: int = 0
    total_links: int = 0