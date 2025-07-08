# managers/coverage.py

import struct
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime

from db import Database
from string_helper import StringHelper


class CoverageManager:
    """
    Coverage tracking with bitsets for efficient node and link coverage calculation.
    Uses bitarrays for memory-efficient coverage tracking of large networks.
    """
    
    def __init__(self, db: Database):
        self.db = db
        self._coverage_cache = {}  # Cache for run coverage data
        
    def initialize_coverage(self, run_id: str, scope: 'CoverageScope') -> None:
        """
        Initialize coverage tracking for a run with the given scope.
        
        Args:
            run_id: Run identifier
            scope: CoverageScope defining the universe for coverage calculation
        """
        # Build node and link mappings for the scope
        node_mapping, link_mapping = self._build_scope_mappings(scope)
        
        # Update scope with mappings
        scope.node_id_mapping = node_mapping
        scope.link_id_mapping = link_mapping
        scope.total_nodes = len(node_mapping)
        scope.total_links = len(link_mapping)
        
        # Create coverage summary record
        scope_filters = self._serialize_scope_filters(scope)
        
        sql = '''
            INSERT INTO tb_run_coverage_summary (
                run_id, total_nodes_in_scope, total_links_in_scope,
                covered_nodes, covered_links, node_coverage_pct, link_coverage_pct,
                overall_coverage_pct, unique_paths_count, scope_filters, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            scope.total_nodes,
            scope.total_links,
            0,  # covered_nodes - initial
            0,  # covered_links - initial
            0.0,  # node_coverage_pct - initial
            0.0,  # link_coverage_pct - initial
            0.0,  # overall_coverage_pct - initial
            0,  # unique_paths_count - initial
            scope_filters,
            datetime.now()
        ]
        
        self.db.update(sql, params)
        
        # Cache the scope for this run
        self._coverage_cache[run_id] = {
            'scope': scope,
            'covered_nodes': set(),
            'covered_links': set(),
            'unique_paths': 0
        }
        
        print(f'Coverage initialized for run {run_id}: {scope.total_nodes} nodes, {scope.total_links} links in scope')
    
    def update_coverage(self, run_id: str, path_nodes: List[int], path_links: List[int]) -> Dict[str, float]:
        """
        Update coverage with a new path's nodes and links.
        
        Args:
            run_id: Run identifier
            path_nodes: List of node IDs in the path
            path_links: List of link IDs in the path
            
        Returns:
            Dictionary with updated coverage percentages
        """
        # Get cached coverage data
        if run_id not in self._coverage_cache:
            self._load_coverage_cache(run_id)
        
        cache = self._coverage_cache[run_id]
        scope = cache['scope']
        
        # Filter nodes and links to those in scope
        scoped_nodes = [node_id for node_id in path_nodes if node_id in scope.node_id_mapping]
        scoped_links = [link_id for link_id in path_links if link_id in scope.link_id_mapping]
        
        # Track newly covered elements
        new_nodes = set(scoped_nodes) - cache['covered_nodes']
        new_links = set(scoped_links) - cache['covered_links']
        
        # Update cache
        cache['covered_nodes'].update(scoped_nodes)
        cache['covered_links'].update(scoped_links)
        cache['unique_paths'] += 1
        
        # Batch insert new covered nodes
        if new_nodes:
            self._insert_covered_nodes(run_id, new_nodes)
        
        # Batch insert new covered links
        if new_links:
            self._insert_covered_links(run_id, new_links)
        
        # Calculate coverage percentages
        node_coverage = len(cache['covered_nodes']) / scope.total_nodes if scope.total_nodes > 0 else 0.0
        link_coverage = len(cache['covered_links']) / scope.total_links if scope.total_links > 0 else 0.0
        overall_coverage = (node_coverage + link_coverage) / 2.0
        
        # Update summary
        self._update_coverage_summary(run_id, cache, node_coverage, link_coverage, overall_coverage)
        
        return {
            'node_coverage': node_coverage,
            'link_coverage': link_coverage,
            'overall_coverage': overall_coverage,
            'new_nodes_count': len(new_nodes),
            'new_links_count': len(new_links)
        }
    
    def calculate_coverage_percentage(self, run_id: str) -> float:
        """
        Calculate current overall coverage percentage for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Overall coverage percentage (0.0 to 1.0)
        """
        sql = '''
            SELECT overall_coverage_pct
            FROM tb_run_coverage_summary
            WHERE run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows:
            return 0.0
        
        return float(rows[0][0]) / 100.0  # Convert percentage to decimal
    
    def fetch_coverage_summary(self, run_id: str) -> Dict[str, Any]:
        """
        Fetch comprehensive coverage summary for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with coverage metrics and statistics
        """
        sql = '''
            SELECT total_nodes_in_scope, total_links_in_scope,
                   covered_nodes, covered_links, node_coverage_pct, link_coverage_pct,
                   overall_coverage_pct, unique_paths_count, scope_filters, created_at
            FROM tb_run_coverage_summary
            WHERE run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows:
            return {}
        
        row = rows[0]
        
        # Get path statistics
        path_stats = self._calculate_path_statistics(run_id)
        
        return {
            'run_id': run_id,
            'total_nodes_in_scope': row[0],
            'total_links_in_scope': row[1],
            'covered_nodes': row[2],
            'covered_links': row[3],
            'node_coverage_pct': float(row[4]),
            'link_coverage_pct': float(row[5]),
            'overall_coverage_pct': float(row[6]),
            'unique_paths_count': row[7],
            'scope_filters': row[8],
            'created_at': row[9],
            'avg_path_nodes': path_stats.get('avg_nodes'),
            'avg_path_links': path_stats.get('avg_links'),
            'avg_path_length': path_stats.get('avg_length')
        }
    
    def fetch_covered_elements(self, run_id: str, element_type: str = 'both') -> Dict[str, Set[int]]:
        """
        Fetch covered nodes and/or links for a run.
        
        Args:
            run_id: Run identifier
            element_type: 'nodes', 'links', or 'both'
            
        Returns:
            Dictionary with sets of covered element IDs
        """
        result = {}
        
        if element_type in ['nodes', 'both']:
            sql = 'SELECT node_id FROM tb_run_covered_nodes WHERE run_id = ?'
            rows = self.db.query(sql, [run_id])
            result['nodes'] = {row[0] for row in rows}
        
        if element_type in ['links', 'both']:
            sql = 'SELECT link_id FROM tb_run_covered_links WHERE run_id = ?'
            rows = self.db.query(sql, [run_id])
            result['links'] = {row[0] for row in rows}
        
        return result
    
    def calculate_incremental_coverage(self, run_id: str, candidate_nodes: List[int], 
                                     candidate_links: List[int]) -> Dict[str, Any]:
        """
        Calculate the incremental coverage that would be gained by adding a path.
        
        Args:
            run_id: Run identifier
            candidate_nodes: Candidate path nodes
            candidate_links: Candidate path links
            
        Returns:
            Dictionary with incremental coverage metrics
        """
        # Get current coverage
        covered_elements = self.fetch_covered_elements(run_id)
        covered_nodes = covered_elements.get('nodes', set())
        covered_links = covered_elements.get('links', set())
        
        # Calculate incremental elements
        new_nodes = set(candidate_nodes) - covered_nodes
        new_links = set(candidate_links) - covered_links
        
        # Get scope information
        summary = self.fetch_coverage_summary(run_id)
        total_nodes = summary.get('total_nodes_in_scope', 1)
        total_links = summary.get('total_links_in_scope', 1)
        
        # Calculate incremental percentages
        incremental_node_coverage = len(new_nodes) / total_nodes
        incremental_link_coverage = len(new_links) / total_links
        incremental_overall_coverage = (incremental_node_coverage + incremental_link_coverage) / 2.0
        
        return {
            'new_nodes_count': len(new_nodes),
            'new_links_count': len(new_links),
            'incremental_node_coverage': incremental_node_coverage,
            'incremental_link_coverage': incremental_link_coverage,
            'incremental_overall_coverage': incremental_overall_coverage,
            'efficiency_score': len(new_nodes) + len(new_links)  # Simple efficiency metric
        }
    
    def fetch_coverage_gaps(self, run_id: str, gap_type: str = 'both') -> Dict[str, List[int]]:
        """
        Identify gaps in coverage (uncovered nodes/links in scope).
        
        Args:
            run_id: Run identifier
            gap_type: 'nodes', 'links', or 'both'
            
        Returns:
            Dictionary with lists of uncovered element IDs
        """
        if run_id not in self._coverage_cache:
            self._load_coverage_cache(run_id)
        
        cache = self._coverage_cache[run_id]
        scope = cache['scope']
        
        result = {}
        
        if gap_type in ['nodes', 'both']:
            all_nodes_in_scope = set(scope.node_id_mapping.keys())
            uncovered_nodes = all_nodes_in_scope - cache['covered_nodes']
            result['nodes'] = list(uncovered_nodes)
        
        if gap_type in ['links', 'both']:
            all_links_in_scope = set(scope.link_id_mapping.keys())
            uncovered_links = all_links_in_scope - cache['covered_links']
            result['links'] = list(uncovered_links)
        
        return result
    
    def reset_coverage(self, run_id: str) -> None:
        """
        Reset coverage tracking for a run.
        
        Args:
            run_id: Run identifier
        """
        # Delete covered elements
        self.db.update('DELETE FROM tb_run_covered_nodes WHERE run_id = ?', [run_id])
        self.db.update('DELETE FROM tb_run_covered_links WHERE run_id = ?', [run_id])
        
        # Reset summary
        sql = '''
            UPDATE tb_run_coverage_summary 
            SET covered_nodes = 0, covered_links = 0,
                node_coverage_pct = 0.0, link_coverage_pct = 0.0,
                overall_coverage_pct = 0.0, unique_paths_count = 0
            WHERE run_id = ?
        '''
        self.db.update(sql, [run_id])
        
        # Clear cache
        if run_id in self._coverage_cache:
            cache = self._coverage_cache[run_id]
            cache['covered_nodes'].clear()
            cache['covered_links'].clear()
            cache['unique_paths'] = 0
    
    def compare_coverage(self, run_id_1: str, run_id_2: str) -> Dict[str, Any]:
        """
        Compare coverage between two runs.
        
        Args:
            run_id_1: First run identifier
            run_id_2: Second run identifier
            
        Returns:
            Dictionary with coverage comparison metrics
        """
        summary_1 = self.fetch_coverage_summary(run_id_1)
        summary_2 = self.fetch_coverage_summary(run_id_2)
        
        covered_1 = self.fetch_covered_elements(run_id_1)
        covered_2 = self.fetch_covered_elements(run_id_2)
        
        # Calculate overlaps and differences
        node_overlap = covered_1.get('nodes', set()) & covered_2.get('nodes', set())
        link_overlap = covered_1.get('links', set()) & covered_2.get('links', set())
        
        nodes_only_1 = covered_1.get('nodes', set()) - covered_2.get('nodes', set())
        nodes_only_2 = covered_2.get('nodes', set()) - covered_1.get('nodes', set())
        
        links_only_1 = covered_1.get('links', set()) - covered_2.get('links', set())
        links_only_2 = covered_2.get('links', set()) - covered_1.get('links', set())
        
        return {
            'run_1': {
                'id': run_id_1,
                'overall_coverage': summary_1.get('overall_coverage_pct', 0.0),
                'unique_nodes': len(covered_1.get('nodes', set())),
                'unique_links': len(covered_1.get('links', set()))
            },
            'run_2': {
                'id': run_id_2,
                'overall_coverage': summary_2.get('overall_coverage_pct', 0.0),
                'unique_nodes': len(covered_2.get('nodes', set())),
                'unique_links': len(covered_2.get('links', set()))
            },
            'overlap': {
                'nodes': len(node_overlap),
                'links': len(link_overlap)
            },
            'differences': {
                'nodes_only_1': len(nodes_only_1),
                'nodes_only_2': len(nodes_only_2),
                'links_only_1': len(links_only_1),
                'links_only_2': len(links_only_2)
            }
        }
    
    def _build_scope_mappings(self, scope: 'CoverageScope') -> Tuple[Dict[int, int], Dict[int, int]]:
        """
        Build node and link ID mappings for the given scope.
        
        Args:
            scope: CoverageScope with filter criteria
            
        Returns:
            Tuple of (node_mapping, link_mapping) dictionaries
        """
        # Build filter conditions
        filters = {}
        
        if scope.fab_no is not None:
            filters['fab_no'] = ('=', scope.fab_no)
        if scope.phase_no is not None:
            filters['phase_no'] = ('=', scope.phase_no)
        if scope.model_no is not None:
            filters['model_no'] = ('=', scope.model_no)
        if scope.e2e_group_no is not None:
            filters['e2e_group_no'] = ('=', scope.e2e_group_no)
        
        # Fetch nodes in scope
        node_where, node_params = StringHelper.build_where_clause(filters)
        node_sql = f'SELECT id FROM nw_nodes{node_where} ORDER BY id'
        node_rows = self.db.query(node_sql, node_params)
        
        # Create node mapping (node_id -> bitarray_index)
        node_mapping = {row[0]: idx for idx, row in enumerate(node_rows)}
        
        # Fetch links in scope
        link_where, link_params = StringHelper.build_where_clause(filters)
        link_sql = f'SELECT id FROM nw_links{link_where} ORDER BY id'
        link_rows = self.db.query(link_sql, link_params)
        
        # Create link mapping (link_id -> bitarray_index)
        link_mapping = {row[0]: idx for idx, row in enumerate(link_rows)}
        
        return node_mapping, link_mapping
    
    def _serialize_scope_filters(self, scope: 'CoverageScope') -> str:
        """Serialize scope filters to string for storage."""
        filters = {}
        
        if scope.toolset:
            filters['toolset'] = scope.toolset
        if scope.fab_no is not None:
            filters['fab_no'] = scope.fab_no
        if scope.phase_no is not None:
            filters['phase_no'] = scope.phase_no
        if scope.model_no is not None:
            filters['model_no'] = scope.model_no
        if scope.e2e_group_no is not None:
            filters['e2e_group_no'] = scope.e2e_group_no
        
        return str(filters)
    
    def _load_coverage_cache(self, run_id: str) -> None:
        """Load coverage cache from database."""
        # Get covered elements
        covered_elements = self.fetch_covered_elements(run_id)
        
        # Get summary info
        summary = self.fetch_coverage_summary(run_id)
        
        # Reconstruct scope (simplified - you may need more detailed reconstruction)
        scope = CoverageScope(
            total_nodes=summary.get('total_nodes_in_scope', 0),
            total_links=summary.get('total_links_in_scope', 0)
        )
        
        # Cache the data
        self._coverage_cache[run_id] = {
            'scope': scope,
            'covered_nodes': covered_elements.get('nodes', set()),
            'covered_links': covered_elements.get('links', set()),
            'unique_paths': summary.get('unique_paths_count', 0)
        }
    
    def _insert_covered_nodes(self, run_id: str, node_ids: Set[int]) -> None:
        """Batch insert covered nodes."""
        if not node_ids:
            return
        
        # Prepare batch insert
        values = []
        for node_id in node_ids:
            values.extend([run_id, node_id, datetime.now()])
        
        # Create batch insert SQL
        placeholders = ','.join(['(?, ?, ?)'] * len(node_ids))
        sql = f'''
            INSERT INTO tb_run_covered_nodes (run_id, node_id, covered_at)
            VALUES {placeholders}
        '''
        
        self.db.update(sql, values)
    
    def _insert_covered_links(self, run_id: str, link_ids: Set[int]) -> None:
        """Batch insert covered links."""
        if not link_ids:
            return
        
        # Prepare batch insert
        values = []
        for link_id in link_ids:
            values.extend([run_id, link_id, datetime.now()])
        
        # Create batch insert SQL
        placeholders = ','.join(['(?, ?, ?)'] * len(link_ids))
        sql = f'''
            INSERT INTO tb_run_covered_links (run_id, link_id, covered_at)
            VALUES {placeholders}
        '''
        
        self.db.update(sql, values)
    
    def _update_coverage_summary(self, run_id: str, cache: Dict, 
                                node_coverage: float, link_coverage: float, overall_coverage: float) -> None:
        """Update coverage summary in database."""
        sql = '''
            UPDATE tb_run_coverage_summary
            SET covered_nodes = ?, covered_links = ?,
                node_coverage_pct = ?, link_coverage_pct = ?, overall_coverage_pct = ?,
                unique_paths_count = ?
            WHERE run_id = ?
        '''
        
        params = [
            len(cache['covered_nodes']),
            len(cache['covered_links']),
            node_coverage * 100.0,  # Convert to percentage
            link_coverage * 100.0,
            overall_coverage * 100.0,
            cache['unique_paths'],
            run_id
        ]
        
        self.db.update(sql, params)
    
    def _calculate_path_statistics(self, run_id: str) -> Dict[str, Optional[float]]:
        """Calculate path-related statistics for a run."""
        sql = '''
            SELECT AVG(CAST(pd.node_count AS FLOAT)), 
                   AVG(CAST(pd.link_count AS FLOAT)), 
                   AVG(pd.total_length_mm)
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows or not rows[0][0]:
            return {'avg_nodes': None, 'avg_links': None, 'avg_length': None}
        
        row = rows[0]
        return {
            'avg_nodes': float(row[0]) if row[0] else None,
            'avg_links': float(row[1]) if row[1] else None,
            'avg_length': float(row[2]) if row[2] else None
        }
    
    def export_coverage_bitset(self, run_id: str) -> Dict[str, bytes]:
        """
        Export coverage as bitsets for external analysis.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with 'nodes' and 'links' bitsets as bytes
        """
        if run_id not in self._coverage_cache:
            self._load_coverage_cache(run_id)
        
        cache = self._coverage_cache[run_id]
        scope = cache['scope']
        
        # Create bitsets
        node_bitset = bytearray((scope.total_nodes + 7) // 8)  # Ceiling division for byte count
        link_bitset = bytearray((scope.total_links + 7) // 8)
        
        # Set bits for covered nodes
        for node_id in cache['covered_nodes']:
            if node_id in scope.node_id_mapping:
                bit_index = scope.node_id_mapping[node_id]
                byte_index = bit_index // 8
                bit_offset = bit_index % 8
                node_bitset[byte_index] |= (1 << bit_offset)
        
        # Set bits for covered links
        for link_id in cache['covered_links']:
            if link_id in scope.link_id_mapping:
                bit_index = scope.link_id_mapping[link_id]
                byte_index = bit_index // 8
                bit_offset = bit_index % 8
                link_bitset[byte_index] |= (1 << bit_offset)
        
        return {
            'nodes': bytes(node_bitset),
            'links': bytes(link_bitset)
        }