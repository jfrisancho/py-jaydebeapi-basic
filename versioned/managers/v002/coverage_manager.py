# managers/coverage.py

from typing import Dict, Any, Optional, List, Set
from datetime import datetime
from bitarray import bitarray

from db import Database
from string_helper import StringHelper


class CoverageScope:
    """Defines the scope for coverage calculation with bitset optimization."""
    
    def __init__(self, toolset: Optional[str] = None, fab_no: Optional[int] = None,
                 phase_no: Optional[int] = None, model_no: Optional[int] = None,
                 e2e_group_no: Optional[int] = None):
        self.toolset = toolset
        self.fab_no = fab_no
        self.phase_no = phase_no
        self.model_no = model_no
        self.e2e_group_no = e2e_group_no
        
        # Bitarray tracking
        self.node_bitarray = None
        self.link_bitarray = None
        
        # ID mappings for bitarray indexing
        self.node_id_mapping = {}  # actual_node_id -> bitarray_index
        self.link_id_mapping = {}  # actual_link_id -> bitarray_index
        
        # Reverse mappings for lookups
        self.node_index_mapping = {}  # bitarray_index -> actual_node_id
        self.link_index_mapping = {}  # bitarray_index -> actual_link_id
        
        # Scope totals
        self.total_nodes = 0
        self.total_links = 0


class CoverageManager:
    """Coverage tracking with bitsets for efficient computation."""
    
    def __init__(self, db: Database):
        self.db = db
        self._scope_cache = {}  # Cache for coverage scopes

    def initialize_scope(self, fab_no: Optional[int] = None, phase_no: Optional[int] = None,
                        model_no: Optional[int] = None, e2e_group_no: Optional[int] = None,
                        toolset: Optional[str] = None) -> CoverageScope:
        """Initialize coverage scope with bitarray optimization."""
        
        # Create cache key
        cache_key = f'{fab_no}_{phase_no}_{model_no}_{e2e_group_no}_{toolset}'
        
        if cache_key in self._scope_cache:
            return self._scope_cache[cache_key]
        
        scope = CoverageScope(toolset, fab_no, phase_no, model_no, e2e_group_no)
        
        # Build node scope
        self._build_node_scope(scope)
        
        # Build link scope
        self._build_link_scope(scope)
        
        print(f'Initialized coverage scope: {scope.total_nodes} nodes, {scope.total_links} links')
        
        # Cache the scope
        self._scope_cache[cache_key] = scope
        
        return scope

    def _build_node_scope(self, scope: CoverageScope):
        """Build node scope with ID mappings and bitarray."""
        filters = self._build_scope_filters(scope)
        
        base_sql = 'SELECT DISTINCT n.id FROM nw_nodes n'
        
        # Add joins if needed for filtering
        joins = []
        if scope.toolset or scope.e2e_group_no:
            joins.append('JOIN tb_toolsets t ON n.e2e_group_no = t.e2e_group_no')
        
        if joins:
            base_sql += ' ' + ' '.join(joins)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY n.id'
        
        rows = self.db.query(sql, params)
        node_ids = [row[0] for row in rows]
        
        # Build mappings
        scope.total_nodes = len(node_ids)
        for index, node_id in enumerate(node_ids):
            scope.node_id_mapping[node_id] = index
            scope.node_index_mapping[index] = node_id
        
        # Initialize bitarray
        scope.node_bitarray = bitarray(scope.total_nodes)
        scope.node_bitarray.setall(0)

    def _build_link_scope(self, scope: CoverageScope):
        """Build link scope with ID mappings and bitarray."""
        filters = self._build_scope_filters(scope, table_prefix='l')
        
        base_sql = '''
        SELECT DISTINCT l.id 
        FROM nw_links l
        JOIN nw_nodes n1 ON l.start_node_id = n1.id
        JOIN nw_nodes n2 ON l.end_node_id = n2.id
        '''
        
        # Add joins if needed for filtering
        joins = []
        if scope.toolset or scope.e2e_group_no:
            joins.append('JOIN tb_toolsets t ON n1.e2e_group_no = t.e2e_group_no')
        
        if joins:
            base_sql += ' ' + ' '.join(joins)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY l.id'
        
        rows = self.db.query(sql, params)
        link_ids = [row[0] for row in rows]
        
        # Build mappings
        scope.total_links = len(link_ids)
        for index, link_id in enumerate(link_ids):
            scope.link_id_mapping[link_id] = index
            scope.link_index_mapping[index] = link_id
        
        # Initialize bitarray
        scope.link_bitarray = bitarray(scope.total_links)
        scope.link_bitarray.setall(0)

    def _build_scope_filters(self, scope: CoverageScope, table_prefix: str = 'n') -> Dict[str, tuple]:
        """Build filters for scope queries."""
        filters = {}
        
        if scope.fab_no:
            filters[f'{table_prefix}.fab_no'] = ('=', scope.fab_no)
        if scope.phase_no:
            filters[f'{table_prefix}.model_no'] = ('=', scope.model_no)
        if scope.model_no:
            filters[f'{table_prefix}.model_no'] = ('=', scope.model_no)
        if scope.e2e_group_no:
            filters[f'{table_prefix}.e2e_group_no'] = ('=', scope.e2e_group_no)
        
        # Toolset filtering requires join
        if scope.toolset:
            filters['t.code'] = ('=', scope.toolset)
        
        return filters

    def update_coverage_from_path(self, scope: CoverageScope, nodes: List[int], 
                                 links: List[int]) -> Dict[str, int]:
        """Update coverage bitarrays from a path and return new coverage counts."""
        new_nodes_covered = 0
        new_links_covered = 0
        
        # Update node coverage
        for node_id in nodes:
            if node_id in scope.node_id_mapping:
                index = scope.node_id_mapping[node_id]
                if not scope.node_bitarray[index]:
                    scope.node_bitarray[index] = 1
                    new_nodes_covered += 1
        
        # Update link coverage
        for link_id in links:
            if link_id in scope.link_id_mapping:
                index = scope.link_id_mapping[link_id]
                if not scope.link_bitarray[index]:
                    scope.link_bitarray[index] = 1
                    new_links_covered += 1
        
        return {
            'new_nodes_covered': new_nodes_covered,
            'new_links_covered': new_links_covered
        }

    def calculate_coverage(self, run_id: str, scope: CoverageScope) -> Dict[str, Any]:
        """Calculate comprehensive coverage metrics for a run."""
        
        # Get covered nodes and links from database
        covered_nodes = self._fetch_covered_nodes(run_id)
        covered_links = self._fetch_covered_links(run_id)
        
        # Update bitarrays with covered elements
        for node_id in covered_nodes:
            if node_id in scope.node_id_mapping:
                index = scope.node_id_mapping[node_id]
                scope.node_bitarray[index] = 1
        
        for link_id in covered_links:
            if link_id in scope.link_id_mapping:
                index = scope.link_id_mapping[link_id]
                scope.link_bitarray[index] = 1
        
        # Calculate coverage percentages
        nodes_covered_count = scope.node_bitarray.count()
        links_covered_count = scope.link_bitarray.count()
        
        node_coverage_pct = (nodes_covered_count / scope.total_nodes * 100) if scope.total_nodes > 0 else 0
        link_coverage_pct = (links_covered_count / scope.total_links * 100) if scope.total_links > 0 else 0
        
        # Overall coverage (weighted average)
        overall_coverage_pct = (node_coverage_pct + link_coverage_pct) / 2.0
        
        # Get unique paths count
        unique_paths_count = self._count_unique_paths(run_id)
        
        # Store coverage summary
        self._store_coverage_summary(run_id, scope, nodes_covered_count, links_covered_count,
                                   node_coverage_pct, link_coverage_pct, overall_coverage_pct,
                                   unique_paths_count)
        
        return {
            'total_nodes_in_scope': scope.total_nodes,
            'total_links_in_scope': scope.total_links,
            'nodes_covered': nodes_covered_count,
            'links_covered': links_covered_count,
            'node_coverage_pct': node_coverage_pct,
            'link_coverage_pct': link_coverage_pct,
            'overall_coverage_pct': overall_coverage_pct,
            'achieved_coverage': overall_coverage_pct / 100.0,
            'unique_paths_count': unique_paths_count
        }

    def get_coverage_gaps(self, scope: CoverageScope) -> Dict[str, List[int]]:
        """Identify nodes and links not yet covered."""
        uncovered_nodes = []
        uncovered_links = []
        
        # Find uncovered nodes
        for index in range(scope.total_nodes):
            if not scope.node_bitarray[index]:
                node_id = scope.node_index_mapping[index]
                uncovered_nodes.append(node_id)
        
        # Find uncovered links
        for index in range(scope.total_links):
            if not scope.link_bitarray[index]:
                link_id = scope.link_index_mapping[index]
                uncovered_links.append(link_id)
        
        return {
            'uncovered_nodes': uncovered_nodes,
            'uncovered_links': uncovered_links,
            'uncovered_node_count': len(uncovered_nodes),
            'uncovered_link_count': len(uncovered_links)
        }

    def get_coverage_hotspots(self, run_id: str, limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Identify most frequently covered nodes and links."""
        
        # Get node coverage frequency
        node_sql = '''
        SELECT node_id, COUNT(*) as coverage_count
        FROM tb_run_covered_nodes
        WHERE run_id = ?
        GROUP BY node_id
        ORDER BY coverage_count DESC
        LIMIT ?
        '''
        
        node_rows = self.db.query(node_sql, [run_id, limit])
        node_hotspots = [
            {'node_id': row[0], 'coverage_count': row[1]}
            for row in node_rows
        ]
        
        # Get link coverage frequency
        link_sql = '''
        SELECT link_id, COUNT(*) as coverage_count
        FROM tb_run_covered_links
        WHERE run_id = ?
        GROUP BY link_id
        ORDER BY coverage_count DESC
        LIMIT ?
        '''
        
        link_rows = self.db.query(link_sql, [run_id, limit])
        link_hotspots = [
            {'link_id': row[0], 'coverage_count': row[1]}
            for row in link_rows
        ]
        
        return {
            'node_hotspots': node_hotspots,
            'link_hotspots': link_hotspots
        }

    def get_coverage_progression(self, run_id: str) -> List[Dict[str, Any]]:
        """Get coverage progression over time during the run."""
        
        # Get coverage progression for nodes
        node_sql = '''
        SELECT 
            covered_at,
            COUNT(*) OVER (ORDER BY covered_at) as cumulative_nodes
        FROM tb_run_covered_nodes
        WHERE run_id = ?
        ORDER BY covered_at
        '''
        
        node_rows = self.db.query(node_sql, [run_id])
        
        # Get coverage progression for links
        link_sql = '''
        SELECT 
            covered_at,
            COUNT(*) OVER (ORDER BY covered_at) as cumulative_links
        FROM tb_run_covered_links
        WHERE run_id = ?
        ORDER BY covered_at
        '''
        
        link_rows = self.db.query(link_sql, [run_id])
        
        # Combine and create progression timeline
        progression = []
        
        # Create timeline from both node and link coverage
        all_timestamps = set()
        if node_rows:
            all_timestamps.update(row[0] for row in node_rows)
        if link_rows:
            all_timestamps.update(row[0] for row in link_rows)
        
        for timestamp in sorted(all_timestamps):
            # Find cumulative counts at this timestamp
            node_count = 0
            link_count = 0
            
            for row in node_rows:
                if row[0] <= timestamp:
                    node_count = row[1]
                else:
                    break
            
            for row in link_rows:
                if row[0] <= timestamp:
                    link_count = row[1]
                else:
                    break
            
            progression.append({
                'timestamp': timestamp,
                'cumulative_nodes': node_count,
                'cumulative_links': link_count
            })
        
        return progression

    def store_coverage_checkpoint(self, run_id: str, scope: CoverageScope, 
                                 checkpoint_name: str = 'auto') -> int:
        """Store a coverage checkpoint for rollback or analysis."""
        
        covered_nodes = []
        covered_links = []
        
        # Extract covered elements from bitarrays
        for index in range(scope.total_nodes):
            if scope.node_bitarray[index]:
                node_id = scope.node_index_mapping[index]
                covered_nodes.append(node_id)
        
        for index in range(scope.total_links):
            if scope.link_bitarray[index]:
                link_id = scope.link_index_mapping[index]
                covered_links.append(link_id)
        
        # Store checkpoint data (simplified - could use separate checkpoint table)
        coverage_data = {
            'checkpoint_name': checkpoint_name,
            'covered_nodes': covered_nodes,
            'covered_links': covered_links,
            'timestamp': datetime.now()
        }
        
        # For this implementation, we'll store in the coverage summary
        return len(covered_nodes) + len(covered_links)

    def fetch_coverage_summary(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Fetch stored coverage summary for a run."""
        sql = 'SELECT * FROM tb_run_coverage_summary WHERE run_id = ?'
        rows = self.db.query(sql, [run_id])
        
        if not rows:
            return None
        
        row = rows[0]
        return {
            'run_id': row[0],
            'total_nodes_in_scope': row[1],
            'total_links_in_scope': row[2],
            'covered_nodes': row[3],
            'covered_links': row[4],
            'node_coverage_pct': float(row[5]),
            'link_coverage_pct': float(row[6]),
            'overall_coverage_pct': float(row[7]),
            'unique_paths_count': row[8],
            'scope_filters': row[9],
            'created_at': row[10]
        }

    def compare_coverage(self, run_id1: str, run_id2: str) -> Dict[str, Any]:
        """Compare coverage between two runs."""
        summary1 = self.fetch_coverage_summary(run_id1)
        summary2 = self.fetch_coverage_summary(run_id2)
        
        if not summary1 or not summary2:
            return {}
        
        return {
            'run1_id': run_id1,
            'run2_id': run_id2,
            'node_coverage_diff': summary2['node_coverage_pct'] - summary1['node_coverage_pct'],
            'link_coverage_diff': summary2['link_coverage_pct'] - summary1['link_coverage_pct'],
            'overall_coverage_diff': summary2['overall_coverage_pct'] - summary1['overall_coverage_pct'],
            'paths_diff': summary2['unique_paths_count'] - summary1['unique_paths_count']
        }

    def _fetch_covered_nodes(self, run_id: str) -> Set[int]:
        """Fetch all covered nodes for a run."""
        sql = 'SELECT DISTINCT node_id FROM tb_run_covered_nodes WHERE run_id = ?'
        rows = self.db.query(sql, [run_id])
        return set(row[0] for row in rows)

    def _fetch_covered_links(self, run_id: str) -> Set[int]:
        """Fetch all covered links for a run."""
        sql = 'SELECT DISTINCT link_id FROM tb_run_covered_links WHERE run_id = ?'
        rows = self.db.query(sql, [run_id])
        return set(row[0] for row in rows)

    def _count_unique_paths(self, run_id: str) -> int:
        """Count unique paths for a run."""
        sql = '''
        SELECT COUNT(DISTINCT pd.path_hash)
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        return rows[0][0] if rows else 0

    def _store_coverage_summary(self, run_id: str, scope: CoverageScope, 
                               nodes_covered: int, links_covered: int,
                               node_coverage_pct: float, link_coverage_pct: float,
                               overall_coverage_pct: float, unique_paths_count: int):
        """Store coverage summary in database."""
        
        # Build scope filters description
        scope_filters = []
        if scope.fab_no:
            scope_filters.append(f'fab_no={scope.fab_no}')
        if scope.phase_no:
            scope_filters.append(f'phase_no={scope.phase_no}')
        if scope.model_no:
            scope_filters.append(f'model_no={scope.model_no}')
        if scope.e2e_group_no:
            scope_filters.append(f'e2e_group_no={scope.e2e_group_no}')
        if scope.toolset:
            scope_filters.append(f'toolset={scope.toolset}')
        
        scope_filters_str = ';'.join(scope_filters)
        
        # Insert or update coverage summary
        sql = '''
        INSERT OR REPLACE INTO tb_run_coverage_summary (
            run_id, total_nodes_in_scope, total_links_in_scope,
            covered_nodes, covered_links, node_coverage_pct,
            link_coverage_pct, overall_coverage_pct, unique_paths_count,
            scope_filters, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id, scope.total_nodes, scope.total_links,
            nodes_covered, links_covered, node_coverage_pct,
            link_coverage_pct, overall_coverage_pct, unique_paths_count,
            scope_filters_str, datetime.now()
        ]
        
        self.db.update(sql, params)