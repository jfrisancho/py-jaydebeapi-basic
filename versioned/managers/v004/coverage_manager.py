# managers/coverage.py

from datetime import datetime
from typing import Optional, list, dict
import array

from db import Database
from string_helper import StringHelper

class CoverageManager:
    """Coverage tracking with bitsets for efficient memory usage."""
    
    def __init__(self, db: Database):
        self.db = db
        self._coverage_scopes = {}  # Cache for coverage scopes
        self._node_bitsets = {}     # Cache for node coverage bitsets
        self._link_bitsets = {}     # Cache for link coverage bitsets
    
    def initialize_coverage_scope(self, fab_no: Optional[int] = None, 
                                phase_no: Optional[int] = None,
                                model_no: Optional[int] = None, 
                                e2e_group_no: Optional[int] = None) -> 'CoverageScope':
        """Initialize coverage scope and create bitsets for tracking."""
        scope_key = f'{fab_no}_{phase_no}_{model_no}_{e2e_group_no}'
        
        # Check cache first
        if scope_key in self._coverage_scopes:
            return self._coverage_scopes[scope_key]
        
        # Fetch nodes and links in scope
        nodes_in_scope = self._fetch_nodes_in_scope(fab_no, phase_no, model_no, e2e_group_no)
        links_in_scope = self._fetch_links_in_scope(fab_no, phase_no, model_no, e2e_group_no)
        
        # Create ID mappings for bitset indexing
        node_id_mapping = {node_id: idx for idx, node_id in enumerate(nodes_in_scope)}
        link_id_mapping = {link_id: idx for idx, link_id in enumerate(links_in_scope)}
        
        # Create coverage scope
        coverage_scope = CoverageScope(
            fab_no=fab_no,
            phase_no=phase_no,
            model_no=model_no,
            e2e_group_no=e2e_group_no,
            total_nodes=len(nodes_in_scope),
            total_links=len(links_in_scope),
            node_id_mapping=node_id_mapping,
            link_id_mapping=link_id_mapping
        )
        
        # Initialize bitsets for this scope
        self._node_bitsets[scope_key] = array.array('b', [0] * len(nodes_in_scope))
        self._link_bitsets[scope_key] = array.array('b', [0] * len(links_in_scope))
        
        # Cache the scope
        self._coverage_scopes[scope_key] = coverage_scope
        
        print(f'Initialized coverage scope: {coverage_scope.total_nodes} nodes, {coverage_scope.total_links} links')
        
        return coverage_scope
    
    def update_coverage(self, run_id: str, nodes: list[int], links: list[int]):
        """Update coverage tracking with new path nodes and links."""
        # Get scope for this run
        scope = self._get_run_scope(run_id)
        if not scope:
            return
        
        scope_key = f'{scope.fab_no}_{scope.phase_no}_{scope.model_no}_{scope.e2e_group_no}'
        
        # Update node coverage
        node_bitset = self._node_bitsets.get(scope_key)
        if node_bitset and scope.node_id_mapping:
            for node_id in nodes:
                if node_id in scope.node_id_mapping:
                    idx = scope.node_id_mapping[node_id]
                    if idx < len(node_bitset):
                        node_bitset[idx] = 1
        
        # Update link coverage
        link_bitset = self._link_bitsets.get(scope_key)
        if link_bitset and scope.link_id_mapping:
            for link_id in links:
                if link_id in scope.link_id_mapping:
                    idx = scope.link_id_mapping[link_id]
                    if idx < len(link_bitset):
                        link_bitset[idx] = 1
        
        # Store covered nodes and links in database
        self._store_covered_nodes(run_id, nodes)
        self._store_covered_links(run_id, links)
    
    def calculate_current_coverage(self, run_id: str, scope: 'CoverageScope') -> float:
        """Calculate current coverage percentage for a run."""
        if scope.total_nodes == 0 and scope.total_links == 0:
            return 0.0
        
        scope_key = f'{scope.fab_no}_{scope.phase_no}_{scope.model_no}_{scope.e2e_group_no}'
        
        # Count covered nodes
        covered_nodes = 0
        node_bitset = self._node_bitsets.get(scope_key)
        if node_bitset:
            covered_nodes = sum(node_bitset)
        
        # Count covered links
        covered_links = 0
        link_bitset = self._link_bitsets.get(scope_key)
        if link_bitset:
            covered_links = sum(link_bitset)
        
        # Calculate overall coverage (weighted average of nodes and links)
        total_elements = scope.total_nodes + scope.total_links
        covered_elements = covered_nodes + covered_links
        
        coverage = covered_elements / total_elements if total_elements > 0 else 0.0
        
        # Update coverage summary in database
        self._update_coverage_summary(run_id, scope, covered_nodes, covered_links, coverage)
        
        return coverage
    
    def fetch_coverage_metrics(self, run_id: str) -> dict:
        """Fetch detailed coverage metrics for a run."""
        sql = '''
            SELECT 
                total_nodes_in_scope,
                total_links_in_scope,
                covered_nodes,
                covered_links,
                node_coverage_pct,
                link_coverage_pct,
                overall_coverage_pct,
                unique_paths_count
            FROM tb_run_coverage_summary
            WHERE run_id = ?
        '''
        
        result = self.db.query(sql, [run_id])
        if result:
            row = result[0]
            return {
                'total_nodes_in_scope': row[0],
                'total_links_in_scope': row[1],
                'covered_nodes': row[2],
                'covered_links': row[3],
                'node_coverage_pct': row[4],
                'link_coverage_pct': row[5],
                'achieved_coverage': row[6] / 100.0 if row[6] else 0.0,  # Convert to decimal
                'unique_paths_count': row[7]
            }
        
        # If no summary exists, calculate from run data
        return self._calculate_coverage_from_run_data(run_id)
    
    def fetch_coverage_gaps(self, run_id: str, scope: 'CoverageScope') -> dict:
        """Identify uncovered nodes and links."""
        scope_key = f'{scope.fab_no}_{scope.phase_no}_{scope.model_no}_{scope.e2e_group_no}'
        
        uncovered_nodes = []
        uncovered_links = []
        
        # Find uncovered nodes
        node_bitset = self._node_bitsets.get(scope_key)
        if node_bitset and scope.node_id_mapping:
            for node_id, idx in scope.node_id_mapping.items():
                if idx < len(node_bitset) and node_bitset[idx] == 0:
                    uncovered_nodes.append(node_id)
        
        # Find uncovered links
        link_bitset = self._link_bitsets.get(scope_key)
        if link_bitset and scope.link_id_mapping:
            for link_id, idx in scope.link_id_mapping.items():
                if idx < len(link_bitset) and link_bitset[idx] == 0:
                    uncovered_links.append(link_id)
        
        return {
            'uncovered_nodes': uncovered_nodes,
            'uncovered_links': uncovered_links,
            'uncovered_node_count': len(uncovered_nodes),
            'uncovered_link_count': len(uncovered_links)
        }
    
    def fetch_coverage_evolution(self, run_id: str) -> list[dict]:
        """Fetch coverage evolution over time during a run."""
        # Get coverage at different time points by counting cumulative covered elements
        sql = '''
            SELECT 
                covered_at,
                COUNT(*) OVER (ORDER BY covered_at) as cumulative_nodes
            FROM tb_run_covered_nodes
            WHERE run_id = ?
            ORDER BY covered_at
        '''
        
        node_results = self.db.query(sql, [run_id])
        
        sql = '''
            SELECT 
                covered_at,
                COUNT(*) OVER (ORDER BY covered_at) as cumulative_links
            FROM tb_run_covered_links
            WHERE run_id = ?
            ORDER BY covered_at
        '''
        
        link_results = self.db.query(sql, [run_id])
        
        # Combine and calculate coverage percentages
        evolution = []
        scope = self._get_run_scope(run_id)
        
        # Merge node and link data by timestamp
        timestamps = set()
        if node_results:
            timestamps.update([row[0] for row in node_results])
        if link_results:
            timestamps.update([row[0] for row in link_results])
        
        for timestamp in sorted(timestamps):
            # Find cumulative counts at this timestamp
            nodes_count = 0
            links_count = 0
            
            for row in node_results:
                if row[0] <= timestamp:
                    nodes_count = row[1]
            
            for row in link_results:
                if row[0] <= timestamp:
                    links_count = row[1]
            
            # Calculate coverage
            total_elements = (scope.total_nodes + scope.total_links) if scope else 1
            coverage_pct = ((nodes_count + links_count) / total_elements) * 100.0
            
            evolution.append({
                'timestamp': timestamp,
                'cumulative_nodes': nodes_count,
                'cumulative_links': links_count,
                'coverage_pct': coverage_pct
            })
        
        return evolution
    
    def clear_run_coverage(self, run_id: str):
        """Clear coverage data for a specific run."""
        # Delete coverage records
        self.db.update('DELETE FROM tb_run_covered_nodes WHERE run_id = ?', [run_id])
        self.db.update('DELETE FROM tb_run_covered_links WHERE run_id = ?', [run_id])
        self.db.update('DELETE FROM tb_run_coverage_summary WHERE run_id = ?', [run_id])
        
        # Reset bitsets if this was the only run using the scope
        scope = self._get_run_scope(run_id)
        if scope:
            scope_key = f'{scope.fab_no}_{scope.phase_no}_{scope.model_no}_{scope.e2e_group_no}'
            
            # Check if other runs use this scope
            other_runs = self._check_other_runs_with_scope(scope, run_id)
            if not other_runs:
                # Reset bitsets
                if scope_key in self._node_bitsets:
                    self._node_bitsets[scope_key] = array.array('b', [0] * scope.total_nodes)
                if scope_key in self._link_bitsets:
                    self._link_bitsets[scope_key] = array.array('b', [0] * scope.total_links)
    
    def _fetch_nodes_in_scope(self, fab_no: Optional[int], phase_no: Optional[int], 
                            model_no: Optional[int], e2e_group_no: Optional[int]) -> list[int]:
        """Fetch all node IDs within the specified scope."""
        filters = {}
        
        if fab_no:
            filters['fab_no'] = ('=', fab_no)
        if phase_no:
            filters['model_no'] = ('=', phase_no)  # Assuming phase maps to model in nw_nodes
        if model_no:
            filters['model_no'] = ('=', model_no)
        if e2e_group_no:
            filters['e2e_group_no'] = ('=', e2e_group_no)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id
            FROM nw_nodes
            {where_clause}
            ORDER BY id
        '''
        
        results = self.db.query(sql, params)
        return [row[0] for row in results]
    
    def _fetch_links_in_scope(self, fab_no: Optional[int], phase_no: Optional[int],
                            model_no: Optional[int], e2e_group_no: Optional[int]) -> list[int]:
        """Fetch all link IDs within the specified scope."""
        # Get nodes in scope first
        node_ids = self._fetch_nodes_in_scope(fab_no, phase_no, model_no, e2e_group_no)
        
        if not node_ids:
            return []
        
        # Get links that connect nodes in scope
        node_placeholders = ','.join(['?'] * len(node_ids))
        sql = f'''
            SELECT DISTINCT id
            FROM nw_links
            WHERE start_node_id IN ({node_placeholders})
               OR end_node_id IN ({node_placeholders})
            ORDER BY id
        '''
        
        params = node_ids + node_ids  # Double the list for both IN clauses
        results = self.db.query(sql, params)
        return [row[0] for row in results]
    
    def _get_run_scope(self, run_id: str) -> Optional['CoverageScope']:
        """Get coverage scope for a specific run."""
        sql = '''
            SELECT fab_no, phase_no, model_no, toolset
            FROM tb_runs
            WHERE id = ?
        '''
        
        result = self.db.query(sql, [run_id])
        if result:
            row = result[0]
            fab_no, phase_no, model_no, toolset = row
            
            # Convert toolset to e2e_group_no if needed
            e2e_group_no = self._resolve_toolset_to_e2e_group(toolset) if toolset else None
            
            scope_key = f'{fab_no}_{phase_no}_{model_no}_{e2e_group_no}'
            return self._coverage_scopes.get(scope_key)
        
        return None
    
    def _resolve_toolset_to_e2e_group(self, toolset: str) -> Optional[int]:
        """Resolve toolset code to e2e_group_no."""
        sql = 'SELECT e2e_group_no FROM tb_toolsets WHERE code = ?'
        result = self.db.query(sql, [toolset])
        return result[0][0] if result else None
    
    def _store_covered_nodes(self, run_id: str, nodes: list[int]):
        """Store covered nodes for a run (avoid duplicates)."""
        if not nodes:
            return
        
        # Check existing covered nodes to avoid duplicates
        existing_sql = f'''
            SELECT node_id FROM tb_run_covered_nodes 
            WHERE run_id = ? AND node_id IN ({','.join(['?'] * len(nodes))})
        '''
        existing_result = self.db.query(existing_sql, [run_id] + nodes)
        existing_nodes = {row[0] for row in existing_result} if existing_result else set()
        
        # Insert only new nodes
        new_nodes = [node_id for node_id in nodes if node_id not in existing_nodes]
        
        for node_id in new_nodes:
            sql = '''
                INSERT INTO tb_run_covered_nodes (run_id, node_id, covered_at)
                VALUES (?, ?, ?)
            '''
            params = [run_id, node_id, StringHelper.datetime_to_sqltimestamp(datetime.now())]
            self.db.update(sql, params)
    
    def _store_covered_links(self, run_id: str, links: list[int]):
        """Store covered links for a run (avoid duplicates)."""
        if not links:
            return
        
        # Check existing covered links to avoid duplicates
        existing_sql = f'''
            SELECT link_id FROM tb_run_covered_links 
            WHERE run_id = ? AND link_id IN ({','.join(['?'] * len(links))})
        '''
        existing_result = self.db.query(existing_sql, [run_id] + links)
        existing_links = {row[0] for row in existing_result} if existing_result else set()
        
        # Insert only new links
        new_links = [link_id for link_id in links if link_id not in existing_links]
        
        for link_id in new_links:
            sql = '''
                INSERT INTO tb_run_covered_links (run_id, link_id, covered_at)
                VALUES (?, ?, ?)
            '''
            params = [run_id, link_id, StringHelper.datetime_to_sqltimestamp(datetime.now())]
            self.db.update(sql, params)
    
    def _update_coverage_summary(self, run_id: str, scope: 'CoverageScope', 
                                covered_nodes: int, covered_links: int, overall_coverage: float):
        """Update or create coverage summary for a run."""
        # Check if summary exists
        existing_sql = 'SELECT run_id FROM tb_run_coverage_summary WHERE run_id = ?'
        existing = self.db.query(existing_sql, [run_id])
        
        # Calculate percentages
        node_coverage_pct = (covered_nodes / scope.total_nodes * 100.0) if scope.total_nodes > 0 else 0.0
        link_coverage_pct = (covered_links / scope.total_links * 100.0) if scope.total_links > 0 else 0.0
        overall_coverage_pct = overall_coverage * 100.0
        
        # Get unique paths count
        unique_paths = self._get_unique_paths_count(run_id)
        
        if existing:
            # Update existing summary
            sql = '''
                UPDATE tb_run_coverage_summary SET
                    total_nodes_in_scope = ?,
                    total_links_in_scope = ?,
                    covered_nodes = ?,
                    covered_links = ?,
                    node_coverage_pct = ?,
                    link_coverage_pct = ?,
                    overall_coverage_pct = ?,
                    unique_paths_count = ?,
                    created_at = ?
                WHERE run_id = ?
            '''
            params = [
                scope.total_nodes, scope.total_links, covered_nodes, covered_links,
                node_coverage_pct, link_coverage_pct, overall_coverage_pct,
                unique_paths, StringHelper.datetime_to_sqltimestamp(datetime.now()), run_id
            ]
        else:
            # Create new summary
            sql = '''
                INSERT INTO tb_run_coverage_summary (
                    run_id, total_nodes_in_scope, total_links_in_scope,
                    covered_nodes, covered_links, node_coverage_pct,
                    link_coverage_pct, overall_coverage_pct, unique_paths_count,
                    scope_filters, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            scope_filters = f'fab_no:{scope.fab_no},phase_no:{scope.phase_no},model_no:{scope.model_no},e2e_group_no:{scope.e2e_group_no}'
            params = [
                run_id, scope.total_nodes, scope.total_links, covered_nodes, covered_links,
                node_coverage_pct, link_coverage_pct, overall_coverage_pct, unique_paths,
                scope_filters, StringHelper.datetime_to_sqltimestamp(datetime.now())
            ]
        
        self.db.update(sql, params)
    
    def _get_unique_paths_count(self, run_id: str) -> int:
        """Get count of unique paths for a run."""
        sql = '''
            SELECT COUNT(DISTINCT pd.path_hash)
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        '''
        result = self.db.query(sql, [run_id])
        return result[0][0] if result else 0
    
    def _calculate_coverage_from_run_data(self, run_id: str) -> dict:
        """Calculate coverage metrics from run data when summary doesn't exist."""
        # Count covered nodes and links
        node_sql = 'SELECT COUNT(DISTINCT node_id) FROM tb_run_covered_nodes WHERE run_id = ?'
        link_sql = 'SELECT COUNT(DISTINCT link_id) FROM tb_run_covered_links WHERE run_id = ?'
        
        node_result = self.db.query(node_sql, [run_id])
        link_result = self.db.query(link_sql, [run_id])
        
        covered_nodes = node_result[0][0] if node_result else 0
        covered_links = link_result[0][0] if link_result else 0
        
        # Get scope info
        scope = self._get_run_scope(run_id)
        total_nodes = scope.total_nodes if scope else 1
        total_links = scope.total_links if scope else 1
        
        return {
            'total_nodes_in_scope': total_nodes,
            'total_links_in_scope': total_links,
            'covered_nodes': covered_nodes,
            'covered_links': covered_links,
            'node_coverage_pct': (covered_nodes / total_nodes * 100.0) if total_nodes > 0 else 0.0,
            'link_coverage_pct': (covered_links / total_links * 100.0) if total_links > 0 else 0.0,
            'achieved_coverage': ((covered_nodes + covered_links) / (total_nodes + total_links)) if (total_nodes + total_links) > 0 else 0.0,
            'unique_paths_count': self._get_unique_paths_count(run_id)
        }
    
    def _check_other_runs_with_scope(self, scope: 'CoverageScope', exclude_run_id: str) -> bool:
        """Check if other runs are using the same scope."""
        filters = {}
        if scope.fab_no:
            filters['fab_no'] = ('=', scope.fab_no)
        if scope.phase_no:
            filters['phase_no'] = ('=', scope.phase_no)
        if scope.model_no:
            filters['model_no'] = ('=', scope.model_no)
        
        filters['id'] = ('!=', exclude_run_id)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'SELECT COUNT(*) FROM tb_runs {where_clause}'
        result = self.db.query(sql, params)
        return (result[0][0] if result else 0) > 0


class CoverageScope:
    """Defines the scope for coverage calculation."""
    
    def __init__(self, fab_no: Optional[int] = None, phase_no: Optional[int] = None,
                 model_no: Optional[int] = None, e2e_group_no: Optional[int] = None,
                 total_nodes: int = 0, total_links: int = 0,
                 node_id_mapping: Optional[dict[int, int]] = None,
                 link_id_mapping: Optional[dict[int, int]] = None):
        self.fab_no = fab_no
        self.phase_no = phase_no
        self.model_no = model_no
        self.e2e_group_no = e2e_group_no
        self.total_nodes = total_nodes
        self.total_links = total_links
        self.node_id_mapping = node_id_mapping or {}
        self.link_id_mapping = link_id_mapping or {}