# managers/coverage.py

from bitarray import bitarray
from typing import Optional, Any
from dataclasses import dataclass
from datetime import datetime

from db import Database
from string_helper import StringHelper
from sample_models import CoverageScope, RandomRunConfig, PathResult


@dataclass
class CoverageMetrics:
    """Coverage calculation results."""
    total_nodes_in_scope: int
    total_links_in_scope: int
    covered_nodes: int
    covered_links: int
    node_coverage_pct: float
    link_coverage_pct: float
    overall_coverage_pct: float
    unique_paths_count: int


class CoverageManager:
    """Manages coverage tracking using bitarrays for efficient operations."""
    
    def __init__(self, db: Database):
        self.db = db
        self._coverage_scopes = {}  # Cache for coverage scopes
    
    def build_coverage_scope(self, config: RandomRunConfig) -> CoverageScope:
        """Build coverage scope with node/link mappings for the given configuration."""
        cache_key = self._get_scope_cache_key(config)
        
        if cache_key in self._coverage_scopes:
            return self._coverage_scopes[cache_key]
        
        # Build filters for scope
        filters = {}
        
        if config.fab_no:
            filters['n.fab_no'] = ('=', config.fab_no)
        if config.phase_no:
            filters['n.model_no'] = ('=', config.phase_no)  # Note: phase maps to model in nw_nodes
        if config.model_no:
            filters['n.data_code'] = ('IN', self._get_model_data_codes(config.model_no))
        if config.e2e_group_no:
            filters['n.e2e_group_no'] = ('=', config.e2e_group_no)
        
        # Get nodes in scope
        node_mappings, total_nodes = self._build_node_mappings(filters)
        
        # Get links in scope (links between nodes in scope)
        link_mappings, total_links = self._build_link_mappings(node_mappings, filters)
        
        scope = CoverageScope(
            toolset=config.toolset,
            fab_no=config.fab_no,
            phase_no=config.phase_no,
            model_no=config.model_no,
            e2e_group_no=config.e2e_group_no,
            total_nodes=total_nodes,
            total_links=total_links,
            node_id_mapping=node_mappings,
            link_id_mapping=link_mappings
        )
        
        self._coverage_scopes[cache_key] = scope
        return scope
    
    def initialize_run_coverage(self, run_id: str, scope: CoverageScope) -> tuple[bitarray, bitarray]:
        """Initialize coverage tracking for a run and return empty bitarrays."""
        # Store coverage scope summary
        self._store_coverage_scope_summary(run_id, scope)
        
        # Initialize empty bitarrays
        covered_nodes = bitarray(scope.total_nodes)
        covered_links = bitarray(scope.total_links)
        covered_nodes.setall(0)
        covered_links.setall(0)
        
        return covered_nodes, covered_links
    
    def update_coverage_with_path(self, run_id: str, scope: CoverageScope,
                                 covered_nodes: bitarray, covered_links: bitarray,
                                 path_result: PathResult) -> tuple[int, int]:
        """Update coverage bitarrays with a new path and return new nodes/links covered."""
        new_nodes_covered = 0
        new_links_covered = 0
        
        # Track which specific nodes/links are newly covered for database storage
        newly_covered_node_ids = []
        newly_covered_link_ids = []
        
        # Update node coverage
        for node_id in path_result.nodes:
            if node_id in scope.node_id_mapping:
                bit_index = scope.node_id_mapping[node_id]
                if not covered_nodes[bit_index]:
                    covered_nodes[bit_index] = 1
                    new_nodes_covered += 1
                    newly_covered_node_ids.append(node_id)
        
        # Update link coverage
        for link_id in path_result.links:
            if link_id in scope.link_id_mapping:
                bit_index = scope.link_id_mapping[link_id]
                if not covered_links[bit_index]:
                    covered_links[bit_index] = 1
                    new_links_covered += 1
                    newly_covered_link_ids.append(link_id)
        
        # Store newly covered nodes and links in database
        if newly_covered_node_ids:
            self._store_covered_nodes(run_id, newly_covered_node_ids)
        
        if newly_covered_link_ids:
            self._store_covered_links(run_id, newly_covered_link_ids)
        
        return new_nodes_covered, new_links_covered
    
    def calculate_coverage_metrics(self, scope: CoverageScope,
                                  covered_nodes: bitarray, covered_links: bitarray,
                                  unique_paths_count: int) -> CoverageMetrics:
        """Calculate current coverage metrics."""
        covered_node_count = covered_nodes.count()
        covered_link_count = covered_links.count()
        
        node_coverage_pct = (covered_node_count / scope.total_nodes * 100) if scope.total_nodes > 0 else 0
        link_coverage_pct = (covered_link_count / scope.total_links * 100) if scope.total_links > 0 else 0
        
        # Overall coverage is weighted average of node and link coverage
        total_elements = scope.total_nodes + scope.total_links
        if total_elements > 0:
            overall_coverage_pct = ((covered_node_count + covered_link_count) / total_elements) * 100
        else:
            overall_coverage_pct = 0
        
        return CoverageMetrics(
            total_nodes_in_scope=scope.total_nodes,
            total_links_in_scope=scope.total_links,
            covered_nodes=covered_node_count,
            covered_links=covered_link_count,
            node_coverage_pct=node_coverage_pct,
            link_coverage_pct=link_coverage_pct,
            overall_coverage_pct=overall_coverage_pct,
            unique_paths_count=unique_paths_count
        )
    
    def store_final_coverage_summary(self, run_id: str, metrics: CoverageMetrics,
                                   scope_filters: dict[str, Any]) -> None:
        """Store final coverage summary for the run."""
        summary_data = {
            'run_id': run_id,
            'total_nodes_in_scope': metrics.total_nodes_in_scope,
            'total_links_in_scope': metrics.total_links_in_scope,
            'covered_nodes': metrics.covered_nodes,
            'covered_links': metrics.covered_links,
            'node_coverage_pct': metrics.node_coverage_pct,
            'link_coverage_pct': metrics.link_coverage_pct,
            'overall_coverage_pct': metrics.overall_coverage_pct,
            'unique_paths_count': metrics.unique_paths_count,
            'scope_filters': self._serialize_scope_filters(scope_filters)
        }
        
        columns = list(summary_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_run_coverage_summary ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(summary_data.values()))
    
    def fetch_coverage_summary(self, run_id: str) -> Optional[CoverageMetrics]:
        """Fetch coverage summary for a run."""
        sql = '''
            SELECT total_nodes_in_scope, total_links_in_scope, covered_nodes, covered_links,
                   node_coverage_pct, link_coverage_pct, overall_coverage_pct, unique_paths_count
            FROM tb_run_coverage_summary 
            WHERE run_id = ?
        '''
        rows = self.db.query(sql, [run_id])
        
        if not rows:
            return None
        
        row = rows[0]
        return CoverageMetrics(
            total_nodes_in_scope=row[0],
            total_links_in_scope=row[1],
            covered_nodes=row[2],
            covered_links=row[3],
            node_coverage_pct=row[4],
            link_coverage_pct=row[5],
            overall_coverage_pct=row[6],
            unique_paths_count=row[7]
        )
    
    def fetch_covered_nodes(self, run_id: str) -> list[int]:
        """Fetch list of covered node IDs for a run."""
        sql = 'SELECT node_id FROM tb_run_covered_nodes WHERE run_id = ? ORDER BY covered_at'
        rows = self.db.query(sql, [run_id])
        return [row[0] for row in rows]
    
    def fetch_covered_links(self, run_id: str) -> list[int]:
        """Fetch list of covered link IDs for a run."""
        sql = 'SELECT link_id FROM tb_run_covered_links WHERE run_id = ? ORDER BY covered_at'
        rows = self.db.query(sql, [run_id])
        return [row[0] for row in rows]
    
    def has_coverage_target_met(self, metrics: CoverageMetrics, target_coverage: float) -> bool:
        """Check if coverage target has been met."""
        return metrics.overall_coverage_pct >= (target_coverage * 100)
    
    def get_coverage_efficiency(self, metrics: CoverageMetrics, target_coverage: float) -> float:
        """Calculate coverage efficiency (achieved/target ratio)."""
        target_pct = target_coverage * 100
        return metrics.overall_coverage_pct / target_pct if target_pct > 0 else 0
    
    def _build_node_mappings(self, filters: dict[str, tuple[str, Any]]) -> tuple[dict[int, int], int]:
        """Build node ID to bitarray index mappings."""
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id 
            FROM nw_nodes n
            {where_clause}
            ORDER BY id
        '''
        
        rows = self.db.query(sql, params)
        
        # Create mapping from node_id to bitarray index
        node_mappings = {}
        for index, row in enumerate(rows):
            node_id = row[0]
            node_mappings[node_id] = index
        
        return node_mappings, len(rows)
    
    def _build_link_mappings(self, node_mappings: dict[int, int], 
                           filters: dict[str, tuple[str, Any]]) -> tuple[dict[int, int], int]:
        """Build link ID to bitarray index mappings for links between nodes in scope."""
        if not node_mappings:
            return {}, 0
        
        # Get all node IDs in scope
        node_ids_in_scope = list(node_mappings.keys())
        
        # Build placeholders for IN clause
        node_placeholders = ', '.join(['?' for _ in node_ids_in_scope])
        
        sql = f'''
            SELECT id 
            FROM nw_links 
            WHERE s_node_id IN ({node_placeholders}) 
              AND e_node_id IN ({node_placeholders})
            ORDER BY id
        '''
        
        # Parameters: node_ids twice (for s_node_id and e_node_id)
        params = node_ids_in_scope + node_ids_in_scope
        rows = self.db.query(sql, params)
        
        # Create mapping from link_id to bitarray index
        link_mappings = {}
        for index, row in enumerate(rows):
            link_id = row[0]
            link_mappings[link_id] = index
        
        return link_mappings, len(rows)
    
    def _get_model_data_codes(self, model_no: int) -> list[int]:
        """Get data codes associated with a model number."""
        # This is a placeholder - actual implementation would depend on 
        # how model numbers map to data codes in your system
        sql = '''
            SELECT DISTINCT data_code 
            FROM nw_nodes 
            WHERE model_no = ?
        '''
        rows = self.db.query(sql, [model_no])
        return [row[0] for row in rows]
    
    def _store_coverage_scope_summary(self, run_id: str, scope: CoverageScope) -> None:
        """Store initial coverage scope information."""
        # This could be used for debugging/analysis but is not strictly necessary
        # for the core functionality
        pass
    
    def _store_covered_nodes(self, run_id: str, node_ids: list[int]) -> None:
        """Store newly covered nodes."""
        if not node_ids:
            return
        
        # Batch insert for performance
        values = [(run_id, node_id) for node_id in node_ids]
        placeholders = ', '.join(['(?, ?)' for _ in values])
        sql = f'INSERT INTO tb_run_covered_nodes (run_id, node_id) VALUES {placeholders}'
        
        # Flatten the values list
        flat_params = []
        for run_id_val, node_id in values:
            flat_params.extend([run_id_val, node_id])
        
        self.db.update(sql, flat_params)
    
    def _store_covered_links(self, run_id: str, link_ids: list[int]) -> None:
        """Store newly covered links."""
        if not link_ids:
            return
        
        # Batch insert for performance
        values = [(run_id, link_id) for link_id in link_ids]
        placeholders = ', '.join(['(?, ?)' for _ in values])
        sql = f'INSERT INTO tb_run_covered_links (run_id, link_id) VALUES {placeholders}'
        
        # Flatten the values list
        flat_params = []
        for run_id_val, link_id in values:
            flat_params.extend([run_id_val, link_id])
        
        self.db.update(sql, flat_params)
    
    def _get_scope_cache_key(self, config: RandomRunConfig) -> str:
        """Generate cache key for coverage scope."""
        key_parts = [
            f'fab:{config.fab_no}' if config.fab_no else 'fab:None',
            f'phase:{config.phase_no}' if config.phase_no else 'phase:None',
            f'model:{config.model_no}' if config.model_no else 'model:None',
            f'toolset:{config.toolset}' if config.toolset else 'toolset:None',
            f'e2e:{config.e2e_group_no}' if config.e2e_group_no else 'e2e:None'
        ]
        return '|'.join(key_parts)
    
    def _serialize_scope_filters(self, filters: dict[str, Any]) -> str:
        """Serialize scope filters for storage."""
        import json
        return json.dumps(filters, default=str)
    
    def clear_scope_cache(self) -> None:
        """Clear the coverage scope cache to free memory."""
        self._coverage_scopes.clear()
    
    def get_coverage_progress_info(self, metrics: CoverageMetrics, target_coverage: float) -> dict[str, Any]:
        """Get detailed coverage progress information."""
        target_pct = target_coverage * 100
        remaining_pct = max(0, target_pct - metrics.overall_coverage_pct)
        
        return {
            'current_coverage_pct': metrics.overall_coverage_pct,
            'target_coverage_pct': target_pct,
            'remaining_coverage_pct': remaining_pct,
            'coverage_efficiency': self.get_coverage_efficiency(metrics, target_coverage),
            'is_target_met': self.has_coverage_target_met(metrics, target_coverage),
            'nodes_covered_ratio': f'{metrics.covered_nodes}/{metrics.total_nodes_in_scope}',
            'links_covered_ratio': f'{metrics.covered_links}/{metrics.total_links_in_scope}',
            'total_elements_in_scope': metrics.total_nodes_in_scope + metrics.total_links_in_scope,
            'total_elements_covered': metrics.covered_nodes + metrics.covered_links
        }