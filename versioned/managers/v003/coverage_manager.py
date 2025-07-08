# managers/coverage.py

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from bitarray import bitarray

from db import Database
from string_helper import StringHelper


class CoverageManager:
    """Coverage tracking with bitsets for efficient node and link coverage calculation."""
    
    def __init__(self, db: Database):
        self.db = db
        self._coverage_scopes = {}  # Cache for coverage scopes
    
    def initialize_coverage_scope(self, config: 'RandomRunConfig') -> 'CoverageScope':
        """Initialize coverage scope with bitarrays for efficient tracking."""
        
        # Build filters for nodes and links in scope
        node_filters = self._build_scope_filters(config)
        link_filters = self._build_scope_filters(config)
        
        # Fetch nodes in scope
        nodes_in_scope = self._fetch_nodes_in_scope(node_filters)
        
        # Fetch links in scope
        links_in_scope = self._fetch_links_in_scope(link_filters)
        
        if not nodes_in_scope and not links_in_scope:
            raise ValueError('No nodes or links found in the specified scope')
        
        # Create node and link mappings for bitarray indexing
        node_id_mapping = {node_id: idx for idx, node_id in enumerate(nodes_in_scope)}
        link_id_mapping = {link_id: idx for idx, link_id in enumerate(links_in_scope)}
        
        # Initialize bitarrays
        covered_nodes = bitarray(len(nodes_in_scope))
        covered_links = bitarray(len(links_in_scope))
        covered_nodes.setall(0)  # Initialize to all uncovered
        covered_links.setall(0)  # Initialize to all uncovered
        
        coverage_scope = CoverageScope(
            config=config,
            total_nodes=len(nodes_in_scope),
            total_links=len(links_in_scope),
            node_id_mapping=node_id_mapping,
            link_id_mapping=link_id_mapping,
            covered_nodes=covered_nodes,
            covered_links=covered_links,
            nodes_in_scope=nodes_in_scope,
            links_in_scope=links_in_scope
        )
        
        # Store coverage summary
        self._store_coverage_summary(config, coverage_scope)
        
        # Cache for reuse
        scope_key = self._generate_scope_key(config)
        self._coverage_scopes[scope_key] = coverage_scope
        
        return coverage_scope
    
    def update_coverage(self, coverage_scope: 'CoverageScope', 
                       path_nodes: List[int], path_links: List[int]) -> Dict[str, int]:
        """Update coverage with new path nodes and links."""
        
        nodes_added = 0
        links_added = 0
        
        # Update node coverage
        for node_id in path_nodes:
            if node_id in coverage_scope.node_id_mapping:
                bit_index = coverage_scope.node_id_mapping[node_id]
                if not coverage_scope.covered_nodes[bit_index]:
                    coverage_scope.covered_nodes[bit_index] = 1
                    nodes_added += 1
                    
                    # Store covered node record
                    self._store_covered_node(coverage_scope.config, node_id)
        
        # Update link coverage
        for link_id in path_links:
            if link_id in coverage_scope.link_id_mapping:
                bit_index = coverage_scope.link_id_mapping[link_id]
                if not coverage_scope.covered_links[bit_index]:
                    coverage_scope.covered_links[bit_index] = 1
                    links_added += 1
                    
                    # Store covered link record
                    self._store_covered_link(coverage_scope.config, link_id)
        
        # Update coverage statistics
        coverage_scope.covered_node_count = coverage_scope.covered_nodes.count(1)
        coverage_scope.covered_link_count = coverage_scope.covered_links.count(1)
        
        return {
            'nodes_added': nodes_added,
            'links_added': links_added,
            'total_covered_nodes': coverage_scope.covered_node_count,
            'total_covered_links': coverage_scope.covered_link_count
        }
    
    def is_target_achieved(self, coverage_scope: 'CoverageScope', target_coverage: float) -> bool:
        """Check if target coverage has been achieved."""
        
        if target_coverage <= 0:
            return False
        
        current_coverage = self.calculate_current_coverage(coverage_scope)
        return current_coverage >= target_coverage
    
    def calculate_current_coverage(self, coverage_scope: 'CoverageScope') -> float:
        """Calculate current coverage percentage."""
        
        if coverage_scope.total_nodes == 0 and coverage_scope.total_links == 0:
            return 0.0
        
        total_elements = coverage_scope.total_nodes + coverage_scope.total_links
        covered_elements = coverage_scope.covered_node_count + coverage_scope.covered_link_count
        
        return (covered_elements / total_elements) * 100.0
    
    def calculate_final_coverage(self, config: 'RandomRunConfig') -> 'CoverageMetrics':
        """Calculate final coverage metrics for a run."""
        
        scope_key = self._generate_scope_key(config)
        coverage_scope = self._coverage_scopes.get(scope_key)
        
        if not coverage_scope:
            # Reconstruct from database if not in cache
            coverage_scope = self._reconstruct_coverage_scope(config)
        
        if not coverage_scope:
            return CoverageMetrics()
        
        # Calculate detailed metrics
        node_coverage = (coverage_scope.covered_node_count / coverage_scope.total_nodes * 100.0 
                        if coverage_scope.total_nodes > 0 else 0.0)
        
        link_coverage = (coverage_scope.covered_link_count / coverage_scope.total_links * 100.0 
                        if coverage_scope.total_links > 0 else 0.0)
        
        overall_coverage = self.calculate_current_coverage(coverage_scope)
        
        return CoverageMetrics(
            total_nodes=coverage_scope.total_nodes,
            total_links=coverage_scope.total_links,
            covered_nodes=coverage_scope.covered_node_count,
            covered_links=coverage_scope.covered_link_count,
            node_coverage_pct=node_coverage,
            link_coverage_pct=link_coverage,
            achieved_coverage=overall_coverage
        )
    
    def _build_scope_filters(self, config: 'RandomRunConfig') -> Dict:
        """Build database filters based on configuration."""
        
        filters = {}
        
        if config.fab_no:
            filters['fab_no'] = ('=', config.fab_no)
        
        if config.model_no:
            filters['model_no'] = ('=', config.model_no)
        
        if config.phase_no:
            filters['phase_no'] = ('=', config.phase_no)
        
        if config.e2e_group_no:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        
        return filters
    
    def _fetch_nodes_in_scope(self, filters: Dict) -> List[int]:
        """Fetch all node IDs within the specified scope."""
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT DISTINCT id 
            FROM nw_nodes
            {where_clause}
            ORDER BY id
        '''
        
        rows = self.db.query(sql, params)
        return [row[0] for row in rows]
    
    def _fetch_links_in_scope(self, filters: Dict) -> List[int]:
        """Fetch all link IDs within the specified scope."""
        
        # For links, we need to join with nodes to apply scope filters
        where_clause, params = StringHelper.build_where_clause(filters)
        
        if where_clause:
            # If there are scope filters, join with nodes
            sql = f'''
                SELECT DISTINCT l.id
                FROM nw_links l
                INNER JOIN nw_nodes n1 ON l.start_node_id = n1.id
                INNER JOIN nw_nodes n2 ON l.end_node_id = n2.id
                {where_clause.replace('WHERE', 'WHERE (n1.')} 
                {' AND '.join([f'n1.{condition}' for condition in where_clause.replace('WHERE ', '').split(' AND ')]).replace('n1.', 'n1.')}
                AND {' AND '.join([f'n2.{condition}' for condition in where_clause.replace('WHERE ', '').split(' AND ')]).replace('n1.', 'n2.')}
                ORDER BY l.id
            '''
            # Duplicate params for both node joins
            extended_params = params + params
        else:
            # No scope filters, get all links
            sql = '''
                SELECT DISTINCT id 
                FROM nw_links
                ORDER BY id
            '''
            extended_params = []
        
        rows = self.db.query(sql, extended_params)
        return [row[0] for row in rows]
    
    def _store_coverage_summary(self, config: 'RandomRunConfig', coverage_scope: 'CoverageScope'):
        """Store initial coverage summary."""
        
        # Build scope filters description
        scope_filters = []
        if config.fab_no:
            scope_filters.append(f'fab_no={config.fab_no}')
        if config.model_no:
            scope_filters.append(f'model_no={config.model_no}')
        if config.phase_no:
            scope_filters.append(f'phase_no={config.phase_no}')
        if config.e2e_group_no:
            scope_filters.append(f'e2e_group_no={config.e2e_group_no}')
        
        scope_filters_str = ';'.join(scope_filters) if scope_filters else 'NO_FILTERS'
        
        sql = '''
            INSERT INTO tb_run_coverage_summary (
                run_id, total_nodes_in_scope, total_links_in_scope,
                covered_nodes, covered_links, node_coverage_pct,
                link_coverage_pct, overall_coverage_pct, unique_paths_count,
                scope_filters, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            getattr(config, 'run_id', 'UNKNOWN'),
            coverage_scope.total_nodes,
            coverage_scope.total_links,
            0,  # Initial covered nodes
            0,  # Initial covered links
            0.0,  # Initial node coverage
            0.0,  # Initial link coverage
            0.0,  # Initial overall coverage
            0,  # Initial unique paths
            scope_filters_str,
            datetime.now()
        ]
        
        self.db.update(sql, params)
    
    def _store_covered_node(self, config: 'RandomRunConfig', node_id: int):
        """Store a newly covered node."""
        
        sql = '''
            INSERT INTO tb_run_covered_nodes (run_id, node_id, covered_at)
            VALUES (?, ?, ?)
        '''
        
        params = [
            getattr(config, 'run_id', 'UNKNOWN'),
            node_id,
            datetime.now()
        ]
        
        self.db.update(sql, params)
    
    def _store_covered_link(self, config: 'RandomRunConfig', link_id: int):
        """Store a newly covered link."""
        
        sql = '''
            INSERT INTO tb_run_covered_links (run_id, link_id, covered_at)
            VALUES (?, ?, ?)
        '''
        
        params = [
            getattr(config, 'run_id', 'UNKNOWN'),
            link_id,
            datetime.now()
        ]
        
        self.db.update(sql, params)
    
    def _generate_scope_key(self, config: 'RandomRunConfig') -> str:
        """Generate a unique key for caching coverage scopes."""
        
        key_parts = [
            f'fab:{config.fab_no or "ALL"}',
            f'model:{config.model_no or "ALL"}',
            f'phase:{config.phase_no or "ALL"}',
            f'e2e:{config.e2e_group_no or "ALL"}',
            f'toolset:{config.toolset or "ALL"}'
        ]
        
        return '_'.join(key_parts)
    
    def _reconstruct_coverage_scope(self, config: 'RandomRunConfig') -> Optional['CoverageScope']:
        """Reconstruct coverage scope from database records."""
        
        run_id = getattr(config, 'run_id', None)
        if not run_id:
            return None
        
        # Get coverage summary
        sql = '''
            SELECT total_nodes_in_scope, total_links_in_scope, covered_nodes, covered_links
            FROM tb_run_coverage_summary
            WHERE run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        if not rows:
            return None
        
        total_nodes, total_links, covered_nodes_count, covered_links_count = rows[0]
        
        # Get covered nodes
        sql = 'SELECT node_id FROM tb_run_covered_nodes WHERE run_id = ?'
        covered_node_ids = [row[0] for row in self.db.query(sql, [run_id])]
        
        # Get covered links
        sql = 'SELECT link_id FROM tb_run_covered_links WHERE run_id = ?'
        covered_link_ids = [row[0] for row in self.db.query(sql, [run_id])]
        
        # Reconstruct scope (simplified - might not have all original data)
        return CoverageScope(
            config=config,
            total_nodes=total_nodes,
            total_links=total_links,
            covered_node_count=covered_nodes_count,
            covered_link_count=covered_links_count,
            node_id_mapping={},  # Would need to be reconstructed
            link_id_mapping={},  # Would need to be reconstructed
            covered_nodes=bitarray(),  # Would need to be reconstructed
            covered_links=bitarray(),  # Would need to be reconstructed
            nodes_in_scope=[],
            links_in_scope=[]
        )
    
    def fetch_coverage_summary(self, run_id: str) -> Optional[Dict]:
        """Fetch coverage summary for a run."""
        
        sql = '''
            SELECT run_id, total_nodes_in_scope, total_links_in_scope,
                   covered_nodes, covered_links, node_coverage_pct,
                   link_coverage_pct, overall_coverage_pct, unique_paths_count,
                   scope_filters, created_at
            FROM tb_run_coverage_summary
            WHERE run_id = ?
        '''
        
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
            'node_coverage_pct': row[5],
            'link_coverage_pct': row[6],
            'overall_coverage_pct': row[7],
            'unique_paths_count': row[8],
            'scope_filters': row[9],
            'created_at': row[10]
        }
    
    def update_coverage_summary(self, run_id: str, coverage_scope: 'CoverageScope', 
                               unique_paths_count: int):
        """Update coverage summary with final metrics."""
        
        node_coverage = (coverage_scope.covered_node_count / coverage_scope.total_nodes * 100.0 
                        if coverage_scope.total_nodes > 0 else 0.0)
        
        link_coverage = (coverage_scope.covered_link_count / coverage_scope.total_links * 100.0 
                        if coverage_scope.total_links > 0 else 0.0)
        
        overall_coverage = self.calculate_current_coverage(coverage_scope)
        
        sql = '''
            UPDATE tb_run_coverage_summary
            SET covered_nodes = ?, covered_links = ?, node_coverage_pct = ?,
                link_coverage_pct = ?, overall_coverage_pct = ?, unique_paths_count = ?
            WHERE run_id = ?
        '''
        
        params = [
            coverage_scope.covered_node_count,
            coverage_scope.covered_link_count,
            node_coverage,
            link_coverage,
            overall_coverage,
            unique_paths_count,
            run_id
        ]
        
        self.db.update(sql, params)


@dataclass
class CoverageScope:
    """Coverage scope configuration and tracking."""
    config: 'RandomRunConfig'
    total_nodes: int
    total_links: int
    node_id_mapping: Dict[int, int]  # Maps actual node_id to bitarray index
    link_id_mapping: Dict[int, int]  # Maps actual link_id to bitarray index
    covered_nodes: bitarray
    covered_links: bitarray
    nodes_in_scope: List[int]
    links_in_scope: List[int]
    covered_node_count: int = 0
    covered_link_count: int = 0


@dataclass
class CoverageMetrics:
    """Coverage calculation results."""
    total_nodes: int = 0
    total_links: int = 0
    covered_nodes: int = 0
    covered_links: int = 0
    node_coverage_pct: float = 0.0
    link_coverage_pct: float = 0.0
    achieved_coverage: float = 0.0