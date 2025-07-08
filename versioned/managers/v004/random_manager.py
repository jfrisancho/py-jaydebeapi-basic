# managers/random.py

import random
import time
from datetime import datetime
from typing import Optional, list, dict, tuple

from db import Database
from string_helper import StringHelper

class RandomManager:
    """Random path generation with bias mitigation."""
    
    def __init__(self, db: Database):
        self.db = db
        self.bias_config = BiasReduction()
    
    def execute_sampling(self, run_id: str, config: 'RandomRunConfig', coverage_scope: 'CoverageScope') -> dict:
        """Execute random sampling until coverage target is achieved."""
        from .path import PathManager
        from .coverage import CoverageManager
        
        path_manager = PathManager(self.db)
        coverage_manager = CoverageManager(self.db)
        
        total_attempts = 0
        paths_found = 0
        current_coverage = 0.0
        
        # Initialize sampling universe
        sampling_universe = self._build_sampling_universe(config)
        
        print(f'Starting random sampling for run {run_id}')
        print(f'Target coverage: {config.coverage_target * 100:.1f}%')
        print(f'Sampling universe: {len(sampling_universe)} toolsets')
        
        start_time = time.time()
        
        while current_coverage < config.coverage_target:
            total_attempts += 1
            
            # Select random PoC pair with bias mitigation
            poc_pair = self._select_random_poc_pair(sampling_universe, config)
            
            if not poc_pair:
                print(f'Warning: Could not select PoC pair after {total_attempts} attempts')
                break
            
            # Check if path exists between PoCs
            path_result = self._find_path_between_pocs(poc_pair)
            
            if path_result:
                # Store path and update coverage
                path_def_id = path_manager.store_path_definition(path_result, run_id)
                path_manager.store_attempt_path(run_id, path_def_id, poc_pair, path_result)
                
                # Update coverage
                coverage_manager.update_coverage(run_id, path_result.nodes, path_result.links)
                current_coverage = coverage_manager.calculate_current_coverage(run_id, coverage_scope)
                
                paths_found += 1
                
                if paths_found % 10 == 0:
                    elapsed = time.time() - start_time
                    print(f'Progress: {paths_found} paths found, {current_coverage * 100:.2f}% coverage, {elapsed:.1f}s elapsed')
            else:
                # Check if unused PoCs should be flagged for review
                self._check_unused_pocs(run_id, poc_pair)
            
            # Safety break for very long runs
            if total_attempts > 100000:
                print(f'Warning: Reached maximum attempts limit ({total_attempts})')
                break
        
        elapsed_time = time.time() - start_time
        print(f'Sampling completed: {paths_found} paths found in {total_attempts} attempts ({elapsed_time:.1f}s)')
        
        return {
            'total_attempts': total_attempts,
            'paths_found': paths_found,
            'final_coverage': current_coverage,
            'elapsed_time': elapsed_time
        }
    
    def _build_sampling_universe(self, config: 'RandomRunConfig') -> list[dict]:
        """Build the universe of toolsets available for sampling."""
        filters = {}
        
        if config.fab_no:
            filters['fab_no'] = ('=', config.fab_no)
        if config.phase_no:
            filters['phase_no'] = ('=', config.phase_no)
        if config.model_no:
            filters['model_no'] = ('=', config.model_no)
        if config.e2e_group_no:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT DISTINCT code, fab_no, phase_no, model_no, e2e_group_no
            FROM tb_toolsets
            {where_clause}
            AND is_active = 1
        '''
        
        results = self.db.query(sql, params)
        
        toolsets = []
        for row in results:
            toolsets.append({
                'code': row[0],
                'fab_no': row[1],
                'phase_no': row[2],
                'model_no': row[3],
                'e2e_group_no': row[4]
            })
        
        return toolsets
    
    def _select_random_poc_pair(self, sampling_universe: list[dict], config: 'RandomRunConfig') -> Optional[dict]:
        """Select random PoC pair with bias mitigation."""
        max_toolset_attempts = self.bias_config.max_attempts_per_toolset
        max_equipment_attempts = self.bias_config.max_attempts_per_equipment
        
        for _ in range(max_toolset_attempts):
            # Select random toolset
            toolset = random.choice(sampling_universe)
            
            # Get equipment for this toolset
            equipment_list = self._fetch_toolset_equipment(toolset['code'])
            
            if len(equipment_list) < 2:
                continue  # Need at least 2 equipment for a pair
            
            for _ in range(max_equipment_attempts):
                # Select two different equipment
                eq1, eq2 = random.sample(equipment_list, 2)
                
                # Get PoCs for each equipment
                pocs1 = self._fetch_equipment_pocs(eq1['id'])
                pocs2 = self._fetch_equipment_pocs(eq2['id'])
                
                if not pocs1 or not pocs2:
                    continue
                
                # Select random PoCs with bias mitigation
                poc1 = self._select_poc_with_bias_mitigation(pocs1)
                poc2 = self._select_poc_with_bias_mitigation(pocs2)
                
                if poc1 and poc2:
                    # Check minimum distance requirement
                    if self._check_minimum_distance(poc1['node_id'], poc2['node_id']):
                        return {
                            'start_poc': poc1,
                            'end_poc': poc2,
                            'start_equipment': eq1,
                            'end_equipment': eq2,
                            'toolset': toolset
                        }
        
        return None
    
    def _fetch_toolset_equipment(self, toolset_code: str) -> list[dict]:
        """Fetch all equipment for a given toolset."""
        sql = '''
            SELECT id, guid, node_id, data_code, category_no, kind, name
            FROM tb_equipments
            WHERE toolset = ? AND is_active = 1
        '''
        
        results = self.db.query(sql, [toolset_code])
        
        equipment = []
        for row in results:
            equipment.append({
                'id': row[0],
                'guid': row[1],
                'node_id': row[2],
                'data_code': row[3],
                'category_no': row[4],
                'kind': row[5],
                'name': row[6]
            })
        
        return equipment
    
    def _fetch_equipment_pocs(self, equipment_id: int) -> list[dict]:
        """Fetch all PoCs for a given equipment."""
        sql = '''
            SELECT id, node_id, markers, utility_no, reference, flow, is_used, is_loopback
            FROM tb_equipment_pocs
            WHERE equipment_id = ? AND is_active = 1
        '''
        
        results = self.db.query(sql, [equipment_id])
        
        pocs = []
        for row in results:
            pocs.append({
                'id': row[0],
                'node_id': row[1],
                'markers': row[2],
                'utility_no': row[3],
                'reference': row[4],
                'flow': row[5],
                'is_used': bool(row[6]),
                'is_loopback': bool(row[7])
            })
        
        return pocs
    
    def _select_poc_with_bias_mitigation(self, pocs: list[dict]) -> Optional[dict]:
        """Select PoC with bias mitigation strategies."""
        if not pocs:
            return None
        
        # Apply utility diversity weighting
        weighted_pocs = []
        utility_counts = {}
        
        # Count utilities
        for poc in pocs:
            utility = poc.get('utility_no')
            utility_counts[utility] = utility_counts.get(utility, 0) + 1
        
        # Weight PoCs inversely by utility frequency
        for poc in pocs:
            utility = poc.get('utility_no')
            weight = 1.0
            
            if utility and utility_counts[utility] > 1:
                weight *= (1.0 - self.bias_config.utility_diversity_weight)
            
            # Prefer used PoCs slightly
            if poc.get('is_used'):
                weight *= 1.1
            
            weighted_pocs.append((poc, weight))
        
        # Weighted random selection
        weights = [w for _, w in weighted_pocs]
        selected_poc = random.choices([poc for poc, _ in weighted_pocs], weights=weights, k=1)[0]
        
        return selected_poc
    
    def _check_minimum_distance(self, node1_id: int, node2_id: int) -> bool:
        """Check if nodes meet minimum distance requirement."""
        # For now, just check they're not the same node
        # In a real implementation, you might check spatial distance
        return node1_id != node2_id
    
    def _find_path_between_pocs(self, poc_pair: dict) -> Optional['PathResult']:
        """Find path between two PoCs using network traversal."""
        start_node = poc_pair['start_poc']['node_id']
        end_node = poc_pair['end_poc']['node_id']
        
        # Use a simple BFS pathfinding algorithm
        path_data = self._bfs_pathfind(start_node, end_node)
        
        if not path_data:
            return None
        
        # Create PathResult object
        return PathResult(
            start_node_id=start_node,
            start_poc_id=poc_pair['start_poc']['id'],
            start_equipment_id=poc_pair['start_equipment']['id'],
            end_node_id=end_node,
            end_poc_id=poc_pair['end_poc']['id'],
            end_equipment_id=poc_pair['end_equipment']['id'],
            nodes=path_data['nodes'],
            links=path_data['links'],
            total_cost=path_data['total_cost'],
            total_length_mm=path_data['total_length'],
            toolset_nos=path_data.get('toolset_nos', []),
            data_codes=path_data.get('data_codes', []),
            utility_nos=path_data.get('utility_nos', []),
            references=path_data.get('references', [])
        )
    
    def _bfs_pathfind(self, start_node: int, end_node: int) -> Optional[dict]:
        """BFS pathfinding between two nodes."""
        if start_node == end_node:
            return None
        
        # Get all links for traversal
        links_sql = '''
            SELECT id, start_node_id, end_node_id, cost, bidirected
            FROM nw_links
            WHERE start_node_id = ? OR end_node_id = ?
        '''
        
        # Build adjacency list
        adjacency = {}
        link_costs = {}
        
        # First pass: get all connected nodes from start
        visited_nodes = set()
        queue = [start_node]
        visited_nodes.add(start_node)
        
        max_depth = 20  # Limit search depth to prevent infinite loops
        current_depth = 0
        
        while queue and current_depth < max_depth:
            current_batch = queue[:]
            queue = []
            
            for node in current_batch:
                links = self.db.query(links_sql, [node, node])
                
                for link_row in links:
                    link_id, start_id, end_id, cost, bidirected = link_row
                    
                    # Determine connected node
                    if start_id == node:
                        connected_node = end_id
                    elif end_id == node and bidirected == 'Y':
                        connected_node = start_id
                    else:
                        continue
                    
                    # Add to adjacency list
                    if node not in adjacency:
                        adjacency[node] = []
                    adjacency[node].append((connected_node, link_id, cost or 0.0))
                    
                    # Add to queue if not visited
                    if connected_node not in visited_nodes:
                        visited_nodes.add(connected_node)
                        queue.append(connected_node)
                        
                        # Check if we found the target
                        if connected_node == end_node:
                            # Reconstruct path
                            return self._reconstruct_path(start_node, end_node, adjacency)
            
            current_depth += 1
        
        return None
    
    def _reconstruct_path(self, start_node: int, end_node: int, adjacency: dict) -> dict:
        """Reconstruct path from BFS traversal."""
        # For simplicity, use another BFS to find the actual path
        queue = [(start_node, [start_node], [], 0.0)]
        visited = {start_node}
        
        while queue:
            current_node, path_nodes, path_links, total_cost = queue.pop(0)
            
            if current_node == end_node:
                # Get additional path data
                path_data = self._get_path_data(path_nodes, path_links)
                
                return {
                    'nodes': path_nodes,
                    'links': path_links,
                    'total_cost': total_cost,
                    'total_length': path_data.get('total_length', 0.0),
                    'data_codes': path_data.get('data_codes', []),
                    'utility_nos': path_data.get('utility_nos', []),
                    'references': path_data.get('references', [])
                }
            
            if current_node in adjacency:
                for next_node, link_id, cost in adjacency[current_node]:
                    if next_node not in visited:
                        visited.add(next_node)
                        new_path_nodes = path_nodes + [next_node]
                        new_path_links = path_links + [link_id]
                        new_cost = total_cost + cost
                        queue.append((next_node, new_path_nodes, new_path_links, new_cost))
        
        return None
    
    def _get_path_data(self, nodes: list[int], links: list[int]) -> dict:
        """Get additional data for path nodes and links."""
        if not nodes:
            return {}
        
        # Get node data
        node_placeholders = ','.join(['?'] * len(nodes))
        node_sql = f'''
            SELECT data_code, utility_no, markers, e2e_group_no
            FROM nw_nodes
            WHERE id IN ({node_placeholders})
        '''
        
        node_results = self.db.query(node_sql, nodes)
        
        data_codes = []
        utility_nos = []
        references = []
        
        for row in node_results:
            if row[0]:  # data_code
                data_codes.append(row[0])
            if row[1]:  # utility_no
                utility_nos.append(row[1])
            if row[2]:  # markers - extract reference
                markers = row[2]
                # Simple extraction of reference from markers
                # This would need proper parsing in real implementation
                if markers:
                    references.append(markers.split('|')[0] if '|' in markers else markers[:8])
        
        # Calculate approximate length (would need spatial data in real implementation)
        total_length = len(links) * 1000.0  # Assume 1m per link segment
        
        return {
            'total_length': total_length,
            'data_codes': list(set(data_codes)),
            'utility_nos': list(set(utility_nos)),
            'references': list(set(references))
        }
    
    def _check_unused_pocs(self, run_id: str, poc_pair: dict):
        """Check if unused PoCs should be flagged for review."""
        start_poc = poc_pair['start_poc']
        end_poc = poc_pair['end_poc']
        
        # Flag unused PoCs that should have paths
        for poc_info in [(start_poc, poc_pair['start_equipment']), (end_poc, poc_pair['end_equipment'])]:
            poc, equipment = poc_info
            
            if poc.get('is_used') and not poc.get('is_loopback'):
                # This PoC is marked as used but no path found - flag for review
                self._create_review_flag(
                    run_id=run_id,
                    flag_type='CONNECTIVITY_ISSUE',
                    severity='MEDIUM',
                    reason='Used PoC has no connectivity',
                    poc=poc,
                    equipment=equipment
                )
    
    def _create_review_flag(self, run_id: str, flag_type: str, severity: str, reason: str, poc: dict, equipment: dict):
        """Create a review flag for manual inspection."""
        sql = '''
            INSERT INTO tb_review_flags (
                run_id, flag_type, severity, reason, object_type, object_id,
                object_guid, object_data_code, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            flag_type,
            severity,
            reason,
            'POC',
            poc['id'],
            equipment.get('guid', ''),
            equipment.get('data_code'),
            StringHelper.datetime_to_sqltimestamp(datetime.now()),
            f'PoC node_id: {poc["node_id"]}, Equipment: {equipment.get("name", "")}'
        ]
        
        self.db.update(sql, params)


class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    
    def __init__(self):
        self.max_attempts_per_toolset = 5
        self.max_attempts_per_equipment = 3
        self.min_distance_between_nodes = 10
        self.utility_diversity_weight = 0.3
        self.phase_diversity_weight = 0.2


class PathResult:
    """Result of a path finding operation."""
    
    def __init__(self, start_node_id: int, start_poc_id: int, start_equipment_id: int,
                 end_node_id: int, end_poc_id: int, end_equipment_id: int,
                 nodes: list[int], links: list[int], total_cost: float, total_length_mm: float,
                 toolset_nos: list[int] = None, data_codes: list[int] = None,
                 utility_nos: list[int] = None, references: list[str] = None):
        self.start_node_id = start_node_id
        self.start_poc_id = start_poc_id
        self.start_equipment_id = start_equipment_id
        self.end_node_id = end_node_id
        self.end_poc_id = end_poc_id
        self.end_equipment_id = end_equipment_id
        self.nodes = nodes or []
        self.links = links or []
        self.total_cost = total_cost
        self.total_length_mm = total_length_mm
        self.toolset_nos = toolset_nos or []
        self.data_codes = data_codes or []
        self.utility_nos = utility_nos or []
        self.references = references or []