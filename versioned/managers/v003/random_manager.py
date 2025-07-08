# managers/random.py

import random
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from dataclasses import dataclass

from db import Database
from string_helper import StringHelper


class RandomManager:
    """Random path generation with bias mitigation."""
    
    def __init__(self, db: Database):
        self.db = db
        self._sampling_stats = defaultdict(int)  # Track sampling frequency for bias mitigation
        self._equipment_usage = defaultdict(int)  # Track equipment usage
        self._toolset_usage = defaultdict(int)  # Track toolset usage
    
    def initialize_sampling_universe(self, config: 'RandomRunConfig') -> List[Dict]:
        """Initialize the sampling universe based on configuration filters."""
        
        # Build filters for toolsets
        filters = {}
        
        if config.fab_no:
            filters['fab_no'] = ('=', config.fab_no)
        if config.model_no:
            filters['model_no'] = ('=', config.model_no)
        if config.phase_no:
            filters['phase_no'] = ('=', config.phase_no)
        if config.e2e_group_no:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        if config.toolset:
            filters['code'] = ('=', config.toolset)
        
        # Always filter for active toolsets
        filters['is_active'] = ('=', 1)
        
        # Fetch toolsets in scope
        toolsets = self._fetch_toolsets_in_scope(filters)
        
        if not toolsets:
            raise ValueError('No toolsets found matching the specified criteria')
        
        # For each toolset, get equipment and POC information
        sampling_universe = []
        
        for toolset in toolsets:
            equipment_pocs = self._fetch_equipment_pocs_for_toolset(toolset['code'])
            
            if equipment_pocs:
                sampling_universe.append({
                    'toolset': toolset,
                    'equipment_pocs': equipment_pocs
                })
        
        if not sampling_universe:
            raise ValueError('No equipment POCs found for the specified toolsets')
        
        return sampling_universe
    
    def select_random_poc_pair(self, sampling_universe: List[Dict], 
                              bias_reduction: 'BiasReduction') -> Optional[Dict]:
        """Select a random POC pair with bias mitigation."""
        
        max_attempts = 100  # Prevent infinite loops
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            
            # Apply bias mitigation in toolset selection
            toolset_data = self._select_weighted_toolset(sampling_universe, bias_reduction)
            
            if not toolset_data:
                continue
            
            # Select equipment pair from the chosen toolset
            equipment_pair = self._select_equipment_pair(
                toolset_data['equipment_pocs'], bias_reduction
            )
            
            if not equipment_pair:
                continue
            
            # Select POC pair from the chosen equipment
            poc_pair = self._select_poc_pair_from_equipment(
                equipment_pair, bias_reduction
            )
            
            if poc_pair:
                # Update usage statistics for bias mitigation
                self._update_sampling_stats(toolset_data, equipment_pair, poc_pair)
                return poc_pair
        
        # If we couldn't find a valid pair after max attempts, return None
        return None
    
    def find_path_between_pocs(self, start_poc: Dict, end_poc: Dict) -> Optional['PathResult']:
        """Find path between two POCs using spatial network traversal."""
        
        start_node_id = start_poc['node_id']
        end_node_id = end_poc['node_id']
        
        # Use a simple breadth-first search for path finding
        path_data = self._find_shortest_path(start_node_id, end_node_id)
        
        if not path_data:
            return None
        
        # Extract detailed path information
        path_details = self._extract_path_details(path_data)
        
        if not path_details:
            return None
        
        # Create PathResult object
        return PathResult(
            start_node_id=start_node_id,
            start_poc_id=start_poc['id'],
            start_equipment_id=start_poc['equipment_id'],
            end_node_id=end_node_id,
            end_poc_id=end_poc['id'],
            end_equipment_id=end_poc['equipment_id'],
            nodes=path_details['nodes'],
            links=path_details['links'],
            total_cost=path_details['total_cost'],
            total_length_mm=path_details['total_length_mm'],
            toolset_nos=path_details['toolset_nos'],
            data_codes=path_details['data_codes'],
            utility_nos=path_details['utility_nos'],
            references=path_details['references']
        )
    
    def _fetch_toolsets_in_scope(self, filters: Dict) -> List[Dict]:
        """Fetch toolsets matching the scope criteria."""
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT code, fab_no, model_no, phase_no, e2e_group_no, description
            FROM tb_toolsets
            {where_clause}
            ORDER BY code
        '''
        
        rows = self.db.query(sql, params)
        
        return [
            {
                'code': row[0],
                'fab_no': row[1],
                'model_no': row[2],
                'phase_no': row[3],
                'e2e_group_no': row[4],
                'description': row[5]
            }
            for row in rows
        ]
    
    def _fetch_equipment_pocs_for_toolset(self, toolset_code: str) -> List[Dict]:
        """Fetch equipment and their POCs for a specific toolset."""
        
        sql = '''
            SELECT 
                e.id as equipment_id,
                e.guid as equipment_guid,
                e.node_id as equipment_node_id,
                e.data_code as equipment_data_code,
                e.category_no as equipment_category_no,
                e.kind as equipment_kind,
                e.name as equipment_name,
                p.id as poc_id,
                p.node_id as poc_node_id,
                p.is_used as poc_is_used,
                p.markers as poc_markers,
                p.utility_no as poc_utility_no,
                p.reference as poc_reference,
                p.flow as poc_flow,
                p.is_loopback as poc_is_loopback
            FROM tb_equipments e
            INNER JOIN tb_equipment_pocs p ON e.id = p.equipment_id
            WHERE e.toolset = ? 
              AND e.is_active = 1 
              AND p.is_active = 1
            ORDER BY e.id, p.id
        '''
        
        rows = self.db.query(sql, [toolset_code])
        
        # Group POCs by equipment
        equipment_dict = {}
        
        for row in rows:
            equipment_id = row[0]
            
            if equipment_id not in equipment_dict:
                equipment_dict[equipment_id] = {
                    'id': row[0],
                    'guid': row[1],
                    'node_id': row[2],
                    'data_code': row[3],
                    'category_no': row[4],
                    'kind': row[5],
                    'name': row[6],
                    'pocs': []
                }
            
            equipment_dict[equipment_id]['pocs'].append({
                'id': row[7],
                'node_id': row[8],
                'is_used': bool(row[9]),
                'markers': row[10],
                'utility_no': row[11],
                'reference': row[12],
                'flow': row[13],
                'is_loopback': bool(row[14]),
                'equipment_id': equipment_id
            })
        
        return list(equipment_dict.values())
    
    def _select_weighted_toolset(self, sampling_universe: List[Dict], 
                                bias_reduction: 'BiasReduction') -> Optional[Dict]:
        """Select toolset with bias mitigation weighting."""
        
        if not sampling_universe:
            return None
        
        # Calculate weights based on usage frequency (less used = higher weight)
        weighted_choices = []
        
        for toolset_data in sampling_universe:
            toolset_code = toolset_data['toolset']['code']
            usage_count = self._toolset_usage[toolset_code]
            
            # Higher weight for less used toolsets
            weight = max(1, bias_reduction.max_attempts_per_toolset - usage_count)
            weighted_choices.extend([toolset_data] * weight)
        
        if not weighted_choices:
            return random.choice(sampling_universe)
        
        return random.choice(weighted_choices)
    
    def _select_equipment_pair(self, equipment_pocs: List[Dict], 
                              bias_reduction: 'BiasReduction') -> Optional[Tuple[Dict, Dict]]:
        """Select a pair of equipment with available POCs."""
        
        # Filter equipment that have POCs
        available_equipment = [eq for eq in equipment_pocs if eq['pocs']]
        
        if len(available_equipment) < 2:
            return None
        
        # Apply bias mitigation for equipment selection
        weighted_equipment = []
        
        for equipment in available_equipment:
            equipment_id = equipment['id']
            usage_count = self._equipment_usage[equipment_id]
            
            # Higher weight for less used equipment
            weight = max(1, bias_reduction.max_attempts_per_equipment - usage_count)
            weighted_equipment.extend([equipment] * weight)
        
        if len(weighted_equipment) < 2:
            weighted_equipment = available_equipment
        
        # Select two different equipment
        first_equipment = random.choice(weighted_equipment)
        remaining_equipment = [eq for eq in weighted_equipment if eq['id'] != first_equipment['id']]
        
        if not remaining_equipment:
            return None
        
        second_equipment = random.choice(remaining_equipment)
        
        return (first_equipment, second_equipment)
    
    def _select_poc_pair_from_equipment(self, equipment_pair: Tuple[Dict, Dict], 
                                       bias_reduction: 'BiasReduction') -> Optional[Dict]:
        """Select POC pair from equipment pair."""
        
        first_equipment, second_equipment = equipment_pair
        
        # Get available POCs from each equipment
        first_pocs = first_equipment['pocs']
        second_pocs = second_equipment['pocs']
        
        if not first_pocs or not second_pocs:
            return None
        
        # Apply utility diversity weighting if specified
        if bias_reduction.utility_diversity_weight > 0:
            start_poc = self._select_poc_with_utility_diversity(first_pocs, bias_reduction)
            end_poc = self._select_poc_with_utility_diversity(second_pocs, bias_reduction)
        else:
            start_poc = random.choice(first_pocs)
            end_poc = random.choice(second_pocs)
        
        if not start_poc or not end_poc:
            return None
        
        # Check minimum distance requirement
        if bias_reduction.min_distance_between_nodes > 0:
            if not self._check_minimum_distance(
                start_poc['node_id'], end_poc['node_id'], 
                bias_reduction.min_distance_between_nodes
            ):
                return None
        
        return {
            'start_poc': start_poc,
            'end_poc': end_poc,
            'start_equipment': first_equipment,
            'end_equipment': second_equipment
        }
    
    def _select_poc_with_utility_diversity(self, pocs: List[Dict], 
                                          bias_reduction: 'BiasReduction') -> Optional[Dict]:
        """Select POC with utility diversity consideration."""
        
        # Group POCs by utility
        utility_groups = defaultdict(list)
        
        for poc in pocs:
            utility_no = poc.get('utility_no') or 'NONE'
            utility_groups[utility_no].append(poc)
        
        # Weight selection towards less common utilities
        weighted_pocs = []
        
        for utility_no, utility_pocs in utility_groups.items():
            usage_count = self._sampling_stats[f'utility_{utility_no}']
            weight = max(1, 10 - usage_count)  # Arbitrary weighting
            weighted_pocs.extend(utility_pocs * weight)
        
        return random.choice(weighted_pocs) if weighted_pocs else random.choice(pocs)
    
    def _check_minimum_distance(self, node_id1: int, node_id2: int, min_distance: int) -> bool:
        """Check if nodes meet minimum distance requirement."""
        
        # This is a simplified distance check - in a real implementation,
        # you might want to use actual spatial coordinates
        sql = '''
            SELECT COUNT(*) 
            FROM nw_links 
            WHERE (start_node_id = ? AND end_node_id = ?) 
               OR (start_node_id = ? AND end_node_id = ?)
        '''
        
        result = self.db.query(sql, [node_id1, node_id2, node_id2, node_id1])
        direct_connection = result[0][0] > 0
        
        # If directly connected, distance is 1 (too close if min_distance > 1)
        return not direct_connection if min_distance > 1 else True
    
    def _find_shortest_path(self, start_node_id: int, end_node_id: int) -> Optional[Dict]:
        """Find shortest path between two nodes using BFS approach."""
        
        # Simple BFS implementation for path finding
        # Note: In a production system, you might want to use a more sophisticated
        # algorithm or leverage spatial database functions
        
        if start_node_id == end_node_id:
            return None
        
        # Get all links for pathfinding
        sql = '''
            SELECT start_node_id, end_node_id, id, cost
            FROM nw_links
            WHERE bidirected = 'Y'
        '''
        
        links = self.db.query(sql)
        
        # Build adjacency list
        graph = defaultdict(list)
        link_map = {}
        
        for link in links:
            start_id, end_id, link_id, cost = link
            graph[start_id].append((end_id, link_id, cost))
            graph[end_id].append((start_id, link_id, cost))
            link_map[link_id] = (start_id, end_id, cost)
        
        # BFS to find path
        from collections import deque
        
        queue = deque([(start_node_id, [start_node_id], [], 0.0)])
        visited = set()
        
        while queue:
            current_node, path_nodes, path_links, total_cost = queue.popleft()
            
            if current_node in visited:
                continue
            
            visited.add(current_node)
            
            if current_node == end_node_id:
                return {
                    'nodes': path_nodes,
                    'links': path_links,
                    'total_cost': total_cost
                }
            
            # Explore neighbors
            for neighbor_node, link_id, cost in graph[current_node]:
                if neighbor_node not in visited:
                    new_path_nodes = path_nodes + [neighbor_node]
                    new_path_links = path_links + [link_id]
                    new_total_cost = total_cost + cost
                    
                    queue.append((neighbor_node, new_path_nodes, new_path_links, new_total_cost))
        
        return None  # No path found
    
    def _extract_path_details(self, path_data: Dict) -> Optional[Dict]:
        """Extract detailed information about the path."""
        
        nodes = path_data['nodes']
        links = path_data['links']
        
        if not nodes or not links:
            return None
        
        # Get node details
        node_placeholders = ','.join(['?' for _ in nodes])
        node_sql = f'''
            SELECT id, data_code, utility_no, markers, e2e_group_no
            FROM nw_nodes
            WHERE id IN ({node_placeholders})
        '''
        
        node_rows = self.db.query(node_sql, nodes)
        
        # Get link details for length calculation
        link_placeholders = ','.join(['?' for _ in links])
        link_sql = f'''
            SELECT id, cost
            FROM nw_links
            WHERE id IN ({link_placeholders})
        '''
        
        link_rows = self.db.query(link_sql, links)
        
        # Extract unique values
        data_codes = list(set([row[1] for row in node_rows if row[1] is not None]))
        utility_nos = list(set([row[2] for row in node_rows if row[2] is not None]))
        toolset_nos = list(set([row[4] for row in node_rows if row[4] is not None]))
        
        # Extract references from markers
        references = []
        for row in node_rows:
            if row[3]:  # markers field
                # Extract reference from markers (simplified parsing)
                markers = row[3]
                if '=' in markers:
                    parts = markers.split('=')
                    if len(parts) > 1:
                        ref = parts[0].strip()
                        if ref not in references:
                            references.append(ref)
        
        # Calculate total length (assume cost represents length in mm)
        total_length_mm = sum([row[1] for row in link_rows if row[1] is not None])
        
        return {
            'nodes': nodes,
            'links': links,
            'total_cost': path_data['total_cost'],
            'total_length_mm': total_length_mm,
            'toolset_nos': toolset_nos,
            'data_codes': data_codes,
            'utility_nos': utility_nos,
            'references': references
        }
    
    def _update_sampling_stats(self, toolset_data: Dict, equipment_pair: Tuple[Dict, Dict], 
                              poc_pair: Dict):
        """Update sampling statistics for bias mitigation."""
        
        # Update toolset usage
        toolset_code = toolset_data['toolset']['code']
        self._toolset_usage[toolset_code] += 1
        
        # Update equipment usage
        start_equipment, end_equipment = equipment_pair
        self._equipment_usage[start_equipment['id']] += 1
        self._equipment_usage[end_equipment['id']] += 1
        
        # Update utility usage
        start_poc = poc_pair['start_poc']
        end_poc = poc_pair['end_poc']
        
        if start_poc.get('utility_no'):
            self._sampling_stats[f'utility_{start_poc["utility_no"]}'] += 1
        
        if end_poc.get('utility_no'):
            self._sampling_stats[f'utility_{end_poc["utility_no"]}'] += 1