# managers/random.py

import random
import time
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass
from datetime import datetime

from db import Database
from string_helper import StringHelper


class RandomManager:
    """
    Random path generation with bias mitigation for equipment PoC sampling.
    Implements intelligent sampling strategies to achieve coverage targets efficiently.
    """
    
    def __init__(self, db: Database):
        self.db = db
        self.bias_reduction = BiasReduction()
        self.sampling_universe = {}  # Cache for sampling universe
        
    def execute_random_sampling(self, config: 'RunConfig', scope: 'CoverageScope') -> 'RandomRunSummary':
        """
        Execute random sampling until coverage target is reached.
        
        Args:
            config: RunConfig with random sampling parameters
            scope: CoverageScope defining the sampling universe
            
        Returns:
            RandomRunSummary with sampling results
        """
        random_config = config.random_config
        run_id = config.run_id
        coverage_target = random_config.coverage_target
        
        # Initialize sampling universe
        self._initialize_sampling_universe(random_config, scope)
        
        # Initialize tracking variables
        total_attempts = 0
        total_paths_found = 0
        unique_paths = 0
        current_coverage = 0.0
        
        start_time = time.time()
        
        print(f'Starting random sampling for run {run_id} with target coverage {coverage_target*100:.1f}%')
        
        # Import managers after avoiding circular imports
        from .path import PathManager
        from .coverage import CoverageManager
        
        path_manager = PathManager(self.db)
        coverage_manager = CoverageManager(self.db)
        
        while current_coverage < coverage_target:
            # Apply bias mitigation by selecting diverse PoC pairs
            poc_pair = self._select_random_poc_pair(random_config, scope)
            
            if not poc_pair:
                print(f'Warning: Could not find valid PoC pair after maximum attempts')
                break
                
            total_attempts += 1
            
            # Record attempt
            self._record_attempt(run_id, poc_pair)
            
            # Find path between PoCs
            path_result = self._find_path_between_pocs(poc_pair)
            
            if path_result:
                total_paths_found += 1
                
                # Store path and check if it's unique
                path_hash = StringHelper.generate_path_hash(path_result.nodes, path_result.links)
                
                if self._is_unique_path(run_id, path_hash):
                    unique_paths += 1
                    
                    # Store path definition
                    path_manager.store_path_definition(run_id, path_result, path_hash)
                    
                    # Update coverage
                    coverage_manager.update_coverage(run_id, path_result.nodes, path_result.links)
                    
                    # Calculate current coverage
                    current_coverage = coverage_manager.calculate_coverage_percentage(run_id)
                    
                    if config.verbose_mode:
                        print(f'Path found: {len(path_result.nodes)} nodes, {len(path_result.links)} links. Coverage: {current_coverage*100:.2f}%')
            else:
                # Check if PoCs should be flagged for review
                self._check_poc_connectivity_issues(run_id, poc_pair)
            
            # Progress reporting
            if total_attempts % 100 == 0:
                elapsed = time.time() - start_time
                print(f'Progress: {total_attempts} attempts, {total_paths_found} paths, {unique_paths} unique, {current_coverage*100:.2f}% coverage ({elapsed:.1f}s)')
        
        # Calculate final metrics
        final_coverage = coverage_manager.calculate_coverage_percentage(run_id)
        coverage_efficiency = final_coverage / coverage_target if coverage_target > 0 else 0.0
        success_rate = total_paths_found / total_attempts if total_attempts > 0 else 0.0
        
        # Get coverage details for metrics
        coverage_details = coverage_manager.fetch_coverage_summary(run_id)
        
        return RandomRunSummary(
            total_attempts=total_attempts,
            total_paths_found=total_paths_found,
            unique_paths=unique_paths,
            target_coverage=coverage_target,
            achieved_coverage=final_coverage,
            coverage_efficiency=coverage_efficiency,
            total_nodes=coverage_details.get('covered_nodes', 0),
            total_links=coverage_details.get('covered_links', 0),
            avg_path_nodes=coverage_details.get('avg_path_nodes'),
            avg_path_links=coverage_details.get('avg_path_links'),
            avg_path_length=coverage_details.get('avg_path_length'),
            success_rate=success_rate
        )
    
    def _initialize_sampling_universe(self, config: 'RandomRunConfig', scope: 'CoverageScope') -> None:
        """
        Initialize the sampling universe based on configuration parameters.
        Creates cached lookups for efficient random selection.
        """
        filters = {}
        
        # Build base filters
        if config.fab_no is not None:
            filters['ts.fab_no'] = ('=', config.fab_no)
        if config.phase_no is not None:
            filters['ts.phase_no'] = ('=', config.phase_no)
        if config.model_no is not None:
            filters['ts.model_no'] = ('=', config.model_no)
        if config.e2e_group_no is not None:
            filters['ts.e2e_group_no'] = ('=', config.e2e_group_no)
        if config.toolset:
            filters['ts.code'] = ('=', config.toolset)
        
        # Add active filter
        filters['ts.is_active'] = ('=', 1)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        # Fetch toolsets in scope
        toolset_sql = f'''
            SELECT ts.code, ts.fab_no, ts.phase_no, ts.model_no, ts.e2e_group_no,
                   COUNT(eq.id) as equipment_count
            FROM tb_toolsets ts
            JOIN tb_equipments eq ON ts.code = eq.toolset
            {where_clause}
            AND eq.is_active = 1
            GROUP BY ts.code, ts.fab_no, ts.phase_no, ts.model_no, ts.e2e_group_no
            HAVING COUNT(eq.id) >= 2
        '''
        
        toolsets = self.db.query(toolset_sql, params)
        
        self.sampling_universe = {
            'toolsets': [],
            'equipment_by_toolset': {},
            'poc_by_equipment': {}
        }
        
        for row in toolsets:
            toolset_code = row[0]
            self.sampling_universe['toolsets'].append({
                'code': toolset_code,
                'fab_no': row[1],
                'phase_no': row[2],
                'model_no': row[3],
                'e2e_group_no': row[4],
                'equipment_count': row[5]
            })
            
            # Cache equipment for this toolset
            self._cache_toolset_equipment(toolset_code)
        
        print(f'Sampling universe initialized: {len(self.sampling_universe["toolsets"])} toolsets')
    
    def _cache_toolset_equipment(self, toolset_code: str) -> None:
        """Cache equipment and PoCs for a specific toolset."""
        
        # Fetch equipment in toolset
        eq_sql = '''
            SELECT id, guid, node_id, data_code, category_no, kind, name
            FROM tb_equipments
            WHERE toolset = ? AND is_active = 1
        '''
        
        equipment = self.db.query(eq_sql, [toolset_code])
        
        self.sampling_universe['equipment_by_toolset'][toolset_code] = []
        
        for eq_row in equipment:
            equipment_id = eq_row[0]
            equipment_info = {
                'id': equipment_id,
                'guid': eq_row[1],
                'node_id': eq_row[2],
                'data_code': eq_row[3],
                'category_no': eq_row[4],
                'kind': eq_row[5],
                'name': eq_row[6]
            }
            
            self.sampling_universe['equipment_by_toolset'][toolset_code].append(equipment_info)
            
            # Cache PoCs for this equipment
            self._cache_equipment_pocs(equipment_id)
    
    def _cache_equipment_pocs(self, equipment_id: int) -> None:
        """Cache PoCs for a specific equipment."""
        
        poc_sql = '''
            SELECT id, node_id, is_used, markers, utility_no, reference, flow, is_loopback
            FROM tb_equipment_pocs
            WHERE equipment_id = ? AND is_active = 1
        '''
        
        pocs = self.db.query(poc_sql, [equipment_id])
        
        poc_list = []
        for poc_row in pocs:
            poc_list.append({
                'id': poc_row[0],
                'node_id': poc_row[1],
                'is_used': poc_row[2],
                'markers': poc_row[3],
                'utility_no': poc_row[4],
                'reference': poc_row[5],
                'flow': poc_row[6],
                'is_loopback': poc_row[7]
            })
        
        self.sampling_universe['poc_by_equipment'][equipment_id] = poc_list
    
    def _select_random_poc_pair(self, config: 'RandomRunConfig', scope: 'CoverageScope') -> Optional[Tuple[Dict, Dict]]:
        """
        Select a random pair of PoCs with bias mitigation.
        
        Returns:
            Tuple of (start_poc_info, end_poc_info) or None if no valid pair found
        """
        max_attempts = 50
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            
            # Step 1: Select random toolset(s)
            if config.is_inter_toolset:
                # Inter-toolset: select two different toolsets
                if len(self.sampling_universe['toolsets']) < 2:
                    return None
                    
                toolset1, toolset2 = random.sample(self.sampling_universe['toolsets'], 2)
                start_equipment = self._select_random_equipment(toolset1['code'])
                end_equipment = self._select_random_equipment(toolset2['code'])
            else:
                # Intra-toolset: select one toolset, two different equipment
                toolset = random.choice(self.sampling_universe['toolsets'])
                toolset_code = toolset['code']
                
                equipment_list = self.sampling_universe['equipment_by_toolset'].get(toolset_code, [])
                if len(equipment_list) < 2:
                    continue
                    
                start_equipment, end_equipment = random.sample(equipment_list, 2)
            
            if not start_equipment or not end_equipment:
                continue
            
            # Step 2: Select PoCs from each equipment
            start_poc = self._select_random_poc(start_equipment['id'])
            end_poc = self._select_random_poc(end_equipment['id'])
            
            if not start_poc or not end_poc:
                continue
            
            # Step 3: Apply bias mitigation checks
            if self._passes_bias_mitigation(start_poc, end_poc):
                return (
                    {**start_poc, 'equipment': start_equipment},
                    {**end_poc, 'equipment': end_equipment}
                )
        
        return None
    
    def _select_random_equipment(self, toolset_code: str) -> Optional[Dict]:
        """Select random equipment from toolset."""
        equipment_list = self.sampling_universe['equipment_by_toolset'].get(toolset_code, [])
        return random.choice(equipment_list) if equipment_list else None
    
    def _select_random_poc(self, equipment_id: int) -> Optional[Dict]:
        """Select random PoC from equipment, preferring used PoCs."""
        poc_list = self.sampling_universe['poc_by_equipment'].get(equipment_id, [])
        
        if not poc_list:
            return None
        
        # Prefer used PoCs (higher probability of having connections)
        used_pocs = [poc for poc in poc_list if poc['is_used']]
        if used_pocs and random.random() < 0.8:  # 80% chance to pick used PoC
            return random.choice(used_pocs)
        
        return random.choice(poc_list)
    
    def _passes_bias_mitigation(self, start_poc: Dict, end_poc: Dict) -> bool:
        """
        Apply bias mitigation checks to PoC pair.
        
        Args:
            start_poc: Start PoC information
            end_poc: End PoC information
            
        Returns:
            True if pair passes bias mitigation checks
        """
        # Check minimum distance between nodes
        node_distance = abs(start_poc['node_id'] - end_poc['node_id'])
        if node_distance < self.bias_reduction.min_distance_between_nodes:
            return False
        
        # Check utility diversity (if utilities are different, higher weight)
        start_utility = start_poc.get('utility_no')
        end_utility = end_poc.get('utility_no')
        
        if start_utility and end_utility and start_utility != end_utility:
            # Different utilities - apply diversity weight
            if random.random() < self.bias_reduction.utility_diversity_weight:
                return True
        
        # Check phase diversity
        start_equipment = start_poc.get('equipment', {})
        end_equipment = end_poc.get('equipment', {})
        
        if start_equipment and end_equipment:
            start_phase = start_equipment.get('phase_no')
            end_phase = end_equipment.get('phase_no')
            
            if start_phase and end_phase and start_phase != end_phase:
                # Different phases - apply diversity weight
                if random.random() < self.bias_reduction.phase_diversity_weight:
                    return True
        
        # Default acceptance for other cases
        return True
    
    def _find_path_between_pocs(self, poc_pair: Tuple[Dict, Dict]) -> Optional['PathResult']:
        """
        Find path between two PoCs using network traversal.
        
        Args:
            poc_pair: Tuple of (start_poc_info, end_poc_info)
            
        Returns:
            PathResult if path found, None otherwise
        """
        start_poc, end_poc = poc_pair
        start_node_id = start_poc['node_id']
        end_node_id = end_poc['node_id']
        
        # Simple breadth-first search for path finding
        # This is a placeholder - you may want to implement more sophisticated pathfinding
        path_nodes, path_links, total_cost = self._bfs_pathfind(start_node_id, end_node_id)
        
        if not path_nodes:
            return None
        
        # Calculate path metrics
        total_length_mm = self._calculate_path_length(path_links)
        toolset_nos, data_codes, utility_nos, references = self._extract_path_attributes(path_nodes)
        
        return PathResult(
            start_node_id=start_node_id,
            start_poc_id=start_poc['id'],
            start_equipment_id=start_poc['equipment']['id'],
            end_node_id=end_node_id,
            end_poc_id=end_poc['id'],
            end_equipment_id=end_poc['equipment']['id'],
            nodes=path_nodes,
            links=path_links,
            total_cost=total_cost,
            total_length_mm=total_length_mm,
            toolset_nos=toolset_nos,
            data_codes=data_codes,
            utility_nos=utility_nos,
            references=references
        )
    
    def _bfs_pathfind(self, start_node_id: int, end_node_id: int) -> Tuple[List[int], List[int], float]:
        """
        Breadth-first search pathfinding between nodes.
        
        Args:
            start_node_id: Starting node ID
            end_node_id: Target node ID
            
        Returns:
            Tuple of (path_nodes, path_links, total_cost)
        """
        if start_node_id == end_node_id:
            return [start_node_id], [], 0.0
        
        # BFS implementation
        from collections import deque
        
        queue = deque([(start_node_id, [start_node_id], [], 0.0)])
        visited = {start_node_id}
        max_depth = 100  # Limit search depth
        
        while queue and len(visited) < max_depth:
            current_node, path_nodes, path_links, total_cost = queue.popleft()
            
            # Find connected nodes
            sql = '''
                SELECT end_node_id, id, cost 
                FROM nw_links 
                WHERE start_node_id = ?
                UNION ALL
                SELECT start_node_id, id, cost 
                FROM nw_links 
                WHERE end_node_id = ? AND bidirected = 'Y'
            '''
            
            connected = self.db.query(sql, [current_node, current_node])
            
            for next_node_id, link_id, link_cost in connected:
                if next_node_id in visited:
                    continue
                
                new_path_nodes = path_nodes + [next_node_id]
                new_path_links = path_links + [link_id]
                new_total_cost = total_cost + (link_cost or 0.0)
                
                if next_node_id == end_node_id:
                    return new_path_nodes, new_path_links, new_total_cost
                
                visited.add(next_node_id)
                queue.append((next_node_id, new_path_nodes, new_path_links, new_total_cost))
        
        return [], [], 0.0
    
    def _calculate_path_length(self, path_links: List[int]) -> float:
        """Calculate total path length from links."""
        if not path_links:
            return 0.0
        
        # This is a placeholder - actual length calculation would depend on your spatial data
        # For now, assume each link has unit length
        return float(len(path_links))
    
    def _extract_path_attributes(self, path_nodes: List[int]) -> Tuple[List[int], List[int], List[int], List[str]]:
        """Extract attributes from path nodes."""
        if not path_nodes:
            return [], [], [], []
        
        placeholders = ','.join(['?'] * len(path_nodes))
        sql = f'''
            SELECT DISTINCT e2e_group_no, data_code, utility_no, markers
            FROM nw_nodes
            WHERE id IN ({placeholders})
            AND e2e_group_no IS NOT NULL
        '''
        
        rows = self.db.query(sql, path_nodes)
        
        toolset_nos = []
        data_codes = []
        utility_nos = []
        references = []
        
        for row in rows:
            if row[0] is not None:
                toolset_nos.append(row[0])
            if row[1] is not None:
                data_codes.append(row[1])
            if row[2] is not None:
                utility_nos.append(row[2])
            if row[3] is not None:
                # Extract reference from markers (first part before delimiter)
                markers = row[3]
                if markers:
                    ref = markers.split('|')[0] if '|' in markers else markers[:8]
                    references.append(ref)
        
        return list(set(toolset_nos)), list(set(data_codes)), list(set(utility_nos)), list(set(references))
    
    def _record_attempt(self, run_id: str, poc_pair: Tuple[Dict, Dict]) -> None:
        """Record sampling attempt in database."""
        start_poc, end_poc = poc_pair
        
        sql = '''
            INSERT INTO tb_attempt_paths (
                run_id, path_definition_id, start_node_id, end_node_id, picked_at
            ) VALUES (?, ?, ?, ?, ?)
        '''
        
        # For attempts, we use NULL for path_definition_id since path may not exist yet
        params = [
            run_id,
            None,  # Will be updated if path is found
            start_poc['node_id'],
            end_poc['node_id'],
            datetime.now()
        ]
        
        self.db.update(sql, params)
    
    def _is_unique_path(self, run_id: str, path_hash: str) -> bool:
        """Check if path hash is unique for this run."""
        sql = '''
            SELECT COUNT(*) 
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ? AND pd.path_hash = ?
        '''
        
        count = self.db.query(sql, [run_id, path_hash])[0][0]
        return count == 0
    
    def _check_poc_connectivity_issues(self, run_id: str, poc_pair: Tuple[Dict, Dict]) -> None:
        """
        Check for PoC connectivity issues and flag for review if necessary.
        
        Args:
            run_id: Current run ID
            poc_pair: Tuple of PoC information
        """
        start_poc, end_poc = poc_pair
        
        # Check if both PoCs are marked as used but no path exists
        if start_poc.get('is_used') and end_poc.get('is_used'):
            # Flag for manual review
            from .validation import ValidationManager
            validation_manager = ValidationManager(self.db)
            
            validation_manager.flag_connectivity_issue(
                run_id=run_id,
                start_poc=start_poc,
                end_poc=end_poc,
                reason='Used PoCs without connectivity path'
            )