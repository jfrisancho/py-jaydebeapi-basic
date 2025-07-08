# managers/random.py

import random
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict
from datetime import datetime

from db import Database
from string_helper import StringHelper


class RandomManager:
    """Random path generation with bias mitigation."""
    
    def __init__(self, db: Database):
        self.db = db
        self.sampling_universe = {}  # Cache for toolsets and equipment
        self.bias_reduction = {
            'max_attempts_per_toolset': 5,
            'max_attempts_per_equipment': 3,
            'min_distance_between_nodes': 10,
            'utility_diversity_weight': 0.3,
            'phase_diversity_weight': 0.2
        }

    def execute_sampling(self, run_id: str, config: 'RandomRunConfig', 
                        coverage_scope: 'CoverageScope') -> Dict[str, Any]:
        """Execute random sampling until coverage target is achieved."""
        
        # Initialize sampling universe
        self._initialize_sampling_universe(config)
        
        # Initialize tracking variables
        total_attempts = 0
        total_paths_found = 0
        unique_paths = 0
        current_coverage = 0.0
        
        # Bias mitigation tracking
        toolset_attempts = defaultdict(int)
        equipment_attempts = defaultdict(int)
        utility_distribution = defaultdict(int)
        
        # Path metrics accumulation
        path_metrics = {
            'total_nodes': 0,
            'total_links': 0,
            'total_length': 0.0,
            'node_counts': [],
            'link_counts': [],
            'length_values': []
        }
        
        print(f'Starting random sampling for run {run_id}')
        print(f'Target coverage: {config.coverage_target * 100:.1f}%')
        print(f'Scope: {coverage_scope.total_nodes} nodes, {coverage_scope.total_links} links')
        
        while current_coverage < config.coverage_target:
            total_attempts += 1
            
            # Select random PoC pair with bias mitigation
            poc_pair = self._select_random_poc_pair(
                config, toolset_attempts, equipment_attempts, utility_distribution
            )
            
            if not poc_pair:
                print(f'No more PoC pairs available after {total_attempts} attempts')
                break
            
            # Check if path exists between PoCs
            path_result = self._find_path_between_pocs(poc_pair)
            
            if path_result:
                # Store path and update coverage
                path_stored = self._store_path_result(run_id, path_result, coverage_scope)
                
                if path_stored:
                    total_paths_found += 1
                    unique_paths += 1  # Will be recalculated later for deduplication
                    
                    # Update path metrics
                    self._update_path_metrics(path_metrics, path_result)
                    
                    # Update coverage
                    current_coverage = self._calculate_current_coverage(coverage_scope)
                    
                    if total_attempts % 100 == 0:
                        print(f'Attempts: {total_attempts}, Paths: {total_paths_found}, Coverage: {current_coverage * 100:.2f}%')
            
            else:
                # Check if PoCs should be flagged for review
                self._check_unused_pocs(run_id, poc_pair)
            
            # Safety break for infinite loops
            if total_attempts >= 100000:
                print(f'Maximum attempts reached ({total_attempts})')
                break
        
        # Calculate final metrics
        avg_path_nodes = sum(path_metrics['node_counts']) / len(path_metrics['node_counts']) if path_metrics['node_counts'] else 0
        avg_path_links = sum(path_metrics['link_counts']) / len(path_metrics['link_counts']) if path_metrics['link_counts'] else 0
        avg_path_length = sum(path_metrics['length_values']) / len(path_metrics['length_values']) if path_metrics['length_values'] else 0
        
        # Get actual unique paths count
        unique_paths = self._count_unique_paths(run_id)
        
        print(f'Sampling completed: {total_paths_found} paths found in {total_attempts} attempts')
        print(f'Final coverage: {current_coverage * 100:.2f}%')
        
        return {
            'total_attempts': total_attempts,
            'total_paths_found': total_paths_found,
            'unique_paths': unique_paths,
            'target_coverage': config.coverage_target,
            'achieved_coverage': current_coverage,
            'avg_path_nodes': avg_path_nodes,
            'avg_path_links': avg_path_links,
            'avg_path_length': avg_path_length
        }

    def _initialize_sampling_universe(self, config: 'RandomRunConfig'):
        """Initialize the sampling universe based on configuration filters."""
        filters = {}
        
        if config.fab_no:
            filters['fab_no'] = ('=', config.fab_no)
        if config.phase_no:
            filters['phase_no'] = ('=', config.phase_no)
        if config.model_no:
            filters['model_no'] = ('=', config.model_no)
        if config.e2e_group_no:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        
        # Fetch available toolsets
        base_sql = '''
        SELECT DISTINCT t.code, t.fab_no, t.model_no, t.phase_no, t.e2e_group_no
        FROM tb_toolsets t
        WHERE t.is_active = 1
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause
        
        toolsets = self.db.query(sql, params)
        
        # Initialize sampling universe structure
        self.sampling_universe = {
            'toolsets': [row[0] for row in toolsets],  # toolset codes
            'toolset_details': {row[0]: {
                'fab_no': row[1],
                'model_no': row[2], 
                'phase_no': row[3],
                'e2e_group_no': row[4]
            } for row in toolsets},
            'equipment_cache': {},
            'poc_cache': {}
        }
        
        print(f'Initialized sampling universe with {len(self.sampling_universe["toolsets"])} toolsets')

    def _select_random_poc_pair(self, config: 'RandomRunConfig', toolset_attempts: Dict, 
                               equipment_attempts: Dict, utility_distribution: Dict) -> Optional[Tuple]:
        """Select random PoC pair with bias mitigation."""
        
        max_toolset_attempts = 10
        
        for _ in range(max_toolset_attempts):
            # Select random toolset with bias consideration
            toolset = self._select_random_toolset(toolset_attempts)
            if not toolset:
                continue
            
            # Get equipment for this toolset
            equipment_list = self._get_toolset_equipment(toolset)
            if len(equipment_list) < 2:
                continue
            
            # Select two different equipment with bias consideration
            equipment_pair = self._select_equipment_pair(equipment_list, equipment_attempts)
            if not equipment_pair:
                continue
            
            # Get PoCs for each equipment
            eq1_pocs = self._get_equipment_pocs(equipment_pair[0])
            eq2_pocs = self._get_equipment_pocs(equipment_pair[1])
            
            if not eq1_pocs or not eq2_pocs:
                continue
            
            # Select random PoCs from each equipment
            poc1 = random.choice(eq1_pocs)
            poc2 = random.choice(eq2_pocs)
            
            # Apply distance and utility diversity checks
            if self._validate_poc_pair(poc1, poc2, utility_distribution):
                # Update attempt counters
                toolset_attempts[toolset] += 1
                equipment_attempts[equipment_pair[0]] += 1
                equipment_attempts[equipment_pair[1]] += 1
                
                return (poc1, poc2, toolset)
        
        return None

    def _select_random_toolset(self, toolset_attempts: Dict) -> Optional[str]:
        """Select random toolset with bias mitigation."""
        available_toolsets = [
            ts for ts in self.sampling_universe['toolsets']
            if toolset_attempts[ts] < self.bias_reduction['max_attempts_per_toolset']
        ]
        
        if not available_toolsets:
            # Reset counters if all toolsets have been exhausted
            toolset_attempts.clear()
            available_toolsets = self.sampling_universe['toolsets']
        
        return random.choice(available_toolsets) if available_toolsets else None

    def _get_toolset_equipment(self, toolset: str) -> List[int]:
        """Get equipment list for a toolset with caching."""
        if toolset not in self.sampling_universe['equipment_cache']:
            sql = '''
            SELECT id FROM tb_equipments 
            WHERE toolset = ? AND is_active = 1
            '''
            rows = self.db.query(sql, [toolset])
            self.sampling_universe['equipment_cache'][toolset] = [row[0] for row in rows]
        
        return self.sampling_universe['equipment_cache'][toolset]

    def _select_equipment_pair(self, equipment_list: List[int], 
                              equipment_attempts: Dict) -> Optional[Tuple[int, int]]:
        """Select pair of equipment with bias consideration."""
        # Filter equipment that haven't exceeded attempt limits
        available_equipment = [
            eq for eq in equipment_list
            if equipment_attempts[eq] < self.bias_reduction['max_attempts_per_equipment']
        ]
        
        if len(available_equipment) < 2:
            # Reset if not enough equipment available
            for eq in equipment_list:
                equipment_attempts[eq] = 0
            available_equipment = equipment_list
        
        if len(available_equipment) >= 2:
            return tuple(random.sample(available_equipment, 2))
        
        return None

    def _get_equipment_pocs(self, equipment_id: int) -> List[Dict[str, Any]]:
        """Get PoCs for equipment with caching."""
        if equipment_id not in self.sampling_universe['poc_cache']:
            sql = '''
            SELECT p.id, p.node_id, p.equipment_id, p.utility_no, p.markers, p.reference
            FROM tb_equipment_pocs p
            WHERE p.equipment_id = ? AND p.is_active = 1 AND p.is_used = 1
            '''
            rows = self.db.query(sql, [equipment_id])
            self.sampling_universe['poc_cache'][equipment_id] = [
                {
                    'poc_id': row[0],
                    'node_id': row[1],
                    'equipment_id': row[2],
                    'utility_no': row[3],
                    'markers': row[4],
                    'reference': row[5]
                }
                for row in rows
            ]
        
        return self.sampling_universe['poc_cache'][equipment_id]

    def _validate_poc_pair(self, poc1: Dict, poc2: Dict, utility_distribution: Dict) -> bool:
        """Validate PoC pair based on bias reduction criteria."""
        # Check minimum distance between nodes
        node_distance = abs(poc1['node_id'] - poc2['node_id'])
        if node_distance < self.bias_reduction['min_distance_between_nodes']:
            return False
        
        # Apply utility diversity weighting
        utility1 = poc1.get('utility_no')
        utility2 = poc2.get('utility_no')
        
        if utility1 and utility2:
            utility_key = f'{min(utility1, utility2)}_{max(utility1, utility2)}'
            utility_count = utility_distribution.get(utility_key, 0)
            
            # Apply probability based on utility diversity weight
            diversity_factor = 1.0 / (1.0 + utility_count * self.bias_reduction['utility_diversity_weight'])
            if random.random() > diversity_factor:
                return False
            
            utility_distribution[utility_key] += 1
        
        return True

    def _find_path_between_pocs(self, poc_pair: Tuple) -> Optional[Dict[str, Any]]:
        """Find path between two PoCs using network traversal."""
        poc1, poc2, toolset = poc_pair
        
        start_node_id = poc1['node_id']
        end_node_id = poc2['node_id']
        
        # Simple pathfinding query (can be enhanced with actual pathfinding algorithm)
        sql = '''
        WITH RECURSIVE path_finder(node_id, path_nodes, path_links, total_cost, depth) AS (
            SELECT ?, CAST(? AS VARCHAR(1000)), '', 0.0, 0
            
            UNION ALL
            
            SELECT 
                l.end_node_id,
                pf.path_nodes || ',' || CAST(l.end_node_id AS VARCHAR),
                pf.path_links || ',' || CAST(l.id AS VARCHAR),
                pf.total_cost + l.cost,
                pf.depth + 1
            FROM path_finder pf
            JOIN nw_links l ON l.start_node_id = pf.node_id
            WHERE pf.depth < 50
            AND l.end_node_id NOT IN (
                SELECT CAST(value AS BIGINT) 
                FROM STRING_SPLIT(pf.path_nodes, ',')
            )
        )
        SELECT path_nodes, path_links, total_cost
        FROM path_finder 
        WHERE node_id = ?
        ORDER BY total_cost ASC
        LIMIT 1
        '''
        
        try:
            rows = self.db.query(sql, [start_node_id, start_node_id, end_node_id])
            
            if rows:
                path_nodes_str, path_links_str, total_cost = rows[0]
                
                # Parse path components
                path_nodes = [int(x) for x in path_nodes_str.split(',') if x]
                path_links = [int(x) for x in path_links_str.split(',') if x]
                
                # Get additional path metadata
                path_metadata = self._get_path_metadata(path_nodes, path_links)
                
                return {
                    'start_node_id': start_node_id,
                    'start_poc_id': poc1['poc_id'],
                    'start_equipment_id': poc1['equipment_id'],
                    'end_node_id': end_node_id,
                    'end_poc_id': poc2['poc_id'],
                    'end_equipment_id': poc2['equipment_id'],
                    'nodes': path_nodes,
                    'links': path_links,
                    'total_cost': total_cost,
                    'total_length_mm': path_metadata.get('total_length_mm', 0.0),
                    'toolset_nos': path_metadata.get('toolset_nos', []),
                    'data_codes': path_metadata.get('data_codes', []),
                    'utility_nos': path_metadata.get('utility_nos', []),
                    'references': path_metadata.get('references', [])
                }
                
        except Exception as e:
            print(f'Error finding path: {e}')
        
        return None

    def _get_path_metadata(self, path_nodes: List[int], path_links: List[int]) -> Dict[str, Any]:
        """Get additional metadata for the path."""
        if not path_nodes:
            return {}
        
        # Get node metadata
        node_placeholders = ','.join(['?' for _ in path_nodes])
        sql = f'''
        SELECT DISTINCT data_code, e2e_group_no, utility_no, markers
        FROM nw_nodes 
        WHERE id IN ({node_placeholders})
        '''
        
        node_rows = self.db.query(sql, path_nodes)
        
        # Get link metadata  
        if path_links:
            link_placeholders = ','.join(['?' for _ in path_links])
            link_sql = f'''
            SELECT SUM(cost) as total_length
            FROM nw_links 
            WHERE id IN ({link_placeholders})
            '''
            link_rows = self.db.query(link_sql, path_links)
            total_length = link_rows[0][0] if link_rows and link_rows[0][0] else 0.0
        else:
            total_length = 0.0
        
        # Extract unique values
        data_codes = list(set(row[0] for row in node_rows if row[0]))
        toolset_nos = list(set(row[1] for row in node_rows if row[1]))
        utility_nos = list(set(row[2] for row in node_rows if row[2]))
        references = list(set(row[3] for row in node_rows if row[3]))
        
        return {
            'total_length_mm': total_length,
            'data_codes': data_codes,
            'toolset_nos': toolset_nos,
            'utility_nos': utility_nos,
            'references': references
        }

    def _store_path_result(self, run_id: str, path_result: Dict[str, Any], 
                          coverage_scope: 'CoverageScope') -> bool:
        """Store path result and update coverage."""
        try:
            # Generate path hash for deduplication
            path_hash = StringHelper.generate_path_hash(
                path_result['nodes'], path_result['links']
            )
            
            # Store attempt path
            attempt_sql = '''
            INSERT INTO tb_attempt_paths (
                run_id, path_definition_id, start_node_id, end_node_id, 
                cost, picked_at, tested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            
            # First store the path definition if it doesn't exist
            path_def_id = self._store_path_definition(path_result, path_hash)
            
            attempt_params = [
                run_id, path_def_id, path_result['start_node_id'],
                path_result['end_node_id'], path_result['total_cost'],
                datetime.now(), datetime.now()
            ]
            
            self.db.update(attempt_sql, attempt_params)
            
            # Update coverage tracking
            self._update_coverage_tracking(run_id, path_result, coverage_scope)
            
            return True
            
        except Exception as e:
            print(f'Error storing path result: {e}')
            return False

    def _store_path_definition(self, path_result: Dict[str, Any], path_hash: str) -> int:
        """Store path definition and return its ID."""
        # Check if path definition already exists
        check_sql = 'SELECT id FROM tb_path_definitions WHERE path_hash = ?'
        existing = self.db.query(check_sql, [path_hash])
        
        if existing:
            return existing[0][0]
        
        # Store new path definition
        sql = '''
        INSERT INTO tb_path_definitions (
            path_hash, source_type, scope, node_count, link_count,
            total_length_mm, coverage, data_codes_scope, utilities_scope,
            references_scope, path_context, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            path_hash, 'RANDOM', 'CONNECTIVITY',
            len(path_result['nodes']), len(path_result['links']),
            path_result['total_length_mm'], 0.0,  # coverage will be calculated later
            ','.join(map(str, path_result['data_codes'])),
            ','.join(map(str, path_result['utility_nos'])),
            ','.join(path_result['references']),
            f"nodes:{','.join(map(str, path_result['nodes']))};links:{','.join(map(str, path_result['links']))}",
            datetime.now()
        ]
        
        self.db.update(sql, params)
        
        # Get the inserted ID
        id_rows = self.db.query(check_sql, [path_hash])
        return id_rows[0][0] if id_rows else None

    def _update_coverage_tracking(self, run_id: str, path_result: Dict[str, Any], 
                                 coverage_scope: 'CoverageScope'):
        """Update coverage tracking tables."""
        # Store covered nodes
        for node_id in path_result['nodes']:
            try:
                sql = '''
                INSERT INTO tb_run_covered_nodes (run_id, node_id, covered_at)
                VALUES (?, ?, ?)
                '''
                self.db.update(sql, [run_id, node_id, datetime.now()])
            except:
                # Node already covered, ignore duplicate
                pass
        
        # Store covered links
        for link_id in path_result['links']:
            try:
                sql = '''
                INSERT INTO tb_run_covered_links (run_id, link_id, covered_at)
                VALUES (?, ?, ?)
                '''
                self.db.update(sql, [run_id, link_id, datetime.now()])
            except:
                # Link already covered, ignore duplicate
                pass

    def _calculate_current_coverage(self, coverage_scope: 'CoverageScope') -> float:
        """Calculate current coverage percentage."""
        # This would typically be handled by CoverageManager
        # Simplified calculation here
        return 0.5  # Placeholder

    def _check_unused_pocs(self, run_id: str, poc_pair: Tuple):
        """Check if PoCs should be flagged for review."""
        poc1, poc2, toolset = poc_pair
        
        # Check if both PoCs are marked as used but no path exists
        if poc1.get('is_used') and poc2.get('is_used'):
            # Flag for manual review
            flag_sql = '''
            INSERT INTO tb_review_flags (
                run_id, flag_type, severity, reason, object_type,
                object_id, object_guid, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            reason = f'No path found between used PoCs {poc1["poc_id"]} and {poc2["poc_id"]}'
            
            params = [
                run_id, 'CONNECTIVITY_ISSUE', 'MEDIUM', reason,
                'POC', poc1['poc_id'], f'poc_{poc1["poc_id"]}', datetime.now()
            ]
            
            self.db.update(flag_sql, params)

    def _count_unique_paths(self, run_id: str) -> int:
        """Count unique paths for the run."""
        sql = '''
        SELECT COUNT(DISTINCT pd.path_hash)
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        return rows[0][0] if rows else 0

    def _update_path_metrics(self, path_metrics: Dict, path_result: Dict[str, Any]):
        """Update accumulated path metrics."""
        path_metrics['total_nodes'] += len(path_result['nodes'])
        path_metrics['total_links'] += len(path_result['links'])
        path_metrics['total_length'] += path_result['total_length_mm']
        
        path_metrics['node_counts'].append(len(path_result['nodes']))
        path_metrics['link_counts'].append(len(path_result['links']))
        path_metrics['length_values'].append(path_result['total_length_mm'])