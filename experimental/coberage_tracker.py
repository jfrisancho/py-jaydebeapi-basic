from collections import deque
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any
from bitarray import bitarray

@dataclass
class BiasReduction:
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3
    category_diversity_weight: float = 0.2
    phase_diversity_weight: float = 0.2
    
    # Plateau detection parameters
    plateau_threshold: int = 50
    min_coverage_improvement: float = 0.01
    coverage_history_size: int = 10

class CoverageTracker:
    def __init__(self, total_nodes, total_links, config):
        self.node_coverage = bitarray(total_nodes)
        self.link_coverage = bitarray(total_links)
        self.node_coverage.setall(0)
        self.link_coverage.setall(0)
        
        self.total_nodes = total_nodes
        self.total_links = total_links
        self.config = config
        
        # Plateau detection
        self.coverage_history = deque(maxlen=config.coverage_history_size)
        self.attempts_without_improvement = 0
        self.best_coverage = 0.0
    
    def get_current_coverage(self):
        """Calculate current coverage percentage using bitarray."""
        nodes_covered = self.node_coverage.count()
        links_covered = self.link_coverage.count()
        
        node_coverage_pct = nodes_covered / self.total_nodes if self.total_nodes > 0 else 0
        link_coverage_pct = links_covered / self.total_links if self.total_links > 0 else 0
        
        # Combined coverage (you can weight these differently)
        return (node_coverage_pct + link_coverage_pct) / 2
    
    def fast_coverage_check(self, path_result):
        """Fast check if path would improve coverage using bitarray operations."""
        # Create temporary bitarrays for the path
        path_nodes = bitarray(len(self.node_coverage))
        path_links = bitarray(len(self.link_coverage))
        
        path_nodes.setall(0)
        path_links.setall(0)
        
        # Set bits for path elements
        for node_idx in path_result.nodes:
            if 0 <= node_idx < len(path_nodes):
                path_nodes[node_idx] = 1
        for link_idx in path_result.links:
            if 0 <= link_idx < len(path_links):
                path_links[link_idx] = 1
        
        # Check improvement using bitwise operations
        node_improvement = (path_nodes & ~self.node_coverage).count()
        link_improvement = (path_links & ~self.link_coverage).count()
        
        return node_improvement + link_improvement > 0
    
    def update_coverage(self, path_result):
        """Update coverage bitarrays with new path and return improvement."""
        old_coverage = self.get_current_coverage()
        
        # Set bits for nodes in the path
        for node_idx in path_result.nodes:
            if 0 <= node_idx < len(self.node_coverage):
                self.node_coverage[node_idx] = 1
        
        # Set bits for links in the path
        for link_idx in path_result.links:
            if 0 <= link_idx < len(self.link_coverage):
                self.link_coverage[link_idx] = 1
        
        new_coverage = self.get_current_coverage()
        improvement = new_coverage - old_coverage
        
        if improvement > self.config.min_coverage_improvement:
            self.best_coverage = new_coverage
            self.attempts_without_improvement = 0
        else:
            self.attempts_without_improvement += 1
        
        self.coverage_history.append(new_coverage)
        return improvement > 0
    
    def is_plateau(self):
        """Check if we've hit a coverage plateau."""
        return self.attempts_without_improvement >= self.config.plateau_threshold
    
    def get_uncovered_elements(self):
        """Get indices of uncovered nodes and links for coverage-guided sampling."""
        uncovered_nodes = []
        uncovered_links = []
        
        # Find uncovered nodes
        for i in range(len(self.node_coverage)):
            if not self.node_coverage[i]:
                uncovered_nodes.append(i)
        
        # Find uncovered links  
        for i in range(len(self.link_coverage)):
            if not self.link_coverage[i]:
                uncovered_links.append(i)
        
        return uncovered_nodes, uncovered_links

def execute_random_sampling(self, run_id: str, config: RandomRunConfig) -> dict[str, Any]:
    """Execute random sampling until coverage target is achieved."""
    from .path import PathManager
    from .coverage import CoverageManager
    
    path_manager = PathManager(self.db)
    coverage_manager = CoverageManager(self.db)
    
    # Get universe size for bitarray initialization
    total_nodes, total_links = coverage_manager.get_universe_size()
    
    # Initialize coverage tracker with bitarray
    coverage_tracker = CoverageTracker(total_nodes, total_links, config.bias_reduction)
    
    metrics.total_attempts = 0
    metrics.total_paths_found = 0
    metrics.failed_attempts = 0
    
    # Initialize sampling universe
    sampling_universe = self._build_sampling_universe(config)
    
    print(f'Starting random sampling for run {run_id}')
    print(f'Target coverage: {config.coverage_target * 100:.1f}%')
    print(f'Sampling universe: {len(sampling_universe)} toolsets')
    print(f'Universe size: {total_nodes} nodes, {total_links} links')
    
    start_time = perf_counter_ns()
    
    # Store original configuration for restoration
    original_min_distance = config.bias_reduction.min_distance_between_nodes
    relaxation_level = 0
    max_relaxation_levels = 3
    
    while coverage_tracker.get_current_coverage() < config.coverage_target:
        metrics.total_attempts += 1
        
        # Select random PoC pair with bias mitigation
        poc_pair = self._select_random_poc_pair(sampling_universe, config)
        
        if not poc_pair:
            print(f'Warning: Could not select PoC pair after {metrics.total_attempts} attempts')
            break
        
        definition_id = path_manager.create_path_definition(poc_pair, config)
        attempt_id = path_manager.create_attempt_path(run_id, definition_id)
        
        # Check if path exists between PoCs
        path_result = self._find_path_between_pocs(poc_pair)
        
        if path_result:
            # USE fast_coverage_check HERE - before doing expensive operations
            if coverage_tracker.fast_coverage_check(path_result):
                path_manager.update_attempt_path_status(attempt_id, 'FOUND')
                
                # Update coverage using bitarray tracker
                coverage_improved = coverage_tracker.update_coverage(path_result)
                current_coverage = coverage_tracker.get_current_coverage()
                
                # Also update the database coverage manager
                coverage_manager.update_coverage(run_id, path_result.nodes, path_result.links)
                
                metrics.total_paths_found += 1
                
                if metrics.total_paths_found % 10 == 0:
                    elapsed = (perf_counter_ns() - start_time) / 1_000_000_000
                    print(f'Progress: {metrics.total_paths_found} paths found, {current_coverage * 100:.2f}% coverage, {elapsed:.1f}s elapsed')
            else:
                # Path doesn't improve coverage, mark as found but don't count it
                path_manager.update_attempt_path_status(attempt_id, 'FOUND_NO_IMPROVEMENT')
                print(f'Path found but no coverage improvement at attempt {metrics.total_attempts}')
        else:
            # Check if unused PoCs should be flagged for review
            metrics.failed_attempts += 1
            path_manager.update_attempt_path_status(attempt_id, 'NOT_FOUND')
            self._check_unused_pocs(run_id, poc_pair, metrics)
        
        # CHECK FOR PLATEAU and apply progressive relaxation
        if coverage_tracker.is_plateau():
            if relaxation_level < max_relaxation_levels:
                # Relax constraints progressively
                new_min_distance = max(1, original_min_distance - relaxation_level * 2)
                config.bias_reduction.min_distance_between_nodes = new_min_distance
                relaxation_level += 1
                
                print(f'Plateau detected at {coverage_tracker.get_current_coverage() * 100:.2f}% coverage')
                print(f'Relaxing constraints: min_distance_between_nodes = {new_min_distance}')
                
                # Reset plateau counter
                coverage_tracker.attempts_without_improvement = 0
            else:
                # Maximum relaxation reached, accept current coverage
                print(f'Maximum relaxation reached. Accepting coverage: {coverage_tracker.get_current_coverage() * 100:.2f}%')
                break
        
        # Safety break for very long runs
        if metrics.total_attempts > 100000:
            print(f'Warning: Reached maximum attempts limit ({metrics.total_attempts})')
            break
    
    # Restore original configuration
    config.bias_reduction.min_distance_between_nodes = original_min_distance
    
    elapsed_time = (perf_counter_ns() - start_time) / 1_000_000_000
    final_coverage = coverage_tracker.get_current_coverage()
    
    print(f'Sampling completed: {metrics.total_paths_found} paths found in {metrics.total_attempts} attempts ({elapsed_time:.1f}s)')
    print(f'Final coverage: {final_coverage * 100:.2f}%')
    
    return {
        'total_attempts': metrics.total_attempts,
        'total_paths_found': metrics.total_paths_found,
        'failed_attempts': metrics.failed_attempts,
        'toolsets_sampled': metrics.toolsets_sampled,
        'final_coverage': final_coverage,
        'total_nodes': total_nodes,
        'total_links': total_links,
        'elapsed_time': elapsed_time,
    }

def _select_random_poc_pair_with_coverage_guidance(self, sampling_universe, config, coverage_tracker):
    """Enhanced PoC selection that considers coverage gaps."""
    current_coverage = coverage_tracker.get_current_coverage()
    
    # Use coverage-guided selection when we have some coverage
    if current_coverage > 0.1:  # 10% threshold
        uncovered_nodes, uncovered_links = coverage_tracker.get_uncovered_elements()
        
        # Score toolsets based on potential to cover uncovered elements
        toolset_scores = []
        for toolset in sampling_universe:
            score = 0
            for poc in toolset.pocs:
                # Score based on how many uncovered nodes/links this PoC might reach
                # This is a heuristic - you might want to refine based on your domain
                if hasattr(poc, 'reachable_nodes'):
                    score += len(set(poc.reachable_nodes) & set(uncovered_nodes))
                if hasattr(poc, 'reachable_links'):
                    score += len(set(poc.reachable_links) & set(uncovered_links))
            toolset_scores.append(score)
        
        # Weighted selection based on coverage potential
        if sum(toolset_scores) > 0:
            selected_toolset = self._weighted_random_selection(sampling_universe, toolset_scores)
            return self._select_poc_pair_from_toolset(selected_toolset, config)
    
    # Fallback to original random selection
    return self._select_random_poc_pair(sampling_universe, config)
