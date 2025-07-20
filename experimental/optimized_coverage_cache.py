from dataclasses import dataclass, field
from typing import Optional, Union
from collections import deque
import weakref
from bitarray import bitarray
import numpy as np
from array import array

@dataclass
class CoverageScope:
    """Defines the scope for coverage calculation with memory optimization."""
    fab_no: Optional[int] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    e2e_group_nos: Optional[tuple[int, ...]] = None  # Use tuple instead of list for immutability
    
    total_nodes: int = 0
    total_links: int = 0
    
    # Use numpy arrays for better memory efficiency with large mappings
    _node_ids: Optional[np.ndarray] = field(default=None, init=False)
    _link_ids: Optional[np.ndarray] = field(default=None, init=False)
    _node_id_to_index: Optional[dict[int, int]] = field(default=None, init=False)
    _link_id_to_index: Optional[dict[int, int]] = field(default=None, init=False)
    
    def __post_init__(self):
        if self.e2e_group_nos is not None and isinstance(self.e2e_group_nos, list):
            # Convert to tuple for memory efficiency and immutability
            object.__setattr__(self, 'e2e_group_nos', tuple(self.e2e_group_nos))
    
    def initialize_mappings(self, node_ids: list[int], link_ids: list[int]):
        """Initialize ID mappings efficiently."""
        # Sort IDs for potential optimizations
        sorted_node_ids = sorted(set(node_ids))
        sorted_link_ids = sorted(set(link_ids))
        
        # Use numpy arrays for the actual IDs (more memory efficient)
        self._node_ids = np.array(sorted_node_ids, dtype=np.int32)
        self._link_ids = np.array(sorted_link_ids, dtype=np.int32)
        
        # Create mappings using dict comprehension (faster than loop)
        self._node_id_to_index = {node_id: idx for idx, node_id in enumerate(sorted_node_ids)}
        self._link_id_to_index = {link_id: idx for idx, link_id in enumerate(sorted_link_ids)}
        
        # Update totals
        object.__setattr__(self, 'total_nodes', len(sorted_node_ids))
        object.__setattr__(self, 'total_links', len(sorted_link_ids))
    
    def get_node_index(self, node_id: int) -> int:
        """Get bitarray index for node_id."""
        return self._node_id_to_index.get(node_id, -1)
    
    def get_link_index(self, link_id: int) -> int:
        """Get bitarray index for link_id."""
        return self._link_id_to_index.get(link_id, -1)
    
    def clear_mappings(self):
        """Clear mappings to free memory."""
        self._node_ids = None
        self._link_ids = None
        self._node_id_to_index = None
        self._link_id_to_index = None

class PathFoundRef:
    """Lightweight reference to PathFound to avoid storing full objects."""
    __slots__ = ('path_id', 'hash_value', 'coverage_contribution')
    
    def __init__(self, path_id: Union[str, int], hash_value: int, coverage_contribution: float):
        self.path_id = path_id
        self.hash_value = hash_value
        self.coverage_contribution = coverage_contribution
    
    def __hash__(self):
        return self.hash_value
    
    def __eq__(self, other):
        return isinstance(other, PathFoundRef) and self.hash_value == other.hash_value

@dataclass
class CoverageCache:
    """Optimized coverage cache for handling millions of paths."""
    scope: CoverageScope
    
    # Use more memory-efficient bitarrays
    node_coverage: bitarray = field(init=False)
    link_coverage: bitarray = field(init=False)
    
    # Store lightweight references instead of full PathFound objects
    covered_paths: set[PathFoundRef] = field(default_factory=set)
    
    # Use weak reference for last_updated to prevent memory leaks
    _last_updated_ref: Optional[weakref.ref] = field(default=None, init=False)
    
    # Use deque with limited history and store only essential data
    coverage_history: deque = field(init=False)
    
    # Tracking variables
    attempts_without_improvement: int = 0
    best_coverage: float = 0.0
    
    # Cache frequently computed values
    _cached_coverage_ratio: Optional[float] = field(default=None, init=False)
    _coverage_dirty: bool = field(default=True, init=False)
    
    def __post_init__(self):
        # Initialize bitarrays with proper size - they start with undefined values
        # so we need to explicitly set them to 0
        self.node_coverage = bitarray(self.scope.total_nodes)
        self.link_coverage = bitarray(self.scope.total_links)
        
        # IMPORTANT: bitarray() creates uninitialized memory, so we MUST call setall(0)
        # This is true regardless of field(init=False) - that only affects __init__ parameters
        self.node_coverage.setall(0)  # Still needed!
        self.link_coverage.setall(0)  # Still needed!
        
        # Initialize history with limited size and store tuples instead of objects
        from config import config  # Import here to avoid circular imports
        self.coverage_history = deque(maxlen=config.coverage_history_size)
    
    @property
    def last_updated(self):
        """Get the last updated PathFound object."""
        if self._last_updated_ref is not None:
            return self._last_updated_ref()
        return None
    
    @last_updated.setter
    def last_updated(self, path_found):
        """Set the last updated PathFound object using weak reference."""
        if path_found is not None:
            self._last_updated_ref = weakref.ref(path_found)
        else:
            self._last_updated_ref = None
    
    def add_path(self, path_found, node_indices: list[int], link_indices: list[int]) -> bool:
        """
        Add a path to coverage and return True if coverage improved.
        
        Args:
            path_found: The PathFound object
            node_indices: Pre-computed node indices for the path
            link_indices: Pre-computed link indices for the path
        """
        # Calculate coverage contribution before adding
        old_node_count = self.node_coverage.count()
        old_link_count = self.link_coverage.count()
        
        # Update coverage efficiently using bitwise operations
        path_improved_coverage = False
        
        # Check and update node coverage
        for idx in node_indices:
            if 0 <= idx < len(self.node_coverage) and not self.node_coverage[idx]:
                self.node_coverage[idx] = 1
                path_improved_coverage = True
        
        # Check and update link coverage
        for idx in link_indices:
            if 0 <= idx < len(self.link_coverage) and not self.link_coverage[idx]:
                self.link_coverage[idx] = 1
                path_improved_coverage = True
        
        if path_improved_coverage:
            # Calculate coverage contribution
            new_node_count = self.node_coverage.count()
            new_link_count = self.link_coverage.count()
            coverage_contribution = ((new_node_count - old_node_count) + 
                                   (new_link_count - old_link_count)) / (
                                   self.scope.total_nodes + self.scope.total_links)
            
            # Create lightweight reference
            path_ref = PathFoundRef(
                path_id=getattr(path_found, 'id', id(path_found)),
                hash_value=hash(path_found),
                coverage_contribution=coverage_contribution
            )
            
            self.covered_paths.add(path_ref)
            self.last_updated = path_found
            
            # Update history with essential data only
            current_coverage = self.get_coverage_ratio()
            self.coverage_history.append((
                len(self.covered_paths),  # Number of paths
                current_coverage,        # Coverage ratio
                coverage_contribution    # This path's contribution
            ))
            
            # Update best coverage
            if current_coverage > self.best_coverage:
                self.best_coverage = current_coverage
                self.attempts_without_improvement = 0
            else:
                self.attempts_without_improvement += 1
            
            # Mark coverage as dirty for cache invalidation
            self._coverage_dirty = True
            
            return True
        
        self.attempts_without_improvement += 1
        return False
    
    def get_coverage_ratio(self) -> float:
        """Get current coverage ratio with caching."""
        if not self._coverage_dirty and self._cached_coverage_ratio is not None:
            return self._cached_coverage_ratio
        
        total_possible = self.scope.total_nodes + self.scope.total_links
        if total_possible == 0:
            ratio = 0.0
        else:
            covered = self.node_coverage.count() + self.link_coverage.count()
            ratio = covered / total_possible
        
        self._cached_coverage_ratio = ratio
        self._coverage_dirty = False
        return ratio
    
    def get_uncovered_nodes(self) -> bitarray:
        """Get bitarray of uncovered nodes."""
        uncovered = self.node_coverage.copy()
        uncovered.invert()
        return uncovered
    
    def get_uncovered_links(self) -> bitarray:
        """Get bitarray of uncovered links."""
        uncovered = self.link_coverage.copy()
        uncovered.invert()
        return uncovered
    
    def cleanup(self):
        """Clean up resources to free memory."""
        self.covered_paths.clear()
        self.coverage_history.clear()
        self._last_updated_ref = None
        self._cached_coverage_ratio = None
        
        # Reset bitarrays
        self.node_coverage.setall(0)
        self.link_coverage.setall(0)
        
        # Clear scope mappings if needed
        if hasattr(self.scope, 'clear_mappings'):
            self.scope.clear_mappings()
    
    def get_memory_stats(self) -> dict:
        """Get memory usage statistics."""
        import sys
        
        return {
            'node_coverage_bytes': self.node_coverage.buffer_info()[1] * self.node_coverage.itemsize,
            'link_coverage_bytes': self.link_coverage.buffer_info()[1] * self.link_coverage.itemsize,
            'covered_paths_count': len(self.covered_paths),
            'history_size': len(self.coverage_history),
            'estimated_total_bytes': (
                sys.getsizeof(self.node_coverage) +
                sys.getsizeof(self.link_coverage) +
                sys.getsizeof(self.covered_paths) +
                sys.getsizeof(self.coverage_history)
            )
        }

# Factory function for creating optimized coverage cache
def create_coverage_cache(scope_params: dict, node_ids: list[int], link_ids: list[int]) -> CoverageCache:
    """Create an optimized coverage cache."""
    scope = CoverageScope(**scope_params)
    scope.initialize_mappings(node_ids, link_ids)
    return CoverageCache(scope=scope)