import gc
from typing import Optional, Protocol
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor
import psutil
import os

class CoverageManager:
    """Optimized coverage manager with memory management."""
    
    def __init__(self, coverage_cache: 'CoverageCache'):
        self._cache = coverage_cache
        self._lock = threading.RLock()  # For thread safety
        self._memory_threshold = 0.85  # 85% memory usage threshold
        
    def update_coverage(self, path_found, node_ids: list[int], link_ids: list[int]) -> bool:
        """Update coverage with pre-computed indices."""
        with self._lock:
            # Pre-compute indices to avoid repeated lookups
            node_indices = [self._cache.scope.get_node_index(nid) for nid in node_ids]
            link_indices = [self._cache.scope.get_link_index(lid) for lid in link_ids]
            
            # Filter out invalid indices
            valid_node_indices = [idx for idx in node_indices if idx >= 0]
            valid_link_indices = [idx for idx in link_indices if idx >= 0]
            
            return self._cache.add_path(path_found, valid_node_indices, valid_link_indices)
    
    def check_memory_pressure(self) -> bool:
        """Check if system is under memory pressure."""
        memory_percent = psutil.virtual_memory().percent / 100.0
        return memory_percent > self._memory_threshold
    
    def force_cleanup_if_needed(self):
        """Force cleanup if memory pressure is high."""
        if self.check_memory_pressure():
            # Remove oldest paths if too many stored
            if len(self._cache.covered_paths) > 10000:  # Configurable threshold
                # Keep only the most recent paths based on coverage contribution
                sorted_paths = sorted(
                    self._cache.covered_paths, 
                    key=lambda p: p.coverage_contribution, 
                    reverse=True
                )
                self._cache.covered_paths = set(sorted_paths[:5000])  # Keep top 50%
            
            # Force garbage collection
            gc.collect()

class MemoryPool:
    """Memory pool for reusing objects."""
    
    def __init__(self, max_size: int = 1000):
        self._pools = {}
        self._max_size = max_size
        self._lock = threading.Lock()
    
    def get_object(self, obj_type: type):
        """Get an object from the pool or create new one."""
        with self._lock:
            pool = self._pools.setdefault(obj_type, [])
            if pool:
                return pool.pop()
            return obj_type()
    
    def return_object(self, obj, obj_type: type):
        """Return an object to the pool."""
        with self._lock:
            pool = self._pools.setdefault(obj_type, [])
            if len(pool) < self._max_size:
                # Reset object state if it has a reset method
                if hasattr(obj, 'reset'):
                    obj.reset()
                pool.append(obj)

class BatchProcessor:
    """Process paths in batches for better memory management."""
    
    def __init__(self, coverage_manager: CoverageManager, batch_size: int = 1000):
        self.coverage_manager = coverage_manager
        self.batch_size = batch_size
        self.memory_pool = MemoryPool()
    
    def process_paths_batch(self, paths_data: list[tuple]) -> int:
        """
        Process a batch of paths efficiently.
        
        Args:
            paths_data: List of (path_found, node_ids, link_ids) tuples
            
        Returns:
            Number of paths that improved coverage
        """
        improvements = 0
        processed = 0
        
        for path_found, node_ids, link_ids in paths_data:
            if self.coverage_manager.update_coverage(path_found, node_ids, link_ids):
                improvements += 1
            
            processed += 1
            
            # Check memory pressure every 100 paths
            if processed % 100 == 0:
                self.coverage_manager.force_cleanup_if_needed()
        
        return improvements
    
    @contextmanager
    def batch_context(self):
        """Context manager for batch processing."""
        try:
            # Disable garbage collection during batch processing for performance
            gc.disable()
            yield
        finally:
            # Re-enable garbage collection and force cleanup
            gc.enable()
            gc.collect()

class OptimizedRandomManager:
    """Random manager with optimized resource usage."""
    
    def __init__(self, coverage_manager: CoverageManager, path_manager: 'PathManager'):
        self.coverage_manager = coverage_manager
        self.path_manager = path_manager
        self.batch_processor = BatchProcessor(coverage_manager)
        self._stats = {
            'total_processed': 0,
            'coverage_improvements': 0,
            'memory_cleanups': 0
        }
    
    def sample_paths_optimized(self, coverage_target: float, max_attempts: int = 1000000):
        """
        Optimized path sampling with memory management.
        
        Args:
            coverage_target: Target coverage ratio (0.0 to 1.0)
            max_attempts: Maximum number of attempts
        """
        batch_data = []
        attempts = 0
        
        with self.batch_processor.batch_context():
            while (self.coverage_manager._cache.get_coverage_ratio() < coverage_target and 
                   attempts < max_attempts):
                
                # Generate path (this should be implemented in your path_manager)
                path_found, node_ids, link_ids = self.path_manager.generate_random_path()
                
                if path_found is not None:
                    batch_data.append((path_found, node_ids, link_ids))
                
                # Process in batches
                if len(batch_data) >= self.batch_processor.batch_size:
                    improvements = self.batch_processor.process_paths_batch(batch_data)
                    self._stats['coverage_improvements'] += improvements
                    self._stats['total_processed'] += len(batch_data)
                    batch_data.clear()
                
                attempts += 1
                
                # Periodic memory check
                if attempts % 10000 == 0:
                    if self.coverage_manager.check_memory_pressure():
                        self.coverage_manager.force_cleanup_if_needed()
                        self._stats['memory_cleanups'] += 1
            
            # Process remaining batch
            if batch_data:
                improvements = self.batch_processor.process_paths_batch(batch_data)
                self._stats['coverage_improvements'] += improvements
                self._stats['total_processed'] += len(batch_data)
    
    def get_stats(self) -> dict:
        """Get processing statistics."""
        return {
            **self._stats,
            'coverage_ratio': self.coverage_manager._cache.get_coverage_ratio(),
            'memory_stats': self.coverage_manager._cache.get_memory_stats()
        }

# Usage example and configuration
class Config:
    """Configuration for optimized processing."""
    coverage_history_size = 100  # Reduced from potentially larger value
    batch_size = 1000
    memory_threshold = 0.85
    max_cached_paths = 10000
    cleanup_interval = 10000  # Process count between cleanups

# Factory function for creating optimized system
def create_optimized_coverage_system(scope_params: dict, node_ids: list[int], 
                                   link_ids: list[int], path_manager) -> tuple:
    """Create optimized coverage system."""
    # Create cache
    coverage_cache = create_coverage_cache(scope_params, node_ids, link_ids)
    
    # Create managers
    coverage_manager = CoverageManager(coverage_cache)
    random_manager = OptimizedRandomManager(coverage_manager, path_manager)
    
    return coverage_cache, coverage_manager, random_manager

# Memory monitoring utility
def monitor_memory_usage():
    """Monitor and log memory usage."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    return {
        'rss_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size in MB
        'vms_mb': memory_info.vms / 1024 / 1024,  # Virtual Memory Size in MB
        'percent': process.memory_percent(),
        'available_mb': psutil.virtual_memory().available / 1024 / 1024
    }