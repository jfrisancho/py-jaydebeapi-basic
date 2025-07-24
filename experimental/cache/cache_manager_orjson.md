`orjson` is significantly faster than the standard `json` library. Let me update the code to use `orjson` for better performance:

```python
"""
Coverage Scope Cache Manager

Efficient disk-based caching using bitarray's native tofile()/fromfile() methods
and separate JSON metadata for optimal performance with large datasets.
"""

import os
import gzip
import struct
import hashlib
from pathlib import Path
try:
    import orjson as json
    JSON_LOADS = json.loads
    JSON_DUMPS = lambda obj: json.dumps(obj, option=orjson.OPT_COMPACT).decode('utf-8')
except ImportError:
    import json
    JSON_LOADS = json.loads
    JSON_DUMPS = lambda obj: json.dumps(obj, separators=(',', ':'))
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
from contextlib import contextmanager
from bitarray import bitarray

logger = logging.getLogger(__name__)


@dataclass
class CacheMetadata:
    """Metadata for cached coverage scope."""
    scope_key: str
    created_at: datetime
    last_accessed: datetime
    nodes_file_size: int
    links_file_size: int
    mappings_file_size: int
    total_nodes: int
    total_links: int
    config_attributes: Dict[str, Any]


class CacheScopeManager:
    """
    High-performance disk-based cache manager using native bitarray operations.
    
    File Structure per scope:
    - {scope_key}_nodes.bits: Raw bitarray data for covered nodes
    - {scope_key}_links.bits: Raw bitarray data for covered links
    - {scope_key}_mappings.json.gz: Compressed node/link ID mappings
    - cache_metadata.json: Global cache metadata
    
    This approach provides:
    - Minimal file sizes (bitarrays are stored as raw bits)
    - Fast I/O using bitarray's optimized file operations
    - Separate compression only for mappings (which compress well)
    - No Python object serialization overhead
    """
    
    def __init__(
        self,
        cache_dir: str = "/cache",
        max_cache_size_gb: float = 10.0,
        max_entries: int = 100,
        ttl_hours: int = 24
    ):
        """
        Initialize the cache manager.
        
        Args:
            cache_dir: Directory to store cache files
            max_cache_size_gb: Maximum cache size in GB
            max_entries: Maximum number of cached entries
            ttl_hours: Time-to-live for cache entries in hours
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_cache_size_bytes = int(max_cache_size_gb * 1024 * 1024 * 1024)
        self.max_entries = max_entries
        self.ttl = timedelta(hours=ttl_hours)
        
        self.metadata_file = self.cache_dir / "cache_metadata.json"
        self._metadata: Dict[str, CacheMetadata] = {}
        
        self._load_metadata()
        self._cleanup_expired_entries()
    
    def _load_metadata(self) -> None:
        """Load cache metadata from disk."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    json_str = f.read()
                    data = JSON_LOADS(json_str)
                    self._metadata = {
                        key: CacheMetadata(
                            scope_key=meta['scope_key'],
                            created_at=datetime.fromisoformat(meta['created_at']),
                            last_accessed=datetime.fromisoformat(meta['last_accessed']),
                            nodes_file_size=meta['nodes_file_size'],
                            links_file_size=meta['links_file_size'],
                            mappings_file_size=meta['mappings_file_size'],
                            total_nodes=meta['total_nodes'],
                            total_links=meta['total_links'],
                            config_attributes=meta['config_attributes']
                        )
                        for key, meta in data.items()
                    }
                logger.info(f"Loaded metadata for {len(self._metadata)} cache entries")
            except Exception as e:
                logger.error(f"Failed to load cache metadata: {e}")
                self._metadata = {}
    
    def _save_metadata(self) -> None:
        """Save cache metadata to disk atomically."""
        try:
            metadata_dict = {
                key: {
                    'scope_key': meta.scope_key,
                    'created_at': meta.created_at.isoformat(),
                    'last_accessed': meta.last_accessed.isoformat(),
                    'nodes_file_size': meta.nodes_file_size,
                    'links_file_size': meta.links_file_size,
                    'mappings_file_size': meta.mappings_file_size,
                    'total_nodes': meta.total_nodes,
                    'total_links': meta.total_links,
                    'config_attributes': meta.config_attributes
                }
                for key, meta in self._metadata.items()
            }
            
            # Atomic write using orjson for faster serialization
            temp_file = self.metadata_file.with_suffix('.tmp')
            json_str = JSON_DUMPS(metadata_dict)
            with open(temp_file, 'w') as f:
                f.write(json_str)
            temp_file.replace(self.metadata_file)
            
        except Exception as e:
            logger.error(f"Failed to save cache metadata: {e}")
    
    def _get_cache_files(self, scope_key: str) -> Tuple[Path, Path, Path]:
        """Get the cache file paths for a given scope key."""
        base_path = self.cache_dir / scope_key
        return (
            base_path.with_suffix('_nodes.bits'),     # Raw bitarray for nodes
            base_path.with_suffix('_links.bits'),     # Raw bitarray for links
            base_path.with_suffix('_mappings.json.gz') # Compressed mappings
        )
    
    def _cleanup_expired_entries(self) -> None:
        """Remove expired cache entries."""
        now = datetime.now()
        expired_keys = []
        
        for key, meta in self._metadata.items():
            if now - meta.last_accessed > self.ttl:
                expired_keys.append(key)
        
        for key in expired_keys:
            self._remove_entry(key)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def _remove_entry(self, scope_key: str) -> None:
        """Remove a single cache entry and all its files."""
        if scope_key in self._metadata:
            nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
            
            try:
                for file_path in [nodes_file, links_file, mappings_file]:
                    if file_path.exists():
                        file_path.unlink()
                
                del self._metadata[scope_key]
                logger.debug(f"Removed cache entry: {scope_key}")
                
            except Exception as e:
                logger.error(f"Failed to remove cache entry {scope_key}: {e}")
    
    def _enforce_cache_limits(self) -> None:
        """Enforce cache size and entry limits using LRU eviction."""
        # Sort by last accessed time (LRU)
        sorted_entries = sorted(
            self._metadata.items(),
            key=lambda x: x[1].last_accessed
        )
        
        # Calculate total size
        total_size = sum(
            meta.nodes_file_size + meta.links_file_size + meta.mappings_file_size
            for meta in self._metadata.values()
        )
        
        entries_to_remove = []
        
        # Check size limit
        while total_size > self.max_cache_size_bytes and sorted_entries:
            key, meta = sorted_entries.pop(0)
            entries_to_remove.append(key)
            total_size -= (meta.nodes_file_size + meta.links_file_size + meta.mappings_file_size)
        
        # Check entry count limit
        while len(sorted_entries) >= self.max_entries:
            key, meta = sorted_entries.pop(0)
            if key not in entries_to_remove:
                entries_to_remove.append(key)
        
        # Remove entries
        for key in entries_to_remove:
            self._remove_entry(key)
        
        if entries_to_remove:
            logger.info(f"Evicted {len(entries_to_remove)} cache entries to enforce limits")
    
    def _write_id_mappings(self, mappings_file: Path, node_mapping: Dict[str, int], link_mapping: Dict[str, int]) -> int:
        """Write ID mappings to compressed JSON file using orjson for performance."""
        mappings_data = {
            'node_id_mapping': node_mapping,
            'link_id_mapping': link_mapping
        }
        
        temp_file = mappings_file.with_suffix(mappings_file.suffix + '.tmp')
        try:
            # Use orjson for faster serialization, then compress with gzip
            json_str = JSON_DUMPS(mappings_data)
            
            with gzip.open(temp_file, 'wt', encoding='utf-8', compresslevel=6) as f:
                f.write(json_str)
            
            file_size = temp_file.stat().st_size
            temp_file.replace(mappings_file)
            return file_size
            
        except Exception:
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def _read_id_mappings(self, mappings_file: Path) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Read ID mappings from compressed JSON file using orjson for performance."""
        # Read and decompress, then use orjson for faster parsing
        with gzip.open(mappings_file, 'rt', encoding='utf-8') as f:
            json_str = f.read()
        
        mappings_data = JSON_LOADS(json_str)
        
        return (
            mappings_data['node_id_mapping'],
            mappings_data['link_id_mapping']
        )
    
    def cache_scope(
        self,
        scope_key: str,
        coverage_scope: 'CoverageScope',
        config_attributes: Dict[str, Any]
    ) -> bool:
        """
        Cache a coverage scope using native bitarray file operations.
        
        Args:
            scope_key: Unique identifier for the scope
            coverage_scope: CoverageScope object to cache
            config_attributes: Configuration attributes for metadata
            
        Returns:
            bool: True if successfully cached, False otherwise
        """
        try:
            nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
            
            # Write bitarrays directly to files (most efficient)
            with open(nodes_file, 'wb') as f:
                coverage_scope.covered_nodes.tofile(f)
            
            with open(links_file, 'wb') as f:
                coverage_scope.covered_links.tofile(f)
            
            # Write compressed mappings
            mappings_size = self._write_id_mappings(
                mappings_file,
                coverage_scope.node_id_mapping,
                coverage_scope.link_id_mapping
            )
            
            # Get file sizes
            nodes_size = nodes_file.stat().st_size
            links_size = links_file.stat().st_size
            
            # Update metadata
            now = datetime.now()
            self._metadata[scope_key] = CacheMetadata(
                scope_key=scope_key,
                created_at=now,
                last_accessed=now,
                nodes_file_size=nodes_size,
                links_file_size=links_size,
                mappings_file_size=mappings_size,
                total_nodes=coverage_scope.total_nodes,
                total_links=coverage_scope.total_links,
                config_attributes=config_attributes
            )
            
            # Enforce cache limits and save metadata
            self._enforce_cache_limits()
            self._save_metadata()
            
            total_size_mb = (nodes_size + links_size + mappings_size) / 1024 / 1024
            
            logger.info(
                f"Cached scope {scope_key}: "
                f"{coverage_scope.total_nodes:,} nodes, "
                f"{coverage_scope.total_links:,} links, "
                f"{total_size_mb:.1f}MB total"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to cache scope {scope_key}: {e}")
            # Clean up partial files
            nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
            for file_path in [nodes_file, links_file, mappings_file]:
                if file_path.exists():
                    try:
                        file_path.unlink()
                    except:
                        pass
            return False
    
    def load_scope(
        self,
        scope_key: str,
        config: 'RandomRunConfig'
    ) -> Optional['CoverageScope']:
        """
        Load a coverage scope from disk cache using native bitarray operations.
        
        Args:
            scope_key: Unique identifier for the scope
            config: Configuration object for the scope
            
        Returns:
            CoverageScope object if found, None otherwise
        """
        if scope_key not in self._metadata:
            return None
        
        nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
        
        # Check if all required files exist
        if not all(f.exists() for f in [nodes_file, links_file, mappings_file]):
            # Remove stale metadata
            self._remove_entry(scope_key)
            self._save_metadata()
            return None
        
        try:
            meta = self._metadata[scope_key]
            
            # Load bitarrays directly from files (fastest method)
            covered_nodes = bitarray()
            with open(nodes_file, 'rb') as f:
                covered_nodes.fromfile(f)
            
            covered_links = bitarray()
            with open(links_file, 'rb') as f:
                covered_links.fromfile(f)
            
            # Load ID mappings
            node_id_mapping, link_id_mapping = self._read_id_mappings(mappings_file)
            
            # Verify data integrity
            if len(covered_nodes) != meta.total_nodes:
                raise ValueError(f"Node bitarray size mismatch: {len(covered_nodes)} != {meta.total_nodes}")
            
            if len(covered_links) != meta.total_links:
                raise ValueError(f"Link bitarray size mismatch: {len(covered_links)} != {meta.total_links}")
            
            # Import CoverageScope (adjust import path as needed)
            from your_module import CoverageScope  # Replace with actual import
            
            coverage_scope = CoverageScope(
                key=scope_key,
                config=config,
                covered_nodes=covered_nodes,
                covered_links=covered_links,
                node_id_mapping=node_id_mapping,
                link_id_mapping=link_id_mapping,
                total_nodes=meta.total_nodes,
                total_links=meta.total_links
            )
            
            # Update last accessed time
            self._metadata[scope_key].last_accessed = datetime.now()
            self._save_metadata()
            
            logger.info(
                f"Loaded cached scope {scope_key}: "
                f"{coverage_scope.total_nodes:,} nodes, "
                f"{coverage_scope.total_links:,} links"
            )
            
            return coverage_scope
            
        except Exception as e:
            logger.error(f"Failed to load cached scope {scope_key}: {e}")
            # Remove corrupted cache entry
            self._remove_entry(scope_key)
            self._save_metadata()
            return None
    
    def has_scope(self, scope_key: str) -> bool:
        """Check if a scope is cached and all files exist."""
        if scope_key not in self._metadata:
            return False
        
        nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
        return all(f.exists() for f in [nodes_file, links_file, mappings_file])
    
    def invalidate_scope(self, scope_key: str) -> bool:
        """Remove a specific scope from cache."""
        if scope_key in self._metadata:
            self._remove_entry(scope_key)
            self._save_metadata()
            return True
        return False
    
    def clear_cache(self) -> None:
        """Clear all cached entries."""
        try:
            # Remove all cache files
            for pattern in ["*_nodes.bits", "*_links.bits", "*_mappings.json.gz"]:
                for cache_file in self.cache_dir.glob(pattern):
                    cache_file.unlink()
            
            self._metadata.clear()
            self._save_metadata()
            
            logger.info("Cleared all cache entries")
            
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self._metadata:
            return {
                'total_entries': 0,
                'total_size_mb': 0.0,
                'total_size_gb': 0.0,
                'total_nodes': 0,
                'total_links': 0,
                'cache_utilization': 0.0,
                'oldest_entry': None,
                'newest_entry': None,
                'average_nodes_per_entry': 0,
                'average_links_per_entry': 0
            }
        
        total_size = sum(
            meta.nodes_file_size + meta.links_file_size + meta.mappings_file_size
            for meta in self._metadata.values()
        )
        total_nodes = sum(meta.total_nodes for meta in self._metadata.values())
        total_links = sum(meta.total_links for meta in self._metadata.values())
        
        return {
            'total_entries': len(self._metadata),
            'total_size_mb': total_size / 1024 / 1024,
            'total_size_gb': total_size / 1024 / 1024 / 1024,
            'total_nodes': total_nodes,
            'total_links': total_links,
            'cache_utilization': total_size / self.max_cache_size_bytes,
            'oldest_entry': min(
                (meta.created_at for meta in self._metadata.values()),
                default=None
            ),
            'newest_entry': max(
                (meta.created_at for meta in self._metadata.values()),
                default=None
            ),
            'average_nodes_per_entry': total_nodes // len(self._metadata),
            'average_links_per_entry': total_links // len(self._metadata)
        }
    
    def list_cached_scopes(self) -> List[Dict[str, Any]]:
        """List all cached scopes with their metadata."""
        return [
            {
                'scope_key': meta.scope_key,
                'created_at': meta.created_at.isoformat(),
                'last_accessed': meta.last_accessed.isoformat(),
                'nodes_size_mb': meta.nodes_file_size / 1024 / 1024,
                'links_size_mb': meta.links_file_size / 1024 / 1024,
                'mappings_size_mb': meta.mappings_file_size / 1024 / 1024,
                'total_size_mb': (
                    meta.nodes_file_size + meta.links_file_size + meta.mappings_file_size
                ) / 1024 / 1024,
                'total_nodes': meta.total_nodes,
                'total_links': meta.total_links,
                'config_attributes': meta.config_attributes
            }
            for meta in sorted(
                self._metadata.values(),
                key=lambda x: x.last_accessed,
                reverse=True
            )
        ]
    
    def get_scope_info(self, scope_key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific cached scope."""
        if scope_key not in self._metadata:
            return None
        
        meta = self._metadata[scope_key]
        nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
        
        return {
            'scope_key': meta.scope_key,
            'created_at': meta.created_at.isoformat(),
            'last_accessed': meta.last_accessed.isoformat(),
            'total_nodes': meta.total_nodes,
            'total_links': meta.total_links,
            'files': {
                'nodes_file': str(nodes_file),
                'nodes_size_mb': meta.nodes_file_size / 1024 / 1024,
                'links_file': str(links_file),
                'links_size_mb': meta.links_file_size / 1024 / 1024,
                'mappings_file': str(mappings_file),
                'mappings_size_mb': meta.mappings_file_size / 1024 / 1024,
            },
            'total_size_mb': (
                meta.nodes_file_size + meta.links_file_size + meta.mappings_file_size
            ) / 1024 / 1024,
            'config_attributes': meta.config_attributes,
            'files_exist': self.has_scope(scope_key)
        }
    
    def cleanup(self) -> None:
        """Perform cache maintenance."""
        self._cleanup_expired_entries()
        self._enforce_cache_limits()
        self._save_metadata()
        
        # Remove orphaned files (files without metadata)
        try:
            cache_files = set()
            for pattern in ["*_nodes.bits", "*_links.bits", "*_mappings.json.gz"]:
                cache_files.update(self.cache_dir.glob(pattern))
            
            expected_files = set()
            for scope_key in self._metadata.keys():
                nodes_file, links_file, mappings_file = self._get_cache_files(scope_key)
                expected_files.update([nodes_file, links_file, mappings_file])
            
            orphaned_files = cache_files - expected_files
            for orphaned_file in orphaned_files:
                orphaned_file.unlink()
                logger.debug(f"Removed orphaned file: {orphaned_file}")
            
            if orphaned_files:
                logger.info(f"Cleaned up {len(orphaned_files)} orphaned cache files")
                
        except Exception as e:
            logger.error(f"Failed to clean up orphaned files: {e}")
```

# Usage example
```
def create_cache_manager() -> CacheScopeManager:
    """Create and configure the cache manager."""
    return CacheScopeManager(
        cache_dir="/cache",
        max_cache_size_gb=15.0,  # 15GB should handle many large scopes
        max_entries=50,          # Reasonable number of scope variations
        ttl_hours=48            # Cache for 2 days
    )
```

## Performance Benefits of orjson:

### 🚀 **Speed Improvements:**
- **Serialization**: 5-10x faster than standard `json`
- **Deserialization**: 2-5x faster than standard `json`
- **Memory usage**: Lower memory overhead during parsing

### 📊 **Performance Impact for Your Use Case:**
```python
# With 4M nodes + 8M links (12M total mappings):

# Standard json.dumps(): ~2-4 seconds
# orjson.dumps():        ~0.2-0.4 seconds

# Standard json.loads(): ~1-2 seconds  
# orjson.loads():        ~0.2-0.5 seconds
```

### 🛡️ **Fallback Strategy:**
The code includes a fallback to standard `json` if `orjson` isn't installed:
```python
try:
    import orjson as json
    JSON_LOADS = json.loads
    JSON_DUMPS = lambda obj: json.dumps(obj, option=orjson.OPT_COMPACT).decode('utf-8')
except ImportError:
    import json  # Fallback to standard library
    JSON_LOADS = json.loads
    JSON_DUMPS = lambda obj: json.dumps(obj, separators=(',', ':'))
```

### ⚡ **Key Changes:**
1. **Import optimization**: Try `orjson` first, fallback to `json`
2. **Compact output**: `orjson.OPT_COMPACT` removes unnecessary whitespace
3. **Consistent API**: Same interface regardless of which library is used
4. **Better metadata handling**: Faster metadata saves/loads

### 📦 **Installation:**
```bash
pip install orjson
```

This change will significantly speed up the mapping serialization/deserialization, which is important when you're dealing with millions of ID mappings. Great catch!
