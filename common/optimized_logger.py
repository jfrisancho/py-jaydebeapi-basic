import logging
import sys
from functools import cached_property
from typing import Final

class OptimizedLogger:
    """Python 3.11 optimized logger with cached properties and constants"""
    
    # Class constants (Final annotation for better optimization)
    SILENT_LEVEL: Final[int] = logging.CRITICAL
    NORMAL_LEVEL: Final[int] = logging.INFO
    VERBOSE_LEVEL: Final[int] = logging.DEBUG
    
    def __init__(self, name: str, verbose: bool = False, silent: bool = False):
        self._name = name
        self._verbose = verbose
        self._silent = silent
        self._setup_logging()
    
    @cached_property
    def logger(self) -> logging.Logger:
        """Cached property for logger instance (Python 3.8+, optimized in 3.11)"""
        return logging.getLogger(self._name)
    
    @cached_property
    def _handler(self) -> logging.StreamHandler:
        """Cached handler creation"""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self._formatter)
        return handler
    
    @cached_property
    def _formatter(self) -> logging.Formatter:
        """Cached formatter based on verbosity"""
        return logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            if self._verbose else '%(levelname)s: %(message)s'
        )
    
    def _setup_logging(self) -> None:
        """Setup with improved Python 3.11 performance"""
        self.logger.handlers.clear()
        
        # Use walrus operator for cleaner code
        if level := self._get_log_level():
            self.logger.setLevel(level)
            
            # Only add handler if not silent
            if not self._silent:
                self.logger.addHandler(self._handler)
    
    def _get_log_level(self) -> int:
        """Get appropriate log level using match-case"""
        match (self._verbose, self._silent):
            case (_, True):
                return self.SILENT_LEVEL
            case (True, False):
                return self.VERBOSE_LEVEL
            case (False, False):
                return self.NORMAL_LEVEL
            case _:
                return self.NORMAL_LEVELK
