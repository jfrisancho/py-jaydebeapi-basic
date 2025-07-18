import logging
import sys
from typing import Optional, Dict, Any
from enum import Enum

class LogLevel(Enum):
    SILENT = logging.CRITICAL
    NORMAL = logging.INFO
    VERBOSE = logging.DEBUG

class LoggingManager:
    """Centralized logging configuration manager"""
    _configured_loggers: Dict[str, bool] = {}
    
    @classmethod
    def setup_logger(cls, name: str, verbose: bool = False, silent: bool = False) -> logging.Logger:
        """Setup a logger with the given configuration"""
        logger = logging.getLogger(name)
        
        # Only configure if not already configured
        if name not in cls._configured_loggers:
            cls._configure_logger(logger, verbose, silent)
            cls._configured_loggers[name] = True
        
        return logger
    
    @classmethod
    def _configure_logger(cls, logger: logging.Logger, verbose: bool, silent: bool) -> None:
        """Configure a logger instance"""
        # Clear existing handlers
        logger.handlers.clear()
        
        # Set log level
        match (verbose, silent):
            case (_, True):
                logger.setLevel(LogLevel.SILENT.value)
                return  # No handler needed for silent mode
            case (True, False):
                logger.setLevel(LogLevel.VERBOSE.value)
                formatter_pattern = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            case (False, False):
                logger.setLevel(LogLevel.NORMAL.value)
                formatter_pattern = '%(levelname)s: %(message)s'
        
        # Add console handler
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(formatter_pattern)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    @classmethod
    def reset_logger(cls, name: str) -> None:
        """Reset a logger configuration"""
        if name in cls._configured_loggers:
            logger = logging.getLogger(name)
            logger.handlers.clear()
            del cls._configured_loggers[name]

# Solution 1: Different modules = different loggers
class DatabaseManager:
    def __init__(self, verbose: bool = False, silent: bool = False):
        # Uses module name as logger name
        self.logger = LoggingManager.setup_logger(__name__ + '.DatabaseManager', verbose, silent)
    
    def connect(self):
        self.logger.info("Connecting to database...")
        self.logger.debug("Database connection details: host=localhost, port=5432")

class FileManager:
    def __init__(self, verbose: bool = False, silent: bool = False):
        # Uses module name as logger name
        self.logger = LoggingManager.setup_logger(__name__ + '.FileManager', verbose, silent)
    
    def read_file(self, filename: str):
        self.logger.info(f"Reading file: {filename}")
        self.logger.debug(f"File size: 1024 bytes")

class NetworkManager:
    def __init__(self, verbose: bool = False, silent: bool = False):
        # Uses module name as logger name
        self.logger = LoggingManager.setup_logger(__name__ + '.NetworkManager', verbose, silent)
    
    def send_request(self, url: str):
        self.logger.info(f"Sending request to: {url}")
        self.logger.debug("Request headers: {'User-Agent': 'MyApp'}")

# Solution 2: Instance-specific loggers
class InstanceManager:
    _instance_counter = 0
    
    def __init__(self, name: Optional[str] = None, verbose: bool = False, silent: bool = False):
        # Generate unique logger name
        if name is None:
            InstanceManager._instance_counter += 1
            name = f"{self.__class__.__name__}_{InstanceManager._instance_counter}"
        
        self.name = name
        self.logger = LoggingManager.setup_logger(name, verbose, silent)
    
    def do_work(self):
        self.logger.info(f"Manager {self.name} is working...")
        self.logger.debug("Work details here")

# Solution 3: Hierarchical loggers
class HierarchicalManager:
    def __init__(self, manager_type: str, verbose: bool = False, silent: bool = False):
        # Create hierarchical logger name
        logger_name = f"app.managers.{manager_type}"
        self.logger = LoggingManager.setup_logger(logger_name, verbose, silent)
        self.manager_type = manager_type
    
    def process(self):
        self.logger.info(f"{self.manager_type} manager processing...")
        self.logger.debug("Processing details")

# Demonstration
if __name__ == "__main__":
    print("=== Testing Multiple Managers ===")
    
    # Test 1: Different module-based managers
    print("\n1. Module-based managers:")
    db_mgr = DatabaseManager(verbose=True)
    file_mgr = FileManager(silent=True)
    net_mgr = NetworkManager()  # normal mode
    
    db_mgr.connect()
    file_mgr.read_file("test.txt")
    net_mgr.send_request("https://api.example.com")
    
    # Test 2: Instance-specific managers
    print("\n2. Instance-specific managers:")
    mgr1 = InstanceManager("Worker1", verbose=True)
    mgr2 = InstanceManager("Worker2", silent=True)
    mgr3 = InstanceManager()  # auto-generated name
    
    mgr1.do_work()
    mgr2.do_work()
    mgr3.do_work()
    
    # Test 3: Hierarchical managers
    print("\n3. Hierarchical managers:")
    auth_mgr = HierarchicalManager("auth", verbose=True)
    cache_mgr = HierarchicalManager("cache", silent=True)
    
    auth_mgr.process()
    cache_mgr.process()
    
    # Test 4: Same class, different configs
    print("\n4. Same class, different configs:")
    verbose_db = DatabaseManager(verbose=True)
    silent_db = DatabaseManager(silent=True)
    
    print("Verbose DB manager:")
    verbose_db.connect()
    print("Silent DB manager:")
    silent_db.connect()
