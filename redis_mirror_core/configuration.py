"""
Redis Mirror CE Configuration Module

This module provides standardized configuration options for the Redis Mirror CE SDK.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List, Union
from .errors import ConfigurationError

# Configure logging
logger = logging.getLogger(__name__)


class Configuration:
    """
    Configuration class for Redis Mirror CE SDK.
    
    This class provides a unified interface for managing configuration options
    for the Redis Mirror CE SDK, including Redis connection settings, ACL settings,
    and other SDK-specific options.
    """
    
    # Default configuration values
    DEFAULT_CONFIG = {
        "wsp_redis": {
            "host": "localhost",
            "port": 6379,
            "password": "",
            "db": 0,
            "decode_responses": True
        },
        "replica_redis": {
            "host": "localhost",
            "port": 6379,
            "password": "",
            "db": 0,
            "decode_responses": True
        },
        "acl": {
            "exchange_username": "exchange",
            "exchange_password": "exchange_password",
            "wsp_username": "wsp",
            "wsp_password": "wsp_password",
            "stream_patterns": ["exchange:stream:*", "mirror:credit:response", "mirror:settlement:*:response"]
        },
        "streams": {
            "consumer_group_prefix": "redis_mirror_ce",
            "batch_size": 10,
            "block_ms": 2000,
            "max_retry_count": 3,
            "retry_delay_ms": 5000
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the configuration with default values and optional overrides.
        
        Args:
            config: Optional configuration dictionary to override default values
        """
        self.config = self.DEFAULT_CONFIG.copy()
        
        if config:
            self._merge_config(config)
            
        # Configure logging based on configuration
        self._configure_logging()
        
        logger.debug("Configuration initialized")
    
    def _merge_config(self, config: Dict[str, Any]):
        """
        Merge the provided configuration with the default configuration.
        
        Args:
            config: Configuration dictionary to merge
        """
        for section, section_config in config.items():
            if section in self.config:
                if isinstance(section_config, dict) and isinstance(self.config[section], dict):
                    # Merge dictionaries
                    self.config[section].update(section_config)
                else:
                    # Replace value
                    self.config[section] = section_config
            else:
                # Add new section
                self.config[section] = section_config
    
    def _configure_logging(self):
        """Configure logging based on the configuration."""
        log_config = self.config.get("logging", {})
        log_level = log_config.get("level", "INFO")
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # Convert string log level to logging constant
        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ConfigurationError(f"Invalid log level: {log_level}", "logging.level")
        
        # Configure root logger
        logging.basicConfig(level=numeric_level, format=log_format)
    
    def get_wsp_redis_config(self) -> Dict[str, Any]:
        """Get the WSP Redis configuration."""
        return self.config.get("wsp_redis", {})
    
    def get_replica_redis_config(self) -> Dict[str, Any]:
        """Get the Stream-Writeable Replica configuration."""
        return self.config.get("replica_redis", {})
    
    def get_acl_config(self) -> Dict[str, Any]:
        """Get the ACL configuration."""
        return self.config.get("acl", {})
    
    def get_streams_config(self) -> Dict[str, Any]:
        """Get the streams configuration."""
        return self.config.get("streams", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get the logging configuration."""
        return self.config.get("logging", {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: Configuration key (can be nested using dot notation, e.g., "wsp_redis.host")
            default: Default value to return if the key is not found
            
        Returns:
            Configuration value or default if not found
        """
        keys = key.split(".")
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        Set a configuration value by key.
        
        Args:
            key: Configuration key (can be nested using dot notation, e.g., "wsp_redis.host")
            value: Value to set
        """
        keys = key.split(".")
        config = self.config
        
        # Navigate to the nested dictionary
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            elif not isinstance(config[k], dict):
                config[k] = {}
            
            config = config[k]
        
        # Set the value
        config[keys[-1]] = value
    
    @classmethod
    def from_file(cls, file_path: str) -> 'Configuration':
        """
        Load configuration from a JSON file.
        
        Args:
            file_path: Path to the JSON configuration file
            
        Returns:
            Configuration instance
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
            
            logger.info(f"Loaded configuration from {file_path}")
            return cls(config)
        except FileNotFoundError:
            logger.warning(f"Configuration file not found: {file_path}")
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {file_path}")
            raise ConfigurationError(f"Invalid JSON in configuration file: {file_path} - {str(e)}")
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {str(e)}")
            raise ConfigurationError(f"Error loading configuration from {file_path}: {str(e)}")
    
    @classmethod
    def from_env(cls, prefix: str = "REDIS_MIRROR_") -> 'Configuration':
        """
        Load configuration from environment variables.
        
        Environment variables should be prefixed with the specified prefix and use
        double underscores to indicate nesting. For example, REDIS_MIRROR_WSP_REDIS__HOST
        would set the wsp_redis.host configuration value.
        
        Args:
            prefix: Prefix for environment variables
            
        Returns:
            Configuration instance
        """
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and convert to lowercase
                key = key[len(prefix):].lower()
                
                # Split by double underscore to handle nesting
                parts = key.split("__")
                
                # Navigate to the nested dictionary
                current = config
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                
                # Set the value (try to convert to appropriate type)
                try:
                    # Try to convert to int
                    current[parts[-1]] = int(value)
                except ValueError:
                    try:
                        # Try to convert to float
                        current[parts[-1]] = float(value)
                    except ValueError:
                        # Try to convert to bool
                        if value.lower() in ("true", "yes", "1"):
                            current[parts[-1]] = True
                        elif value.lower() in ("false", "no", "0"):
                            current[parts[-1]] = False
                        else:
                            # Keep as string
                            current[parts[-1]] = value
        
        logger.info(f"Loaded configuration from environment variables with prefix {prefix}")
        return cls(config)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the configuration to a dictionary.
        
        Returns:
            Configuration as a dictionary
        """
        return self.config.copy()
    
    def save_to_file(self, file_path: str):
        """
        Save the configuration to a JSON file.
        
        Args:
            file_path: Path to the JSON configuration file
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            
            logger.info(f"Saved configuration to {file_path}")
        except Exception as e:
            logger.error(f"Error saving configuration to {file_path}: {str(e)}")
            raise ConfigurationError(f"Error saving configuration to {file_path}: {str(e)}")


class ConfigurationProfile:
    """
    Predefined configuration profiles for common Redis Mirror CE setups.
    """
    
    @staticmethod
    def development() -> Configuration:
        """
        Development configuration profile.
        
        This profile is suitable for local development with a single Redis instance.
        
        Returns:
            Configuration instance
        """
        config = {
            "wsp_redis": {
                "host": "localhost",
                "port": 6379,
                "password": "",
                "db": 0,
                "decode_responses": True
            },
            "replica_redis": {
                "host": "localhost",
                "port": 6379,
                "password": "",
                "db": 0,
                "decode_responses": True
            },
            "logging": {
                "level": "DEBUG"
            }
        }
        
        return Configuration(config)
    
    @staticmethod
    def production() -> Configuration:
        """
        Production configuration profile.
        
        This profile is suitable for production deployments with separate Redis instances
        for WSP and Stream-Writeable Replica.
        
        Returns:
            Configuration instance
        """
        config = {
            "wsp_redis": {
                "host": os.environ.get("WSP_REDIS_HOST", "localhost"),
                "port": int(os.environ.get("WSP_REDIS_PORT", "6379")),
                "password": os.environ.get("WSP_REDIS_PASSWORD", ""),
                "db": int(os.environ.get("WSP_REDIS_DB", "0")),
                "decode_responses": True,
                "socket_timeout": 10,
                "socket_connect_timeout": 15,
                "health_check_interval": 30
            },
            "replica_redis": {
                "host": os.environ.get("REPLICA_REDIS_HOST", "localhost"),
                "port": int(os.environ.get("REPLICA_REDIS_PORT", "6379")),
                "password": os.environ.get("REPLICA_REDIS_PASSWORD", ""),
                "db": int(os.environ.get("REPLICA_REDIS_DB", "0")),
                "decode_responses": True,
                "socket_timeout": 10,
                "socket_connect_timeout": 15,
                "health_check_interval": 30
            },
            "acl": {
                "exchange_username": os.environ.get("EXCHANGE_USERNAME", "exchange"),
                "exchange_password": os.environ.get("EXCHANGE_PASSWORD", "exchange_password"),
                "wsp_username": os.environ.get("WSP_USERNAME", "wsp"),
                "wsp_password": os.environ.get("WSP_PASSWORD", "wsp_password")
            },
            "logging": {
                "level": os.environ.get("LOG_LEVEL", "INFO")
            }
        }
        
        return Configuration(config)
    
    @staticmethod
    def testing() -> Configuration:
        """
        Testing configuration profile.
        
        This profile is suitable for testing with a local Redis instance.
        
        Returns:
            Configuration instance
        """
        config = {
            "wsp_redis": {
                "host": "localhost",
                "port": 6379,
                "password": "",
                "db": 1,  # Use a different DB for testing
                "decode_responses": True
            },
            "replica_redis": {
                "host": "localhost",
                "port": 6379,
                "password": "",
                "db": 1,  # Use a different DB for testing
                "decode_responses": True
            },
            "logging": {
                "level": "DEBUG"
            },
            "streams": {
                "consumer_group_prefix": "redis_mirror_ce_test",
                "batch_size": 5,
                "block_ms": 1000,
                "max_retry_count": 2,
                "retry_delay_ms": 1000
            }
        }
        
        return Configuration(config)