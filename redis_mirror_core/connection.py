"""
Redis Mirror CE Connection Management Module

This module provides utilities for creating and managing Redis connections
for the Redis Mirror Community Edition architecture, which consists of:
1. A main Redis instance at the WSP
2. A stream-writeable replica with special ACL configuration at the Exchange
"""

import redis
import logging
from typing import Optional, Dict, Any, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_redis_client(
    host: str = "localhost",
    port: int = 6379,
    password: str = "",
    db: int = 0,
    decode_responses: bool = True,
    **kwargs
) -> redis.Redis:
    """
    Create a Redis client for the WSP Redis instance.
    
    Args:
        host: Redis server hostname
        port: Redis server port
        password: Redis server password
        db: Redis database number
        decode_responses: Whether to decode byte responses to strings
        **kwargs: Additional arguments to pass to Redis client
        
    Returns:
        A configured Redis client instance
    """
    logger.debug(f"Creating Redis client for {host}:{port}")
    return redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db,
        decode_responses=decode_responses,
        socket_timeout=5,
        socket_keepalive=True,
        socket_connect_timeout=10,
        health_check_interval=30,
        **kwargs
    )


def create_stream_writeable_replica_client(
    host: str = "localhost",
    port: int = 6379,
    password: str = "",
    db: int = 0,
    decode_responses: bool = True,
    **kwargs
) -> redis.Redis:
    """
    Create a Redis client for the Stream-Writeable Replica instance.
    
    This is a specialized Redis client for connecting to the replica that
    has been configured with 'replica-read-only no' and appropriate ACLs
    to allow the Exchange to write to stream keys.
    
    Args:
        host: Redis replica hostname
        port: Redis replica port
        password: Redis replica password
        db: Redis database number
        decode_responses: Whether to decode byte responses to strings
        **kwargs: Additional arguments to pass to Redis client
        
    Returns:
        A configured Redis client instance for the stream-writeable replica
    """
    logger.debug(f"Creating Stream-Writeable Replica client for {host}:{port}")
    return redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db,
        decode_responses=decode_responses,
        socket_timeout=5,
        socket_keepalive=True,
        socket_connect_timeout=10,
        health_check_interval=30,
        **kwargs
    )


class RedisConnectionManager:
    """
    Manages Redis connections for both WSP Redis and Stream-Writeable Replica.
    
    This class provides a unified interface for working with both Redis instances
    in the Redis Mirror CE architecture.
    """
    
    def __init__(
        self,
        wsp_config: Dict[str, Any],
        replica_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the connection manager with configuration for both Redis instances.
        
        Args:
            wsp_config: Configuration for the WSP Redis instance
            replica_config: Configuration for the Stream-Writeable Replica instance.
                           If None, will use the same config as wsp_config but with
                           different host/port if specified.
        """
        logger.info("Initializing Redis Connection Manager")
        self.wsp_client = create_redis_client(**wsp_config)
        
        if replica_config is None:
            replica_config = wsp_config
            
        self.replica_client = create_stream_writeable_replica_client(**replica_config)
    
    def get_wsp_client(self) -> redis.Redis:
        """Get the WSP Redis client"""
        return self.wsp_client
    
    def get_replica_client(self) -> redis.Redis:
        """Get the Stream-Writeable Replica client"""
        return self.replica_client
    
    def close(self):
        """Close all Redis connections"""
        logger.info("Closing Redis connections")
        self.wsp_client.close()
        self.replica_client.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are closed"""
        self.close()


def check_connection(client: redis.Redis) -> bool:
    """
    Check if a Redis connection is working.
    
    Args:
        client: Redis client to check
        
    Returns:
        True if connection is working, False otherwise
    """
    try:
        return client.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking connection: {e}")
        return False


def get_connection_info(client: redis.Redis) -> Dict[str, Any]:
    """
    Get information about a Redis connection.
    
    Args:
        client: Redis client to get information for
        
    Returns:
        Dictionary with connection information
    """
    try:
        info = client.info()
        return {
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "role": info.get("role"),
            "connected": True
        }
    except Exception as e:
        logger.error(f"Error getting connection info: {e}")
        return {
            "error": str(e),
            "connected": False
        }