"""
Redis Mirror CE ACL Management Module

This module provides utilities for setting up and managing Redis ACLs
for the Redis Mirror Community Edition architecture, which requires
specific ACL configurations for the Stream-Writeable Replica.
"""

import redis
import logging
from typing import List, Optional, Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


def setup_exchange_acl(
    client: redis.Redis,
    username: str,
    password: str,
    stream_patterns: Optional[List[str]] = None
) -> bool:
    """
    Set up ACL for the Exchange user on the Stream-Writeable Replica.
    
    This configures the Exchange user with restricted permissions that only allow
    writing to stream keys and basic connection commands.
    
    Args:
        client: Redis client with admin privileges
        username: Username for the Exchange user
        password: Password for the Exchange user
        stream_patterns: Optional list of stream key patterns to restrict access to.
                        If None, allows access to all keys.
        
    Returns:
        True if ACL setup was successful, False otherwise
    """
    try:
        # Create a basic command list for the Exchange user
        commands = ["+xadd", "+auth", "+ping", "+info"]
        
        # Add extended stream commands if needed
        extended_commands = [
            "+xrange", "+xlen", "+xread", "+xgroup", 
            "+xreadgroup", "+xack", "+xclaim", "+xpending"
        ]
        commands.extend(extended_commands)
        
        # Build the ACL rule
        acl_rule = f"on >{password}"
        
        # Add key patterns if specified
        if stream_patterns:
            for pattern in stream_patterns:
                acl_rule += f" ~{pattern}"
        else:
            # Default to all exchange stream keys
            acl_rule += " ~exchange:stream:*"
            acl_rule += " ~mirror:credit:response"
            acl_rule += " ~mirror:settlement:*:response"
        
        # Add command restrictions
        acl_rule += " -@all"
        for cmd in commands:
            acl_rule += f" {cmd}"
        
        # Apply the ACL rule
        client.acl_setuser(username, acl_rule)
        
        # Verify the ACL was set correctly
        user_info = client.acl_getuser(username)
        logger.info(f"Exchange ACL set up for user {username}")
        return user_info is not None
    
    except redis.exceptions.RedisError as e:
        logger.error(f"Error setting up Exchange ACL: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error setting up Exchange ACL: {e}")
        return False


def setup_wsp_acl(
    client: redis.Redis,
    username: str,
    password: str
) -> bool:
    """
    Set up ACL for the WSP user with full access except dangerous commands.
    
    Args:
        client: Redis client with admin privileges
        username: Username for the WSP user
        password: Password for the WSP user
        
    Returns:
        True if ACL setup was successful, False otherwise
    """
    try:
        # Build the ACL rule for WSP user with full access except dangerous commands
        acl_rule = f"on >{password} ~* +@all -@dangerous"
        
        # Apply the ACL rule
        client.acl_setuser(username, acl_rule)
        
        # Verify the ACL was set correctly
        user_info = client.acl_getuser(username)
        logger.info(f"WSP ACL set up for user {username}")
        return user_info is not None
    
    except redis.exceptions.RedisError as e:
        logger.error(f"Error setting up WSP ACL: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error setting up WSP ACL: {e}")
        return False


def disable_default_user(client: redis.Redis) -> bool:
    """
    Disable the default user for security.
    
    Args:
        client: Redis client with admin privileges
        
    Returns:
        True if disabling was successful, False otherwise
    """
    try:
        client.acl_setuser("default", "off")
        logger.info("Default user disabled")
        return True
    except redis.exceptions.RedisError as e:
        logger.error(f"Error disabling default user: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error disabling default user: {e}")
        return False


def setup_basic_acls(
    client: redis.Redis,
    exchange_username: str,
    exchange_password: str,
    wsp_username: str,
    wsp_password: str,
    stream_patterns: Optional[List[str]] = None
) -> bool:
    """
    Set up basic ACLs for both Exchange and WSP users.
    
    This is a convenience function that sets up both users and disables
    the default user for security.
    
    Args:
        client: Redis client with admin privileges
        exchange_username: Username for the Exchange user
        exchange_password: Password for the Exchange user
        wsp_username: Username for the WSP user
        wsp_password: Password for the WSP user
        stream_patterns: Optional list of stream key patterns for Exchange user
        
    Returns:
        True if all ACL setup was successful, False otherwise
    """
    try:
        logger.info("Setting up basic ACLs")
        
        # Disable default user
        if not disable_default_user(client):
            logger.warning("Failed to disable default user")
            return False
        
        # Set up Exchange user
        if not setup_exchange_acl(client, exchange_username, exchange_password, stream_patterns):
            logger.warning("Failed to set up Exchange ACL")
            return False
        
        # Set up WSP user
        if not setup_wsp_acl(client, wsp_username, wsp_password):
            logger.warning("Failed to set up WSP ACL")
            return False
        
        logger.info("Basic ACLs set up successfully")
        return True
    
    except redis.exceptions.RedisError as e:
        logger.error(f"Error setting up basic ACLs: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error setting up basic ACLs: {e}")
        return False


def test_exchange_acl(
    client: redis.Redis,
    stream_key: str
) -> bool:
    """
    Test if the current connection can write to a stream key.
    
    This is useful for verifying that the Exchange ACL is set up correctly.
    
    Args:
        client: Redis client with Exchange user credentials
        stream_key: Stream key to test writing to
        
    Returns:
        True if writing to the stream was successful, False otherwise
    """
    try:
        # Try to add a message to the stream
        result = client.xadd(stream_key, {"test": "value"})
        logger.info(f"Successfully wrote to stream {stream_key}")
        return result is not None
    except redis.exceptions.RedisError as e:
        logger.error(f"Error testing Exchange ACL: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error testing Exchange ACL: {e}")
        return False


def test_wsp_acl(client: redis.Redis) -> bool:
    """
    Test if the current connection has full WSP access.
    
    This is useful for verifying that the WSP ACL is set up correctly.
    
    Args:
        client: Redis client with WSP user credentials
        
    Returns:
        True if the connection has full WSP access, False otherwise
    """
    try:
        # Try a few operations that should be allowed for WSP
        test_key = "test:wsp:acl"
        
        # Set a value
        client.set(test_key, "test")
        
        # Get the value
        value = client.get(test_key)
        
        # Delete the key
        client.delete(test_key)
        
        logger.info("WSP ACL test successful")
        return value == "test"
    except redis.exceptions.RedisError as e:
        logger.error(f"Error testing WSP ACL: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error testing WSP ACL: {e}")
        return False


def get_acl_list(client: redis.Redis) -> List[Dict[str, Any]]:
    """
    Get a list of all ACL users and their rules.
    
    Args:
        client: Redis client with admin privileges
        
    Returns:
        List of dictionaries containing user information
    """
    try:
        acl_list = client.acl_list()
        result = []
        
        for acl_string in acl_list:
            parts = acl_string.split()
            user_info = {"username": parts[1]}
            
            # Extract flags
            flags = []
            for part in parts:
                if part in ["on", "off", "nopass", "skip"]:
                    flags.append(part)
            user_info["flags"] = flags
            
            # Extract key patterns
            patterns = []
            for i, part in enumerate(parts):
                if part.startswith("~"):
                    patterns.append(part[1:])
            user_info["patterns"] = patterns
            
            result.append(user_info)
        
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error getting ACL list: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error getting ACL list: {e}")
        return []


class ACLManager:
    """
    Manages ACLs for the Redis Mirror CE system.
    
    This class provides a unified interface for setting up and testing
    ACLs for both WSP and Exchange users.
    """
    
    def __init__(self, admin_client: redis.Redis):
        """
        Initialize the ACL manager with an admin client.
        
        Args:
            admin_client: Redis client with admin privileges
        """
        self.admin_client = admin_client
    
    def setup_exchange_user(
        self,
        username: str,
        password: str,
        stream_patterns: Optional[List[str]] = None
    ) -> bool:
        """Set up the Exchange user ACL"""
        return setup_exchange_acl(
            self.admin_client, username, password, stream_patterns
        )
    
    def setup_wsp_user(self, username: str, password: str) -> bool:
        """Set up the WSP user ACL"""
        return setup_wsp_acl(self.admin_client, username, password)
    
    def setup_all_users(
        self,
        exchange_username: str,
        exchange_password: str,
        wsp_username: str,
        wsp_password: str,
        stream_patterns: Optional[List[str]] = None
    ) -> bool:
        """Set up all users and disable default user"""
        return setup_basic_acls(
            self.admin_client,
            exchange_username,
            exchange_password,
            wsp_username,
            wsp_password,
            stream_patterns
        )
    
    def test_exchange_permissions(
        self,
        client: redis.Redis,
        stream_key: str
    ) -> bool:
        """Test Exchange user permissions"""
        return test_exchange_acl(client, stream_key)
    
    def test_wsp_permissions(self, client: redis.Redis) -> bool:
        """Test WSP user permissions"""
        return test_wsp_acl(client)
    
    def get_users(self) -> List[Dict[str, Any]]:
        """Get a list of all ACL users and their rules"""
        return get_acl_list(self.admin_client)
    
    def disable_default_user(self) -> bool:
        """Disable the default user for security"""
        return disable_default_user(self.admin_client)