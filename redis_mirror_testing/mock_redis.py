"""
Redis Mirror CE Mock Redis Client

This module provides a mock implementation of the Redis client for testing purposes.
It simulates Redis functionality without requiring an actual Redis server.
"""

import time
import json
import re
import threading
from typing import Dict, List, Any, Optional, Callable, Union, Tuple, Set
from collections import defaultdict, deque

# Configure logging
import logging
logger = logging.getLogger(__name__)


class MockRedisClient:
    """
    Mock implementation of the Redis client for testing purposes.
    
    This class simulates Redis functionality without requiring an actual Redis server.
    It implements the most commonly used Redis commands, with a focus on those used
    by the Redis Mirror CE SDK.
    
    Attributes:
        data: Dictionary storing key-value pairs
        streams: Dictionary storing stream data
        consumer_groups: Dictionary storing consumer group data
        pubsub_channels: Dictionary storing pubsub channel subscribers
    """
    
    def __init__(self):
        """Initialize the mock Redis client."""
        # Key-value store
        self.data = {}
        
        # Stream data: stream_key -> list of (id, fields) tuples
        self.streams = defaultdict(list)
        
        # Stream IDs: stream_key -> set of IDs
        self.stream_ids = defaultdict(set)
        
        # Consumer groups: stream_key -> group_name -> {consumers, pending}
        self.consumer_groups = defaultdict(lambda: defaultdict(lambda: {"consumers": set(), "pending": {}}))
        
        # PubSub channels: channel -> list of callbacks
        self.pubsub_channels = defaultdict(list)
        
        # Locks for thread safety
        self._data_lock = threading.RLock()
        self._stream_lock = threading.RLock()
        self._pubsub_lock = threading.RLock()
        
        logger.debug("MockRedisClient initialized")
    
    def reset(self):
        """Reset the mock Redis client to its initial state."""
        with self._data_lock:
            self.data = {}
        
        with self._stream_lock:
            self.streams = defaultdict(list)
            self.stream_ids = defaultdict(set)
            self.consumer_groups = defaultdict(lambda: defaultdict(lambda: {"consumers": set(), "pending": {}}))
        
        with self._pubsub_lock:
            self.pubsub_channels = defaultdict(list)
        
        logger.debug("MockRedisClient reset")
    
    #
    # Key-Value Operations
    #
    
    def set(self, key: str, value: str) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: Key to set
            value: Value to set
            
        Returns:
            True if successful
        """
        with self._data_lock:
            self.data[key] = value
            return True
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value by key.
        
        Args:
            key: Key to get
            
        Returns:
            Value if found, None otherwise
        """
        with self._data_lock:
            return self.data.get(key)
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.
        
        Args:
            *keys: Keys to delete
            
        Returns:
            Number of keys deleted
        """
        count = 0
        with self._data_lock:
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    count += 1
        return count
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Args:
            key: Key to check
            
        Returns:
            True if the key exists, False otherwise
        """
        with self._data_lock:
            return key in self.data
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get keys matching a pattern.
        
        Args:
            pattern: Pattern to match
            
        Returns:
            List of matching keys
        """
        with self._data_lock:
            if pattern == "*":
                return list(self.data.keys())
            
            # Convert Redis pattern to regex
            regex_pattern = "^" + pattern.replace("*", ".*") + "$"
            regex = re.compile(regex_pattern)
            
            return [key for key in self.data.keys() if regex.match(key)]
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set a key's time to live in seconds.
        
        Note: This mock implementation doesn't actually expire keys.
        
        Args:
            key: Key to set expiry on
            seconds: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        with self._data_lock:
            if key in self.data:
                # In a real implementation, we would set an expiry
                # For now, we just return True if the key exists
                return True
            return False
    
    #
    # Hash Operations
    #
    
    def hset(self, key: str, field: str, value: str) -> int:
        """
        Set a hash field to a value.
        
        Args:
            key: Hash key
            field: Hash field
            value: Value to set
            
        Returns:
            1 if field is new, 0 if field existed
        """
        with self._data_lock:
            if key not in self.data:
                self.data[key] = {}
            
            if not isinstance(self.data[key], dict):
                # Convert to dict if not already
                self.data[key] = {}
            
            is_new = field not in self.data[key]
            self.data[key][field] = value
            return 1 if is_new else 0
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """
        Get the value of a hash field.
        
        Args:
            key: Hash key
            field: Hash field
            
        Returns:
            Value if found, None otherwise
        """
        with self._data_lock:
            if key not in self.data or not isinstance(self.data[key], dict):
                return None
            
            return self.data[key].get(field)
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """
        Get all fields and values in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            Dictionary of fields and values
        """
        with self._data_lock:
            if key not in self.data:
                return {}
            
            if not isinstance(self.data[key], dict):
                return {}
            
            return dict(self.data[key])
    
    def hdel(self, key: str, *fields: str) -> int:
        """
        Delete one or more hash fields.
        
        Args:
            key: Hash key
            *fields: Fields to delete
            
        Returns:
            Number of fields deleted
        """
        count = 0
        with self._data_lock:
            if key not in self.data or not isinstance(self.data[key], dict):
                return 0
            
            for field in fields:
                if field in self.data[key]:
                    del self.data[key][field]
                    count += 1
        
        return count
    
    def hexists(self, key: str, field: str) -> bool:
        """
        Check if a hash field exists.
        
        Args:
            key: Hash key
            field: Hash field
            
        Returns:
            True if the field exists, False otherwise
        """
        with self._data_lock:
            if key not in self.data or not isinstance(self.data[key], dict):
                return False
            
            return field in self.data[key]
    
    def hkeys(self, key: str) -> List[str]:
        """
        Get all field names in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            List of field names
        """
        with self._data_lock:
            if key not in self.data or not isinstance(self.data[key], dict):
                return []
            
            return list(self.data[key].keys())
    
    def hvals(self, key: str) -> List[str]:
        """
        Get all values in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            List of values
        """
        with self._data_lock:
            if key not in self.data or not isinstance(self.data[key], dict):
                return []
            
            return list(self.data[key].values())
    
    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """
        Increment the integer value of a hash field by the given amount.
        
        Args:
            key: Hash key
            field: Hash field
            amount: Amount to increment by
            
        Returns:
            New value
        """
        with self._data_lock:
            if key not in self.data:
                self.data[key] = {}
            
            if not isinstance(self.data[key], dict):
                self.data[key] = {}
            
            if field not in self.data[key]:
                self.data[key][field] = "0"
            
            try:
                value = int(self.data[key][field]) + amount
                self.data[key][field] = str(value)
                return value
            except (ValueError, TypeError):
                # If the field value is not an integer, set it to amount
                self.data[key][field] = str(amount)
                return amount
    
    def hincrbyfloat(self, key: str, field: str, amount: float) -> float:
        """
        Increment the float value of a hash field by the given amount.
        
        Args:
            key: Hash key
            field: Hash field
            amount: Amount to increment by
            
        Returns:
            New value
        """
        with self._data_lock:
            if key not in self.data:
                self.data[key] = {}
            
            if not isinstance(self.data[key], dict):
                self.data[key] = {}
            
            if field not in self.data[key]:
                self.data[key][field] = "0"
            
            try:
                value = float(self.data[key][field]) + amount
                self.data[key][field] = str(value)
                return value
            except (ValueError, TypeError):
                # If the field value is not a float, set it to amount
                self.data[key][field] = str(amount)
                return amount
    
    #
    # Stream Operations
    #
    
    def xadd(
        self,
        name: str,
        fields: Dict[str, str],
        id: str = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True
    ) -> str:
        """
        Add a new entry to a stream.
        
        Args:
            name: Stream name
            fields: Dictionary of field-value pairs
            id: Entry ID (default: auto-generated)
            maxlen: Maximum length of the stream
            approximate: Whether maxlen is approximate
            
        Returns:
            ID of the added entry
        """
        with self._stream_lock:
            # Generate ID if not provided
            if id == "*":
                timestamp = int(time.time() * 1000)
                sequence = 0
                
                # Find the highest sequence number for this timestamp
                for existing_id in self.stream_ids[name]:
                    if existing_id.split("-")[0] == str(timestamp):
                        seq = int(existing_id.split("-")[1])
                        sequence = max(sequence, seq + 1)
                
                id = f"{timestamp}-{sequence}"
            
            # Add entry to stream
            self.streams[name].append((id, fields))
            self.stream_ids[name].add(id)
            
            # Trim stream if maxlen is specified
            if maxlen is not None and len(self.streams[name]) > maxlen:
                # Sort by ID and remove oldest entries
                self.streams[name].sort(key=lambda x: x[0])
                excess = len(self.streams[name]) - maxlen
                
                if excess > 0:
                    removed_entries = self.streams[name][:excess]
                    self.streams[name] = self.streams[name][excess:]
                    
                    # Remove IDs from stream_ids
                    for removed_id, _ in removed_entries:
                        self.stream_ids[name].remove(removed_id)
            
            return id
    
    def xread(
        self,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None
    ) -> Optional[Dict[str, List[Tuple[str, Dict[str, str]]]]]:
        """
        Read from multiple streams, starting at the given IDs.
        
        Args:
            streams: Dictionary mapping stream names to IDs
            count: Maximum number of entries to return per stream
            block: Milliseconds to block for, 0 for indefinite, None for no blocking
            
        Returns:
            Dictionary mapping stream names to lists of (ID, fields) tuples
        """
        with self._stream_lock:
            result = {}
            
            for stream_name, last_id in streams.items():
                if stream_name not in self.streams:
                    continue
                
                # Get entries with ID greater than last_id
                entries = []
                
                for entry_id, fields in self.streams[stream_name]:
                    if last_id == "$" or entry_id > last_id:
                        entries.append((entry_id, fields))
                
                # Sort by ID
                entries.sort(key=lambda x: x[0])
                
                # Limit to count if specified
                if count is not None and len(entries) > count:
                    entries = entries[:count]
                
                if entries:
                    result[stream_name] = entries
            
            if not result and block is not None and block > 0:
                # In a real implementation, we would block here
                # For now, we just return None to simulate no new entries
                return None
            
            return result if result else None
    
    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False
    ) -> bool:
        """
        Create a consumer group.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            id: ID to start consuming from
            mkstream: Whether to create the stream if it doesn't exist
            
        Returns:
            True if successful
        """
        with self._stream_lock:
            if name not in self.streams and not mkstream:
                return False
            
            if name not in self.streams:
                self.streams[name] = []
                self.stream_ids[name] = set()
            
            # Create consumer group
            self.consumer_groups[name][groupname] = {
                "consumers": set(),
                "pending": {},
                "last_id": id
            }
            
            return True
    
    def xgroup_destroy(self, name: str, groupname: str) -> bool:
        """
        Destroy a consumer group.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            
        Returns:
            True if successful
        """
        with self._stream_lock:
            if name not in self.consumer_groups or groupname not in self.consumer_groups[name]:
                return False
            
            del self.consumer_groups[name][groupname]
            return True
    
    def xgroup_delconsumer(self, name: str, groupname: str, consumername: str) -> bool:
        """
        Delete a consumer from a consumer group.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            consumername: Consumer name
            
        Returns:
            True if successful
        """
        with self._stream_lock:
            if (name not in self.consumer_groups or
                groupname not in self.consumer_groups[name] or
                consumername not in self.consumer_groups[name][groupname]["consumers"]):
                return False
            
            self.consumer_groups[name][groupname]["consumers"].remove(consumername)
            return True
    
    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False
    ) -> Optional[Dict[str, List[Tuple[str, Dict[str, str]]]]]:
        """
        Read from multiple streams as part of a consumer group.
        
        Args:
            groupname: Consumer group name
            consumername: Consumer name
            streams: Dictionary mapping stream names to IDs
            count: Maximum number of entries to return per stream
            block: Milliseconds to block for, 0 for indefinite, None for no blocking
            noack: Whether to automatically acknowledge the entries
            
        Returns:
            Dictionary mapping stream names to lists of (ID, fields) tuples
        """
        with self._stream_lock:
            result = {}
            
            for stream_name, id_selector in streams.items():
                if stream_name not in self.streams:
                    continue
                
                if (stream_name not in self.consumer_groups or
                    groupname not in self.consumer_groups[stream_name]):
                    continue
                
                # Add consumer to group if not already present
                self.consumer_groups[stream_name][groupname]["consumers"].add(consumername)
                
                # Get entries based on ID selector
                entries = []
                
                if id_selector == ">":
                    # Get new entries (not yet delivered to this group)
                    last_id = self.consumer_groups[stream_name][groupname].get("last_id", "$")
                    
                    for entry_id, fields in self.streams[stream_name]:
                        if last_id == "$" or entry_id > last_id:
                            entries.append((entry_id, fields))
                            
                            # Add to pending if not noack
                            if not noack:
                                self.consumer_groups[stream_name][groupname]["pending"][entry_id] = {
                                    "consumer": consumername,
                                    "time": int(time.time() * 1000)
                                }
                    
                    # Update last_id
                    if entries:
                        self.consumer_groups[stream_name][groupname]["last_id"] = entries[-1][0]
                else:
                    # Get specific entry by ID
                    for entry_id, fields in self.streams[stream_name]:
                        if entry_id == id_selector:
                            entries.append((entry_id, fields))
                            
                            # Add to pending if not noack
                            if not noack:
                                self.consumer_groups[stream_name][groupname]["pending"][entry_id] = {
                                    "consumer": consumername,
                                    "time": int(time.time() * 1000)
                                }
                            
                            break
                
                # Sort by ID
                entries.sort(key=lambda x: x[0])
                
                # Limit to count if specified
                if count is not None and len(entries) > count:
                    entries = entries[:count]
                
                if entries:
                    result[stream_name] = entries
            
            if not result and block is not None and block > 0:
                # In a real implementation, we would block here
                # For now, we just return None to simulate no new entries
                return None
            
            return result if result else None
    
    def xack(self, name: str, groupname: str, *ids: str) -> int:
        """
        Acknowledge one or more messages as processed.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            *ids: Message IDs to acknowledge
            
        Returns:
            Number of messages acknowledged
        """
        count = 0
        with self._stream_lock:
            if name not in self.consumer_groups or groupname not in self.consumer_groups[name]:
                return 0
            
            for id in ids:
                if id in self.consumer_groups[name][groupname]["pending"]:
                    del self.consumer_groups[name][groupname]["pending"][id]
                    count += 1
        
        return count
    
    def xpending(
        self,
        name: str,
        groupname: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        count: Optional[int] = None,
        consumername: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get information about pending messages in a consumer group.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            start: Start ID
            end: End ID
            count: Maximum number of messages to return
            consumername: Filter by consumer name
            
        Returns:
            List of dictionaries with message information
        """
        with self._stream_lock:
            if name not in self.consumer_groups or groupname not in self.consumer_groups[name]:
                return []
            
            pending = self.consumer_groups[name][groupname]["pending"]
            result = []
            
            for id, info in pending.items():
                if start is not None and id < start:
                    continue
                
                if end is not None and id > end:
                    continue
                
                if consumername is not None and info["consumer"] != consumername:
                    continue
                
                result.append({
                    "id": id,
                    "consumer": info["consumer"],
                    "time": int(time.time() * 1000) - info["time"],
                    "deliveries": 1
                })
            
            # Sort by ID
            result.sort(key=lambda x: x["id"])
            
            # Limit to count if specified
            if count is not None and len(result) > count:
                result = result[:count]
            
            return result
    
    def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        *ids: str,
        **kwargs
    ) -> List[Tuple[str, Dict[str, str]]]:
        """
        Claim pending messages for a consumer.
        
        Args:
            name: Stream name
            groupname: Consumer group name
            consumername: Consumer name
            min_idle_time: Minimum idle time in milliseconds
            *ids: Message IDs to claim
            **kwargs: Additional arguments
            
        Returns:
            List of (ID, fields) tuples for claimed messages
        """
        with self._stream_lock:
            if name not in self.consumer_groups or groupname not in self.consumer_groups[name]:
                return []
            
            pending = self.consumer_groups[name][groupname]["pending"]
            result = []
            
            for id in ids:
                if id not in pending:
                    continue
                
                info = pending[id]
                idle_time = int(time.time() * 1000) - info["time"]
                
                if idle_time >= min_idle_time:
                    # Claim message
                    pending[id] = {
                        "consumer": consumername,
                        "time": int(time.time() * 1000)
                    }
                    
                    # Find message fields
                    for entry_id, fields in self.streams[name]:
                        if entry_id == id:
                            result.append((id, fields))
                            break
            
            return result
    
    #
    # PubSub Operations
    #
    
    def publish(self, channel: str, message: str) -> int:
        """
        Publish a message to a channel.
        
        Args:
            channel: Channel name
            message: Message to publish
            
        Returns:
            Number of clients that received the message
        """
        with self._pubsub_lock:
            callbacks = self.pubsub_channels.get(channel, [])
            
            for callback in callbacks:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"Error in pubsub callback: {e}")
            
            return len(callbacks)
    
    def subscribe(self, channel: str, callback: Callable[[str], None]) -> None:
        """
        Subscribe to a channel.
        
        Args:
            channel: Channel name
            callback: Callback function to call when a message is received
        """
        with self._pubsub_lock:
            self.pubsub_channels[channel].append(callback)
    
    def unsubscribe(self, channel: str, callback: Callable[[str], None]) -> None:
        """
        Unsubscribe from a channel.
        
        Args:
            channel: Channel name
            callback: Callback function to remove
        """
        with self._pubsub_lock:
            if channel in self.pubsub_channels:
                try:
                    self.pubsub_channels[channel].remove(callback)
                except ValueError:
                    pass
    
    #
    # ACL Operations
    #
    
    def acl_list(self) -> List[str]:
        """
        Get the list of ACL rules.
        
        Returns:
            List of ACL rules
        """
        # Mock implementation - return a default ACL rule
        return ["user default on nopass ~* &* +@all"]
    
    def acl_setuser(self, username: str, *rules: str) -> bool:
        """
        Set ACL rules for a user.
        
        Args:
            username: Username
            *rules: ACL rules
            
        Returns:
            True if successful
        """
        # Mock implementation - always return True
        return True
    
    def acl_getuser(self, username: str) -> Dict[str, Any]:
        """
        Get ACL rules for a user.
        
        Args:
            username: Username
            
        Returns:
            Dictionary with user information
        """
        # Mock implementation - return a default user
        return {
            "flags": ["on"],
            "passwords": [],
            "commands": "+@all",
            "keys": ["~*"],
            "channels": ["&*"]
        }
    
    def acl_deluser(self, *usernames: str) -> int:
        """
        Delete one or more users.
        
        Args:
            *usernames: Usernames to delete
            
        Returns:
            Number of users deleted
        """
        # Mock implementation - always return the number of usernames
        return len(usernames)