"""
Redis Mirror CE Stream Processing Module

This module provides utilities for working with Redis Streams in the
Redis Mirror Community Edition architecture, including consumer groups,
message processing, and error handling.
"""

import redis
import time
import json
import logging
import uuid
from typing import Dict, List, Any, Callable, Optional, Tuple, Union
from .errors import StreamError, TimeoutError, handle_redis_error

# Configure logging
logger = logging.getLogger(__name__)


def create_consumer_group(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    start_id: str = "0",
    mkstream: bool = True
) -> bool:
    """
    Create a consumer group for a Redis stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        start_id: ID to start consuming from ('0' for all messages, '$' for new messages only)
        mkstream: Whether to create the stream if it doesn't exist
        
    Returns:
        True if the consumer group was created successfully, False otherwise
    """
    try:
        client.xgroup_create(stream_name, group_name, id=start_id, mkstream=mkstream)
        logger.info(f"Created consumer group '{group_name}' for stream '{stream_name}'")
        return True
    except redis.exceptions.ResponseError as e:
        # Ignore error if group already exists
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{group_name}' already exists for stream '{stream_name}'")
            return True
        logger.error(f"Error creating consumer group: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error creating consumer group: {e}")
        return False


def add_message_to_stream(
    client: redis.Redis,
    stream_name: str,
    message: Dict[str, Any],
    max_len: int = 1000,
    approximate: bool = True,
    id: str = "*"
) -> Optional[str]:
    """
    Add a message to a Redis stream with automatic trimming.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        message: Dictionary containing the message fields and values
        max_len: Maximum length of the stream
        approximate: Whether to use approximate trimming (more efficient)
        id: Message ID ('*' for auto-generation)
        
    Returns:
        The ID of the added message if successful, None otherwise
    """
    try:
        # Convert any non-string values to strings
        string_message = {k: str(v) if not isinstance(v, str) else v 
                         for k, v in message.items()}
        
        # Add the message with trimming
        if approximate:
            result = client.xadd(stream_name, string_message, id=id, maxlen="~", approximate=True, limit=max_len)
        else:
            result = client.xadd(stream_name, string_message, id=id, maxlen=max_len)
        
        logger.debug(f"Added message to stream '{stream_name}' with ID {result}")
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error adding message to stream '{stream_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error adding message to stream: {e}")
        return None


def read_messages(
    client: redis.Redis,
    stream_name: str,
    count: int = 10,
    block: Optional[int] = None,
    last_id: str = "$"
) -> List[List[Tuple[bytes, Dict[bytes, bytes]]]]:
    """
    Read messages from a Redis stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        count: Maximum number of messages to read
        block: Milliseconds to block waiting for messages (None for non-blocking)
        last_id: ID to start reading from ('$' for new messages only, '0' for all messages)
        
    Returns:
        List of stream entries with message ID and data
    """
    try:
        streams = {stream_name: last_id}
        result = client.xread(streams, count=count, block=block)
        logger.debug(f"Read {len(result)} messages from stream '{stream_name}'")
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error reading from stream '{stream_name}': {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading from stream: {e}")
        return []


def read_messages_from_group(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    count: int = 10,
    block: Optional[int] = None,
    noack: bool = False,
    last_id: str = ">"
) -> List[List[Tuple[bytes, Dict[bytes, bytes]]]]:
    """
    Read messages from a Redis stream using a consumer group.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        consumer_name: Name of the consumer
        count: Maximum number of messages to read
        block: Milliseconds to block waiting for messages (None for non-blocking)
        noack: Whether to automatically acknowledge messages
        last_id: ID to start reading from ('>' for new messages only, '0' for pending messages)
        
    Returns:
        List of stream entries with message ID and data
    """
    try:
        streams = {stream_name: last_id}
        result = client.xreadgroup(
            group_name, 
            consumer_name, 
            streams, 
            count=count, 
            block=block, 
            noack=noack
        )
        logger.debug(f"Read {len(result)} messages from group '{group_name}' on stream '{stream_name}'")
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error reading from group '{group_name}' on stream '{stream_name}': {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading from group: {e}")
        return []


def acknowledge_message(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    message_id: str
) -> bool:
    """
    Acknowledge a message in a consumer group.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        message_id: ID of the message to acknowledge
        
    Returns:
        True if the message was acknowledged successfully, False otherwise
    """
    try:
        result = client.xack(stream_name, group_name, message_id)
        logger.debug(f"Acknowledged message '{message_id}' in group '{group_name}'")
        return result > 0
    except redis.exceptions.RedisError as e:
        logger.error(f"Error acknowledging message '{message_id}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error acknowledging message: {e}")
        return False


def get_pending_messages(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    count: int = 10,
    consumer: Optional[str] = None,
    min_idle_time: Optional[int] = None,
    start: str = "-",
    end: str = "+"
) -> List[Dict[str, Any]]:
    """
    Get information about pending messages in a consumer group.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        count: Maximum number of messages to return
        consumer: Filter by consumer name
        min_idle_time: Filter by minimum idle time in milliseconds
        start: Start ID range
        end: End ID range
        
    Returns:
        List of pending message information
    """
    try:
        result = client.xpending_range(
            stream_name, 
            group_name, 
            start, 
            end, 
            count, 
            consumer=consumer,
            idle=min_idle_time
        )
        logger.debug(f"Found {len(result)} pending messages in group '{group_name}'")
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error getting pending messages: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error getting pending messages: {e}")
        return []


def claim_pending_messages(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    min_idle_time: int,
    message_ids: List[str]
) -> List[List[Tuple[bytes, Dict[bytes, bytes]]]]:
    """
    Claim pending messages from another consumer.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        consumer_name: Name of the consumer claiming the messages
        min_idle_time: Minimum idle time in milliseconds
        message_ids: List of message IDs to claim
        
    Returns:
        List of claimed messages
    """
    try:
        result = client.xclaim(
            stream_name,
            group_name,
            consumer_name,
            min_idle_time,
            message_ids
        )
        logger.debug(f"Claimed {len(result)} messages in group '{group_name}'")
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error claiming pending messages: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error claiming messages: {e}")
        return []


def trim_stream(
    client: redis.Redis,
    stream_name: str,
    max_len: int,
    approximate: bool = True
) -> bool:
    """
    Trim a Redis stream to a maximum length.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        max_len: Maximum length of the stream
        approximate: Whether to use approximate trimming (more efficient)
        
    Returns:
        True if the stream was trimmed successfully, False otherwise
    """
    try:
        if approximate:
            client.xtrim(stream_name, maxlen="~", approximate=True, limit=max_len)
        else:
            client.xtrim(stream_name, maxlen=max_len)
        logger.debug(f"Trimmed stream '{stream_name}' to max length {max_len}")
        return True
    except redis.exceptions.RedisError as e:
        logger.error(f"Error trimming stream '{stream_name}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error trimming stream: {e}")
        return False


def delete_message(
    client: redis.Redis,
    stream_name: str,
    message_id: str
) -> bool:
    """
    Delete a message from a stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        message_id: ID of the message to delete
        
    Returns:
        True if the message was deleted successfully, False otherwise
    """
    try:
        result = client.xdel(stream_name, message_id)
        logger.debug(f"Deleted message '{message_id}' from stream '{stream_name}'")
        return result > 0
    except redis.exceptions.RedisError as e:
        logger.error(f"Error deleting message '{message_id}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting message: {e}")
        return False


def get_stream_info(
    client: redis.Redis,
    stream_name: str
) -> Dict[str, Any]:
    """
    Get information about a stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        
    Returns:
        Dictionary with stream information
    """
    try:
        info = client.xinfo_stream(stream_name)
        # Convert bytes to strings in the result
        result = {}
        for k, v in info.items():
            if isinstance(k, bytes):
                k = k.decode('utf-8')
            if isinstance(v, bytes):
                v = v.decode('utf-8')
            result[k] = v
        return result
    except redis.exceptions.RedisError as e:
        logger.error(f"Error getting stream info for '{stream_name}': {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error getting stream info: {e}")
        return {}


def get_consumer_group_info(
    client: redis.Redis,
    stream_name: str,
    group_name: str
) -> Dict[str, Any]:
    """
    Get information about a consumer group.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        group_name: Name of the consumer group
        
    Returns:
        Dictionary with consumer group information
    """
    try:
        groups = client.xinfo_groups(stream_name)
        for group in groups:
            if group.get(b'name', b'').decode('utf-8') == group_name:
                # Convert bytes to strings in the result
                result = {}
                for k, v in group.items():
                    if isinstance(k, bytes):
                        k = k.decode('utf-8')
                    if isinstance(v, bytes):
                        v = v.decode('utf-8')
                    result[k] = v
                return result
        return {}
    except redis.exceptions.RedisError as e:
        logger.error(f"Error getting consumer group info for '{group_name}': {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error getting consumer group info: {e}")
        return {}


class StreamProcessor:
    """
    Processes messages from Redis Streams with consumer group support,
    error handling, and automatic recovery of failed messages.
    """
    
    def __init__(
        self,
        client: redis.Redis,
        stream_name: str,
        group_name: str,
        consumer_name: Optional[str] = None,
        auto_create_group: bool = True,
        max_retry_count: int = 3,
        retry_delay_ms: int = 5000,
        block_ms: int = 2000,
        batch_size: int = 10
    ):
        """
        Initialize the stream processor.
        
        Args:
            client: Redis client
            stream_name: Name of the stream to process
            group_name: Name of the consumer group
            consumer_name: Name of this consumer (defaults to a UUID)
            auto_create_group: Whether to automatically create the consumer group if it doesn't exist
            max_retry_count: Maximum number of times to retry processing a message
            retry_delay_ms: Delay in milliseconds before retrying failed messages
            block_ms: Milliseconds to block waiting for new messages
            batch_size: Number of messages to process in each batch
        """
        self.client = client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name or f"consumer-{uuid.uuid4()}"
        self.max_retry_count = max_retry_count
        self.retry_delay_ms = retry_delay_ms
        self.block_ms = block_ms
        self.batch_size = batch_size
        self._running = False
        
        # Create the consumer group if needed
        if auto_create_group:
            if not create_consumer_group(client, stream_name, group_name):
                raise StreamError(f"Failed to create consumer group '{group_name}' for stream '{stream_name}'")
    
    def process_messages(
        self,
        callback: Callable[[Dict[str, str]], bool],
        run_once: bool = False,
        process_pending: bool = True
    ):
        """
        Process messages from the stream using the provided callback function.
        
        Args:
            callback: Function that processes a message and returns True if successful
            run_once: If True, process one batch and return; if False, run continuously
            process_pending: Whether to process pending messages
        """
        self._running = True
        
        try:
            while self._running:
                # Process new messages
                self._process_batch(callback, ">")
                
                # Process pending messages if enabled
                if process_pending:
                    self._process_pending_messages(callback)
                
                if run_once:
                    break
                
                # Small delay to prevent CPU spinning
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Stream processing interrupted")
        except Exception as e:
            logger.error(f"Error in process_messages: {e}")
        finally:
            self._running = False
    
    def stop(self):
        """Stop processing messages."""
        self._running = False
    
    def _process_batch(
        self,
        callback: Callable[[Dict[str, str]], bool],
        last_id: str
    ):
        """
        Process a batch of messages from the stream.
        
        Args:
            callback: Function that processes a message and returns True if successful
            last_id: ID to start reading from
        """
        try:
            # Read messages from the stream
            streams = read_messages_from_group(
                self.client,
                self.stream_name,
                self.group_name,
                self.consumer_name,
                count=self.batch_size,
                block=self.block_ms,
                last_id=last_id
            )
            
            # Process each message
            for stream_data in streams:
                for stream_name, messages in stream_data:
                    stream_name_str = stream_name.decode('utf-8') if isinstance(stream_name, bytes) else stream_name
                    for message_id, message_data in messages:
                        message_id_str = message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
                        
                        # Convert bytes to strings
                        message_dict = {}
                        for k, v in message_data.items():
                            k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                            v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                            message_dict[k_str] = v_str
                        
                        # Process the message
                        success = False
                        try:
                            success = callback(message_dict)
                        except Exception as e:
                            logger.error(f"Error processing message {message_id_str}: {e}")
                        
                        # Acknowledge the message if processing was successful
                        if success:
                            acknowledge_message(
                                self.client,
                                stream_name_str,
                                self.group_name,
                                message_id_str
                            )
        
        except Exception as e:
            logger.error(f"Error in _process_batch: {e}")
    
    def _process_pending_messages(self, callback: Callable[[Dict[str, str]], bool]):
        """
        Process pending messages that haven't been acknowledged.
        
        Args:
            callback: Function that processes a message and returns True if successful
        """
        try:
            # Get pending messages
            pending = get_pending_messages(
                self.client,
                self.stream_name,
                self.group_name,
                count=self.batch_size
            )
            
            if not pending:
                return
            
            # Get message IDs to claim
            message_ids = [item['message_id'] for item in pending]
            
            # Claim the messages
            claimed = claim_pending_messages(
                self.client,
                self.stream_name,
                self.group_name,
                self.consumer_name,
                self.retry_delay_ms,
                message_ids
            )
            
            # Process each claimed message
            for message_id, message_data in claimed:
                message_id_str = message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
                
                # Convert bytes to strings
                message_dict = {}
                for k, v in message_data.items():
                    k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                    v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                    message_dict[k_str] = v_str
                
                # Check retry count
                retry_count = int(message_dict.get('__retry_count', 0))
                if retry_count >= self.max_retry_count:
                    logger.warning(f"Message {message_id_str} exceeded max retry count, skipping")
                    acknowledge_message(
                        self.client,
                        self.stream_name,
                        self.group_name,
                        message_id_str
                    )
                    continue
                
                # Increment retry count
                message_dict['__retry_count'] = str(retry_count + 1)
                
                # Process the message
                success = False
                try:
                    success = callback(message_dict)
                except Exception as e:
                    logger.error(f"Error processing pending message {message_id_str}: {e}")
                
                # Acknowledge the message if processing was successful
                if success:
                    acknowledge_message(
                        self.client,
                        self.stream_name,
                        self.group_name,
                        message_id_str
                    )
        
        except Exception as e:
            logger.error(f"Error in _process_pending_messages: {e}")


class StreamPublisher:
    """
    Publishes messages to Redis Streams with reliability features.
    """
    
    def __init__(
        self,
        client: redis.Redis,
        stream_name: str,
        max_len: int = 1000,
        approximate_trimming: bool = True
    ):
        """
        Initialize the stream publisher.
        
        Args:
            client: Redis client
            stream_name: Name of the stream to publish to
            max_len: Maximum length of the stream
            approximate_trimming: Whether to use approximate trimming (more efficient)
        """
        self.client = client
        self.stream_name = stream_name
        self.max_len = max_len
        self.approximate_trimming = approximate_trimming
    
    def publish(
        self,
        message: Dict[str, Any],
        id: str = "*"
    ) -> Optional[str]:
        """
        Publish a message to the stream.
        
        Args:
            message: Dictionary containing the message fields and values
            id: Message ID ('*' for auto-generation)
            
        Returns:
            The ID of the published message if successful, None otherwise
        """
        return add_message_to_stream(
            self.client,
            self.stream_name,
            message,
            max_len=self.max_len,
            approximate=self.approximate_trimming,
            id=id
        )
    
    def publish_event(
        self,
        event_type: str,
        data: Dict[str, Any]
    ) -> Optional[str]:
        """
        Publish an event to the stream.
        
        This is a convenience method that formats the event with a timestamp
        and event type before publishing it to the stream.
        
        Args:
            event_type: Type of event
            data: Event data
            
        Returns:
            The ID of the published message if successful, None otherwise
        """
        # Create the event message
        message = {
            "timestamp": str(int(time.time() * 1000)),
            "event_type": event_type,
            **data
        }
        
        # Publish the message
        return self.publish(message)
    
    def trim(self) -> bool:
        """
        Trim the stream to the maximum length.
        
        Returns:
            True if the stream was trimmed successfully, False otherwise
        """
        return trim_stream(
            self.client,
            self.stream_name,
            self.max_len,
            self.approximate_trimming
        )


def publish_event(
    client: redis.Redis,
    stream_name: str,
    event_type: str,
    data: Dict[str, Any],
    max_len: int = 1000
) -> Optional[str]:
    """
    Publish an event to a Redis stream.
    
    This is a convenience function that formats the event with a timestamp
    and event type before adding it to the stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream
        event_type: Type of event
        data: Event data
        max_len: Maximum length of the stream
        
    Returns:
        The ID of the added message if successful, None otherwise
    """
    publisher = StreamPublisher(client, stream_name, max_len)
    return publisher.publish_event(event_type, data)