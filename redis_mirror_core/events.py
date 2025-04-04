"""
Redis Mirror CE Event Handling Module

This module provides functionality for standardized event handling across the
Redis Mirror Community Edition system, including event publishing, subscription,
and routing.
"""

import enum
import json
import time
import uuid
import logging
import re
import threading
from typing import Dict, Any, Optional, List, Callable, Union, Pattern, Set, Tuple

from .streams import (
    StreamPublisher,
    StreamProcessor,
    add_message_to_stream,
    read_messages,
    read_messages_from_group
)
from .errors import ValidationError, RedisMirrorError

# Configure logging
logger = logging.getLogger(__name__)


class EventType(enum.Enum):
    """
    Standard event types for the Redis Mirror CE system.
    
    Event types are organized by category and name, in the format:
    {category}:{name}
    """
    
    # Credit events
    CREDIT_REQUEST = "credit:request"
    CREDIT_RESPONSE = "credit:response"
    CREDIT_UPDATE = "credit:update"
    
    # Settlement events
    SETTLEMENT_REPORT = "settlement:report"
    SETTLEMENT_CONFIRMATION = "settlement:confirmation"
    SETTLEMENT_COMPLETION = "settlement:completion"
    
    # System events
    SYSTEM_STARTUP = "system:startup"
    SYSTEM_SHUTDOWN = "system:shutdown"
    SYSTEM_ERROR = "system:error"
    
    @classmethod
    def from_string(cls, event_type_str: str) -> 'EventType':
        """
        Convert a string to an EventType.
        
        Args:
            event_type_str: String representation of the event type
            
        Returns:
            EventType enum value
            
        Raises:
            ValueError: If the string does not match any EventType
        """
        for event_type in cls:
            if event_type.value == event_type_str:
                return event_type
        
        raise ValueError(f"Unknown event type: {event_type_str}")
    
    @classmethod
    def is_valid(cls, event_type_str: str) -> bool:
        """
        Check if a string is a valid EventType.
        
        Args:
            event_type_str: String representation of the event type
            
        Returns:
            True if the string is a valid EventType, False otherwise
        """
        try:
            cls.from_string(event_type_str)
            return True
        except ValueError:
            return False
    
    @classmethod
    def get_category(cls, event_type: Union[str, 'EventType']) -> str:
        """
        Get the category of an event type.
        
        Args:
            event_type: Event type as string or EventType
            
        Returns:
            Category of the event type
            
        Raises:
            ValueError: If the event type is invalid
        """
        if isinstance(event_type, cls):
            event_type_str = event_type.value
        else:
            event_type_str = event_type
        
        if not cls.is_valid(event_type_str):
            raise ValueError(f"Invalid event type: {event_type_str}")
        
        return event_type_str.split(':')[0]
    
    @classmethod
    def get_name(cls, event_type: Union[str, 'EventType']) -> str:
        """
        Get the name of an event type.
        
        Args:
            event_type: Event type as string or EventType
            
        Returns:
            Name of the event type
            
        Raises:
            ValueError: If the event type is invalid
        """
        if isinstance(event_type, cls):
            event_type_str = event_type.value
        else:
            event_type_str = event_type
        
        if not cls.is_valid(event_type_str):
            raise ValueError(f"Invalid event type: {event_type_str}")
        
        return event_type_str.split(':')[1]


class Event:
    """
    Represents an event in the Redis Mirror CE system.
    
    Events are used for communication between components and follow a
    standardized format.
    """
    
    def __init__(
        self,
        event_type: Union[str, EventType],
        source: str,
        data: Dict[str, Any],
        event_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize an event.
        
        Args:
            event_type: Type of the event (category:name)
            source: Component that generated the event
            data: Event-specific data
            event_id: Optional event identifier (generated if not provided)
            timestamp: Optional timestamp (current time if not provided)
            metadata: Optional additional context
            
        Raises:
            ValidationError: If the event is invalid
        """
        # Convert EventType enum to string if needed
        if isinstance(event_type, EventType):
            self.event_type = event_type.value
        else:
            self.event_type = event_type
        
        self.source = source
        self.data = data
        self.event_id = event_id or str(uuid.uuid4())
        self.timestamp = timestamp or int(time.time() * 1000)
        self.metadata = metadata or {}
        
        # Validate the event
        self.validate()
    
    def validate(self):
        """
        Validate the event.
        
        Raises:
            ValidationError: If the event is invalid
        """
        # Validate event type
        if not self.event_type:
            raise ValidationError("Event type is required", "event_type")
        
        # Check if event type follows the category:name format
        if not re.match(r'^[a-z_]+:[a-z_]+$', self.event_type):
            raise ValidationError(
                "Event type must follow the format 'category:name'",
                "event_type",
                self.event_type
            )
        
        # Validate source
        if not self.source:
            raise ValidationError("Source is required", "source")
        
        # Validate data
        if self.data is None:
            raise ValidationError("Data is required", "data")
        
        # Validate event ID
        if not self.event_id:
            raise ValidationError("Event ID is required", "event_id")
        
        # Validate timestamp
        if not self.timestamp:
            raise ValidationError("Timestamp is required", "timestamp")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the event to a dictionary.
        
        Returns:
            Dictionary representation of the event
        """
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "timestamp": str(self.timestamp),
            "data": json.dumps(self.data),
            "metadata": json.dumps(self.metadata)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """
        Create an event from a dictionary.
        
        Args:
            data: Dictionary representation of the event
            
        Returns:
            Event instance
            
        Raises:
            ValidationError: If the dictionary is invalid
        """
        try:
            # Parse JSON strings if needed
            event_data = data.get("data", "{}")
            if isinstance(event_data, str):
                event_data = json.loads(event_data)
            
            metadata = data.get("metadata", "{}")
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            
            # Parse timestamp
            timestamp = data.get("timestamp")
            if timestamp and isinstance(timestamp, str):
                timestamp = int(timestamp)
            
            return cls(
                event_type=data.get("event_type"),
                source=data.get("source"),
                data=event_data,
                event_id=data.get("event_id"),
                timestamp=timestamp,
                metadata=metadata
            )
        except (json.JSONDecodeError, ValueError) as e:
            raise ValidationError(f"Invalid event data: {e}", "data")
    
    @classmethod
    def from_stream_message(cls, message: Dict[bytes, bytes]) -> 'Event':
        """
        Create an event from a Redis stream message.
        
        Args:
            message: Redis stream message
            
        Returns:
            Event instance
            
        Raises:
            ValidationError: If the message is invalid
        """
        # Convert bytes to strings
        data = {}
        for k, v in message.items():
            k_str = k.decode('utf-8') if isinstance(k, bytes) else k
            v_str = v.decode('utf-8') if isinstance(v, bytes) else v
            data[k_str] = v_str
        
        return cls.from_dict(data)
    
    def get_category(self) -> str:
        """
        Get the category of the event.
        
        Returns:
            Category of the event
        """
        return self.event_type.split(':')[0]
    
    def get_name(self) -> str:
        """
        Get the name of the event.
        
        Returns:
            Name of the event
        """
        return self.event_type.split(':')[1]
    
    def __str__(self) -> str:
        """String representation of the event."""
        return (f"Event(type={self.event_type}, source={self.source}, "
                f"id={self.event_id}, timestamp={self.timestamp})")


class EventPublisher:
    """
    Publishes events to Redis streams.
    
    This class provides functionality for publishing events to Redis streams,
    with support for different event types and automatic validation.
    """
    
    def __init__(
        self,
        client,
        stream_name: str,
        max_len: int = 1000,
        source: str = "unknown"
    ):
        """
        Initialize the event publisher.
        
        Args:
            client: Redis client
            stream_name: Name of the stream to publish to
            max_len: Maximum length of the stream
            source: Default source for events
        """
        self.client = client
        self.stream_name = stream_name
        self.max_len = max_len
        self.source = source
        self.publisher = StreamPublisher(client, stream_name, max_len)
    
    def publish(self, event: Event) -> Optional[str]:
        """
        Publish an event to the stream.
        
        Args:
            event: Event to publish
            
        Returns:
            Message ID if successful, None otherwise
        """
        # Validate the event
        event.validate()
        
        # Convert the event to a dictionary
        event_dict = event.to_dict()
        
        # Publish the event
        logger.debug(f"Publishing event: {event}")
        return self.publisher.publish(event_dict)
    
    def publish_event(
        self,
        event_type: Union[str, EventType],
        data: Dict[str, Any],
        source: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Create and publish an event.
        
        Args:
            event_type: Type of the event
            data: Event-specific data
            source: Optional source (default source if not provided)
            metadata: Optional additional context
            
        Returns:
            Message ID if successful, None otherwise
        """
        # Create the event
        event = Event(
            event_type=event_type,
            source=source or self.source,
            data=data,
            metadata=metadata
        )
        
        # Publish the event
        return self.publish(event)
    
    def publish_credit_request(
        self,
        custodian: str,
        user_id: str,
        asset: str,
        c_change: str,
        ci: str,
        request_id: Optional[str] = None,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Publish a credit request event.
        
        Args:
            custodian: Custodian identifier
            user_id: User identifier
            asset: Asset identifier
            c_change: Credit change amount
            ci: Credit inventory amount
            request_id: Optional request identifier
            chain: Optional blockchain chain
            address: Optional blockchain address
            metadata: Optional additional context
            
        Returns:
            Message ID if successful, None otherwise
        """
        data = {
            "custodian": custodian,
            "uid": user_id,
            "asset": asset,
            "c_change": c_change,
            "ci": ci,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        if chain:
            data["chain"] = chain
        
        if address:
            data["address"] = address
        
        return self.publish_event(
            event_type=EventType.CREDIT_REQUEST,
            data=data,
            metadata=metadata
        )
    
    def publish_credit_response(
        self,
        custodian: str,
        user_id: str,
        asset: str,
        c_change: str,
        ci: str,
        request_id: str,
        status: str,
        reject_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Publish a credit response event.
        
        Args:
            custodian: Custodian identifier
            user_id: User identifier
            asset: Asset identifier
            c_change: Credit change amount
            ci: Credit inventory amount
            request_id: Request identifier
            status: Response status (accepted, rejected)
            reject_reason: Optional rejection reason
            metadata: Optional additional context
            
        Returns:
            Message ID if successful, None otherwise
        """
        data = {
            "custodian": custodian,
            "uid": user_id,
            "asset": asset,
            "c_change": c_change,
            "ci": ci,
            "request_id": request_id,
            "status": status
        }
        
        if reject_reason:
            data["reject_reason"] = reject_reason
        
        return self.publish_event(
            event_type=EventType.CREDIT_RESPONSE,
            data=data,
            metadata=metadata
        )
    
    def publish_settlement_report(
        self,
        report_id: str,
        custodian: str,
        timestamp: int,
        assets: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Publish a settlement report event.
        
        Args:
            report_id: Report identifier
            custodian: Custodian identifier
            timestamp: Report timestamp
            assets: Asset settlement data
            metadata: Optional additional context
            
        Returns:
            Message ID if successful, None otherwise
        """
        data = {
            "report_id": report_id,
            "custodian": custodian,
            "timestamp": str(timestamp),
            "assets": assets
        }
        
        return self.publish_event(
            event_type=EventType.SETTLEMENT_REPORT,
            data=data,
            metadata=metadata
        )
    
    def publish_system_event(
        self,
        event_name: str,
        component: str,
        details: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Publish a system event.
        
        Args:
            event_name: Name of the system event (startup, shutdown, error)
            component: Component that generated the event
            details: Event details
            metadata: Optional additional context
            
        Returns:
            Message ID if successful, None otherwise
        """
        event_type = f"system:{event_name}"
        
        data = {
            "component": component,
            "details": details
        }
        
        return self.publish_event(
            event_type=event_type,
            data=data,
            source=component,
            metadata=metadata
        )


class EventFilter:
    """
    Filters events based on various criteria.
    
    This class provides functionality for filtering events based on event type,
    source, and other attributes.
    """
    
    def __init__(
        self,
        event_types: Optional[List[Union[str, EventType]]] = None,
        sources: Optional[List[str]] = None,
        event_id_pattern: Optional[Union[str, Pattern]] = None,
        data_filter: Optional[Callable[[Dict[str, Any]], bool]] = None
    ):
        """
        Initialize the event filter.
        
        Args:
            event_types: Optional list of event types to include
            sources: Optional list of sources to include
            event_id_pattern: Optional regex pattern for event IDs
            data_filter: Optional function to filter by data
        """
        self.event_types = set()
        if event_types:
            for event_type in event_types:
                if isinstance(event_type, EventType):
                    self.event_types.add(event_type.value)
                else:
                    self.event_types.add(event_type)
        
        self.sources = set(sources) if sources else set()
        
        if event_id_pattern and isinstance(event_id_pattern, str):
            self.event_id_pattern = re.compile(event_id_pattern)
        else:
            self.event_id_pattern = event_id_pattern
        
        self.data_filter = data_filter
    
    def matches(self, event: Event) -> bool:
        """
        Check if an event matches the filter.
        
        Args:
            event: Event to check
            
        Returns:
            True if the event matches the filter, False otherwise
        """
        # Check event type
        if self.event_types and event.event_type not in self.event_types:
            return False
        
        # Check source
        if self.sources and event.source not in self.sources:
            return False
        
        # Check event ID pattern
        if self.event_id_pattern and not self.event_id_pattern.match(event.event_id):
            return False
        
        # Check data filter
        if self.data_filter and not self.data_filter(event.data):
            return False
        
        return True


class EventSubscriber:
    """
    Subscribes to events from Redis streams.
    
    This class provides functionality for subscribing to events from Redis streams,
    with support for filtering and callback handling.
    """
    
    def __init__(
        self,
        client,
        stream_name: str,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
        auto_create_group: bool = True,
        max_retry_count: int = 3,
        retry_delay_ms: int = 5000,
        block_ms: int = 2000,
        batch_size: int = 10
    ):
        """
        Initialize the event subscriber.
        
        Args:
            client: Redis client
            stream_name: Name of the stream to subscribe to
            group_name: Optional consumer group name
            consumer_name: Optional consumer name
            auto_create_group: Whether to automatically create the consumer group
            max_retry_count: Maximum number of times to retry processing an event
            retry_delay_ms: Delay in milliseconds before retrying failed events
            block_ms: Milliseconds to block waiting for new events
            batch_size: Number of events to process in each batch
        """
        self.client = client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name or f"consumer-{uuid.uuid4()}"
        self.max_retry_count = max_retry_count
        self.retry_delay_ms = retry_delay_ms
        self.block_ms = block_ms
        self.batch_size = batch_size
        
        # Create processor if using consumer groups
        self.processor = None
        if group_name:
            self.processor = StreamProcessor(
                client,
                stream_name,
                group_name,
                self.consumer_name,
                auto_create_group=auto_create_group,
                max_retry_count=max_retry_count,
                retry_delay_ms=retry_delay_ms,
                block_ms=block_ms,
                batch_size=batch_size
            )
        
        # Callbacks and filters
        self.callbacks = []  # List of (filter, callback) tuples
        self._running = False
    
    def subscribe(
        self,
        callback: Callable[[Event], bool],
        event_filter: Optional[EventFilter] = None
    ):
        """
        Subscribe to events.
        
        Args:
            callback: Function to call when an event is received
            event_filter: Optional filter for events
        """
        self.callbacks.append((event_filter, callback))
    
    def subscribe_to_event_type(
        self,
        event_type: Union[str, EventType],
        callback: Callable[[Event], bool]
    ):
        """
        Subscribe to events of a specific type.
        
        Args:
            event_type: Event type to subscribe to
            callback: Function to call when an event is received
        """
        event_filter = EventFilter(event_types=[event_type])
        self.subscribe(callback, event_filter)
    
    def subscribe_to_source(
        self,
        source: str,
        callback: Callable[[Event], bool]
    ):
        """
        Subscribe to events from a specific source.
        
        Args:
            source: Source to subscribe to
            callback: Function to call when an event is received
        """
        event_filter = EventFilter(sources=[source])
        self.subscribe(callback, event_filter)
    
    def start(self):
        """Start processing events."""
        if self._running:
            return
        
        self._running = True
        
        # Start processing in a separate thread
        thread = threading.Thread(
            target=self._process_events,
            daemon=True
        )
        thread.start()
    
    def stop(self):
        """Stop processing events."""
        self._running = False
    
    def _process_events(self):
        """Process events from the stream."""
        logger.info(f"Starting event processing for stream '{self.stream_name}'")
        
        if self.processor:
            # Process events using consumer groups
            self._process_events_with_group()
        else:
            # Process events without consumer groups
            self._process_events_without_group()
    
    def _process_events_with_group(self):
        """Process events using consumer groups."""
        def message_handler(message):
            try:
                # Convert the message to an event
                event = Event.from_stream_message(message)
                
                # Process the event
                return self._process_event(event)
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                return False
        
        # Process messages until stopped
        while self._running:
            try:
                self.processor.process_messages(message_handler, run_once=True)
                time.sleep(0.1)  # Short delay to prevent CPU spinning
            except Exception as e:
                logger.error(f"Error in event processor: {e}")
                time.sleep(1)  # Longer delay after error
    
    def _process_events_without_group(self):
        """Process events without consumer groups."""
        last_id = "0"  # Start from the beginning of the stream
        
        while self._running:
            try:
                # Read messages from the stream
                streams = {self.stream_name: last_id}
                messages = self.client.xread(
                    streams,
                    count=self.batch_size,
                    block=self.block_ms
                )
                
                if not messages:
                    continue
                
                # Process each message
                for stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        try:
                            # Convert the message to an event
                            event = Event.from_stream_message(message_data)
                            
                            # Process the event
                            self._process_event(event)
                            
                            # Update last ID
                            last_id = message_id
                        except Exception as e:
                            logger.error(f"Error processing event: {e}")
            
            except Exception as e:
                logger.error(f"Error reading from stream: {e}")
                time.sleep(1)  # Delay after error
    
    def _process_event(self, event: Event) -> bool:
        """
        Process an event.
        
        Args:
            event: Event to process
            
        Returns:
            True if the event was processed successfully, False otherwise
        """
        success = True
        
        # Check each callback
        for event_filter, callback in self.callbacks:
            try:
                # Check if the event matches the filter
                if event_filter and not event_filter.matches(event):
                    continue
                
                # Call the callback
                result = callback(event)
                
                # Update success flag
                success = success and (result is not False)
            except Exception as e:
                logger.error(f"Error in event callback: {e}")
                success = False
        
        return success


class EventRouter:
    """
    Routes events to different handlers based on rules.
    
    This class provides functionality for routing events to different handlers
    based on event type, source, and other attributes.
    """
    
    def __init__(self):
        """Initialize the event router."""
        self.routes = []  # List of (filter, handler) tuples
        self.default_handler = None
    
    def add_route(
        self,
        event_filter: EventFilter,
        handler: Callable[[Event], bool]
    ):
        """
        Add a route.
        
        Args:
            event_filter: Filter for events
            handler: Handler function
        """
        self.routes.append((event_filter, handler))
    
    def add_route_for_event_type(
        self,
        event_type: Union[str, EventType],
        handler: Callable[[Event], bool]
    ):
        """
        Add a route for a specific event type.
        
        Args:
            event_type: Event type to route
            handler: Handler function
        """
        event_filter = EventFilter(event_types=[event_type])
        self.add_route(event_filter, handler)
    
    def add_route_for_source(
        self,
        source: str,
        handler: Callable[[Event], bool]
    ):
        """
        Add a route for a specific source.
        
        Args:
            source: Source to route
            handler: Handler function
        """
        event_filter = EventFilter(sources=[source])
        self.add_route(event_filter, handler)
    
    def set_default_handler(self, handler: Callable[[Event], bool]):
        """
        Set the default handler.
        
        Args:
            handler: Default handler function
        """
        self.default_handler = handler
    
    def route(self, event: Event) -> bool:
        """
        Route an event to the appropriate handler.
        
        Args:
            event: Event to route
            
        Returns:
            True if the event was handled successfully, False otherwise
        """
        # Check each route
        for event_filter, handler in self.routes:
            if event_filter.matches(event):
                try:
                    return handler(event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")
                    return False
        
        # Use default handler if available
        if self.default_handler:
            try:
                return self.default_handler(event)
            except Exception as e:
                logger.error(f"Error in default event handler: {e}")
                return False
        
        # No handler found
        logger.warning(f"No handler found for event: {event}")
        return False


def create_event_publisher(
    client,
    stream_name: str,
    max_len: int = 1000,
    source: str = "unknown"
) -> EventPublisher:
    """
    Create an event publisher.
    
    Args:
        client: Redis client
        stream_name: Name of the stream to publish to
        max_len: Maximum length of the stream
        source: Default source for events
        
    Returns:
        Event publisher
    """
    return EventPublisher(client, stream_name, max_len, source)


def create_event_subscriber(
    client,
    stream_name: str,
    group_name: Optional[str] = None,
    consumer_name: Optional[str] = None,
    auto_create_group: bool = True
) -> EventSubscriber:
    """
    Create an event subscriber.
    
    Args:
        client: Redis client
        stream_name: Name of the stream to subscribe to
        group_name: Optional consumer group name
        consumer_name: Optional consumer name
        auto_create_group: Whether to automatically create the consumer group
        
    Returns:
        Event subscriber
    """
    return EventSubscriber(
        client,
        stream_name,
        group_name,
        consumer_name,
        auto_create_group
    )


def create_event_router() -> EventRouter:
    """
    Create an event router.
    
    Returns:
        Event router
    """
    return EventRouter()


def publish_event(
    client,
    stream_name: str,
    event_type: Union[str, EventType],
    data: Dict[str, Any],
    source: str = "unknown",
    metadata: Optional[Dict[str, Any]] = None,
    max_len: int = 1000
) -> Optional[str]:
    """
    Publish an event to a stream.
    
    Args:
        client: Redis client
        stream_name: Name of the stream to publish to
        event_type: Type of the event
        data: Event-specific data
        source: Source of the event
        metadata: Optional additional context
        max_len: Maximum length of the stream
        
    Returns:
        Message ID if successful, None otherwise
    """
    publisher = EventPublisher(client, stream_name, max_len, source)
    return publisher.publish_event(event_type, data, source, metadata)