"""
Redis Mirror CE SDK - Event Handling Example

This example demonstrates how to use the Event Handling module for publishing,
subscribing to, and routing events.
"""

import time
import logging
import json
import threading
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager,
    EventType,
    Event,
    EventPublisher,
    EventSubscriber,
    EventRouter,
    EventFilter,
    create_event_publisher,
    create_event_subscriber,
    create_event_router
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def event_publisher_example():
    """Example of publishing events."""
    logger.info("Starting event publisher example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Get Redis client
        client = connection_manager.get_wsp_client()
        
        # Create event publisher
        stream_name = "example:events"
        publisher = create_event_publisher(
            client,
            stream_name,
            max_len=1000,
            source="publisher-example"
        )
        
        # Publish a simple event
        event_id = publisher.publish_event(
            event_type=EventType.SYSTEM_STARTUP,
            data={
                "component": "example",
                "version": "1.0.0",
                "environment": "development"
            },
            metadata={
                "importance": "high",
                "tags": ["startup", "example"]
            }
        )
        
        logger.info(f"Published system startup event with ID: {event_id}")
        
        # Publish a credit request event
        event_id = publisher.publish_credit_request(
            custodian="ledger",
            user_id="user123",
            asset="BTC",
            c_change="+10.0",
            ci="10.0",
            chain="Bitcoin",
            address="bc1q...",
            metadata={
                "importance": "high",
                "tags": ["credit", "example"]
            }
        )
        
        logger.info(f"Published credit request event with ID: {event_id}")
        
        # Publish a credit response event
        event_id = publisher.publish_credit_response(
            custodian="ledger",
            user_id="user123",
            asset="BTC",
            c_change="+10.0",
            ci="10.0",
            request_id="req123",
            status="accepted",
            metadata={
                "importance": "high",
                "tags": ["credit", "example"]
            }
        )
        
        logger.info(f"Published credit response event with ID: {event_id}")
        
        # Publish a settlement report event
        event_id = publisher.publish_settlement_report(
            report_id="report123",
            custodian="ledger",
            timestamp=int(time.time()),
            assets={
                "BTC": {
                    "total": "100.0",
                    "settled": "95.0",
                    "pending": "5.0"
                },
                "ETH": {
                    "total": "500.0",
                    "settled": "450.0",
                    "pending": "50.0"
                }
            },
            metadata={
                "importance": "high",
                "tags": ["settlement", "example"]
            }
        )
        
        logger.info(f"Published settlement report event with ID: {event_id}")
        
        # Publish a system error event
        event_id = publisher.publish_system_event(
            event_name="error",
            component="example",
            details={
                "error": "Example error",
                "code": 500,
                "message": "This is an example error"
            },
            metadata={
                "importance": "high",
                "tags": ["error", "example"]
            }
        )
        
        logger.info(f"Published system error event with ID: {event_id}")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def event_subscriber_example():
    """Example of subscribing to events."""
    logger.info("Starting event subscriber example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Get Redis client
        client = connection_manager.get_wsp_client()
        
        # Create event subscriber
        stream_name = "example:events"
        subscriber = create_event_subscriber(
            client,
            stream_name,
            group_name="example-group",
            consumer_name="example-consumer",
            auto_create_group=True
        )
        
        # Define event handlers
        def handle_all_events(event):
            logger.info(f"Received event: {event}")
            return True
        
        def handle_credit_events(event):
            logger.info(f"Received credit event: {event}")
            logger.info(f"Credit event data: {event.data}")
            return True
        
        def handle_settlement_events(event):
            logger.info(f"Received settlement event: {event}")
            logger.info(f"Settlement event data: {event.data}")
            return True
        
        def handle_system_events(event):
            logger.info(f"Received system event: {event}")
            logger.info(f"System event data: {event.data}")
            return True
        
        # Create event filters
        all_events_filter = None  # No filter means all events
        
        credit_events_filter = EventFilter(
            event_types=[
                EventType.CREDIT_REQUEST,
                EventType.CREDIT_RESPONSE,
                EventType.CREDIT_UPDATE
            ]
        )
        
        settlement_events_filter = EventFilter(
            event_types=[
                EventType.SETTLEMENT_REPORT,
                EventType.SETTLEMENT_CONFIRMATION,
                EventType.SETTLEMENT_COMPLETION
            ]
        )
        
        system_events_filter = EventFilter(
            event_types=[
                EventType.SYSTEM_STARTUP,
                EventType.SYSTEM_SHUTDOWN,
                EventType.SYSTEM_ERROR
            ]
        )
        
        # Subscribe to events
        subscriber.subscribe(handle_all_events, all_events_filter)
        subscriber.subscribe(handle_credit_events, credit_events_filter)
        subscriber.subscribe(handle_settlement_events, settlement_events_filter)
        subscriber.subscribe(handle_system_events, system_events_filter)
        
        # Start processing events
        subscriber.start()
        
        # Wait for events to be processed
        logger.info("Waiting for events to be processed...")
        time.sleep(10)
        
    finally:
        # Stop subscriber
        subscriber.stop()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def event_router_example():
    """Example of routing events."""
    logger.info("Starting event router example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Get Redis client
        client = connection_manager.get_wsp_client()
        
        # Create event router
        router = create_event_router()
        
        # Define event handlers
        def handle_credit_request(event):
            logger.info(f"Handling credit request: {event}")
            # Process credit request
            return True
        
        def handle_credit_response(event):
            logger.info(f"Handling credit response: {event}")
            # Process credit response
            return True
        
        def handle_settlement_report(event):
            logger.info(f"Handling settlement report: {event}")
            # Process settlement report
            return True
        
        def handle_system_error(event):
            logger.info(f"Handling system error: {event}")
            # Process system error
            return True
        
        def handle_default(event):
            logger.info(f"Handling default event: {event}")
            # Process other events
            return True
        
        # Add routes
        router.add_route_for_event_type(
            EventType.CREDIT_REQUEST,
            handle_credit_request
        )
        
        router.add_route_for_event_type(
            EventType.CREDIT_RESPONSE,
            handle_credit_response
        )
        
        router.add_route_for_event_type(
            EventType.SETTLEMENT_REPORT,
            handle_settlement_report
        )
        
        router.add_route_for_event_type(
            EventType.SYSTEM_ERROR,
            handle_system_error
        )
        
        # Set default handler
        router.set_default_handler(handle_default)
        
        # Create some example events
        events = [
            Event(
                event_type=EventType.CREDIT_REQUEST,
                source="example",
                data={
                    "custodian": "ledger",
                    "uid": "user123",
                    "asset": "BTC",
                    "c_change": "+10.0",
                    "ci": "10.0",
                    "request_id": "req123",
                    "chain": "Bitcoin",
                    "address": "bc1q..."
                }
            ),
            Event(
                event_type=EventType.CREDIT_RESPONSE,
                source="example",
                data={
                    "custodian": "ledger",
                    "uid": "user123",
                    "asset": "BTC",
                    "c_change": "+10.0",
                    "ci": "10.0",
                    "request_id": "req123",
                    "status": "accepted"
                }
            ),
            Event(
                event_type=EventType.SETTLEMENT_REPORT,
                source="example",
                data={
                    "report_id": "report123",
                    "custodian": "ledger",
                    "timestamp": str(int(time.time())),
                    "assets": {
                        "BTC": {
                            "total": "100.0",
                            "settled": "95.0",
                            "pending": "5.0"
                        }
                    }
                }
            ),
            Event(
                event_type=EventType.SYSTEM_ERROR,
                source="example",
                data={
                    "component": "example",
                    "details": {
                        "error": "Example error",
                        "code": 500,
                        "message": "This is an example error"
                    }
                }
            ),
            Event(
                event_type="custom:event",
                source="example",
                data={
                    "message": "This is a custom event"
                }
            )
        ]
        
        # Route each event
        for event in events:
            logger.info(f"Routing event: {event}")
            result = router.route(event)
            logger.info(f"Routing result: {result}")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def integrated_example():
    """Example of integrated event publishing and subscribing."""
    logger.info("Starting integrated event example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Get Redis client
        client = connection_manager.get_wsp_client()
        
        # Create event publisher
        stream_name = "example:integrated"
        publisher = create_event_publisher(
            client,
            stream_name,
            max_len=1000,
            source="integrated-example"
        )
        
        # Create event subscriber
        subscriber = create_event_subscriber(
            client,
            stream_name,
            group_name="integrated-group",
            consumer_name="integrated-consumer",
            auto_create_group=True
        )
        
        # Create event router
        router = create_event_router()
        
        # Define event handlers
        def handle_credit_request(event):
            logger.info(f"Handling credit request: {event}")
            
            # Process credit request
            data = event.data
            
            # Create and publish response
            publisher.publish_credit_response(
                custodian=data.get("custodian", "unknown"),
                user_id=data.get("uid", "unknown"),
                asset=data.get("asset", "unknown"),
                c_change=data.get("c_change", "0"),
                ci=data.get("ci", "0"),
                request_id=data.get("request_id", "unknown"),
                status="accepted"
            )
            
            return True
        
        def handle_credit_response(event):
            logger.info(f"Handling credit response: {event}")
            # Process credit response
            return True
        
        def handle_default(event):
            logger.info(f"Handling default event: {event}")
            # Process other events
            return True
        
        # Add routes
        router.add_route_for_event_type(
            EventType.CREDIT_REQUEST,
            handle_credit_request
        )
        
        router.add_route_for_event_type(
            EventType.CREDIT_RESPONSE,
            handle_credit_response
        )
        
        # Set default handler
        router.set_default_handler(handle_default)
        
        # Subscribe to events and route them
        def event_handler(event):
            logger.info(f"Received event: {event}")
            return router.route(event)
        
        subscriber.subscribe(event_handler)
        
        # Start subscriber
        subscriber.start()
        
        # Publish some events
        logger.info("Publishing events...")
        
        # Publish a credit request
        publisher.publish_credit_request(
            custodian="ledger",
            user_id="user123",
            asset="BTC",
            c_change="+10.0",
            ci="10.0",
            request_id="req123",
            chain="Bitcoin",
            address="bc1q..."
        )
        
        # Wait for events to be processed
        logger.info("Waiting for events to be processed...")
        time.sleep(10)
        
    finally:
        # Stop subscriber
        subscriber.stop()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the examples."""
    # Event publisher example
    event_publisher_example()
    
    # Event subscriber example
    event_subscriber_example()
    
    # Event router example
    event_router_example()
    
    # Integrated example
    integrated_example()


if __name__ == "__main__":
    main()