"""
Redis Mirror CE SDK - Stream Processing Example

This example demonstrates how to use the stream processing module for both
publishing and consuming messages using Redis Streams.
"""

import time
import logging
import json
import uuid
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager,
    StreamProcessor,
    StreamPublisher,
    create_consumer_group,
    publish_event
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def publisher_example():
    """Example of publishing messages to a stream."""
    logger.info("Starting publisher example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_redis_config()
    )
    
    try:
        # Get Redis client
        client = connection_manager.get_wsp_client()
        
        # Create stream publisher
        stream_name = "example:stream"
        publisher = StreamPublisher(client, stream_name, max_len=1000)
        
        # Publish some messages
        for i in range(5):
            message = {
                "id": str(i),
                "content": f"Test message {i}",
                "timestamp": str(int(time.time() * 1000))
            }
            
            message_id = publisher.publish(message)
            logger.info(f"Published message with ID: {message_id}")
            
            # Short delay between messages
            time.sleep(0.5)
        
        # Publish an event
        event_data = {
            "user_id": "user123",
            "action": "login",
            "ip_address": "192.168.1.1"
        }
        
        event_id = publisher.publish_event("user_login", event_data)
        logger.info(f"Published event with ID: {event_id}")
        
        # Alternative way to publish an event
        event_id = publish_event(
            client,
            stream_name,
            "user_logout",
            {
                "user_id": "user123",
                "action": "logout",
                "ip_address": "192.168.1.1"
            }
        )
        logger.info(f"Published event with ID: {event_id}")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def message_processor(message):
    """
    Process a message from the stream.
    
    Args:
        message: Dictionary containing the message fields and values
        
    Returns:
        True if the message was processed successfully, False otherwise
    """
    try:
        logger.info(f"Processing message: {json.dumps(message, indent=2)}")
        
        # Check if this is an event
        if "event_type" in message:
            event_type = message["event_type"]
            logger.info(f"Received event of type: {event_type}")
            
            # Process different event types
            if event_type == "user_login":
                logger.info(f"User {message.get('user_id')} logged in from {message.get('ip_address')}")
            elif event_type == "user_logout":
                logger.info(f"User {message.get('user_id')} logged out from {message.get('ip_address')}")
        else:
            # Regular message
            logger.info(f"Received message with content: {message.get('content')}")
        
        # Simulate processing time
        time.sleep(0.1)
        
        return True
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False


def consumer_example():
    """Example of consuming messages from a stream."""
    logger.info("Starting consumer example")
    
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
        
        # Stream and consumer group details
        stream_name = "example:stream"
        group_name = "example-group"
        consumer_name = f"consumer-{uuid.uuid4()}"
        
        # Create consumer group if it doesn't exist
        create_consumer_group(client, stream_name, group_name)
        
        # Create stream processor
        processor = StreamProcessor(
            client,
            stream_name,
            group_name,
            consumer_name,
            auto_create_group=True,
            max_retry_count=3,
            retry_delay_ms=1000,
            block_ms=2000,
            batch_size=10
        )
        
        # Process messages
        logger.info(f"Starting to process messages from stream '{stream_name}'")
        logger.info(f"Press Ctrl+C to stop processing")
        
        # Process messages once (for example purposes)
        # In a real application, you would typically set run_once=False
        processor.process_messages(message_processor, run_once=True)
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def consumer_group_example():
    """Example of using multiple consumers in a consumer group."""
    logger.info("Starting consumer group example")
    
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
        
        # Stream and consumer group details
        stream_name = "example:stream"
        group_name = "example-group"
        
        # Create consumer group if it doesn't exist
        create_consumer_group(client, stream_name, group_name)
        
        # Create multiple stream processors (simulating multiple consumers)
        processors = []
        for i in range(3):
            consumer_name = f"consumer-{i}"
            
            processor = StreamProcessor(
                client,
                stream_name,
                group_name,
                consumer_name,
                auto_create_group=False,  # Group already created
                max_retry_count=3,
                retry_delay_ms=1000,
                block_ms=2000,
                batch_size=10
            )
            
            processors.append((consumer_name, processor))
        
        # Process messages with each processor
        for consumer_name, processor in processors:
            logger.info(f"Processing messages with consumer '{consumer_name}'")
            processor.process_messages(message_processor, run_once=True)
            
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the examples."""
    # First publish some messages
    publisher_example()
    
    # Then consume the messages
    consumer_example()
    
    # Demonstrate consumer groups
    consumer_group_example()


if __name__ == "__main__":
    main()