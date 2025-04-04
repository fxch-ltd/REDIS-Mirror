"""
Redis Mirror CE SDK - Mock Redis Example

This example demonstrates how to use the Mock Redis Client for testing
Redis Mirror CE SDK components without requiring an actual Redis server.
"""

import time
import logging
from sdk_ce.redis_mirror_testing import MockRedisClient
from sdk_ce.redis_mirror_core import (
    KeyManager,
    StreamProcessor,
    StreamPublisher
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def basic_operations_example():
    """Example of basic Redis operations with the mock client."""
    logger.info("Starting basic operations example")
    
    # Create a mock Redis client
    client = MockRedisClient()
    
    # Key-Value operations
    logger.info("Testing key-value operations")
    
    # Set a key
    client.set("test:key", "test-value")
    
    # Get a key
    value = client.get("test:key")
    logger.info(f"Retrieved value: {value}")
    
    # Check if a key exists
    exists = client.exists("test:key")
    logger.info(f"Key exists: {exists}")
    
    # Delete a key
    deleted = client.delete("test:key")
    logger.info(f"Deleted {deleted} key(s)")
    
    # Hash operations
    logger.info("Testing hash operations")
    
    # Set hash fields
    client.hset("test:hash", "field1", "value1")
    client.hset("test:hash", "field2", "value2")
    
    # Get hash fields
    field1 = client.hget("test:hash", "field1")
    logger.info(f"Retrieved hash field: {field1}")
    
    # Get all hash fields
    hash_data = client.hgetall("test:hash")
    logger.info(f"Retrieved hash data: {hash_data}")
    
    # Increment hash field
    incremented = client.hincrby("test:hash", "counter", 5)
    logger.info(f"Incremented counter to: {incremented}")
    
    # Delete hash fields
    deleted = client.hdel("test:hash", "field1")
    logger.info(f"Deleted {deleted} hash field(s)")
    
    logger.info("Basic operations example completed")


def stream_operations_example():
    """Example of stream operations with the mock client."""
    logger.info("Starting stream operations example")
    
    # Create a mock Redis client
    client = MockRedisClient()
    
    # Create a stream key
    stream_key = KeyManager.credit_request_stream()
    
    # Create a stream publisher
    publisher = StreamPublisher(client, stream_key)
    
    # Publish messages to the stream
    for i in range(5):
        message = {
            "type": "credit_request",
            "request_id": f"req-{i}",
            "user_id": "user123",
            "asset": "BTC",
            "amount": str(i + 1),
            "timestamp": str(int(time.time()))
        }
        
        message_id = publisher.publish(message)
        logger.info(f"Published message with ID: {message_id}")
    
    # Create a consumer group
    group_name = "test-group"
    consumer_name = "test-consumer"
    
    client.xgroup_create(stream_key, group_name, id="0", mkstream=True)
    logger.info(f"Created consumer group: {group_name}")
    
    # Create a stream processor
    processor = StreamProcessor(
        client,
        stream_key,
        group_name,
        consumer_name=consumer_name,
        auto_create_group=False  # Group already created
    )
    
    # Process messages
    def message_handler(message):
        logger.info(f"Processing message: {message}")
        return True  # Acknowledge the message
    
    # Process a batch of messages
    processor._process_batch(message_handler, ">")
    logger.info("Processed a batch of messages")
    
    # Check pending messages
    pending = client.xpending(stream_key, group_name)
    logger.info(f"Pending messages: {len(pending)}")
    
    logger.info("Stream operations example completed")


def consumer_group_example():
    """Example of consumer group operations with the mock client."""
    logger.info("Starting consumer group example")
    
    # Create a mock Redis client
    client = MockRedisClient()
    
    # Create a stream key
    stream_key = KeyManager.settlement_report_stream()
    
    # Create a stream publisher
    publisher = StreamPublisher(client, stream_key)
    
    # Publish messages to the stream
    for i in range(3):
        message = {
            "type": "settlement",
            "report_id": f"report-{i}",
            "user_id": "user456",
            "timestamp": str(int(time.time())),
            "data": f"{{\"assets\": {{\"bought\": [{i}], \"sold\": [{i+1}]}}}}"
        }
        
        message_id = publisher.publish(message)
        logger.info(f"Published settlement report with ID: {message_id}")
    
    # Create multiple consumer groups
    group1 = "wsp1-group"
    group2 = "wsp2-group"
    
    client.xgroup_create(stream_key, group1, id="0", mkstream=True)
    client.xgroup_create(stream_key, group2, id="0", mkstream=True)
    logger.info(f"Created consumer groups: {group1}, {group2}")
    
    # Read from the first consumer group
    consumer1 = "consumer1"
    result1 = client.xreadgroup(
        group1,
        consumer1,
        {stream_key: ">"},
        count=2  # Only read 2 messages
    )
    
    if result1:
        logger.info(f"Consumer1 read {len(result1[stream_key])} messages")
        for msg_id, msg_data in result1[stream_key]:
            logger.info(f"  Message ID: {msg_id}, Data: {msg_data}")
    
    # Read from the second consumer group
    consumer2 = "consumer2"
    result2 = client.xreadgroup(
        group2,
        consumer2,
        {stream_key: ">"},
        count=None  # Read all available messages
    )
    
    if result2:
        logger.info(f"Consumer2 read {len(result2[stream_key])} messages")
        for msg_id, msg_data in result2[stream_key]:
            logger.info(f"  Message ID: {msg_id}, Data: {msg_data}")
    
    # Acknowledge messages from the first group
    if result1 and stream_key in result1:
        msg_ids = [msg_id for msg_id, _ in result1[stream_key]]
        ack_count = client.xack(stream_key, group1, *msg_ids)
        logger.info(f"Acknowledged {ack_count} messages in group1")
    
    # Check pending messages in both groups
    pending1 = client.xpending(stream_key, group1)
    pending2 = client.xpending(stream_key, group2)
    logger.info(f"Pending messages in group1: {len(pending1)}")
    logger.info(f"Pending messages in group2: {len(pending2)}")
    
    logger.info("Consumer group example completed")


def pubsub_example():
    """Example of PubSub operations with the mock client."""
    logger.info("Starting PubSub example")
    
    # Create a mock Redis client
    client = MockRedisClient()
    
    # Create a channel
    channel = "notifications"
    
    # Create a message handler
    messages_received = []
    
    def message_handler(message):
        logger.info(f"Received message: {message}")
        messages_received.append(message)
    
    # Subscribe to the channel
    client.subscribe(channel, message_handler)
    logger.info(f"Subscribed to channel: {channel}")
    
    # Publish messages to the channel
    for i in range(3):
        message = f"Notification {i+1}"
        recipients = client.publish(channel, message)
        logger.info(f"Published message to {recipients} recipient(s)")
    
    # Check received messages
    logger.info(f"Received {len(messages_received)} messages")
    for i, message in enumerate(messages_received):
        logger.info(f"  Message {i+1}: {message}")
    
    # Unsubscribe from the channel
    client.unsubscribe(channel, message_handler)
    logger.info(f"Unsubscribed from channel: {channel}")
    
    # Publish another message (should not be received)
    recipients = client.publish(channel, "This message should not be received")
    logger.info(f"Published message to {recipients} recipient(s)")
    
    logger.info("PubSub example completed")


def integration_example():
    """Example of using the mock client for integration testing."""
    logger.info("Starting integration example")
    
    # Create a mock Redis client
    client = MockRedisClient()
    
    # Create stream keys
    request_stream = KeyManager.credit_request_stream()
    response_stream = KeyManager.credit_response_stream()
    
    # Create a request publisher
    request_publisher = StreamPublisher(client, request_stream)
    
    # Create a response publisher (simulating the Exchange)
    response_publisher = StreamPublisher(client, response_stream)
    
    # Create consumer groups
    exchange_group = "exchange-group"
    wsp_group = "wsp-group"
    
    client.xgroup_create(request_stream, exchange_group, id="0", mkstream=True)
    client.xgroup_create(response_stream, wsp_group, id="0", mkstream=True)
    
    # Simulate a WSP sending a credit request
    request_id = "req-123"
    request = {
        "type": "credit_request",
        "request_id": request_id,
        "user_id": "user789",
        "asset": "ETH",
        "amount": "10.0",
        "custodian": "ledger",
        "timestamp": str(int(time.time()))
    }
    
    request_msg_id = request_publisher.publish(request)
    logger.info(f"WSP published credit request: {request_id} (Message ID: {request_msg_id})")
    
    # Simulate the Exchange reading the request
    exchange_consumer = "exchange-consumer"
    result = client.xreadgroup(
        exchange_group,
        exchange_consumer,
        {request_stream: ">"}
    )
    
    if result and request_stream in result:
        logger.info("Exchange received credit request")
        
        # Process the first request
        msg_id, msg_data = result[request_stream][0]
        
        # Acknowledge the request
        client.xack(request_stream, exchange_group, msg_id)
        
        # Simulate the Exchange processing the request
        logger.info(f"Exchange processing request: {msg_data}")
        
        # Simulate the Exchange sending a response
        response = {
            "type": "credit_response",
            "request_id": msg_data["request_id"],
            "user_id": msg_data["user_id"],
            "asset": msg_data["asset"],
            "amount": msg_data["amount"],
            "status": "approved",
            "timestamp": str(int(time.time()))
        }
        
        response_msg_id = response_publisher.publish(response)
        logger.info(f"Exchange published credit response (Message ID: {response_msg_id})")
    
    # Simulate the WSP reading the response
    wsp_consumer = "wsp-consumer"
    result = client.xreadgroup(
        wsp_group,
        wsp_consumer,
        {response_stream: ">"}
    )
    
    if result and response_stream in result:
        logger.info("WSP received credit response")
        
        # Process the first response
        msg_id, msg_data = result[response_stream][0]
        
        # Acknowledge the response
        client.xack(response_stream, wsp_group, msg_id)
        
        # Simulate the WSP processing the response
        logger.info(f"WSP processing response: {msg_data}")
        
        # Verify the response matches the request
        if msg_data["request_id"] == request_id:
            logger.info("Response matches the original request")
        else:
            logger.error("Response does not match the original request")
    
    logger.info("Integration example completed")


def main():
    """Run all examples."""
    logger.info("Starting Mock Redis examples")
    
    # Run basic operations example
    basic_operations_example()
    
    # Run stream operations example
    stream_operations_example()
    
    # Run consumer group example
    consumer_group_example()
    
    # Run PubSub example
    pubsub_example()
    
    # Run integration example
    integration_example()
    
    logger.info("All Mock Redis examples completed")


if __name__ == "__main__":
    main()