"""
Redis Mirror CE SDK - Basic Usage Example

This example demonstrates how to use the Redis Mirror CE SDK for both WSP and Exchange integration.
"""

import logging
import json
import os
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager,
    KeyManager
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def wsp_example():
    """Example of WSP integration."""
    logger.info("Starting WSP integration example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_redis_config()
    )
    
    try:
        # Check connections
        wsp_client = connection_manager.get_wsp_client()
        replica_client = connection_manager.get_replica_client()
        
        logger.info("Checking WSP Redis connection")
        if wsp_client.ping():
            logger.info("WSP Redis connection successful")
        else:
            logger.error("WSP Redis connection failed")
            return
        
        logger.info("Checking Replica Redis connection")
        if replica_client.ping():
            logger.info("Replica Redis connection successful")
        else:
            logger.error("Replica Redis connection failed")
            return
        
        # Example: Generate keys
        user_id = "user123"
        asset = "BTC"
        
        ci_key = KeyManager.credit_inventory(user_id, asset)
        logger.info(f"Generated Credit Inventory key: {ci_key}")
        
        # Example: Request credit increase (placeholder until WSPClient is implemented)
        credit_request = {
            "custodian": "ledger",
            "request_id": "req123",
            "uid": user_id,
            "timestamp": 1723629590,
            "asset": asset,
            "c_change": "+10.0",
            "ci": "10.0",
            "chain": "Bitcoin",
            "address": "bc1q..."
        }
        
        # Publish to credit request stream
        credit_request_stream = KeyManager.credit_request_stream()
        logger.info(f"Publishing credit request to {credit_request_stream}")
        
        # Convert dict to string fields for Redis stream
        stream_data = {k: str(v) for k, v in credit_request.items()}
        message_id = wsp_client.xadd(credit_request_stream, stream_data)
        
        logger.info(f"Credit request published with ID: {message_id}")
        
        # Example: Read credit response (placeholder until WSPClient is implemented)
        credit_response_stream = KeyManager.credit_response_stream()
        logger.info(f"Reading credit responses from {credit_response_stream}")
        
        # Read the latest message from the stream
        responses = wsp_client.xread({credit_response_stream: '0'}, count=1, block=1000)
        
        if responses:
            for stream_name, messages in responses:
                for message_id, data in messages:
                    logger.info(f"Received credit response: {data}")
        else:
            logger.info("No credit responses received")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def exchange_example():
    """Example of Exchange integration."""
    logger.info("Starting Exchange integration example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_redis_config()
    )
    
    try:
        # Check connections
        wsp_client = connection_manager.get_wsp_client()
        replica_client = connection_manager.get_replica_client()
        
        logger.info("Checking WSP Redis connection")
        if wsp_client.ping():
            logger.info("WSP Redis connection successful")
        else:
            logger.error("WSP Redis connection failed")
            return
        
        logger.info("Checking Replica Redis connection")
        if replica_client.ping():
            logger.info("Replica Redis connection successful")
        else:
            logger.error("Replica Redis connection failed")
            return
        
        # Example: Read credit requests (placeholder until ExchangeClient is implemented)
        credit_request_stream = KeyManager.credit_request_stream()
        logger.info(f"Reading credit requests from {credit_request_stream}")
        
        # Read the latest message from the stream
        requests = wsp_client.xread({credit_request_stream: '0'}, count=1, block=1000)
        
        if requests:
            for stream_name, messages in requests:
                for message_id, data in messages:
                    logger.info(f"Received credit request: {data}")
                    
                    # Example: Process credit request
                    user_id = data.get(b'uid', b'').decode()
                    asset = data.get(b'asset', b'').decode()
                    c_change = data.get(b'c_change', b'').decode()
                    
                    logger.info(f"Processing credit request for user {user_id}, asset {asset}, change {c_change}")
                    
                    # Example: Update Credit Inventory
                    ci_key = KeyManager.credit_inventory(user_id, asset)
                    logger.info(f"Updating Credit Inventory key: {ci_key}")
                    
                    # Example: Send credit response
                    credit_response = {
                        "custodian": data.get(b'custodian', b'').decode(),
                        "request_id": data.get(b'request_id', b'').decode(),
                        "uid": user_id,
                        "timestamp": int(data.get(b'timestamp', b'0').decode()),
                        "asset": asset,
                        "c_change": c_change,
                        "ci": data.get(b'ci', b'').decode(),
                        "status": "accepted",
                        "reject_reason": ""
                    }
                    
                    # Publish to credit response stream
                    credit_response_stream = KeyManager.credit_response_stream()
                    logger.info(f"Publishing credit response to {credit_response_stream}")
                    
                    # Convert dict to string fields for Redis stream
                    stream_data = {k: str(v) for k, v in credit_response.items()}
                    response_id = replica_client.xadd(credit_response_stream, stream_data)
                    
                    logger.info(f"Credit response published with ID: {response_id}")
        else:
            logger.info("No credit requests received")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


if __name__ == "__main__":
    # Run examples
    wsp_example()
    exchange_example()