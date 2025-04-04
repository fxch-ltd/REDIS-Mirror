"""
Redis Mirror CE SDK - Exchange Client Example

This example demonstrates how to use the Exchange Client to interact with the Redis Mirror CE system,
including credit management, settlement processing, and other operations.
"""

import time
import logging
import json
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager,
    StreamPublisher
)
from sdk_ce.redis_mirror_exchange import (
    ExchangeClient,
    CreditInventory
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def credit_request_callback(request, response):
    """
    Callback function for credit requests.
    
    Args:
        request: Credit request dictionary
        response: Credit response dictionary
    """
    status = response.get("status", "unknown")
    if status == "accepted":
        logger.info(f"Credit request accepted: {request}")
    elif status == "rejected":
        logger.info(f"Credit request rejected: {request}, reason: {response.get('reject_reason', '')}")
    else:
        logger.info(f"Credit request status: {status}")


def exchange_client_example():
    """Example of using the Exchange Client."""
    logger.info("Starting Exchange Client example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create Exchange client
        client = ExchangeClient(
            connection_manager,
            config,
            approved_custodians=["ledger", "exchange"],
            approved_assets=["BTC", "ETH", "USDT"],
            auto_start_processors=True
        )
        
        # Add credit request callback
        client.add_credit_request_handler(credit_request_callback)
        
        # Example 1: Manually process a credit request
        logger.info("Example 1: Manually processing a credit request")
        
        credit_request = {
            "custodian": "ledger",
            "request_id": "req-manual-123",
            "uid": "user789",
            "timestamp": str(int(time.time())),
            "asset": "BTC",
            "c_change": "+15.0",
            "ci": "15.0",
            "chain": "Bitcoin",
            "address": "bc1q..."
        }
        
        response = client.process_credit_request(credit_request)
        logger.info(f"Manual processing response: {response}")
        
        # Example 2: Check and update credit inventory
        logger.info("Example 2: Checking and updating credit inventory")
        
        # Get inventory (should exist after processing the request)
        inventory = client.get_credit_inventory("user789", "BTC")
        if inventory:
            logger.info(f"Credit inventory for user789, BTC: {inventory.amount}")
        else:
            logger.info("No credit inventory found for user789, BTC")
            
            # Create a new inventory if it doesn't exist
            client.update_credit_inventory("user789", "BTC", 15.0, is_increase=True)
            inventory = client.get_credit_inventory("user789", "BTC")
            logger.info(f"Created credit inventory for user789, BTC: {inventory.amount}")
        
        # Update inventory
        updated_inventory = client.update_credit_inventory("user789", "BTC", 5.0, is_increase=False)
        logger.info(f"Updated credit inventory: {updated_inventory.amount}")
        
        # Example 3: Validate a credit request without processing it
        logger.info("Example 3: Validating a credit request")
        
        is_valid, reason = client.validate_credit_request(
            custodian="ledger",
            user_id="user789",
            asset="ETH",  # Different asset
            amount=10.0,
            ci=10.0,
            chain="Ethereum",
            address="0x..."
        )
        
        logger.info(f"Validation result: {is_valid}, reason: {reason}")
        
        # Example 4: Try to use unimplemented features
        logger.info("Example 4: Trying to use unimplemented features")
        
        try:
            client.generate_settlement_reports()
        except NotImplementedError as e:
            logger.info(f"Expected error: {e}")
        
        try:
            client.get_account_balance("user789", "BTC")
        except NotImplementedError as e:
            logger.info(f"Expected error: {e}")
        
        # Example 5: Simulate incoming credit requests
        logger.info("Example 5: Simulating incoming credit requests")
        
        # Publish some credit requests to the stream
        # In a real scenario, these would come from WSPs
        wsp_client = connection_manager.get_wsp_client()
        request_stream = client.credit_manager.request_stream
        publisher = StreamPublisher(wsp_client, request_stream)
        
        # Valid credit increase request
        increase_request = {
            "custodian": "ledger",
            "request_id": "req-stream-123",
            "uid": "user456",
            "timestamp": str(int(time.time())),
            "asset": "ETH",
            "c_change": "+20.0",
            "ci": "20.0",
            "chain": "Ethereum",
            "address": "0x..."
        }
        
        logger.info(f"Publishing credit increase request: {increase_request}")
        publisher.publish(increase_request)
        
        # Invalid credit request (unsupported asset)
        invalid_request = {
            "custodian": "ledger",
            "request_id": "req-stream-124",
            "uid": "user456",
            "timestamp": str(int(time.time())),
            "asset": "XRP",  # Not in approved_assets
            "c_change": "+10.0",
            "ci": "10.0",
            "chain": "Ripple",
            "address": "r..."
        }
        
        logger.info(f"Publishing invalid credit request: {invalid_request}")
        publisher.publish(invalid_request)
        
        # Wait for requests to be processed
        logger.info("Waiting for requests to be processed...")
        time.sleep(5)
        
    finally:
        # Close the client
        if 'client' in locals():
            client.close()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the example."""
    exchange_client_example()


if __name__ == "__main__":
    main()