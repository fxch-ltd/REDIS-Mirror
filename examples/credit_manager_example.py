"""
Redis Mirror CE SDK - Credit Manager Example

This example demonstrates how to use the Exchange Credit Manager to process
credit requests from WSPs, validate them, update credit inventories, and
generate responses.
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
    CreditInventory,
    CreditValidator,
    CreditInventoryManager,
    CreditManager
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


def credit_manager_example():
    """Example of using the Exchange Credit Manager."""
    logger.info("Starting Exchange Credit Manager example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create credit inventory manager
        inventory_manager = CreditInventoryManager(connection_manager.get_wsp_client())
        
        # Create credit validator
        validator = CreditValidator(
            approved_custodians=["ledger", "exchange"],
            approved_assets=["BTC", "ETH", "USDT"]
        )
        
        # Create credit manager
        manager = CreditManager(
            connection_manager,
            inventory_manager=inventory_manager,
            validator=validator,
            auto_start_processor=True
        )
        
        # Add request callback
        manager.add_request_callback(credit_request_callback)
        
        # Manually publish some credit requests for testing
        # In a real scenario, these would come from WSPs
        wsp_client = connection_manager.get_wsp_client()
        request_stream = manager.request_stream
        publisher = StreamPublisher(wsp_client, request_stream)
        
        # Example 1: Valid credit increase request
        increase_request = {
            "custodian": "ledger",
            "request_id": "req-123",
            "uid": "user123",
            "timestamp": str(int(time.time())),
            "asset": "BTC",
            "c_change": "+10.0",
            "ci": "10.0",
            "chain": "Bitcoin",
            "address": "bc1q..."
        }
        
        logger.info(f"Publishing credit increase request: {increase_request}")
        publisher.publish(increase_request)
        
        # Example 2: Valid credit decrease request
        # Wait a bit to ensure the first request is processed
        time.sleep(2)
        
        decrease_request = {
            "custodian": "ledger",
            "request_id": "req-124",
            "uid": "user123",
            "timestamp": str(int(time.time())),
            "asset": "BTC",
            "c_change": "-5.0",
            "ci": "5.0"
        }
        
        logger.info(f"Publishing credit decrease request: {decrease_request}")
        publisher.publish(decrease_request)
        
        # Example 3: Invalid credit request (unsupported asset)
        # Wait a bit to ensure the second request is processed
        time.sleep(2)
        
        invalid_request = {
            "custodian": "ledger",
            "request_id": "req-125",
            "uid": "user123",
            "timestamp": str(int(time.time())),
            "asset": "XRP",  # Not in approved_assets
            "c_change": "+10.0",
            "ci": "10.0",
            "chain": "Ripple",
            "address": "r..."
        }
        
        logger.info(f"Publishing invalid credit request: {invalid_request}")
        publisher.publish(invalid_request)
        
        # Wait for all requests to be processed
        logger.info("Waiting for requests to be processed...")
        time.sleep(5)
        
        # Check credit inventory
        inventory = inventory_manager.get_inventory("user123", "BTC")
        if inventory:
            logger.info(f"Credit inventory for user123, BTC: {inventory.amount}")
        else:
            logger.info("No credit inventory found for user123, BTC")
        
    finally:
        # Stop request processor
        manager.stop_request_processor()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def manual_credit_management_example():
    """Example of manually managing credit inventories."""
    logger.info("Starting manual credit management example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create credit inventory manager
        inventory_manager = CreditInventoryManager(connection_manager.get_wsp_client())
        
        # Create credit validator
        validator = CreditValidator(
            approved_custodians=["ledger", "exchange"],
            approved_assets=["BTC", "ETH", "USDT"]
        )
        
        # Manually create and update credit inventories
        user_id = "user456"
        asset = "ETH"
        
        # Create a new inventory
        inventory = CreditInventory(user_id, asset, 0.0)
        inventory_manager.update_inventory(inventory)
        logger.info(f"Created inventory: {inventory}")
        
        # Increase inventory
        inventory = inventory_manager.increase_inventory(user_id, asset, 20.0)
        logger.info(f"Increased inventory: {inventory}")
        
        # Validate a credit decrease
        is_valid, reason = validator.validate_decrease(
            custodian="ledger",
            user_id=user_id,
            asset=asset,
            amount=-10.0,
            ci=10.0,
            inventory_manager=inventory_manager
        )
        
        logger.info(f"Validation result: {is_valid}, reason: {reason}")
        
        if is_valid:
            # Decrease inventory
            inventory = inventory_manager.decrease_inventory(user_id, asset, 10.0)
            logger.info(f"Decreased inventory: {inventory}")
        
        # Try to decrease more than available
        is_valid, reason = validator.validate_decrease(
            custodian="ledger",
            user_id=user_id,
            asset=asset,
            amount=-20.0,  # More than available
            ci=0.0,
            inventory_manager=inventory_manager
        )
        
        logger.info(f"Validation result: {is_valid}, reason: {reason}")
        
    finally:
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the examples."""
    # Exchange Credit Manager example
    credit_manager_example()
    
    # Manual credit management example
    manual_credit_management_example()


if __name__ == "__main__":
    main()