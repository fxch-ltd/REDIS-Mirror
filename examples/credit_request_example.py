"""
Redis Mirror CE SDK - Credit Request Example

This example demonstrates how to use the WSP Credit Request Manager to create
and manage credit requests to the Exchange.
"""

import time
import logging
import json
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager
)
from sdk_ce.redis_mirror_wsp import (
    CreditRequest,
    CreditRequestManager
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def credit_response_callback(request):
    """
    Callback function for credit responses.
    
    Args:
        request: Updated credit request
    """
    if request.is_accepted():
        logger.info(f"Credit request accepted: {request}")
    elif request.is_rejected():
        logger.info(f"Credit request rejected: {request}, reason: {request.reject_reason}")
    elif request.is_timeout():
        logger.info(f"Credit request timed out: {request}")
    else:
        logger.info(f"Credit request status updated: {request}")


def credit_increase_example():
    """Example of requesting a credit increase."""
    logger.info("Starting credit increase example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create credit request manager
        custodian_id = "ledger"
        manager = CreditRequestManager(
            connection_manager,
            custodian_id,
            response_timeout=30,
            auto_start_processor=True
        )
        
        # Add response callback
        manager.add_response_callback(credit_response_callback)
        
        # Request credit increase
        user_id = "user123"
        asset = "BTC"
        amount = 10.0
        ci = 10.0  # Total CI after increase
        chain = "Bitcoin"
        address = "bc1q..."
        
        logger.info(f"Requesting credit increase for {user_id}, asset {asset}, amount {amount}")
        
        # Option 1: Create and send request separately
        request = manager.create_credit_request(
            user_id=user_id,
            asset=asset,
            amount=amount,
            ci=ci,
            chain=chain,
            address=address
        )
        
        # Send without waiting for response
        manager.send_credit_request(request, wait_for_response=False)
        
        # Option 2: Request credit increase directly
        user_id = "user456"
        
        try:
            # Request with waiting for response (will timeout if no response)
            request = manager.request_credit_increase(
                user_id=user_id,
                asset=asset,
                amount=amount,
                ci=ci,
                chain=chain,
                address=address,
                wait_for_response=True,
                timeout=5  # Short timeout for example
            )
            
            logger.info(f"Received response: {request.status}")
        except Exception as e:
            logger.error(f"Error requesting credit increase: {e}")
        
        # Wait a bit for async responses
        logger.info("Waiting for async responses...")
        time.sleep(5)
        
        # Check pending requests
        pending = manager.get_pending_requests()
        logger.info(f"Pending requests: {len(pending)}")
        for req in pending:
            logger.info(f"  {req}")
        
    finally:
        # Stop response processor
        manager.stop_response_processor()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def credit_decrease_example():
    """Example of requesting a credit decrease."""
    logger.info("Starting credit decrease example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create credit request manager
        custodian_id = "ledger"
        manager = CreditRequestManager(
            connection_manager,
            custodian_id,
            response_timeout=30,
            auto_start_processor=True
        )
        
        # Add response callback
        manager.add_response_callback(credit_response_callback)
        
        # Request credit decrease
        user_id = "user123"
        asset = "BTC"
        amount = -5.0  # Negative for decrease
        ci = 5.0  # Total CI after decrease
        
        logger.info(f"Requesting credit decrease for {user_id}, asset {asset}, amount {amount}")
        
        # Request credit decrease directly
        try:
            # Request without waiting for response
            manager.request_credit_decrease(
                user_id=user_id,
                asset=asset,
                amount=amount,
                ci=ci,
                wait_for_response=False
            )
            
            logger.info("Credit decrease request sent")
        except Exception as e:
            logger.error(f"Error requesting credit decrease: {e}")
        
        # Wait a bit for async responses
        logger.info("Waiting for async responses...")
        time.sleep(5)
        
        # Check pending requests
        pending = manager.get_pending_requests()
        logger.info(f"Pending requests: {len(pending)}")
        for req in pending:
            logger.info(f"  {req}")
        
    finally:
        # Stop response processor
        manager.stop_response_processor()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the examples."""
    # Credit increase example
    credit_increase_example()
    
    # Credit decrease example
    credit_decrease_example()


if __name__ == "__main__":
    main()