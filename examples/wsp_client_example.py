"""
Redis Mirror CE SDK - WSP Client Example

This example demonstrates how to use the WSP Client to interact with the Redis Mirror CE system,
including credit requests, settlement processing, and other operations.
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
    WSPClient,
    CreditRequest
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


def wsp_client_example():
    """Example of using the WSP Client."""
    logger.info("Starting WSP Client example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create WSP client
        custodian_id = "ledger"
        client = WSPClient(
            connection_manager,
            config,
            custodian_id,
            response_timeout=30,
            auto_start_processors=True
        )
        
        # Add credit response callback
        client.add_credit_response_callback(credit_response_callback)
        
        # Example 1: Request credit increase
        user_id = "user123"
        asset = "BTC"
        amount = 10.0
        chain = "Bitcoin"
        address = "bc1q..."
        
        logger.info(f"Example 1: Requesting credit increase for {user_id}, asset {asset}, amount {amount}")
        
        try:
            # Request without waiting for response
            client.request_credit_increase(
                user_id=user_id,
                asset=asset,
                amount=amount,
                chain=chain,
                address=address,
                wait_for_response=False
            )
            
            logger.info("Credit increase request sent")
        except Exception as e:
            logger.error(f"Error requesting credit increase: {e}")
        
        # Example 2: Request credit decrease
        user_id = "user456"
        asset = "ETH"
        amount = 5.0  # Will be converted to negative for decrease
        
        logger.info(f"Example 2: Requesting credit decrease for {user_id}, asset {asset}, amount {amount}")
        
        try:
            # Request with waiting for response (will timeout if no response)
            try:
                request = client.request_credit_decrease(
                    user_id=user_id,
                    asset=asset,
                    amount=amount,
                    wait_for_response=True,
                    timeout=5  # Short timeout for example
                )
                
                logger.info(f"Received response: {request.status}")
            except Exception as e:
                logger.error(f"Error waiting for response: {e}")
        except Exception as e:
            logger.error(f"Error requesting credit decrease: {e}")
        
        # Example 3: Check pending requests
        logger.info("Example 3: Checking pending credit requests")
        
        pending = client.get_pending_credit_requests()
        logger.info(f"Pending requests: {len(pending)}")
        for req in pending:
            logger.info(f"  {req}")
        
        # Example 4: Try to use unimplemented features
        logger.info("Example 4: Trying to use unimplemented features")
        
        try:
            client.process_settlement_reports()
        except NotImplementedError as e:
            logger.info(f"Expected error: {e}")
        
        try:
            client.create_pledge(
                user_id="user123",
                asset="BTC",
                amount=1.0,
                address="bc1q...",
                chain="Bitcoin"
            )
        except NotImplementedError as e:
            logger.info(f"Expected error: {e}")
        
        # Wait a bit for async responses
        logger.info("Waiting for async responses...")
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
    wsp_client_example()


if __name__ == "__main__":
    main()