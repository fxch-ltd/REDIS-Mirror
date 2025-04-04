"""
Redis Mirror CE SDK - Pledge and Vault Example

This example demonstrates how to use the Pledge Manager and Vault Manager
components of the Redis Mirror CE SDK for WSP integration.
"""

import time
import logging
import json
from typing import Dict, Any

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    Configuration,
    ValidationError
)
from sdk_ce.redis_mirror_wsp import (
    WSPClient,
    PledgeRequest,
    PledgeResponse,
    VaultAsset
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def pledge_example():
    """Example of pledge operations."""
    logger.info("Starting pledge example")
    
    # Create configuration
    config = Configuration({
        "wsp_redis": {
            "host": "localhost",
            "port": 6379,
            "db": 0
        },
        "replica_redis": {
            "host": "localhost",
            "port": 6380,
            "db": 0
        }
    })
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get("wsp_redis"),
        replica_config=config.get("replica_redis")
    )
    
    # Create WSP client
    wsp_client = WSPClient(
        connection_manager,
        config,
        custodian_id="ledger",
        auto_start_processors=True
    )
    
    try:
        # Create a pledge request
        user_id = "user123"
        asset = "BTC"
        amount = 1.5
        chain = "Bitcoin"
        address = "bc1q..."
        
        logger.info(f"Creating pledge request for user {user_id}, asset {asset}, amount {amount}")
        
        # Add a callback for pledge responses
        def pledge_response_callback(response: PledgeResponse):
            logger.info(f"Received pledge response: {response}")
            logger.info(f"Status: {response.status}")
            if response.status == "approved":
                logger.info(f"Vault ID: {response.vault_id}")
            elif response.status == "rejected":
                logger.info(f"Reject reason: {response.reject_reason}")
        
        wsp_client.add_pledge_response_callback(pledge_response_callback)
        
        # Create pledge request
        pledge_request = wsp_client.create_pledge(
            user_id=user_id,
            asset=asset,
            amount=amount,
            address=address,
            chain=chain
        )
        
        logger.info(f"Created pledge request: {pledge_request.request_id}")
        logger.info(f"Status: {pledge_request.status}")
        
        # Process pledge responses (in a real scenario, this would be done automatically)
        logger.info("Processing pledge responses")
        wsp_client.process_pledge_responses(run_once=True)
        
        # Get pending pledge requests
        pending_requests = wsp_client.get_pending_pledge_requests(user_id)
        logger.info(f"Pending pledge requests: {len(pending_requests)}")
        for request in pending_requests:
            logger.info(f"  {request}")
        
        # In a real scenario, we would wait for the Exchange to process the request
        # For this example, we'll simulate a response by directly updating the request
        
        # Simulate an approved response
        pledge_request.mark_approved("vault-123", int(time.time()))
        logger.info(f"Simulated approved response: {pledge_request.status}")
        
        # Get the updated pledge request
        updated_request = wsp_client.get_pledge_request(pledge_request.request_id)
        logger.info(f"Updated pledge request: {updated_request}")
        logger.info(f"Status: {updated_request.status}")
        if updated_request.is_approved():
            logger.info(f"Vault ID: {updated_request.vault_id}")
        
    finally:
        # Close the WSP client
        wsp_client.close()
    
    logger.info("Pledge example completed")


def vault_example():
    """Example of vault operations."""
    logger.info("Starting vault example")
    
    # Create configuration
    config = Configuration({
        "wsp_redis": {
            "host": "localhost",
            "port": 6379,
            "db": 0
        },
        "replica_redis": {
            "host": "localhost",
            "port": 6380,
            "db": 0
        }
    })
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get("wsp_redis"),
        replica_config=config.get("replica_redis")
    )
    
    # Create WSP client
    wsp_client = WSPClient(
        connection_manager,
        config,
        custodian_id="ledger",
        auto_start_processors=True
    )
    
    try:
        # Add assets to the vault
        user_id = "user123"
        asset = "BTC"
        chain = "Bitcoin"
        address = "bc1q..."
        
        logger.info(f"Adding assets to vault for user {user_id}")
        
        # Add locked assets
        locked_assets = []
        for i in range(3):
            amount = 0.5 * (i + 1)
            asset_obj = wsp_client.add_asset_to_vault(
                user_id=user_id,
                asset=asset,
                amount=amount,
                chain=chain,
                address=address,
                status=VaultAsset.STATUS_LOCKED
            )
            locked_assets.append(asset_obj)
            logger.info(f"Added locked asset: {asset_obj.vault_id}, amount: {asset_obj.amount}")
        
        # Get locked assets
        user_locked_assets = wsp_client.get_locked_assets(user_id, asset)
        logger.info(f"Locked assets for user {user_id}: {len(user_locked_assets)}")
        for asset_obj in user_locked_assets:
            logger.info(f"  {asset_obj}")
        
        # Get total locked amount
        total_locked = wsp_client.get_total_locked_amount(user_id, asset)
        logger.info(f"Total locked amount for user {user_id}, asset {asset}: {total_locked}")
        
        # Unlock assets using Lobster Basket Policy
        amount_to_unlock = 0.75
        logger.info(f"Unlocking {amount_to_unlock} {asset} for user {user_id}")
        
        # Create a mock credit inventory
        credit_inventory = {asset: total_locked}
        
        try:
            unlocked_assets = wsp_client.unlock_assets(
                user_id=user_id,
                asset=asset,
                amount=amount_to_unlock,
                credit_inventory=credit_inventory
            )
            
            logger.info(f"Unlocked {len(unlocked_assets)} assets")
            for asset_obj in unlocked_assets:
                logger.info(f"  {asset_obj}")
            
            # Get unlocked assets
            user_unlocked_assets = wsp_client.get_unlocked_assets(user_id, asset)
            logger.info(f"Unlocked assets for user {user_id}: {len(user_unlocked_assets)}")
            for asset_obj in user_unlocked_assets:
                logger.info(f"  {asset_obj}")
            
            # Get updated locked assets
            user_locked_assets = wsp_client.get_locked_assets(user_id, asset)
            logger.info(f"Locked assets for user {user_id}: {len(user_locked_assets)}")
            for asset_obj in user_locked_assets:
                logger.info(f"  {asset_obj}")
            
            # Get total amounts
            total_locked = wsp_client.get_total_locked_amount(user_id, asset)
            total_unlocked = wsp_client.get_total_unlocked_amount(user_id, asset)
            total_amount = wsp_client.get_total_amount(user_id, asset)
            
            logger.info(f"Total locked amount: {total_locked}")
            logger.info(f"Total unlocked amount: {total_unlocked}")
            logger.info(f"Total amount: {total_amount}")
            
        except ValidationError as e:
            logger.error(f"Failed to unlock assets: {e}")
        
        # Lock more assets
        amount_to_lock = 0.5
        logger.info(f"Locking {amount_to_lock} {asset} for user {user_id}")
        
        locked_assets = wsp_client.lock_assets(
            user_id=user_id,
            asset=asset,
            amount=amount_to_lock,
            chain=chain,
            address=address
        )
        
        logger.info(f"Locked {len(locked_assets)} assets")
        for asset_obj in locked_assets:
            logger.info(f"  {asset_obj}")
        
        # Get updated total amounts
        total_locked = wsp_client.get_total_locked_amount(user_id, asset)
        total_unlocked = wsp_client.get_total_unlocked_amount(user_id, asset)
        total_amount = wsp_client.get_total_amount(user_id, asset)
        
        logger.info(f"Updated total locked amount: {total_locked}")
        logger.info(f"Updated total unlocked amount: {total_unlocked}")
        logger.info(f"Updated total amount: {total_amount}")
        
    finally:
        # Close the WSP client
        wsp_client.close()
    
    logger.info("Vault example completed")


def integrated_example():
    """Example of integrated pledge and vault operations."""
    logger.info("Starting integrated pledge and vault example")
    
    # Create configuration
    config = Configuration({
        "wsp_redis": {
            "host": "localhost",
            "port": 6379,
            "db": 0
        },
        "replica_redis": {
            "host": "localhost",
            "port": 6380,
            "db": 0
        }
    })
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get("wsp_redis"),
        replica_config=config.get("replica_redis")
    )
    
    # Create WSP client
    wsp_client = WSPClient(
        connection_manager,
        config,
        custodian_id="ledger",
        auto_start_processors=True
    )
    
    try:
        # Create a pledge request
        user_id = "user456"
        asset = "ETH"
        amount = 2.0
        chain = "Ethereum"
        address = "0x..."
        
        logger.info(f"Creating pledge request for user {user_id}, asset {asset}, amount {amount}")
        
        # Define a callback that adds the asset to the vault when the pledge is approved
        def pledge_response_callback(response: PledgeResponse):
            logger.info(f"Received pledge response: {response}")
            
            if response.status == "approved":
                logger.info(f"Pledge approved with vault ID: {response.vault_id}")
                
                # Add the asset to the vault
                asset_obj = wsp_client.add_asset_to_vault(
                    user_id=response.user_id,
                    asset=response.asset,
                    amount=response.amount,
                    chain=chain,
                    address=address,
                    vault_id=response.vault_id,
                    status=VaultAsset.STATUS_LOCKED,
                    pledge_request_id=response.request_id
                )
                
                logger.info(f"Added asset to vault: {asset_obj.vault_id}")
            
            elif response.status == "rejected":
                logger.info(f"Pledge rejected: {response.reject_reason}")
        
        # Add the callback
        wsp_client.add_pledge_response_callback(pledge_response_callback)
        
        # Create pledge request
        pledge_request = wsp_client.create_pledge(
            user_id=user_id,
            asset=asset,
            amount=amount,
            address=address,
            chain=chain
        )
        
        logger.info(f"Created pledge request: {pledge_request.request_id}")
        
        # In a real scenario, we would wait for the Exchange to process the request
        # For this example, we'll simulate a response by directly updating the request
        
        # Simulate an approved response
        vault_id = f"vault-{int(time.time())}-{user_id}-{asset}"
        pledge_request.mark_approved(vault_id, int(time.time()))
        
        # Process the response (this will trigger our callback)
        response = PledgeResponse(
            request_id=pledge_request.request_id,
            user_id=user_id,
            asset=asset,
            amount=amount,
            status="approved",
            timestamp=int(time.time()),
            vault_id=vault_id
        )
        
        # Manually call the callback (in a real scenario, this would be done by the processor)
        pledge_response_callback(response)
        
        # Get vault assets for the user
        vault_assets = wsp_client.get_vault_assets(user_id)
        logger.info(f"Vault assets for user {user_id}: {len(vault_assets)}")
        for asset_obj in vault_assets:
            logger.info(f"  {asset_obj}")
        
        # Get total locked amount
        total_locked = wsp_client.get_total_locked_amount(user_id, asset)
        logger.info(f"Total locked amount for user {user_id}, asset {asset}: {total_locked}")
        
        # Unlock some assets
        amount_to_unlock = 0.5
        logger.info(f"Unlocking {amount_to_unlock} {asset} for user {user_id}")
        
        # Create a mock credit inventory
        credit_inventory = {asset: total_locked}
        
        try:
            unlocked_assets = wsp_client.unlock_assets(
                user_id=user_id,
                asset=asset,
                amount=amount_to_unlock,
                credit_inventory=credit_inventory
            )
            
            logger.info(f"Unlocked {len(unlocked_assets)} assets")
            
            # Get updated total amounts
            total_locked = wsp_client.get_total_locked_amount(user_id, asset)
            total_unlocked = wsp_client.get_total_unlocked_amount(user_id, asset)
            
            logger.info(f"Updated total locked amount: {total_locked}")
            logger.info(f"Updated total unlocked amount: {total_unlocked}")
            
        except ValidationError as e:
            logger.error(f"Failed to unlock assets: {e}")
        
    finally:
        # Close the WSP client
        wsp_client.close()
    
    logger.info("Integrated pledge and vault example completed")


def main():
    """Run all examples."""
    logger.info("Starting Pledge and Vault examples")
    
    # Run pledge example
    pledge_example()
    
    # Run vault example
    vault_example()
    
    # Run integrated example
    integrated_example()
    
    logger.info("All Pledge and Vault examples completed")


if __name__ == "__main__":
    main()