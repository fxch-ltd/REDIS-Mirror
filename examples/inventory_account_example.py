"""
Redis Mirror CE SDK - Credit Inventory and Account Integration Example

This example demonstrates how to use the Credit Inventory Processor and Account
Integration components of the Redis Mirror CE SDK for Exchange integration.
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
from sdk_ce.redis_mirror_exchange import (
    ExchangeClient,
    CreditInventoryEntry,
    CreditInventoryProcessor,
    AccountEntry,
    AccountIntegration
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def credit_inventory_example():
    """Example of credit inventory operations."""
    logger.info("Starting credit inventory example")
    
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
    
    # Create credit inventory processor
    ci_processor = CreditInventoryProcessor(connection_manager)
    
    try:
        # Set credit inventory for a user and asset
        user_id = "user123"
        asset = "BTC"
        amount = 10.0
        
        logger.info(f"Setting credit inventory for user {user_id}, asset {asset}, amount {amount}")
        
        ci_entry = ci_processor.set_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=amount,
            transaction_id="tx-set-1"
        )
        
        logger.info(f"Credit inventory set: {ci_entry}")
        
        # Get credit inventory
        ci_entry = ci_processor.get_credit_inventory(user_id, asset)
        logger.info(f"Retrieved credit inventory: {ci_entry}")
        
        # Increase credit inventory
        increase_amount = 5.0
        logger.info(f"Increasing credit inventory by {increase_amount}")
        
        ci_entry = ci_processor.increase_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=increase_amount,
            transaction_id="tx-increase-1"
        )
        
        logger.info(f"Credit inventory after increase: {ci_entry}")
        
        # Decrease credit inventory
        decrease_amount = 3.0
        logger.info(f"Decreasing credit inventory by {decrease_amount}")
        
        ci_entry = ci_processor.decrease_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=decrease_amount,
            transaction_id="tx-decrease-1"
        )
        
        logger.info(f"Credit inventory after decrease: {ci_entry}")
        
        # Try to decrease too much (should fail)
        try:
            too_much = 20.0
            logger.info(f"Trying to decrease credit inventory by too much: {too_much}")
            
            ci_processor.decrease_credit_inventory(
                user_id=user_id,
                asset=asset,
                amount=too_much,
                transaction_id="tx-decrease-fail"
            )
        except ValidationError as e:
            logger.info(f"Validation error as expected: {e}")
        
        # Process settlement decrease
        settlement_amount = 2.0
        settlement_id = "settlement-1"
        logger.info(f"Processing settlement decrease: {settlement_amount}")
        
        ci_entry = ci_processor.process_settlement_decrease(
            user_id=user_id,
            asset=asset,
            amount=settlement_amount,
            settlement_id=settlement_id
        )
        
        logger.info(f"Credit inventory after settlement decrease: {ci_entry}")
        
        # Verify double-entry bookkeeping
        user2_id = "user456"
        transaction_id = "tx-double-entry-1"
        double_entry_amount = 1.0
        
        logger.info(f"Setting up double-entry transaction between {user_id} and {user2_id}")
        
        # Decrease first user's credit inventory
        ci_processor.decrease_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=double_entry_amount,
            transaction_id=transaction_id
        )
        
        # Increase second user's credit inventory
        ci_processor.increase_credit_inventory(
            user_id=user2_id,
            asset=asset,
            amount=double_entry_amount,
            transaction_id=transaction_id
        )
        
        # Verify double-entry
        is_valid = ci_processor.verify_double_entry(
            transaction_id=transaction_id,
            user1_id=user_id,
            user2_id=user2_id,
            asset=asset,
            amount=double_entry_amount
        )
        
        logger.info(f"Double-entry verification result: {is_valid}")
        
        # Get all credit inventory for a user
        all_ci = ci_processor.get_all_credit_inventory(user_id)
        logger.info(f"All credit inventory for user {user_id}: {len(all_ci)} assets")
        for asset_id, entry in all_ci.items():
            logger.info(f"  {asset_id}: {entry.amount}")
        
    except Exception as e:
        logger.error(f"Error in credit inventory example: {e}")
    
    logger.info("Credit inventory example completed")


def account_integration_example():
    """Example of account integration operations."""
    logger.info("Starting account integration example")
    
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
    
    # Create account integration
    account_integration = AccountIntegration(connection_manager)
    
    try:
        # Set account for a user and asset
        user_id = "user123"
        asset = "BTC"
        balance = 10.0
        available = 8.0
        reserved = 2.0
        
        logger.info(f"Setting account for user {user_id}, asset {asset}, balance {balance}")
        
        account_entry = account_integration.set_account(
            user_id=user_id,
            asset=asset,
            balance=balance,
            available=available,
            reserved=reserved,
            transaction_id="tx-set-account-1"
        )
        
        logger.info(f"Account set: {account_entry}")
        
        # Get account
        account_entry = account_integration.get_account(user_id, asset)
        logger.info(f"Retrieved account: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Increase balance
        increase_amount = 5.0
        logger.info(f"Increasing balance by {increase_amount}")
        
        account_entry = account_integration.increase_balance(
            user_id=user_id,
            asset=asset,
            amount=increase_amount,
            increase_available=True,
            transaction_id="tx-increase-account-1"
        )
        
        logger.info(f"Account after increase: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Reserve balance
        reserve_amount = 3.0
        logger.info(f"Reserving {reserve_amount} from available balance")
        
        account_entry = account_integration.reserve_balance(
            user_id=user_id,
            asset=asset,
            amount=reserve_amount,
            transaction_id="tx-reserve-1"
        )
        
        logger.info(f"Account after reservation: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Release reservation
        release_amount = 1.0
        logger.info(f"Releasing {release_amount} from reserved balance")
        
        account_entry = account_integration.release_reservation(
            user_id=user_id,
            asset=asset,
            amount=release_amount,
            transaction_id="tx-release-1"
        )
        
        logger.info(f"Account after release: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Decrease balance
        decrease_amount = 2.0
        logger.info(f"Decreasing balance by {decrease_amount}")
        
        account_entry = account_integration.decrease_balance(
            user_id=user_id,
            asset=asset,
            amount=decrease_amount,
            decrease_available=True,
            transaction_id="tx-decrease-account-1"
        )
        
        logger.info(f"Account after decrease: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Try to reserve too much (should fail)
        try:
            too_much = 20.0
            logger.info(f"Trying to reserve too much: {too_much}")
            
            account_integration.reserve_balance(
                user_id=user_id,
                asset=asset,
                amount=too_much,
                transaction_id="tx-reserve-fail"
            )
        except ValidationError as e:
            logger.info(f"Validation error as expected: {e}")
        
        # Process settlement update (bought)
        settlement_amount = 2.0
        settlement_id = "settlement-1"
        logger.info(f"Processing settlement update (bought): {settlement_amount}")
        
        account_entry = account_integration.process_settlement_update(
            user_id=user_id,
            asset=asset,
            amount=settlement_amount,
            is_bought=True,
            settlement_id=settlement_id
        )
        
        logger.info(f"Account after settlement update (bought): {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Process settlement update (sold)
        settlement_amount = 1.0
        settlement_id = "settlement-2"
        logger.info(f"Processing settlement update (sold): {settlement_amount}")
        
        account_entry = account_integration.process_settlement_update(
            user_id=user_id,
            asset=asset,
            amount=settlement_amount,
            is_bought=False,
            settlement_id=settlement_id
        )
        
        logger.info(f"Account after settlement update (sold): {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Get all accounts for a user
        all_accounts = account_integration.get_all_accounts(user_id)
        logger.info(f"All accounts for user {user_id}: {len(all_accounts)} assets")
        for asset_id, entry in all_accounts.items():
            logger.info(f"  {asset_id}: Balance: {entry.balance}, Available: {entry.available}, Reserved: {entry.reserved}")
        
    except Exception as e:
        logger.error(f"Error in account integration example: {e}")
    
    logger.info("Account integration example completed")


def integrated_example():
    """Example of integrated credit inventory and account operations."""
    logger.info("Starting integrated credit inventory and account example")
    
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
    
    # Create credit inventory processor and account integration
    ci_processor = CreditInventoryProcessor(connection_manager)
    account_integration = AccountIntegration(connection_manager)
    
    try:
        # Set up user and asset
        user_id = "user789"
        asset = "ETH"
        initial_amount = 5.0
        
        logger.info(f"Setting up user {user_id} with {initial_amount} {asset}")
        
        # Set credit inventory
        ci_entry = ci_processor.set_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=initial_amount,
            transaction_id="tx-integrated-1"
        )
        
        logger.info(f"Credit inventory set: {ci_entry}")
        
        # Update account from credit inventory
        account_entry = account_integration.update_account_from_credit_inventory(
            user_id=user_id,
            asset=asset,
            ci_amount=ci_entry.amount,
            transaction_id="tx-integrated-2"
        )
        
        logger.info(f"Account updated from credit inventory: {account_entry}")
        logger.info(f"Balance: {account_entry.balance}, Available: {account_entry.available}, Reserved: {account_entry.reserved}")
        
        # Simulate trading activity
        # 1. Reserve balance for a trade
        trade_amount = 2.0
        logger.info(f"Reserving {trade_amount} {asset} for a trade")
        
        account_entry = account_integration.reserve_balance(
            user_id=user_id,
            asset=asset,
            amount=trade_amount,
            transaction_id="tx-trade-1"
        )
        
        logger.info(f"Account after reservation: {account_entry}")
        
        # 2. Execute trade (decrease credit inventory)
        logger.info(f"Executing trade: decreasing credit inventory by {trade_amount}")
        
        ci_entry = ci_processor.decrease_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=trade_amount,
            transaction_id="tx-trade-2"
        )
        
        logger.info(f"Credit inventory after trade: {ci_entry}")
        
        # 3. Update account (decrease balance and release reservation)
        logger.info(f"Updating account after trade")
        
        account_entry = account_integration.decrease_balance(
            user_id=user_id,
            asset=asset,
            amount=trade_amount,
            decrease_available=False,  # Already reserved
            transaction_id="tx-trade-3"
        )
        
        account_entry = account_integration.release_reservation(
            user_id=user_id,
            asset=asset,
            amount=trade_amount,
            transaction_id="tx-trade-4"
        )
        
        logger.info(f"Account after trade: {account_entry}")
        
        # Simulate settlement process
        settlement_id = "settlement-integrated-1"
        bought_asset = "BTC"
        bought_amount = 0.1
        
        logger.info(f"Processing settlement: bought {bought_amount} {bought_asset}")
        
        # 1. Update credit inventory for bought asset
        ci_entry = ci_processor.increase_credit_inventory(
            user_id=user_id,
            asset=bought_asset,
            amount=bought_amount,
            transaction_id=f"settlement:{settlement_id}:buy"
        )
        
        logger.info(f"Credit inventory for bought asset: {ci_entry}")
        
        # 2. Update account for bought asset
        account_entry = account_integration.process_settlement_update(
            user_id=user_id,
            asset=bought_asset,
            amount=bought_amount,
            is_bought=True,
            settlement_id=settlement_id
        )
        
        logger.info(f"Account for bought asset: {account_entry}")
        
        # Verify final state
        ci_eth = ci_processor.get_credit_inventory(user_id, asset)
        ci_btc = ci_processor.get_credit_inventory(user_id, bought_asset)
        
        account_eth = account_integration.get_account(user_id, asset)
        account_btc = account_integration.get_account(user_id, bought_asset)
        
        logger.info(f"Final state for {asset}:")
        logger.info(f"  Credit Inventory: {ci_eth.amount}")
        logger.info(f"  Account Balance: {account_eth.balance}")
        
        logger.info(f"Final state for {bought_asset}:")
        logger.info(f"  Credit Inventory: {ci_btc.amount}")
        logger.info(f"  Account Balance: {account_btc.balance}")
        
    except Exception as e:
        logger.error(f"Error in integrated example: {e}")
    
    logger.info("Integrated credit inventory and account example completed")


def main():
    """Run all examples."""
    logger.info("Starting Credit Inventory and Account Integration examples")
    
    # Run credit inventory example
    credit_inventory_example()
    
    # Run account integration example
    account_integration_example()
    
    # Run integrated example
    integrated_example()
    
    logger.info("All Credit Inventory and Account Integration examples completed")


if __name__ == "__main__":
    main()