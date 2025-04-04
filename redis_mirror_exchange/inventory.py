"""
Redis Mirror CE Exchange Credit Inventory Processor

This module provides functionality for managing credit inventory in the Exchange,
including updating credit inventory during trading activities, handling automatic
credit inventory decreases during settlement, and maintaining credit inventory state.
"""

import time
import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union, Tuple, Set

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    KeyManager,
    RedisMirrorError,
    ValidationError,
    TimeoutError
)

# Configure logging
logger = logging.getLogger(__name__)


class CreditInventoryEntry:
    """
    Represents a credit inventory entry for a user and asset.
    
    This class encapsulates the data and state of a credit inventory entry.
    """
    
    def __init__(
        self,
        user_id: str,
        asset: str,
        amount: float,
        timestamp: Optional[int] = None,
        last_updated: Optional[int] = None,
        transaction_id: Optional[str] = None
    ):
        """
        Initialize a credit inventory entry.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Credit inventory amount
            timestamp: Creation timestamp
            last_updated: Last update timestamp
            transaction_id: Last transaction ID
        """
        self.user_id = user_id
        self.asset = asset
        self.amount = amount
        self.timestamp = timestamp or int(time.time())
        self.last_updated = last_updated or self.timestamp
        self.transaction_id = transaction_id
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the credit inventory entry to a dictionary.
        
        Returns:
            Dictionary representation of the credit inventory entry
        """
        return {
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "last_updated": self.last_updated,
            "transaction_id": self.transaction_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CreditInventoryEntry':
        """
        Create a credit inventory entry from a dictionary.
        
        Args:
            data: Dictionary representation of the credit inventory entry
            
        Returns:
            Credit inventory entry instance
        """
        return cls(
            user_id=data.get("user_id"),
            asset=data.get("asset"),
            amount=float(data.get("amount", 0)),
            timestamp=int(data.get("timestamp", time.time())),
            last_updated=int(data.get("last_updated", time.time())),
            transaction_id=data.get("transaction_id")
        )
    
    def update(self, amount: float, transaction_id: Optional[str] = None) -> None:
        """
        Update the credit inventory amount.
        
        Args:
            amount: New credit inventory amount
            transaction_id: Transaction ID for the update
        """
        self.amount = amount
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def increase(self, amount: float, transaction_id: Optional[str] = None) -> None:
        """
        Increase the credit inventory amount.
        
        Args:
            amount: Amount to increase by
            transaction_id: Transaction ID for the increase
        """
        self.amount += amount
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def decrease(self, amount: float, transaction_id: Optional[str] = None) -> None:
        """
        Decrease the credit inventory amount.
        
        Args:
            amount: Amount to decrease by
            transaction_id: Transaction ID for the decrease
        """
        self.amount -= amount
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def __str__(self) -> str:
        """String representation of the credit inventory entry."""
        return (f"CreditInventoryEntry(user={self.user_id}, asset={self.asset}, "
                f"amount={self.amount})")


class CreditInventoryProcessor:
    """
    Manages credit inventory operations for the Exchange.
    
    This class handles updating credit inventory during trading activities,
    handling automatic credit inventory decreases during settlement, and
    maintaining credit inventory state.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager
    ):
        """
        Initialize the credit inventory processor.
        
        Args:
            connection_manager: Redis connection manager
        """
        self.connection_manager = connection_manager
        
        # Get Redis clients
        self.replica_client = connection_manager.get_replica_client()
        
        # Credit inventory cache (user_id -> {asset -> CreditInventoryEntry})
        self.credit_inventory = {}
        
        # Transaction log (transaction_id -> {user_id, asset, amount, type, timestamp})
        self.transaction_log = {}
        
        logger.info("Credit Inventory Processor initialized")
    
    def get_credit_inventory(
        self,
        user_id: str,
        asset: str,
        refresh: bool = False
    ) -> Optional[CreditInventoryEntry]:
        """
        Get the credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            refresh: Whether to refresh from Redis
            
        Returns:
            Credit inventory entry if found, None otherwise
        """
        # Check cache first
        if not refresh and user_id in self.credit_inventory and asset in self.credit_inventory[user_id]:
            return self.credit_inventory[user_id][asset]
        
        # Get from Redis
        ci_key = KeyManager.credit_inventory(user_id, asset)
        ci_data = self.replica_client.hgetall(ci_key)
        
        if not ci_data:
            return None
        
        # Create credit inventory entry
        ci_entry = CreditInventoryEntry.from_dict({
            "user_id": user_id,
            "asset": asset,
            **ci_data
        })
        
        # Update cache
        if user_id not in self.credit_inventory:
            self.credit_inventory[user_id] = {}
        
        self.credit_inventory[user_id][asset] = ci_entry
        
        return ci_entry
    
    def set_credit_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> CreditInventoryEntry:
        """
        Set the credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Credit inventory amount
            transaction_id: Transaction ID for the update
            
        Returns:
            Updated credit inventory entry
        """
        logger.info(f"Setting credit inventory for user {user_id}, asset {asset}, amount {amount}")
        
        # Get existing credit inventory entry or create a new one
        ci_entry = self.get_credit_inventory(user_id, asset)
        
        if ci_entry:
            # Update existing entry
            ci_entry.update(amount, transaction_id)
        else:
            # Create new entry
            ci_entry = CreditInventoryEntry(
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_id=transaction_id
            )
            
            # Update cache
            if user_id not in self.credit_inventory:
                self.credit_inventory[user_id] = {}
            
            self.credit_inventory[user_id][asset] = ci_entry
        
        # Update Redis
        ci_key = KeyManager.credit_inventory(user_id, asset)
        self.replica_client.hset(ci_key, mapping=ci_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="set"
            )
        
        return ci_entry
    
    def increase_credit_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> CreditInventoryEntry:
        """
        Increase the credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to increase by
            transaction_id: Transaction ID for the increase
            
        Returns:
            Updated credit inventory entry
            
        Raises:
            ValidationError: If the amount is negative
        """
        logger.info(f"Increasing credit inventory for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Increase amount must be positive: {amount}")
        
        # Get existing credit inventory entry or create a new one
        ci_entry = self.get_credit_inventory(user_id, asset)
        
        if ci_entry:
            # Increase existing entry
            ci_entry.increase(amount, transaction_id)
        else:
            # Create new entry
            ci_entry = CreditInventoryEntry(
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_id=transaction_id
            )
            
            # Update cache
            if user_id not in self.credit_inventory:
                self.credit_inventory[user_id] = {}
            
            self.credit_inventory[user_id][asset] = ci_entry
        
        # Update Redis
        ci_key = KeyManager.credit_inventory(user_id, asset)
        self.replica_client.hset(ci_key, mapping=ci_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="increase"
            )
        
        return ci_entry
    
    def decrease_credit_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> CreditInventoryEntry:
        """
        Decrease the credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to decrease by
            transaction_id: Transaction ID for the decrease
            
        Returns:
            Updated credit inventory entry
            
        Raises:
            ValidationError: If the amount is negative or greater than the current amount
        """
        logger.info(f"Decreasing credit inventory for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Decrease amount must be positive: {amount}")
        
        # Get existing credit inventory entry
        ci_entry = self.get_credit_inventory(user_id, asset)
        
        if not ci_entry:
            raise ValidationError(f"No credit inventory found for user {user_id}, asset {asset}")
        
        # Check if there's enough credit inventory
        if ci_entry.amount < amount:
            raise ValidationError(
                f"Insufficient credit inventory: {ci_entry.amount} < {amount}"
            )
        
        # Decrease existing entry
        ci_entry.decrease(amount, transaction_id)
        
        # Update Redis
        ci_key = KeyManager.credit_inventory(user_id, asset)
        self.replica_client.hset(ci_key, mapping=ci_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="decrease"
            )
        
        return ci_entry
    
    def get_all_credit_inventory(self, user_id: str) -> Dict[str, CreditInventoryEntry]:
        """
        Get all credit inventory for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary mapping asset identifiers to credit inventory entries
        """
        # Get from Redis
        ci_keys = self.replica_client.keys(KeyManager.credit_inventory(user_id, "*"))
        
        result = {}
        
        for ci_key in ci_keys:
            # Extract asset from key
            asset = ci_key.split(":")[-1]
            
            # Get credit inventory entry
            ci_entry = self.get_credit_inventory(user_id, asset)
            
            if ci_entry:
                result[asset] = ci_entry
        
        return result
    
    def get_credit_inventory_for_assets(
        self,
        user_id: str,
        assets: List[str]
    ) -> Dict[str, CreditInventoryEntry]:
        """
        Get credit inventory for multiple assets.
        
        Args:
            user_id: User identifier
            assets: List of asset identifiers
            
        Returns:
            Dictionary mapping asset identifiers to credit inventory entries
        """
        result = {}
        
        for asset in assets:
            ci_entry = self.get_credit_inventory(user_id, asset)
            
            if ci_entry:
                result[asset] = ci_entry
        
        return result
    
    def process_settlement_decrease(
        self,
        user_id: str,
        asset: str,
        amount: float,
        settlement_id: str
    ) -> CreditInventoryEntry:
        """
        Process a settlement decrease for a user and asset.
        
        This method is used during the settlement process to decrease the credit
        inventory for assets that were sold.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to decrease by
            settlement_id: Settlement ID
            
        Returns:
            Updated credit inventory entry
        """
        logger.info(
            f"Processing settlement decrease for user {user_id}, "
            f"asset {asset}, amount {amount}, settlement {settlement_id}"
        )
        
        # Create transaction ID
        transaction_id = f"settlement:{settlement_id}:{user_id}:{asset}"
        
        # Decrease credit inventory
        return self.decrease_credit_inventory(
            user_id=user_id,
            asset=asset,
            amount=amount,
            transaction_id=transaction_id
        )
    
    def verify_double_entry(
        self,
        transaction_id: str,
        user1_id: str,
        user2_id: str,
        asset: str,
        amount: float
    ) -> bool:
        """
        Verify double-entry bookkeeping for a transaction.
        
        This method checks that a transaction follows double-entry bookkeeping
        principles, where one user's credit inventory is increased and another
        user's credit inventory is decreased by the same amount.
        
        Args:
            transaction_id: Transaction ID
            user1_id: First user identifier
            user2_id: Second user identifier
            asset: Asset identifier
            amount: Transaction amount
            
        Returns:
            True if the transaction follows double-entry bookkeeping, False otherwise
        """
        logger.info(
            f"Verifying double-entry for transaction {transaction_id}, "
            f"users {user1_id}/{user2_id}, asset {asset}, amount {amount}"
        )
        
        # Get credit inventory entries
        ci_entry1 = self.get_credit_inventory(user1_id, asset, refresh=True)
        ci_entry2 = self.get_credit_inventory(user2_id, asset, refresh=True)
        
        if not ci_entry1 or not ci_entry2:
            logger.warning(f"Missing credit inventory entries for transaction {transaction_id}")
            return False
        
        # Check transaction IDs
        if ci_entry1.transaction_id != transaction_id or ci_entry2.transaction_id != transaction_id:
            logger.warning(f"Transaction IDs don't match for transaction {transaction_id}")
            return False
        
        # Check transaction log
        if transaction_id not in self.transaction_log:
            logger.warning(f"Transaction {transaction_id} not found in transaction log")
            return False
        
        # Get transaction logs
        transaction_logs = [
            log for log in self.transaction_log.values()
            if log["transaction_id"] == transaction_id
        ]
        
        if len(transaction_logs) != 2:
            logger.warning(f"Expected 2 transaction logs for transaction {transaction_id}, found {len(transaction_logs)}")
            return False
        
        # Check transaction types
        increase_log = None
        decrease_log = None
        
        for log in transaction_logs:
            if log["transaction_type"] == "increase":
                increase_log = log
            elif log["transaction_type"] == "decrease":
                decrease_log = log
        
        if not increase_log or not decrease_log:
            logger.warning(f"Missing increase or decrease log for transaction {transaction_id}")
            return False
        
        # Check amounts
        if increase_log["amount"] != decrease_log["amount"]:
            logger.warning(
                f"Amounts don't match for transaction {transaction_id}: "
                f"{increase_log['amount']} != {decrease_log['amount']}"
            )
            return False
        
        # Check users
        if (increase_log["user_id"] != user1_id and increase_log["user_id"] != user2_id) or \
           (decrease_log["user_id"] != user1_id and decrease_log["user_id"] != user2_id):
            logger.warning(f"Users don't match for transaction {transaction_id}")
            return False
        
        # Check asset
        if increase_log["asset"] != asset or decrease_log["asset"] != asset:
            logger.warning(f"Assets don't match for transaction {transaction_id}")
            return False
        
        return True
    
    def _log_transaction(
        self,
        transaction_id: str,
        user_id: str,
        asset: str,
        amount: float,
        transaction_type: str
    ) -> None:
        """
        Log a transaction.
        
        Args:
            transaction_id: Transaction ID
            user_id: User identifier
            asset: Asset identifier
            amount: Transaction amount
            transaction_type: Transaction type (set, increase, decrease)
        """
        # Create transaction log entry
        log_entry = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "asset": asset,
            "amount": amount,
            "transaction_type": transaction_type,
            "timestamp": int(time.time())
        }
        
        # Add to transaction log
        self.transaction_log[transaction_id] = log_entry
        
        # In a real implementation, we would also store this in Redis
        # For now, we'll just log it
        logger.debug(f"Logged transaction: {log_entry}")