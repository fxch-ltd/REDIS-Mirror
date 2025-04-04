"""
Redis Mirror CE Exchange Account Integration

This module provides functionality for integrating with exchange accounts,
including updating user accounts based on credit changes, tracking funding
balances, and interfacing with exchange account systems.
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


class AccountEntry:
    """
    Represents an account entry for a user and asset.
    
    This class encapsulates the data and state of an account entry.
    """
    
    def __init__(
        self,
        user_id: str,
        asset: str,
        balance: float,
        available: float,
        reserved: float = 0.0,
        timestamp: Optional[int] = None,
        last_updated: Optional[int] = None,
        transaction_id: Optional[str] = None
    ):
        """
        Initialize an account entry.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            balance: Total balance
            available: Available balance
            reserved: Reserved balance
            timestamp: Creation timestamp
            last_updated: Last update timestamp
            transaction_id: Last transaction ID
        """
        self.user_id = user_id
        self.asset = asset
        self.balance = balance
        self.available = available
        self.reserved = reserved
        self.timestamp = timestamp or int(time.time())
        self.last_updated = last_updated or self.timestamp
        self.transaction_id = transaction_id
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the account entry to a dictionary.
        
        Returns:
            Dictionary representation of the account entry
        """
        return {
            "user_id": self.user_id,
            "asset": self.asset,
            "balance": self.balance,
            "available": self.available,
            "reserved": self.reserved,
            "timestamp": self.timestamp,
            "last_updated": self.last_updated,
            "transaction_id": self.transaction_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountEntry':
        """
        Create an account entry from a dictionary.
        
        Args:
            data: Dictionary representation of the account entry
            
        Returns:
            Account entry instance
        """
        return cls(
            user_id=data.get("user_id"),
            asset=data.get("asset"),
            balance=float(data.get("balance", 0)),
            available=float(data.get("available", 0)),
            reserved=float(data.get("reserved", 0)),
            timestamp=int(data.get("timestamp", time.time())),
            last_updated=int(data.get("last_updated", time.time())),
            transaction_id=data.get("transaction_id")
        )
    
    def update(
        self,
        balance: Optional[float] = None,
        available: Optional[float] = None,
        reserved: Optional[float] = None,
        transaction_id: Optional[str] = None
    ) -> None:
        """
        Update the account entry.
        
        Args:
            balance: New total balance (if None, unchanged)
            available: New available balance (if None, unchanged)
            reserved: New reserved balance (if None, unchanged)
            transaction_id: Transaction ID for the update
        """
        if balance is not None:
            self.balance = balance
        
        if available is not None:
            self.available = available
        
        if reserved is not None:
            self.reserved = reserved
        
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def increase_balance(
        self,
        amount: float,
        increase_available: bool = True,
        transaction_id: Optional[str] = None
    ) -> None:
        """
        Increase the account balance.
        
        Args:
            amount: Amount to increase by
            increase_available: Whether to increase available balance
            transaction_id: Transaction ID for the increase
        """
        self.balance += amount
        
        if increase_available:
            self.available += amount
        
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def decrease_balance(
        self,
        amount: float,
        decrease_available: bool = True,
        transaction_id: Optional[str] = None
    ) -> None:
        """
        Decrease the account balance.
        
        Args:
            amount: Amount to decrease by
            decrease_available: Whether to decrease available balance
            transaction_id: Transaction ID for the decrease
        """
        self.balance -= amount
        
        if decrease_available:
            self.available -= amount
        
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def reserve(
        self,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> None:
        """
        Reserve an amount from the available balance.
        
        Args:
            amount: Amount to reserve
            transaction_id: Transaction ID for the reservation
            
        Raises:
            ValidationError: If the available balance is insufficient
        """
        if self.available < amount:
            raise ValidationError(
                f"Insufficient available balance: {self.available} < {amount}"
            )
        
        self.available -= amount
        self.reserved += amount
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def release_reservation(
        self,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> None:
        """
        Release a reserved amount back to the available balance.
        
        Args:
            amount: Amount to release
            transaction_id: Transaction ID for the release
            
        Raises:
            ValidationError: If the reserved balance is insufficient
        """
        if self.reserved < amount:
            raise ValidationError(
                f"Insufficient reserved balance: {self.reserved} < {amount}"
            )
        
        self.available += amount
        self.reserved -= amount
        self.last_updated = int(time.time())
        self.transaction_id = transaction_id
    
    def __str__(self) -> str:
        """String representation of the account entry."""
        return (f"AccountEntry(user={self.user_id}, asset={self.asset}, "
                f"balance={self.balance}, available={self.available}, "
                f"reserved={self.reserved})")


class AccountIntegration:
    """
    Manages account integration for the Exchange.
    
    This class handles updating user accounts based on credit changes,
    tracking funding balances, and interfacing with exchange account systems.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager
    ):
        """
        Initialize the account integration.
        
        Args:
            connection_manager: Redis connection manager
        """
        self.connection_manager = connection_manager
        
        # Get Redis clients
        self.replica_client = connection_manager.get_replica_client()
        
        # Account cache (user_id -> {asset -> AccountEntry})
        self.accounts = {}
        
        # Transaction log (transaction_id -> {user_id, asset, amount, type, timestamp})
        self.transaction_log = {}
        
        logger.info("Account Integration initialized")
    
    def get_account(
        self,
        user_id: str,
        asset: str,
        refresh: bool = False
    ) -> Optional[AccountEntry]:
        """
        Get the account for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            refresh: Whether to refresh from Redis
            
        Returns:
            Account entry if found, None otherwise
        """
        # Check cache first
        if not refresh and user_id in self.accounts and asset in self.accounts[user_id]:
            return self.accounts[user_id][asset]
        
        # Get from Redis
        account_key = KeyManager.account(user_id, asset)
        account_data = self.replica_client.hgetall(account_key)
        
        if not account_data:
            return None
        
        # Create account entry
        account_entry = AccountEntry.from_dict({
            "user_id": user_id,
            "asset": asset,
            **account_data
        })
        
        # Update cache
        if user_id not in self.accounts:
            self.accounts[user_id] = {}
        
        self.accounts[user_id][asset] = account_entry
        
        return account_entry
    
    def set_account(
        self,
        user_id: str,
        asset: str,
        balance: float,
        available: Optional[float] = None,
        reserved: float = 0.0,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Set the account for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            balance: Total balance
            available: Available balance (defaults to balance)
            reserved: Reserved balance
            transaction_id: Transaction ID for the update
            
        Returns:
            Updated account entry
        """
        logger.info(f"Setting account for user {user_id}, asset {asset}, balance {balance}")
        
        # Set available to balance if not provided
        if available is None:
            available = balance
        
        # Get existing account entry or create a new one
        account_entry = self.get_account(user_id, asset)
        
        if account_entry:
            # Update existing entry
            account_entry.update(
                balance=balance,
                available=available,
                reserved=reserved,
                transaction_id=transaction_id
            )
        else:
            # Create new entry
            account_entry = AccountEntry(
                user_id=user_id,
                asset=asset,
                balance=balance,
                available=available,
                reserved=reserved,
                transaction_id=transaction_id
            )
            
            # Update cache
            if user_id not in self.accounts:
                self.accounts[user_id] = {}
            
            self.accounts[user_id][asset] = account_entry
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=balance,
                transaction_type="set"
            )
        
        return account_entry
    
    def increase_balance(
        self,
        user_id: str,
        asset: str,
        amount: float,
        increase_available: bool = True,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Increase the account balance for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to increase by
            increase_available: Whether to increase available balance
            transaction_id: Transaction ID for the increase
            
        Returns:
            Updated account entry
            
        Raises:
            ValidationError: If the amount is negative
        """
        logger.info(f"Increasing balance for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Increase amount must be positive: {amount}")
        
        # Get existing account entry or create a new one
        account_entry = self.get_account(user_id, asset)
        
        if account_entry:
            # Increase existing entry
            account_entry.increase_balance(
                amount=amount,
                increase_available=increase_available,
                transaction_id=transaction_id
            )
        else:
            # Create new entry
            account_entry = AccountEntry(
                user_id=user_id,
                asset=asset,
                balance=amount,
                available=amount if increase_available else 0.0,
                reserved=0.0,
                transaction_id=transaction_id
            )
            
            # Update cache
            if user_id not in self.accounts:
                self.accounts[user_id] = {}
            
            self.accounts[user_id][asset] = account_entry
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="increase"
            )
        
        return account_entry
    
    def decrease_balance(
        self,
        user_id: str,
        asset: str,
        amount: float,
        decrease_available: bool = True,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Decrease the account balance for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to decrease by
            decrease_available: Whether to decrease available balance
            transaction_id: Transaction ID for the decrease
            
        Returns:
            Updated account entry
            
        Raises:
            ValidationError: If the amount is negative or greater than the current balance
        """
        logger.info(f"Decreasing balance for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Decrease amount must be positive: {amount}")
        
        # Get existing account entry
        account_entry = self.get_account(user_id, asset)
        
        if not account_entry:
            raise ValidationError(f"No account found for user {user_id}, asset {asset}")
        
        # Check if there's enough balance
        if account_entry.balance < amount:
            raise ValidationError(
                f"Insufficient balance: {account_entry.balance} < {amount}"
            )
        
        # Check if there's enough available balance
        if decrease_available and account_entry.available < amount:
            raise ValidationError(
                f"Insufficient available balance: {account_entry.available} < {amount}"
            )
        
        # Decrease existing entry
        account_entry.decrease_balance(
            amount=amount,
            decrease_available=decrease_available,
            transaction_id=transaction_id
        )
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="decrease"
            )
        
        return account_entry
    
    def reserve_balance(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Reserve an amount from the available balance.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to reserve
            transaction_id: Transaction ID for the reservation
            
        Returns:
            Updated account entry
            
        Raises:
            ValidationError: If the amount is negative or greater than the available balance
        """
        logger.info(f"Reserving balance for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Reserve amount must be positive: {amount}")
        
        # Get existing account entry
        account_entry = self.get_account(user_id, asset)
        
        if not account_entry:
            raise ValidationError(f"No account found for user {user_id}, asset {asset}")
        
        # Reserve amount
        account_entry.reserve(amount, transaction_id)
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="reserve"
            )
        
        return account_entry
    
    def release_reservation(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Release a reserved amount back to the available balance.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to release
            transaction_id: Transaction ID for the release
            
        Returns:
            Updated account entry
            
        Raises:
            ValidationError: If the amount is negative or greater than the reserved balance
        """
        logger.info(f"Releasing reservation for user {user_id}, asset {asset}, amount {amount}")
        
        # Validate amount
        if amount < 0:
            raise ValidationError(f"Release amount must be positive: {amount}")
        
        # Get existing account entry
        account_entry = self.get_account(user_id, asset)
        
        if not account_entry:
            raise ValidationError(f"No account found for user {user_id}, asset {asset}")
        
        # Release reservation
        account_entry.release_reservation(amount, transaction_id)
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                transaction_type="release"
            )
        
        return account_entry
    
    def get_all_accounts(self, user_id: str) -> Dict[str, AccountEntry]:
        """
        Get all accounts for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary mapping asset identifiers to account entries
        """
        # Get from Redis
        account_keys = self.replica_client.keys(KeyManager.account(user_id, "*"))
        
        result = {}
        
        for account_key in account_keys:
            # Extract asset from key
            asset = account_key.split(":")[-1]
            
            # Get account entry
            account_entry = self.get_account(user_id, asset)
            
            if account_entry:
                result[asset] = account_entry
        
        return result
    
    def get_accounts_for_assets(
        self,
        user_id: str,
        assets: List[str]
    ) -> Dict[str, AccountEntry]:
        """
        Get accounts for multiple assets.
        
        Args:
            user_id: User identifier
            assets: List of asset identifiers
            
        Returns:
            Dictionary mapping asset identifiers to account entries
        """
        result = {}
        
        for asset in assets:
            account_entry = self.get_account(user_id, asset)
            
            if account_entry:
                result[asset] = account_entry
        
        return result
    
    def update_account_from_credit_inventory(
        self,
        user_id: str,
        asset: str,
        ci_amount: float,
        transaction_id: Optional[str] = None
    ) -> AccountEntry:
        """
        Update an account based on credit inventory.
        
        This method is used to synchronize the account balance with the credit
        inventory, typically during settlement or reconciliation.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            ci_amount: Credit inventory amount
            transaction_id: Transaction ID for the update
            
        Returns:
            Updated account entry
        """
        logger.info(
            f"Updating account from credit inventory for user {user_id}, "
            f"asset {asset}, CI amount {ci_amount}"
        )
        
        # Get existing account entry or create a new one
        account_entry = self.get_account(user_id, asset)
        
        if account_entry:
            # Calculate difference
            diff = ci_amount - account_entry.balance
            
            if diff > 0:
                # Increase balance
                account_entry.increase_balance(diff, True, transaction_id)
            elif diff < 0:
                # Decrease balance
                account_entry.decrease_balance(abs(diff), True, transaction_id)
            else:
                # No change needed
                return account_entry
        else:
            # Create new entry
            account_entry = AccountEntry(
                user_id=user_id,
                asset=asset,
                balance=ci_amount,
                available=ci_amount,
                reserved=0.0,
                transaction_id=transaction_id
            )
            
            # Update cache
            if user_id not in self.accounts:
                self.accounts[user_id] = {}
            
            self.accounts[user_id][asset] = account_entry
        
        # Update Redis
        account_key = KeyManager.account(user_id, asset)
        self.replica_client.hset(account_key, mapping=account_entry.to_dict())
        
        # Log transaction
        if transaction_id:
            self._log_transaction(
                transaction_id=transaction_id,
                user_id=user_id,
                asset=asset,
                amount=ci_amount,
                transaction_type="sync"
            )
        
        return account_entry
    
    def process_settlement_update(
        self,
        user_id: str,
        asset: str,
        amount: float,
        is_bought: bool,
        settlement_id: str
    ) -> AccountEntry:
        """
        Process a settlement update for a user and asset.
        
        This method is used during the settlement process to update the account
        balance for assets that were bought or sold.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to update by
            is_bought: Whether the asset was bought (True) or sold (False)
            settlement_id: Settlement ID
            
        Returns:
            Updated account entry
        """
        logger.info(
            f"Processing settlement update for user {user_id}, "
            f"asset {asset}, amount {amount}, {'bought' if is_bought else 'sold'}, "
            f"settlement {settlement_id}"
        )
        
        # Create transaction ID
        transaction_id = f"settlement:{settlement_id}:{user_id}:{asset}"
        
        if is_bought:
            # Increase balance for bought assets
            return self.increase_balance(
                user_id=user_id,
                asset=asset,
                amount=amount,
                increase_available=True,
                transaction_id=transaction_id
            )
        else:
            # Decrease balance for sold assets
            return self.decrease_balance(
                user_id=user_id,
                asset=asset,
                amount=amount,
                decrease_available=True,
                transaction_id=transaction_id
            )
    
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
            transaction_type: Transaction type (set, increase, decrease, reserve, release, sync)
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