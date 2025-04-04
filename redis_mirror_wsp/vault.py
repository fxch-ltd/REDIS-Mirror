"""
Redis Mirror CE WSP Vault Manager

This module provides functionality for WSPs to manage vault operations,
including locking and unlocking assets in the vault, applying the Lobster
Basket Policy, and tracking pledged assets.
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


class VaultAsset:
    """
    Represents an asset in the vault.
    
    This class encapsulates the data and state of an asset in the vault.
    """
    
    STATUS_LOCKED = "locked"
    STATUS_UNLOCKED = "unlocked"
    STATUS_PENDING = "pending"
    
    def __init__(
        self,
        vault_id: str,
        user_id: str,
        asset: str,
        amount: float,
        chain: str,
        address: str,
        status: str = STATUS_LOCKED,
        timestamp: Optional[int] = None,
        pledge_request_id: Optional[str] = None
    ):
        """
        Initialize a vault asset.
        
        Args:
            vault_id: Vault identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Asset amount
            chain: Blockchain chain
            address: Blockchain address
            status: Asset status (locked, unlocked, pending)
            timestamp: Timestamp when the asset was added to the vault
            pledge_request_id: Associated pledge request ID
        """
        self.vault_id = vault_id
        self.user_id = user_id
        self.asset = asset
        self.amount = amount
        self.chain = chain
        self.address = address
        self.status = status
        self.timestamp = timestamp or int(time.time())
        self.pledge_request_id = pledge_request_id
        self.unlock_timestamp = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the vault asset to a dictionary.
        
        Returns:
            Dictionary representation of the vault asset
        """
        return {
            "vault_id": self.vault_id,
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": self.amount,
            "chain": self.chain,
            "address": self.address,
            "status": self.status,
            "timestamp": self.timestamp,
            "pledge_request_id": self.pledge_request_id,
            "unlock_timestamp": self.unlock_timestamp
        }
    
    def lock(self):
        """Lock the asset in the vault."""
        self.status = self.STATUS_LOCKED
        self.unlock_timestamp = None
    
    def unlock(self):
        """Unlock the asset in the vault."""
        self.status = self.STATUS_UNLOCKED
        self.unlock_timestamp = int(time.time())
    
    def mark_pending(self):
        """Mark the asset as pending."""
        self.status = self.STATUS_PENDING
    
    def is_locked(self) -> bool:
        """
        Check if the asset is locked.
        
        Returns:
            True if the asset is locked, False otherwise
        """
        return self.status == self.STATUS_LOCKED
    
    def is_unlocked(self) -> bool:
        """
        Check if the asset is unlocked.
        
        Returns:
            True if the asset is unlocked, False otherwise
        """
        return self.status == self.STATUS_UNLOCKED
    
    def is_pending(self) -> bool:
        """
        Check if the asset is pending.
        
        Returns:
            True if the asset is pending, False otherwise
        """
        return self.status == self.STATUS_PENDING
    
    def __str__(self) -> str:
        """String representation of the vault asset."""
        return (f"VaultAsset(id={self.vault_id}, user={self.user_id}, "
                f"asset={self.asset}, amount={self.amount}, status={self.status})")


class LobsterBasketPolicy:
    """
    Implements the Lobster Basket Policy for vault assets.
    
    The Lobster Basket Policy determines which assets can be unlocked
    based on the credit inventory and trading activity.
    """
    
    def __init__(self, vault_manager: 'VaultManager'):
        """
        Initialize the Lobster Basket Policy.
        
        Args:
            vault_manager: Vault manager instance
        """
        self.vault_manager = vault_manager
    
    def apply_policy(
        self,
        user_id: str,
        asset: str,
        amount: float,
        credit_inventory: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, List[VaultAsset], str]:
        """
        Apply the Lobster Basket Policy to determine which assets can be unlocked.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to unlock
            credit_inventory: Optional credit inventory for the user
            
        Returns:
            Tuple of (success, assets_to_unlock, reason)
        """
        logger.info(f"Applying Lobster Basket Policy for user {user_id}, asset {asset}, amount {amount}")
        
        # Get all locked assets for the user and asset
        locked_assets = self.vault_manager.get_locked_assets(user_id, asset)
        
        if not locked_assets:
            return False, [], "No locked assets found"
        
        # Calculate total locked amount
        total_locked = sum(a.amount for a in locked_assets)
        
        if total_locked < amount:
            return False, [], f"Insufficient locked assets: {total_locked} < {amount}"
        
        # Get credit inventory if not provided
        if credit_inventory is None:
            # In a real implementation, we would get the credit inventory from the Exchange
            # For now, we'll assume the credit inventory is sufficient
            credit_inventory = {asset: total_locked}
        
        # Check if credit inventory is sufficient
        if asset not in credit_inventory or credit_inventory[asset] < amount:
            return False, [], f"Insufficient credit inventory: {credit_inventory.get(asset, 0)} < {amount}"
        
        # Select assets to unlock (FIFO)
        assets_to_unlock = []
        remaining_amount = amount
        
        # Sort assets by timestamp (oldest first)
        sorted_assets = sorted(locked_assets, key=lambda a: a.timestamp)
        
        for asset_obj in sorted_assets:
            if remaining_amount <= 0:
                break
            
            if asset_obj.amount <= remaining_amount:
                # Unlock the entire asset
                assets_to_unlock.append(asset_obj)
                remaining_amount -= asset_obj.amount
            else:
                # Split the asset
                new_asset = VaultAsset(
                    vault_id=f"{asset_obj.vault_id}-split-{int(time.time())}",
                    user_id=asset_obj.user_id,
                    asset=asset_obj.asset,
                    amount=remaining_amount,
                    chain=asset_obj.chain,
                    address=asset_obj.address,
                    status=asset_obj.status,
                    timestamp=asset_obj.timestamp,
                    pledge_request_id=asset_obj.pledge_request_id
                )
                
                # Reduce the original asset amount
                asset_obj.amount -= remaining_amount
                
                # Add the new asset to the list
                assets_to_unlock.append(new_asset)
                remaining_amount = 0
        
        return True, assets_to_unlock, ""


class VaultManager:
    """
    Manages vault operations for WSPs.
    
    This class handles locking and unlocking assets in the vault,
    applying the Lobster Basket Policy, and tracking pledged assets.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        custodian_id: str
    ):
        """
        Initialize the vault manager.
        
        Args:
            connection_manager: Redis connection manager
            custodian_id: Custodian identifier
        """
        self.connection_manager = connection_manager
        self.custodian_id = custodian_id
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        
        # Create Lobster Basket Policy
        self.policy = LobsterBasketPolicy(self)
        
        # Vault assets dictionary (vault_id -> VaultAsset)
        self.vault_assets = {}
        
        # User assets dictionary (user_id -> {asset -> [VaultAsset]})
        self.user_assets = {}
        
        logger.info("Vault Manager initialized")
    
    def add_asset(
        self,
        user_id: str,
        asset: str,
        amount: float,
        chain: str,
        address: str,
        vault_id: Optional[str] = None,
        status: str = VaultAsset.STATUS_LOCKED,
        pledge_request_id: Optional[str] = None
    ) -> VaultAsset:
        """
        Add an asset to the vault.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Asset amount
            chain: Blockchain chain
            address: Blockchain address
            vault_id: Optional vault identifier (generated if not provided)
            status: Asset status (locked, unlocked, pending)
            pledge_request_id: Associated pledge request ID
            
        Returns:
            Added vault asset
            
        Raises:
            ValidationError: If the asset is invalid
        """
        logger.info(f"Adding asset to vault: user {user_id}, asset {asset}, amount {amount}")
        
        # Validate inputs
        if amount <= 0:
            raise ValidationError("Amount must be positive")
        
        if not user_id:
            raise ValidationError("User ID is required")
        
        if not asset:
            raise ValidationError("Asset is required")
        
        if not chain:
            raise ValidationError("Chain is required")
        
        if not address:
            raise ValidationError("Address is required")
        
        # Generate vault ID if not provided
        if not vault_id:
            timestamp = int(time.time())
            vault_id = f"vault-{timestamp}-{user_id}-{asset}"
        
        # Create vault asset
        vault_asset = VaultAsset(
            vault_id=vault_id,
            user_id=user_id,
            asset=asset,
            amount=amount,
            chain=chain,
            address=address,
            status=status,
            pledge_request_id=pledge_request_id
        )
        
        # Store vault asset
        self.vault_assets[vault_id] = vault_asset
        
        # Add to user assets
        if user_id not in self.user_assets:
            self.user_assets[user_id] = {}
        
        if asset not in self.user_assets[user_id]:
            self.user_assets[user_id][asset] = []
        
        self.user_assets[user_id][asset].append(vault_asset)
        
        # Store in Redis
        self._store_vault_asset(vault_asset)
        
        return vault_asset
    
    def lock_asset(self, vault_id: str) -> bool:
        """
        Lock an asset in the vault.
        
        Args:
            vault_id: Vault identifier
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Locking asset: {vault_id}")
        
        vault_asset = self.vault_assets.get(vault_id)
        if not vault_asset:
            logger.warning(f"Asset not found: {vault_id}")
            return False
        
        vault_asset.lock()
        
        # Update in Redis
        self._store_vault_asset(vault_asset)
        
        return True
    
    def unlock_asset(self, vault_id: str) -> bool:
        """
        Unlock an asset in the vault.
        
        Args:
            vault_id: Vault identifier
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Unlocking asset: {vault_id}")
        
        vault_asset = self.vault_assets.get(vault_id)
        if not vault_asset:
            logger.warning(f"Asset not found: {vault_id}")
            return False
        
        vault_asset.unlock()
        
        # Update in Redis
        self._store_vault_asset(vault_asset)
        
        return True
    
    def unlock_assets(
        self,
        user_id: str,
        asset: str,
        amount: float,
        credit_inventory: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, List[VaultAsset], str]:
        """
        Unlock assets in the vault using the Lobster Basket Policy.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to unlock
            credit_inventory: Optional credit inventory for the user
            
        Returns:
            Tuple of (success, unlocked_assets, reason)
        """
        logger.info(f"Unlocking assets: user {user_id}, asset {asset}, amount {amount}")
        
        # Apply Lobster Basket Policy
        success, assets_to_unlock, reason = self.policy.apply_policy(
            user_id, asset, amount, credit_inventory
        )
        
        if not success:
            logger.warning(f"Failed to unlock assets: {reason}")
            return False, [], reason
        
        # Unlock assets
        unlocked_assets = []
        for asset_obj in assets_to_unlock:
            asset_obj.unlock()
            unlocked_assets.append(asset_obj)
            
            # Update in Redis
            self._store_vault_asset(asset_obj)
        
        logger.info(f"Unlocked {len(unlocked_assets)} assets")
        return True, unlocked_assets, ""
    
    def get_asset(self, vault_id: str) -> Optional[VaultAsset]:
        """
        Get an asset by vault ID.
        
        Args:
            vault_id: Vault identifier
            
        Returns:
            Vault asset if found, None otherwise
        """
        return self.vault_assets.get(vault_id)
    
    def get_assets(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[VaultAsset]:
        """
        Get vault assets.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            status: Optional status to filter by
            
        Returns:
            List of vault assets
        """
        assets = list(self.vault_assets.values())
        
        # Filter by user ID
        if user_id:
            assets = [a for a in assets if a.user_id == user_id]
        
        # Filter by asset
        if asset:
            assets = [a for a in assets if a.asset == asset]
        
        # Filter by status
        if status:
            assets = [a for a in assets if a.status == status]
        
        return assets
    
    def get_user_assets(self, user_id: str) -> Dict[str, List[VaultAsset]]:
        """
        Get all assets for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary mapping asset identifiers to lists of vault assets
        """
        return self.user_assets.get(user_id, {})
    
    def get_locked_assets(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None
    ) -> List[VaultAsset]:
        """
        Get locked assets.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            
        Returns:
            List of locked vault assets
        """
        return self.get_assets(user_id, asset, VaultAsset.STATUS_LOCKED)
    
    def get_unlocked_assets(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None
    ) -> List[VaultAsset]:
        """
        Get unlocked assets.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            
        Returns:
            List of unlocked vault assets
        """
        return self.get_assets(user_id, asset, VaultAsset.STATUS_UNLOCKED)
    
    def get_pending_assets(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None
    ) -> List[VaultAsset]:
        """
        Get pending assets.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            
        Returns:
            List of pending vault assets
        """
        return self.get_assets(user_id, asset, VaultAsset.STATUS_PENDING)
    
    def get_total_locked_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total locked amount for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total locked amount
        """
        locked_assets = self.get_locked_assets(user_id, asset)
        return sum(a.amount for a in locked_assets)
    
    def get_total_unlocked_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total unlocked amount for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total unlocked amount
        """
        unlocked_assets = self.get_unlocked_assets(user_id, asset)
        return sum(a.amount for a in unlocked_assets)
    
    def get_total_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total amount (locked + unlocked) for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total amount
        """
        assets = self.get_assets(user_id, asset)
        return sum(a.amount for a in assets)
    
    def _store_vault_asset(self, vault_asset: VaultAsset):
        """
        Store a vault asset in Redis.
        
        Args:
            vault_asset: Vault asset to store
        """
        # In a real implementation, we would store the vault asset in Redis
        # For now, we'll just log it
        logger.debug(f"Storing vault asset: {vault_asset}")
        
        # Example Redis storage (commented out)
        # key = KeyManager.vault_asset(vault_asset.vault_id)
        # self.wsp_client.hset(key, mapping=vault_asset.to_dict())
    
    def _load_vault_assets(self):
        """Load vault assets from Redis."""
        # In a real implementation, we would load vault assets from Redis
        # For now, we'll just log it
        logger.debug("Loading vault assets")
        
        # Example Redis loading (commented out)
        # keys = self.wsp_client.keys(KeyManager.vault_asset("*"))
        # for key in keys:
        #     data = self.wsp_client.hgetall(key)
        #     if data:
        #         vault_id = key.split(":")[-1]
        #         vault_asset = VaultAsset(
        #             vault_id=vault_id,
        #             user_id=data["user_id"],
        #             asset=data["asset"],
        #             amount=float(data["amount"]),
        #             chain=data["chain"],
        #             address=data["address"],
        #             status=data["status"],
        #             timestamp=int(data["timestamp"]),
        #             pledge_request_id=data.get("pledge_request_id")
        #         )
        #         if "unlock_timestamp" in data:
        #             vault_asset.unlock_timestamp = int(data["unlock_timestamp"])
        #         
        #         self.vault_assets[vault_id] = vault_asset
        #         
        #         # Add to user assets
        #         user_id = vault_asset.user_id
        #         asset = vault_asset.asset
        #         if user_id not in self.user_assets:
        #             self.user_assets[user_id] = {}
        #         if asset not in self.user_assets[user_id]:
        #             self.user_assets[user_id][asset] = []
        #         self.user_assets[user_id][asset].append(vault_asset)