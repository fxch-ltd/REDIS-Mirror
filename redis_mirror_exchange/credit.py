"""
Redis Mirror CE Exchange Credit Manager

This module provides functionality for the Exchange to process credit requests
from WSPs, including request validation, credit inventory management, and
response generation.
"""

import time
import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union, Tuple
from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    KeyManager,
    StreamProcessor,
    StreamPublisher,
    RedisMirrorError,
    ValidationError
)

# Configure logging
logger = logging.getLogger(__name__)


class CreditInventory:
    """
    Represents a credit inventory record for a user and asset.
    
    This class encapsulates the data and operations for a credit inventory.
    """
    
    def __init__(
        self,
        user_id: str,
        asset: str,
        amount: float = 0.0,
        timestamp: Optional[int] = None
    ):
        """
        Initialize a credit inventory.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit inventory amount
            timestamp: Optional timestamp (current time if not provided)
        """
        self.user_id = user_id
        self.asset = asset
        self.amount = float(amount)
        self.timestamp = timestamp or int(time.time())
    
    def increase(self, amount: float) -> float:
        """
        Increase the credit inventory.
        
        Args:
            amount: Amount to increase by
            
        Returns:
            New credit inventory amount
        """
        self.amount += float(amount)
        self.timestamp = int(time.time())
        return self.amount
    
    def decrease(self, amount: float) -> float:
        """
        Decrease the credit inventory.
        
        Args:
            amount: Amount to decrease by
            
        Returns:
            New credit inventory amount
        """
        self.amount -= float(amount)
        self.timestamp = int(time.time())
        return self.amount
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the credit inventory to a dictionary.
        
        Returns:
            Dictionary representation of the credit inventory
        """
        return {
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": str(self.amount),
            "timestamp": str(self.timestamp)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CreditInventory':
        """
        Create a credit inventory from a dictionary.
        
        Args:
            data: Dictionary representation of the credit inventory
            
        Returns:
            Credit inventory instance
        """
        return cls(
            user_id=data["user_id"],
            asset=data["asset"],
            amount=float(data["amount"]),
            timestamp=int(data["timestamp"]) if "timestamp" in data else None
        )
    
    def __str__(self) -> str:
        """String representation of the credit inventory."""
        return (f"CreditInventory(user={self.user_id}, asset={self.asset}, "
                f"amount={self.amount})")


class CreditValidator:
    """
    Validates credit requests based on Exchange rules.
    
    This class encapsulates the validation logic for credit requests.
    """
    
    def __init__(
        self,
        approved_custodians: Optional[List[str]] = None,
        approved_assets: Optional[List[str]] = None
    ):
        """
        Initialize the credit validator.
        
        Args:
            approved_custodians: List of approved custodian IDs
            approved_assets: List of approved asset IDs
        """
        self.approved_custodians = approved_custodians or []
        self.approved_assets = approved_assets or []
    
    def validate_increase(
        self,
        custodian: str,
        user_id: str,
        asset: str,
        amount: float,
        ci: float,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        inventory_manager: Optional['CreditInventoryManager'] = None
    ) -> Tuple[bool, str]:
        """
        Validate a credit increase request.
        
        Args:
            custodian: Custodian identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Credit increase amount
            ci: Credit inventory amount after increase
            chain: Blockchain chain
            address: Blockchain address
            inventory_manager: Optional inventory manager for additional checks
            
        Returns:
            Tuple of (is_valid, reject_reason)
        """
        # 1. Custodian Validation
        if self.approved_custodians and custodian not in self.approved_custodians:
            return False, f"Custodian '{custodian}' is not approved"
        
        # 2. Account Validation
        # This would typically involve checking if the user exists in the Exchange
        # For this SDK, we'll assume all users are valid
        
        # 3. Asset Validation
        if self.approved_assets and asset not in self.approved_assets:
            return False, f"Asset '{asset}' is not supported"
        
        # 4. Risk Validation
        # This would typically involve checking risk parameters
        # For this SDK, we'll implement a simple check
        if amount <= 0:
            return False, "Credit increase amount must be positive"
        
        # 5. Asset Coverage (Level 0 only)
        # This would typically involve checking if the wallet has sufficient funds
        # For this SDK, we'll assume all wallets have sufficient funds
        if not chain:
            return False, "Blockchain chain is required for credit increases"
        
        if not address:
            return False, "Blockchain address is required for credit increases"
        
        # Additional checks with inventory manager
        if inventory_manager:
            # Check if the CI value matches the expected value
            current_ci = inventory_manager.get_inventory(user_id, asset)
            expected_ci = current_ci.amount + amount if current_ci else amount
            
            if abs(expected_ci - float(ci)) > 0.0001:  # Allow for small floating point differences
                return False, f"Expected CI {expected_ci} does not match requested CI {ci}"
        
        return True, ""
    
    def validate_decrease(
        self,
        custodian: str,
        user_id: str,
        asset: str,
        amount: float,
        ci: float,
        inventory_manager: Optional['CreditInventoryManager'] = None
    ) -> Tuple[bool, str]:
        """
        Validate a credit decrease request.
        
        Args:
            custodian: Custodian identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Credit decrease amount (negative)
            ci: Credit inventory amount after decrease
            inventory_manager: Optional inventory manager for additional checks
            
        Returns:
            Tuple of (is_valid, reject_reason)
        """
        # 1. Custodian Validation
        if self.approved_custodians and custodian not in self.approved_custodians:
            return False, f"Custodian '{custodian}' is not approved"
        
        # 2. Account Validation
        # This would typically involve checking if the user exists in the Exchange
        # For this SDK, we'll assume all users are valid
        
        # 3. Asset Validation
        if self.approved_assets and asset not in self.approved_assets:
            return False, f"Asset '{asset}' is not supported"
        
        # 4. Inventory Validation
        if inventory_manager:
            current_ci = inventory_manager.get_inventory(user_id, asset)
            
            if not current_ci:
                return False, f"No credit inventory found for user {user_id}, asset {asset}"
            
            # Ensure amount is negative
            if amount >= 0:
                return False, "Credit decrease amount must be negative"
            
            # Check if there's enough inventory
            if current_ci.amount < abs(amount):
                return False, f"Insufficient credit inventory: {current_ci.amount} < {abs(amount)}"
            
            # Check if the CI value matches the expected value
            expected_ci = current_ci.amount + amount  # amount is negative
            
            if abs(expected_ci - float(ci)) > 0.0001:  # Allow for small floating point differences
                return False, f"Expected CI {expected_ci} does not match requested CI {ci}"
        
        return True, ""


class CreditInventoryManager:
    """
    Manages credit inventories for users and assets.
    
    This class provides functionality for tracking and updating credit inventories.
    """
    
    def __init__(self, client: Optional[Any] = None):
        """
        Initialize the credit inventory manager.
        
        Args:
            client: Optional Redis client for persistent storage
        """
        self.client = client
        self.inventories = {}  # In-memory cache: {user_id}:{asset} -> CreditInventory
    
    def get_inventory(self, user_id: str, asset: str) -> Optional[CreditInventory]:
        """
        Get a credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Credit inventory if found, None otherwise
        """
        key = f"{user_id}:{asset}"
        
        # Check in-memory cache first
        if key in self.inventories:
            return self.inventories[key]
        
        # If Redis client is available, check Redis
        if self.client:
            redis_key = KeyManager.credit_inventory(user_id, asset)
            data = self.client.hgetall(redis_key)
            
            if data:
                # Convert Redis hash to dictionary
                inventory_data = {
                    "user_id": user_id,
                    "asset": asset,
                    "amount": data.get(b"amount", b"0").decode("utf-8"),
                    "timestamp": data.get(b"timestamp", str(int(time.time())).encode()).decode("utf-8")
                }
                
                inventory = CreditInventory.from_dict(inventory_data)
                self.inventories[key] = inventory
                return inventory
        
        return None
    
    def update_inventory(self, inventory: CreditInventory) -> bool:
        """
        Update a credit inventory.
        
        Args:
            inventory: Credit inventory to update
            
        Returns:
            True if the update was successful, False otherwise
        """
        key = f"{inventory.user_id}:{inventory.asset}"
        self.inventories[key] = inventory
        
        # If Redis client is available, update Redis
        if self.client:
            redis_key = KeyManager.credit_inventory(inventory.user_id, inventory.asset)
            data = {
                "amount": str(inventory.amount),
                "timestamp": str(inventory.timestamp)
            }
            
            try:
                self.client.hmset(redis_key, data)
                return True
            except Exception as e:
                logger.error(f"Error updating credit inventory in Redis: {e}")
                return False
        
        return True
    
    def increase_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float
    ) -> Optional[CreditInventory]:
        """
        Increase a credit inventory.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to increase by
            
        Returns:
            Updated credit inventory if successful, None otherwise
        """
        inventory = self.get_inventory(user_id, asset)
        
        if not inventory:
            inventory = CreditInventory(user_id, asset)
        
        inventory.increase(amount)
        
        if self.update_inventory(inventory):
            return inventory
        
        return None
    
    def decrease_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float
    ) -> Optional[CreditInventory]:
        """
        Decrease a credit inventory.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to decrease by
            
        Returns:
            Updated credit inventory if successful, None otherwise
        """
        inventory = self.get_inventory(user_id, asset)
        
        if not inventory:
            return None
        
        if inventory.amount < amount:
            return None
        
        inventory.decrease(amount)
        
        if self.update_inventory(inventory):
            return inventory
        
        return None


class CreditManager:
    """
    Manages credit requests and responses for the Exchange.
    
    This class provides functionality for processing credit requests from WSPs,
    validating them, updating credit inventories, and generating responses.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        inventory_manager: Optional[CreditInventoryManager] = None,
        validator: Optional[CreditValidator] = None,
        approved_custodians: Optional[List[str]] = None,
        approved_assets: Optional[List[str]] = None,
        auto_start_processor: bool = True
    ):
        """
        Initialize the credit manager.
        
        Args:
            connection_manager: Redis connection manager
            inventory_manager: Optional credit inventory manager
            validator: Optional credit validator
            approved_custodians: Optional list of approved custodian IDs
            approved_assets: Optional list of approved asset IDs
            auto_start_processor: Whether to automatically start the request processor
        """
        self.connection_manager = connection_manager
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        self.replica_client = connection_manager.get_replica_client()
        
        # Create inventory manager if not provided
        self.inventory_manager = inventory_manager or CreditInventoryManager(self.wsp_client)
        
        # Create validator if not provided
        self.validator = validator or CreditValidator(
            approved_custodians=approved_custodians,
            approved_assets=approved_assets
        )
        
        # Create stream processor for credit requests
        self.request_stream = KeyManager.credit_request_stream()
        self.processor = StreamProcessor(
            self.wsp_client,
            self.request_stream,
            "credit-request-processor",
            auto_create_group=True
        )
        
        # Create stream publisher for credit responses
        self.response_stream = KeyManager.credit_response_stream()
        self.publisher = StreamPublisher(self.replica_client, self.response_stream)
        
        # Request callbacks
        self.request_callbacks = []
        
        # Start request processor if auto_start is enabled
        self._processor_running = False
        if auto_start_processor:
            self.start_request_processor()
    
    def process_credit_request(self, request: Dict[str, str]) -> Dict[str, str]:
        """
        Process a credit request.
        
        Args:
            request: Credit request dictionary
            
        Returns:
            Credit response dictionary
        """
        logger.info(f"Processing credit request: {request}")
        
        # Extract request fields
        custodian = request.get("custodian", "")
        request_id = request.get("request_id", "")
        user_id = request.get("uid", "")
        timestamp = request.get("timestamp", str(int(time.time())))
        asset = request.get("asset", "")
        c_change = request.get("c_change", "")
        ci = request.get("ci", "")
        chain = request.get("chain", "")
        address = request.get("address", "")
        
        # Prepare response
        response = {
            "custodian": custodian,
            "request_id": request_id,
            "uid": user_id,
            "timestamp": str(int(time.time())),
            "asset": asset,
            "c_change": c_change,
            "ci": ci,
            "status": "rejected",
            "reject_reason": ""
        }
        
        try:
            # Parse credit change
            try:
                amount = float(c_change)
            except ValueError:
                response["reject_reason"] = f"Invalid credit change amount: {c_change}"
                return response
            
            # Parse CI
            try:
                ci_value = float(ci)
            except ValueError:
                response["reject_reason"] = f"Invalid CI amount: {ci}"
                return response
            
            # Validate request based on type
            is_valid = False
            reject_reason = ""
            
            if amount > 0:  # Credit increase
                is_valid, reject_reason = self.validator.validate_increase(
                    custodian=custodian,
                    user_id=user_id,
                    asset=asset,
                    amount=amount,
                    ci=ci_value,
                    chain=chain,
                    address=address,
                    inventory_manager=self.inventory_manager
                )
            else:  # Credit decrease
                is_valid, reject_reason = self.validator.validate_decrease(
                    custodian=custodian,
                    user_id=user_id,
                    asset=asset,
                    amount=amount,
                    ci=ci_value,
                    inventory_manager=self.inventory_manager
                )
            
            if not is_valid:
                response["reject_reason"] = reject_reason
                return response
            
            # Update credit inventory
            if amount > 0:  # Credit increase
                inventory = self.inventory_manager.increase_inventory(user_id, asset, amount)
                if not inventory:
                    response["reject_reason"] = "Failed to update credit inventory"
                    return response
            else:  # Credit decrease
                inventory = self.inventory_manager.decrease_inventory(user_id, asset, abs(amount))
                if not inventory:
                    response["reject_reason"] = "Failed to update credit inventory"
                    return response
            
            # Update response
            response["status"] = "accepted"
            response["ci"] = str(inventory.amount)
            
            # Call callbacks
            for callback in self.request_callbacks:
                try:
                    callback(request, response)
                except Exception as e:
                    logger.error(f"Error in request callback: {e}")
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing credit request: {e}")
            response["reject_reason"] = f"Internal error: {str(e)}"
            return response
    
    def send_credit_response(self, response: Dict[str, str]) -> Optional[str]:
        """
        Send a credit response.
        
        Args:
            response: Credit response dictionary
            
        Returns:
            Message ID if successful, None otherwise
        """
        logger.info(f"Sending credit response: {response}")
        return self.publisher.publish(response)
    
    def add_request_callback(self, callback: Callable[[Dict[str, str], Dict[str, str]], None]):
        """
        Add a callback for credit requests.
        
        The callback will be called with the request and response dictionaries.
        
        Args:
            callback: Callback function
        """
        self.request_callbacks.append(callback)
    
    def start_request_processor(self):
        """Start the request processor."""
        if self._processor_running:
            return
        
        self._processor_running = True
        
        # Start processing in a separate thread
        import threading
        thread = threading.Thread(
            target=self._process_requests,
            daemon=True
        )
        thread.start()
    
    def stop_request_processor(self):
        """Stop the request processor."""
        self._processor_running = False
    
    def _process_requests(self):
        """Process credit requests."""
        logger.info("Starting credit request processor")
        
        def request_handler(message):
            try:
                # Process the request
                response = self.process_credit_request(message)
                
                # Send the response
                message_id = self.send_credit_response(response)
                
                if not message_id:
                    logger.error(f"Failed to send credit response: {response}")
                    return False
                
                logger.info(f"Sent credit response with ID: {message_id}")
                return True
            except Exception as e:
                logger.error(f"Error processing credit request: {e}")
                return False
        
        # Process messages until stopped
        while self._processor_running:
            try:
                self.processor.process_messages(request_handler, run_once=True)
                time.sleep(0.1)  # Short delay to prevent CPU spinning
            except Exception as e:
                logger.error(f"Error in request processor: {e}")
                time.sleep(1)  # Longer delay after error