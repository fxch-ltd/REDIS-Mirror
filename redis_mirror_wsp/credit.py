"""
Redis Mirror CE WSP Credit Request Manager

This module provides functionality for WSP to create and manage credit requests
to the Exchange, including request creation, tracking, and response handling.
"""

import time
import uuid
import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union
from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    KeyManager,
    StreamPublisher,
    StreamProcessor,
    RedisMirrorError,
    ValidationError,
    TimeoutError
)

# Configure logging
logger = logging.getLogger(__name__)


class CreditRequest:
    """
    Represents a credit request from WSP to Exchange.
    
    This class encapsulates the data and state of a credit request.
    """
    
    STATUS_PENDING = "pending"
    STATUS_ACCEPTED = "accepted"
    STATUS_REJECTED = "rejected"
    STATUS_TIMEOUT = "timeout"
    
    def __init__(
        self,
        custodian_id: str,
        user_id: str,
        asset: str,
        c_change: str,
        ci: str,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        request_id: Optional[str] = None,
        timestamp: Optional[int] = None
    ):
        """
        Initialize a credit request.
        
        Args:
            custodian_id: Custodian identifier
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            c_change: Credit change amount (e.g., "+10.0", "-5.0")
            ci: Credit inventory amount after change
            chain: Optional blockchain chain for the asset
            address: Optional blockchain address for the asset
            request_id: Optional request identifier (generated if not provided)
            timestamp: Optional timestamp (current time if not provided)
        """
        self.custodian_id = custodian_id
        self.user_id = user_id
        self.asset = asset
        self.c_change = c_change
        self.ci = ci
        self.chain = chain
        self.address = address
        self.request_id = request_id or f"req-{uuid.uuid4()}"
        self.timestamp = timestamp or int(time.time())
        self.status = self.STATUS_PENDING
        self.reject_reason = ""
        self.response_timestamp = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the credit request to a dictionary for publishing.
        
        Returns:
            Dictionary representation of the credit request
        """
        result = {
            "custodian": self.custodian_id,
            "request_id": self.request_id,
            "uid": self.user_id,
            "timestamp": str(self.timestamp),
            "asset": self.asset,
            "c_change": self.c_change,
            "ci": self.ci
        }
        
        # Add optional fields if present
        if self.chain:
            result["chain"] = self.chain
        
        if self.address:
            result["address"] = self.address
        
        return result
    
    def update_from_response(self, response: Dict[str, str]):
        """
        Update the credit request state from a response.
        
        Args:
            response: Response dictionary from the Exchange
        """
        self.status = response.get("status", self.STATUS_PENDING)
        self.reject_reason = response.get("reject_reason", "")
        self.response_timestamp = int(time.time())
    
    def is_increase(self) -> bool:
        """
        Check if this is a credit increase request.
        
        Returns:
            True if this is a credit increase request, False otherwise
        """
        return self.c_change.startswith("+")
    
    def is_decrease(self) -> bool:
        """
        Check if this is a credit decrease request.
        
        Returns:
            True if this is a credit decrease request, False otherwise
        """
        return self.c_change.startswith("-")
    
    def is_pending(self) -> bool:
        """
        Check if the request is pending.
        
        Returns:
            True if the request is pending, False otherwise
        """
        return self.status == self.STATUS_PENDING
    
    def is_accepted(self) -> bool:
        """
        Check if the request was accepted.
        
        Returns:
            True if the request was accepted, False otherwise
        """
        return self.status == self.STATUS_ACCEPTED
    
    def is_rejected(self) -> bool:
        """
        Check if the request was rejected.
        
        Returns:
            True if the request was rejected, False otherwise
        """
        return self.status == self.STATUS_REJECTED
    
    def is_timeout(self) -> bool:
        """
        Check if the request timed out.
        
        Returns:
            True if the request timed out, False otherwise
        """
        return self.status == self.STATUS_TIMEOUT
    
    def __str__(self) -> str:
        """String representation of the credit request."""
        return (f"CreditRequest(custodian={self.custodian_id}, user={self.user_id}, "
                f"asset={self.asset}, change={self.c_change}, status={self.status})")


class CreditRequestManager:
    """
    Manages credit requests from WSP to Exchange.
    
    This class provides functionality for creating, tracking, and processing
    credit requests and responses.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        custodian_id: str,
        response_timeout: int = 30,
        auto_start_processor: bool = True
    ):
        """
        Initialize the credit request manager.
        
        Args:
            connection_manager: Redis connection manager
            custodian_id: Custodian identifier
            response_timeout: Timeout in seconds for waiting for responses
            auto_start_processor: Whether to automatically start the response processor
        """
        self.connection_manager = connection_manager
        self.custodian_id = custodian_id
        self.response_timeout = response_timeout
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        
        # Create stream publisher for credit requests
        self.request_stream = KeyManager.credit_request_stream()
        self.publisher = StreamPublisher(self.wsp_client, self.request_stream)
        
        # Create stream processor for credit responses
        self.response_stream = KeyManager.credit_response_stream()
        self.processor = StreamProcessor(
            self.wsp_client,
            self.response_stream,
            f"credit-response-{custodian_id}",
            auto_create_group=True
        )
        
        # Pending requests dictionary (request_id -> CreditRequest)
        self.pending_requests = {}
        
        # Response callbacks
        self.response_callbacks = []
        
        # Start response processor if auto_start is enabled
        self._processor_running = False
        if auto_start_processor:
            self.start_response_processor()
    
    def create_credit_request(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        ci: Union[str, float],
        chain: Optional[str] = None,
        address: Optional[str] = None
    ) -> CreditRequest:
        """
        Create a new credit request.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit change amount (positive for increase, negative for decrease)
            ci: Credit inventory amount after change
            chain: Optional blockchain chain for the asset
            address: Optional blockchain address for the asset
            
        Returns:
            Created credit request
            
        Raises:
            ValidationError: If the request is invalid
        """
        # Format amount as string with sign
        if isinstance(amount, float) or isinstance(amount, int):
            if amount >= 0:
                c_change = f"+{amount}"
            else:
                c_change = str(amount)  # Already has negative sign
        else:
            # Ensure string has sign
            amount_str = str(amount)
            if not (amount_str.startswith("+") or amount_str.startswith("-")):
                if float(amount_str) >= 0:
                    c_change = f"+{amount_str}"
                else:
                    c_change = amount_str
            else:
                c_change = amount_str
        
        # Format CI as string
        ci_str = str(ci)
        
        # Validate request
        self._validate_request(user_id, asset, c_change, ci_str, chain, address)
        
        # Create credit request
        request = CreditRequest(
            custodian_id=self.custodian_id,
            user_id=user_id,
            asset=asset,
            c_change=c_change,
            ci=ci_str,
            chain=chain,
            address=address
        )
        
        return request
    
    def send_credit_request(
        self,
        request: CreditRequest,
        wait_for_response: bool = False,
        timeout: Optional[int] = None
    ) -> Optional[CreditRequest]:
        """
        Send a credit request to the Exchange.
        
        Args:
            request: Credit request to send
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response (default: response_timeout)
            
        Returns:
            Updated credit request if wait_for_response is True, None otherwise
            
        Raises:
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        # Add request to pending requests
        self.pending_requests[request.request_id] = request
        
        # Publish request
        request_dict = request.to_dict()
        logger.info(f"Sending credit request: {request}")
        
        message_id = self.publisher.publish(request_dict)
        
        if not message_id:
            logger.error(f"Failed to publish credit request: {request}")
            del self.pending_requests[request.request_id]
            raise RedisMirrorError(f"Failed to publish credit request: {request}")
        
        logger.debug(f"Published credit request with ID: {message_id}")
        
        # Wait for response if requested
        if wait_for_response:
            timeout_value = timeout or self.response_timeout
            start_time = time.time()
            
            while time.time() - start_time < timeout_value:
                # Check if request has been updated
                if not request.is_pending():
                    return request
                
                # Wait a bit before checking again
                time.sleep(0.1)
            
            # Timeout
            request.status = CreditRequest.STATUS_TIMEOUT
            logger.warning(f"Timeout waiting for response to credit request: {request}")
            raise TimeoutError(f"Timeout waiting for response to credit request: {request}", 
                              operation="credit_request", timeout=timeout_value)
        
        return None
    
    def request_credit_increase(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        ci: Union[str, float],
        chain: Optional[str] = None,
        address: Optional[str] = None,
        wait_for_response: bool = False,
        timeout: Optional[int] = None
    ) -> Optional[CreditRequest]:
        """
        Request a credit increase from the Exchange.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit increase amount (positive)
            ci: Credit inventory amount after increase
            chain: Optional blockchain chain for the asset
            address: Optional blockchain address for the asset
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response
            
        Returns:
            Credit request if wait_for_response is True, None otherwise
            
        Raises:
            ValidationError: If the request is invalid
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        # Ensure amount is positive
        if isinstance(amount, str) and not amount.startswith("+"):
            amount = f"+{amount}"
        elif isinstance(amount, (int, float)) and amount < 0:
            raise ValidationError(f"Credit increase amount must be positive: {amount}")
        
        # Create and send request
        request = self.create_credit_request(user_id, asset, amount, ci, chain, address)
        return self.send_credit_request(request, wait_for_response, timeout)
    
    def request_credit_decrease(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        ci: Union[str, float],
        wait_for_response: bool = False,
        timeout: Optional[int] = None
    ) -> Optional[CreditRequest]:
        """
        Request a credit decrease from the Exchange.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit decrease amount (negative)
            ci: Credit inventory amount after decrease
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response
            
        Returns:
            Credit request if wait_for_response is True, None otherwise
            
        Raises:
            ValidationError: If the request is invalid
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        # Ensure amount is negative
        if isinstance(amount, str):
            if not amount.startswith("-"):
                if amount.startswith("+"):
                    amount = f"-{amount[1:]}"
                else:
                    amount = f"-{amount}"
        elif isinstance(amount, (int, float)) and amount > 0:
            amount = -amount
        
        # Create and send request
        request = self.create_credit_request(user_id, asset, amount, ci)
        return self.send_credit_request(request, wait_for_response, timeout)
    
    def get_pending_requests(self) -> List[CreditRequest]:
        """
        Get all pending credit requests.
        
        Returns:
            List of pending credit requests
        """
        return [req for req in self.pending_requests.values() if req.is_pending()]
    
    def get_request(self, request_id: str) -> Optional[CreditRequest]:
        """
        Get a credit request by ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Credit request if found, None otherwise
        """
        return self.pending_requests.get(request_id)
    
    def add_response_callback(self, callback: Callable[[CreditRequest], None]):
        """
        Add a callback for credit responses.
        
        The callback will be called with the updated credit request when a response is received.
        
        Args:
            callback: Callback function
        """
        self.response_callbacks.append(callback)
    
    def start_response_processor(self):
        """Start the response processor."""
        if self._processor_running:
            return
        
        self._processor_running = True
        
        # Start processing in a separate thread
        import threading
        thread = threading.Thread(
            target=self._process_responses,
            daemon=True
        )
        thread.start()
    
    def stop_response_processor(self):
        """Stop the response processor."""
        self._processor_running = False
    
    def _process_responses(self):
        """Process credit responses."""
        logger.info("Starting credit response processor")
        
        def response_handler(message):
            try:
                # Extract request ID
                request_id = message.get("request_id")
                if not request_id:
                    logger.warning(f"Received credit response without request ID: {message}")
                    return True
                
                # Find matching request
                request = self.pending_requests.get(request_id)
                if not request:
                    logger.warning(f"Received credit response for unknown request ID: {request_id}")
                    return True
                
                # Update request from response
                request.update_from_response(message)
                
                # Call callbacks
                for callback in self.response_callbacks:
                    try:
                        callback(request)
                    except Exception as e:
                        logger.error(f"Error in response callback: {e}")
                
                logger.info(f"Processed credit response for request {request_id}: {request.status}")
                
                return True
            except Exception as e:
                logger.error(f"Error processing credit response: {e}")
                return False
        
        # Process messages until stopped
        while self._processor_running:
            try:
                self.processor.process_messages(response_handler, run_once=True)
                time.sleep(0.1)  # Short delay to prevent CPU spinning
            except Exception as e:
                logger.error(f"Error in response processor: {e}")
                time.sleep(1)  # Longer delay after error
    
    def _validate_request(
        self,
        user_id: str,
        asset: str,
        c_change: str,
        ci: str,
        chain: Optional[str],
        address: Optional[str]
    ):
        """
        Validate a credit request.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            c_change: Credit change amount
            ci: Credit inventory amount
            chain: Blockchain chain
            address: Blockchain address
            
        Raises:
            ValidationError: If the request is invalid
        """
        # Validate user ID
        if not user_id:
            raise ValidationError("User ID is required", "user_id")
        
        # Validate asset
        if not asset:
            raise ValidationError("Asset is required", "asset")
        
        # Validate c_change
        if not c_change:
            raise ValidationError("Credit change amount is required", "c_change")
        
        try:
            # Ensure c_change has a sign and is a valid number
            if not (c_change.startswith("+") or c_change.startswith("-")):
                raise ValidationError(
                    "Credit change amount must start with + or -", 
                    "c_change", 
                    c_change
                )
            
            float(c_change)  # Validate it's a number
        except ValueError:
            raise ValidationError(
                "Credit change amount must be a valid number", 
                "c_change", 
                c_change
            )
        
        # Validate CI
        if not ci:
            raise ValidationError("Credit inventory amount is required", "ci")
        
        try:
            float(ci)  # Validate it's a number
        except ValueError:
            raise ValidationError(
                "Credit inventory amount must be a valid number", 
                "ci", 
                ci
            )
        
        # Validate chain and address for credit increases
        if c_change.startswith("+"):
            if not chain:
                raise ValidationError(
                    "Blockchain chain is required for credit increases", 
                    "chain"
                )
            
            if not address:
                raise ValidationError(
                    "Blockchain address is required for credit increases", 
                    "address"
                )