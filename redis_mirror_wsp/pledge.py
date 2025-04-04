"""
Redis Mirror CE WSP Pledge Manager

This module provides functionality for WSPs to manage pledge operations,
including creating pledge requests, tracking pledge status, and handling
pledge responses from the Exchange.
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
    ValidationError,
    TimeoutError
)

# Configure logging
logger = logging.getLogger(__name__)


class PledgeRequest:
    """
    Represents a pledge request from a WSP to the Exchange.
    
    This class encapsulates the data and state of a pledge request.
    """
    
    STATUS_PENDING = "pending"
    STATUS_SUBMITTED = "submitted"
    STATUS_APPROVED = "approved"
    STATUS_REJECTED = "rejected"
    STATUS_FAILED = "failed"
    
    def __init__(
        self,
        request_id: str,
        user_id: str,
        asset: str,
        amount: float,
        custodian_id: str,
        chain: str,
        address: str,
        timestamp: Optional[int] = None
    ):
        """
        Initialize a pledge request.
        
        Args:
            request_id: Unique request identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to pledge
            custodian_id: Custodian identifier
            chain: Blockchain chain
            address: Blockchain address
            timestamp: Request timestamp (defaults to current time)
        """
        self.request_id = request_id
        self.user_id = user_id
        self.asset = asset
        self.amount = amount
        self.custodian_id = custodian_id
        self.chain = chain
        self.address = address
        self.timestamp = timestamp or int(time.time())
        self.status = self.STATUS_PENDING
        self.response_timestamp = None
        self.reject_reason = None
        self.vault_id = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the pledge request to a dictionary.
        
        Returns:
            Dictionary representation of the pledge request
        """
        return {
            "request_id": self.request_id,
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": self.amount,
            "custodian_id": self.custodian_id,
            "chain": self.chain,
            "address": self.address,
            "timestamp": self.timestamp,
            "status": self.status,
            "response_timestamp": self.response_timestamp,
            "reject_reason": self.reject_reason,
            "vault_id": self.vault_id
        }
    
    def to_stream_data(self) -> Dict[str, str]:
        """
        Convert the pledge request to stream data.
        
        Returns:
            Stream data dictionary
        """
        return {
            "type": "pledge_request",
            "request_id": self.request_id,
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": str(self.amount),
            "custodian_id": self.custodian_id,
            "chain": self.chain,
            "address": self.address,
            "timestamp": str(self.timestamp)
        }
    
    def mark_submitted(self):
        """Mark the pledge request as submitted."""
        self.status = self.STATUS_SUBMITTED
    
    def mark_approved(self, vault_id: str, timestamp: Optional[int] = None):
        """
        Mark the pledge request as approved.
        
        Args:
            vault_id: Vault identifier where the assets are stored
            timestamp: Response timestamp (defaults to current time)
        """
        self.status = self.STATUS_APPROVED
        self.vault_id = vault_id
        self.response_timestamp = timestamp or int(time.time())
    
    def mark_rejected(self, reason: str, timestamp: Optional[int] = None):
        """
        Mark the pledge request as rejected.
        
        Args:
            reason: Rejection reason
            timestamp: Response timestamp (defaults to current time)
        """
        self.status = self.STATUS_REJECTED
        self.reject_reason = reason
        self.response_timestamp = timestamp or int(time.time())
    
    def mark_failed(self, reason: str, timestamp: Optional[int] = None):
        """
        Mark the pledge request as failed.
        
        Args:
            reason: Failure reason
            timestamp: Response timestamp (defaults to current time)
        """
        self.status = self.STATUS_FAILED
        self.reject_reason = reason
        self.response_timestamp = timestamp or int(time.time())
    
    def is_pending(self) -> bool:
        """
        Check if the pledge request is pending.
        
        Returns:
            True if the pledge request is pending, False otherwise
        """
        return self.status == self.STATUS_PENDING
    
    def is_submitted(self) -> bool:
        """
        Check if the pledge request is submitted.
        
        Returns:
            True if the pledge request is submitted, False otherwise
        """
        return self.status == self.STATUS_SUBMITTED
    
    def is_approved(self) -> bool:
        """
        Check if the pledge request is approved.
        
        Returns:
            True if the pledge request is approved, False otherwise
        """
        return self.status == self.STATUS_APPROVED
    
    def is_rejected(self) -> bool:
        """
        Check if the pledge request is rejected.
        
        Returns:
            True if the pledge request is rejected, False otherwise
        """
        return self.status == self.STATUS_REJECTED
    
    def is_failed(self) -> bool:
        """
        Check if the pledge request is failed.
        
        Returns:
            True if the pledge request is failed, False otherwise
        """
        return self.status == self.STATUS_FAILED
    
    def is_completed(self) -> bool:
        """
        Check if the pledge request is completed (approved or rejected).
        
        Returns:
            True if the pledge request is completed, False otherwise
        """
        return self.is_approved() or self.is_rejected() or self.is_failed()
    
    def __str__(self) -> str:
        """String representation of the pledge request."""
        return (f"PledgeRequest(id={self.request_id}, user={self.user_id}, "
                f"asset={self.asset}, amount={self.amount}, status={self.status})")


class PledgeResponse:
    """
    Represents a pledge response from the Exchange to a WSP.
    
    This class encapsulates the data and state of a pledge response.
    """
    
    def __init__(
        self,
        request_id: str,
        user_id: str,
        asset: str,
        amount: float,
        status: str,
        timestamp: int,
        vault_id: Optional[str] = None,
        reject_reason: Optional[str] = None
    ):
        """
        Initialize a pledge response.
        
        Args:
            request_id: Request identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Amount pledged
            status: Response status (approved, rejected)
            timestamp: Response timestamp
            vault_id: Vault identifier (for approved requests)
            reject_reason: Rejection reason (for rejected requests)
        """
        self.request_id = request_id
        self.user_id = user_id
        self.asset = asset
        self.amount = amount
        self.status = status
        self.timestamp = timestamp
        self.vault_id = vault_id
        self.reject_reason = reject_reason
    
    @classmethod
    def from_stream_data(cls, data: Dict[str, str]) -> 'PledgeResponse':
        """
        Create a pledge response from stream data.
        
        Args:
            data: Stream data dictionary
            
        Returns:
            Pledge response instance
            
        Raises:
            ValidationError: If the data is invalid
        """
        try:
            # Extract required fields
            request_id = data.get("request_id")
            user_id = data.get("user_id")
            asset = data.get("asset")
            amount = float(data.get("amount", "0"))
            status = data.get("status")
            timestamp = int(data.get("timestamp", str(int(time.time()))))
            
            # Extract optional fields
            vault_id = data.get("vault_id")
            reject_reason = data.get("reject_reason")
            
            # Validate required fields
            if not request_id:
                raise ValidationError("Pledge response is missing request_id")
            
            if not user_id:
                raise ValidationError("Pledge response is missing user_id")
            
            if not asset:
                raise ValidationError("Pledge response is missing asset")
            
            if not status:
                raise ValidationError("Pledge response is missing status")
            
            # Create pledge response
            return cls(
                request_id=request_id,
                user_id=user_id,
                asset=asset,
                amount=amount,
                status=status,
                timestamp=timestamp,
                vault_id=vault_id,
                reject_reason=reject_reason
            )
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid pledge response data: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the pledge response to a dictionary.
        
        Returns:
            Dictionary representation of the pledge response
        """
        return {
            "request_id": self.request_id,
            "user_id": self.user_id,
            "asset": self.asset,
            "amount": self.amount,
            "status": self.status,
            "timestamp": self.timestamp,
            "vault_id": self.vault_id,
            "reject_reason": self.reject_reason
        }
    
    def __str__(self) -> str:
        """String representation of the pledge response."""
        return (f"PledgeResponse(id={self.request_id}, user={self.user_id}, "
                f"asset={self.asset}, amount={self.amount}, status={self.status})")


class PledgeManager:
    """
    Manages pledge operations for WSPs.
    
    This class handles creating pledge requests, tracking pledge status,
    and processing pledge responses from the Exchange.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        custodian_id: str,
        auto_start_processor: bool = True
    ):
        """
        Initialize the pledge manager.
        
        Args:
            connection_manager: Redis connection manager
            custodian_id: Custodian identifier
            auto_start_processor: Whether to automatically start the response processor
        """
        self.connection_manager = connection_manager
        self.custodian_id = custodian_id
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        self.replica_client = connection_manager.get_replica_client()
        
        # Create stream publisher for pledge requests
        self.request_stream = KeyManager.pledge_request_stream()
        self.publisher = StreamPublisher(self.wsp_client, self.request_stream)
        
        # Create stream processor for pledge responses
        self.response_stream = KeyManager.pledge_response_stream()
        self.processor = StreamProcessor(
            self.replica_client,
            self.response_stream,
            "pledge-manager",
            auto_create_group=True
        )
        
        # Pledge requests dictionary (request_id -> PledgeRequest)
        self.pledge_requests = {}
        
        # Pledge responses dictionary (request_id -> PledgeResponse)
        self.pledge_responses = {}
        
        # Pledge callbacks
        self.response_callbacks = []
        
        # Start response processor if auto_start is enabled
        self._processor_running = False
        if auto_start_processor:
            self.start_response_processor()
        
        logger.info("Pledge Manager initialized")
    
    def start_response_processor(self):
        """Start the pledge response processor."""
        if self._processor_running:
            return
        
        self._processor_running = True
        
        # Start processing in a separate thread
        import threading
        thread = threading.Thread(
            target=self._process_pledge_responses,
            daemon=True
        )
        thread.start()
        
        logger.info("Pledge response processor started")
    
    def stop_response_processor(self):
        """Stop the pledge response processor."""
        self._processor_running = False
        logger.info("Pledge response processor stopped")
    
    def add_response_callback(self, callback: Callable[[PledgeResponse], None]):
        """
        Add a callback for pledge responses.
        
        The callback will be called with the pledge response when a response is received.
        
        Args:
            callback: Callback function that takes a PledgeResponse as its argument
        """
        self.response_callbacks.append(callback)
        logger.debug("Pledge response callback added")
    
    def create_pledge_request(
        self,
        user_id: str,
        asset: str,
        amount: float,
        chain: str,
        address: str,
        request_id: Optional[str] = None,
        publish: bool = True
    ) -> PledgeRequest:
        """
        Create a pledge request.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to pledge
            chain: Blockchain chain
            address: Blockchain address
            request_id: Optional request identifier (generated if not provided)
            publish: Whether to publish the request to the pledge stream
            
        Returns:
            Created pledge request
            
        Raises:
            ValidationError: If the request is invalid
        """
        logger.info(f"Creating pledge request for user {user_id}, asset {asset}, amount {amount}")
        
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
        
        # Generate request ID if not provided
        if not request_id:
            timestamp = int(time.time())
            request_id = f"pledge-{timestamp}-{user_id}-{asset}"
        
        # Create pledge request
        request = PledgeRequest(
            request_id=request_id,
            user_id=user_id,
            asset=asset,
            amount=amount,
            custodian_id=self.custodian_id,
            chain=chain,
            address=address
        )
        
        # Store pledge request
        self.pledge_requests[request_id] = request
        
        # Publish pledge request if requested
        if publish:
            stream_data = request.to_stream_data()
            message_id = self.publisher.publish(stream_data)
            
            if not message_id:
                logger.error(f"Failed to publish pledge request: {request_id}")
                request.mark_failed("Failed to publish request")
            else:
                logger.info(f"Pledge request published: {request_id}")
                request.mark_submitted()
        
        return request
    
    def process_pledge_responses(
        self,
        callback: Optional[Callable[[PledgeResponse], None]] = None,
        run_once: bool = False
    ):
        """
        Process pledge responses from the Exchange.
        
        This method can be used to manually process pledge responses instead of
        using the automatic processor.
        
        Args:
            callback: Optional callback function for pledge responses
            run_once: If True, process one batch and return; if False, run continuously
        """
        if callback:
            self.add_response_callback(callback)
        
        def response_handler(message):
            try:
                # Check if this is a pledge response
                message_type = message.get("type")
                if message_type != "pledge_response":
                    logger.debug(f"Ignoring non-pledge response message: {message_type}")
                    return True
                
                # Parse pledge response
                response = PledgeResponse.from_stream_data(message)
                
                # Store pledge response
                self.pledge_responses[response.request_id] = response
                
                # Update pledge request status if it exists
                request = self.pledge_requests.get(response.request_id)
                if request:
                    if response.status == "approved":
                        request.mark_approved(response.vault_id, response.timestamp)
                    elif response.status == "rejected":
                        request.mark_rejected(response.reject_reason, response.timestamp)
                    
                    logger.info(f"Updated pledge request status: {request.status}")
                
                # Call callbacks
                for callback_fn in self.response_callbacks:
                    try:
                        callback_fn(response)
                    except Exception as e:
                        logger.error(f"Error in pledge response callback: {e}")
                
                return True
            except Exception as e:
                logger.error(f"Error processing pledge response: {e}")
                return False
        
        # Process pledge responses
        if run_once:
            self.processor._process_batch(response_handler, ">")
        else:
            self._processor_running = True
            while self._processor_running:
                self.processor._process_batch(response_handler, ">")
                time.sleep(0.1)
    
    def _process_pledge_responses(self):
        """Process pledge responses from the Exchange."""
        logger.info("Starting pledge response processor")
        
        def response_handler(message):
            try:
                # Check if this is a pledge response
                message_type = message.get("type")
                if message_type != "pledge_response":
                    logger.debug(f"Ignoring non-pledge response message: {message_type}")
                    return True
                
                # Parse pledge response
                response = PledgeResponse.from_stream_data(message)
                
                # Store pledge response
                self.pledge_responses[response.request_id] = response
                
                # Update pledge request status if it exists
                request = self.pledge_requests.get(response.request_id)
                if request:
                    if response.status == "approved":
                        request.mark_approved(response.vault_id, response.timestamp)
                    elif response.status == "rejected":
                        request.mark_rejected(response.reject_reason, response.timestamp)
                    
                    logger.info(f"Updated pledge request status: {request.status}")
                
                # Call callbacks
                for callback_fn in self.response_callbacks:
                    try:
                        callback_fn(response)
                    except Exception as e:
                        logger.error(f"Error in pledge response callback: {e}")
                
                return True
            except Exception as e:
                logger.error(f"Error processing pledge response: {e}")
                return False
        
        # Process pledge responses
        self.processor.process_messages(response_handler)
    
    def get_pledge_request(self, request_id: str) -> Optional[PledgeRequest]:
        """
        Get a pledge request by ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Pledge request if found, None otherwise
        """
        return self.pledge_requests.get(request_id)
    
    def get_pledge_requests(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[PledgeRequest]:
        """
        Get pledge requests.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            status: Optional status to filter by
            
        Returns:
            List of pledge requests
        """
        requests = list(self.pledge_requests.values())
        
        # Filter by user ID
        if user_id:
            requests = [r for r in requests if r.user_id == user_id]
        
        # Filter by asset
        if asset:
            requests = [r for r in requests if r.asset == asset]
        
        # Filter by status
        if status:
            requests = [r for r in requests if r.status == status]
        
        return requests
    
    def get_pledge_response(self, request_id: str) -> Optional[PledgeResponse]:
        """
        Get a pledge response by request ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Pledge response if found, None otherwise
        """
        return self.pledge_responses.get(request_id)
    
    def get_pledge_responses(
        self,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[PledgeResponse]:
        """
        Get pledge responses.
        
        Args:
            user_id: Optional user ID to filter by
            asset: Optional asset to filter by
            status: Optional status to filter by
            
        Returns:
            List of pledge responses
        """
        responses = list(self.pledge_responses.values())
        
        # Filter by user ID
        if user_id:
            responses = [r for r in responses if r.user_id == user_id]
        
        # Filter by asset
        if asset:
            responses = [r for r in responses if r.asset == asset]
        
        # Filter by status
        if status:
            responses = [r for r in responses if r.status == status]
        
        return responses
    
    def get_pending_pledge_requests(self, user_id: Optional[str] = None) -> List[PledgeRequest]:
        """
        Get pending pledge requests.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of pending pledge requests
        """
        return self.get_pledge_requests(user_id=user_id, status=PledgeRequest.STATUS_SUBMITTED)
    
    def get_approved_pledge_requests(self, user_id: Optional[str] = None) -> List[PledgeRequest]:
        """
        Get approved pledge requests.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of approved pledge requests
        """
        return self.get_pledge_requests(user_id=user_id, status=PledgeRequest.STATUS_APPROVED)
    
    def get_rejected_pledge_requests(self, user_id: Optional[str] = None) -> List[PledgeRequest]:
        """
        Get rejected pledge requests.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of rejected pledge requests
        """
        return self.get_pledge_requests(user_id=user_id, status=PledgeRequest.STATUS_REJECTED)