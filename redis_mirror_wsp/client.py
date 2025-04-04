"""
Redis Mirror CE WSP Client

This module provides the main client interface for WSP integration with Redis Mirror CE,
serving as a unified API for all WSP operations including credit requests, settlement
processing, pledge management, and vault operations.
"""

import logging
from typing import Dict, Any, Optional, List, Callable, Union

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    Configuration,
    RedisMirrorError,
    ValidationError,
    TimeoutError
)
from .credit import CreditRequest, CreditResponse, CreditRequestManager
from .settlement import SettlementReport, SettlementConfirmation, SettlementClient
from .pledge import PledgeRequest, PledgeResponse, PledgeManager
from .vault import VaultAsset, VaultManager

# Configure logging
logger = logging.getLogger(__name__)


class WSPClient:
    """
    Main client for WSP integration with Redis Mirror CE.
    
    This class provides a unified API for all WSP operations, including 
    credit requests, settlement processing, pledge management, and vault operations.
    
    Attributes:
        connection_manager: Redis connection manager
        config: Configuration object
        custodian_id: Custodian identifier
        credit_manager: Credit request manager
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        config: Configuration,
        custodian_id: str,
        response_timeout: int = 30,
        auto_start_processors: bool = True
    ):
        """
        Initialize the WSP client.
        
        Args:
            connection_manager: Redis connection manager
            config: Configuration object
            custodian_id: Custodian identifier
            response_timeout: Timeout in seconds for waiting for responses
            auto_start_processors: Whether to automatically start stream processors
        """
        # Initialize components
        self.connection_manager = connection_manager
        self.config = config
        self.custodian_id = custodian_id
        self.response_timeout = response_timeout
        
        # Create component managers
        self.credit_manager = CreditRequestManager(
            connection_manager,
            custodian_id,
            response_timeout=response_timeout,
            auto_start_processor=auto_start_processors
        )
        # Initialize settlement client
        self.settlement_client = SettlementClient(
            connection_manager,
            custodian_id,
            auto_start_processor=auto_start_processors
        )
        
        # Initialize pledge manager
        self.pledge_manager = PledgeManager(
            connection_manager,
            custodian_id,
            auto_start_processor=auto_start_processors
        )
        
        # Initialize vault manager
        self.vault_manager = VaultManager(
            connection_manager,
            custodian_id
        )
        
        logger.info(f"WSP Client initialized for custodian: {custodian_id}")
    
    def close(self):
        """
        Close all connections and stop all processors.
        
        This method should be called when the client is no longer needed
        to ensure proper cleanup of resources.
        """
        logger.info("Closing WSP Client...")
        
        # Stop credit manager processor
        if self.credit_manager:
            self.credit_manager.stop_response_processor()
        
        # Stop settlement client processor
        if self.settlement_client:
            self.settlement_client.stop_settlement_processor()
        
        # Stop pledge manager processor
        if self.pledge_manager:
            self.pledge_manager.stop_response_processor()
        
        logger.info("WSP Client closed")
    
    #
    # Credit Operations
    #
    
    def add_credit_response_callback(self, callback: Callable[[CreditRequest], None]):
        """
        Add a callback for credit responses.
        
        The callback will be called with the updated credit request when a response is received.
        
        Args:
            callback: Callback function that takes a CreditRequest as its argument
        """
        self.credit_manager.add_response_callback(callback)
        logger.debug("Credit response callback added")
    
    def request_credit_increase(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        ci: Optional[Union[str, float]] = None,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        wait_for_response: bool = False,
        timeout: Optional[int] = None,
        **kwargs
    ) -> Optional[CreditRequest]:
        """
        Request a credit increase from the Exchange.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit increase amount (positive)
            ci: Credit inventory amount after increase (calculated if not provided)
            chain: Blockchain chain for the asset (required for credit increases)
            address: Blockchain address for the asset (required for credit increases)
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response
            **kwargs: Additional arguments to pass to the credit manager
            
        Returns:
            Credit request if wait_for_response is True, None otherwise
            
        Raises:
            ValidationError: If the request is invalid
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        logger.info(f"Requesting credit increase for user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is positive
        if isinstance(amount, str) and not amount.startswith("+"):
            amount = f"+{amount}"
        elif isinstance(amount, (int, float)) and amount < 0:
            raise ValidationError(f"Credit increase amount must be positive: {amount}")
        
        # Calculate CI if not provided
        if ci is None:
            # In a real implementation, we would get the current CI from somewhere
            # For now, we'll just use the amount as the CI
            ci = float(amount) if isinstance(amount, str) and amount.startswith("+") else float(amount)
            logger.debug(f"CI not provided, using calculated value: {ci}")
        
        # Validate required parameters for credit increase
        if not chain:
            raise ValidationError("Blockchain chain is required for credit increases")
        
        if not address:
            raise ValidationError("Blockchain address is required for credit increases")
        
        # Delegate to credit manager
        return self.credit_manager.request_credit_increase(
            user_id=user_id,
            asset=asset,
            amount=amount,
            ci=ci,
            chain=chain,
            address=address,
            wait_for_response=wait_for_response,
            timeout=timeout or self.response_timeout
        )
    
    def request_credit_decrease(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        ci: Optional[Union[str, float]] = None,
        wait_for_response: bool = False,
        timeout: Optional[int] = None,
        **kwargs
    ) -> Optional[CreditRequest]:
        """
        Request a credit decrease from the Exchange.
        
        Args:
            user_id: User identifier
            asset: Asset identifier (e.g., BTC, ETH, USDT)
            amount: Credit decrease amount (negative)
            ci: Credit inventory amount after decrease (calculated if not provided)
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response
            **kwargs: Additional arguments to pass to the credit manager
            
        Returns:
            Credit request if wait_for_response is True, None otherwise
            
        Raises:
            ValidationError: If the request is invalid
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        logger.info(f"Requesting credit decrease for user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is negative
        if isinstance(amount, str):
            if not amount.startswith("-"):
                if amount.startswith("+"):
                    amount = f"-{amount[1:]}"
                else:
                    amount = f"-{amount}"
        elif isinstance(amount, (int, float)) and amount > 0:
            amount = -amount
        
        # Calculate CI if not provided
        if ci is None:
            # In a real implementation, we would get the current CI from somewhere
            # For now, we'll just use the amount as the CI
            ci = float(amount) if isinstance(amount, str) else amount
            ci = abs(ci)  # Make sure it's positive
            logger.debug(f"CI not provided, using calculated value: {ci}")
        
        # Delegate to credit manager
        return self.credit_manager.request_credit_decrease(
            user_id=user_id,
            asset=asset,
            amount=amount,
            ci=ci,
            wait_for_response=wait_for_response,
            timeout=timeout or self.response_timeout
        )
    
    def get_pending_credit_requests(self) -> List[CreditRequest]:
        """
        Get all pending credit requests.
        
        Returns:
            List of pending credit requests
        """
        return self.credit_manager.get_pending_requests()
    
    def get_credit_request(self, request_id: str) -> Optional[CreditRequest]:
        """
        Get a credit request by ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Credit request if found, None otherwise
        """
        return self.credit_manager.get_request(request_id)
    
    #
    # Settlement Operations (to be implemented)
    #
    
    def add_settlement_callback(self, callback: Callable[[SettlementReport], None]):
        """
        Add a callback for settlement reports.
        
        The callback will be called with the settlement report when a report is received.
        
        Args:
            callback: Callback function that takes a SettlementReport as its argument
        """
        self.settlement_client.add_settlement_callback(callback)
        logger.debug("Settlement callback added")
    
    def process_settlement_reports(self, callback: Optional[Callable[[SettlementReport], None]] = None):
        """
        Process settlement reports from the Exchange.
        
        Args:
            callback: Optional callback function for settlement reports
        """
        logger.info("Processing settlement reports")
        
        # Add callback if provided
        if callback:
            self.add_settlement_callback(callback)
        
        # Process settlement reports
        self.settlement_client.process_settlement_reports(run_once=True)
    
    def confirm_settlement(
        self,
        settlement_id: str,
        status: str = "completed",
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Confirm a settlement was completed.
        
        Args:
            settlement_id: Settlement ID to confirm
            status: Settlement status (completed, failed, etc.)
            details: Optional details about the settlement
            
        Returns:
            True if the confirmation was sent successfully, False otherwise
        """
        logger.info(f"Confirming settlement: {settlement_id}, status: {status}")
        return self.settlement_client.confirm_settlement(settlement_id, status, details)
    
    def get_settlement_report(self, report_id: str) -> Optional[SettlementReport]:
        """
        Get a settlement report by ID.
        
        Args:
            report_id: Report identifier
            
        Returns:
            Settlement report if found, None otherwise
        """
        return self.settlement_client.get_settlement_report(report_id)
    
    def get_settlement_reports(
        self,
        user_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[SettlementReport]:
        """
        Get settlement reports.
        
        Args:
            user_id: Optional user ID to filter by
            status: Optional status to filter by
            
        Returns:
            List of settlement reports
        """
        return self.settlement_client.get_settlement_reports(user_id, status)
    
    #
    # Pledge Operations (to be implemented)
    #
    
    def add_pledge_response_callback(self, callback: Callable[[PledgeResponse], None]):
        """
        Add a callback for pledge responses.
        
        The callback will be called with the pledge response when a response is received.
        
        Args:
            callback: Callback function that takes a PledgeResponse as its argument
        """
        self.pledge_manager.add_response_callback(callback)
        logger.debug("Pledge response callback added")
    
    def create_pledge(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        address: str,
        chain: str,
        request_id: Optional[str] = None,
        wait_for_response: bool = False,
        timeout: Optional[int] = None,
        **kwargs
    ) -> Optional[PledgeRequest]:
        """
        Create a new pledge request.
        
        Args:
            user_id: User ID to create pledge for
            asset: Asset being pledged
            amount: Amount being pledged
            address: Blockchain address for the pledge
            chain: Blockchain chain for the pledge
            request_id: Optional request identifier (generated if not provided)
            wait_for_response: Whether to wait for a response
            timeout: Timeout in seconds for waiting for a response
            **kwargs: Additional arguments for the pledge
            
        Returns:
            Pledge request if wait_for_response is True, None otherwise
            
        Raises:
            ValidationError: If the request is invalid
            TimeoutError: If wait_for_response is True and no response is received within timeout
        """
        logger.info(f"Creating pledge for user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is positive
        if isinstance(amount, str):
            try:
                amount = float(amount)
            except ValueError:
                raise ValidationError(f"Invalid amount: {amount}")
        
        if amount <= 0:
            raise ValidationError(f"Pledge amount must be positive: {amount}")
        
        # Create pledge request
        request = self.pledge_manager.create_pledge_request(
            user_id=user_id,
            asset=asset,
            amount=amount,
            chain=chain,
            address=address,
            request_id=request_id,
            publish=True
        )
        
        # Wait for response if requested
        if wait_for_response:
            # In a real implementation, we would wait for the response
            # For now, we'll just return the request
            logger.info(f"Waiting for pledge response for request: {request.request_id}")
            
            # TODO: Implement waiting for response
            # This would involve polling or using a callback
            
            # For now, just return the request
            return request
        
        return request
    
    def process_pledge_responses(
        self,
        callback: Optional[Callable[[PledgeResponse], None]] = None,
        run_once: bool = False
    ):
        """
        Process pledge responses from the Exchange.
        
        Args:
            callback: Optional callback function for pledge responses
            run_once: If True, process one batch and return; if False, run continuously
        """
        logger.info("Processing pledge responses")
        
        # Add callback if provided
        if callback:
            self.add_pledge_response_callback(callback)
        
        # Process pledge responses
        self.pledge_manager.process_pledge_responses(run_once=run_once)
    
    def get_pledge_request(self, request_id: str) -> Optional[PledgeRequest]:
        """
        Get a pledge request by ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Pledge request if found, None otherwise
        """
        return self.pledge_manager.get_pledge_request(request_id)
    
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
        return self.pledge_manager.get_pledge_requests(user_id, asset, status)
    
    def get_pending_pledge_requests(self, user_id: Optional[str] = None) -> List[PledgeRequest]:
        """
        Get pending pledge requests.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of pending pledge requests
        """
        return self.pledge_manager.get_pending_pledge_requests(user_id)
    
    #
    # Vault Operations
    #
    
    def add_asset_to_vault(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        chain: str,
        address: str,
        vault_id: Optional[str] = None,
        status: str = "locked",
        pledge_request_id: Optional[str] = None
    ) -> VaultAsset:
        """
        Add an asset to the vault.
        
        Args:
            user_id: User ID to add asset for
            asset: Asset being added
            amount: Amount being added
            chain: Blockchain chain for the asset
            address: Blockchain address for the asset
            vault_id: Optional vault identifier (generated if not provided)
            status: Asset status (locked, unlocked, pending)
            pledge_request_id: Associated pledge request ID
            
        Returns:
            Added vault asset
            
        Raises:
            ValidationError: If the asset is invalid
        """
        logger.info(f"Adding asset to vault: user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is positive
        if isinstance(amount, str):
            try:
                amount = float(amount)
            except ValueError:
                raise ValidationError(f"Invalid amount: {amount}")
        
        if amount <= 0:
            raise ValidationError(f"Asset amount must be positive: {amount}")
        
        # Add asset to vault
        return self.vault_manager.add_asset(
            user_id=user_id,
            asset=asset,
            amount=amount,
            chain=chain,
            address=address,
            vault_id=vault_id,
            status=status,
            pledge_request_id=pledge_request_id
        )
    
    def lock_assets(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        chain: str,
        address: str
    ) -> List[VaultAsset]:
        """
        Lock assets in the vault.
        
        Args:
            user_id: User ID to lock assets for
            asset: Asset being locked
            amount: Amount being locked
            chain: Blockchain chain for the asset
            address: Blockchain address for the asset
            
        Returns:
            List of locked vault assets
            
        Raises:
            ValidationError: If the asset is invalid
        """
        logger.info(f"Locking assets: user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is positive
        if isinstance(amount, str):
            try:
                amount = float(amount)
            except ValueError:
                raise ValidationError(f"Invalid amount: {amount}")
        
        if amount <= 0:
            raise ValidationError(f"Asset amount must be positive: {amount}")
        
        # Add asset to vault with locked status
        asset_obj = self.vault_manager.add_asset(
            user_id=user_id,
            asset=asset,
            amount=amount,
            chain=chain,
            address=address,
            status=VaultAsset.STATUS_LOCKED
        )
        
        return [asset_obj]
    
    def unlock_assets(
        self,
        user_id: str,
        asset: str,
        amount: Union[str, float],
        credit_inventory: Optional[Dict[str, float]] = None
    ) -> List[VaultAsset]:
        """
        Unlock assets from the vault using the Lobster Basket Policy.
        
        Args:
            user_id: User ID to unlock assets for
            asset: Asset being unlocked
            amount: Amount being unlocked
            credit_inventory: Optional credit inventory for the user
            
        Returns:
            List of unlocked vault assets
            
        Raises:
            ValidationError: If the asset is invalid or cannot be unlocked
        """
        logger.info(f"Unlocking assets: user {user_id}, asset {asset}, amount {amount}")
        
        # Ensure amount is positive
        if isinstance(amount, str):
            try:
                amount = float(amount)
            except ValueError:
                raise ValidationError(f"Invalid amount: {amount}")
        
        if amount <= 0:
            raise ValidationError(f"Asset amount must be positive: {amount}")
        
        # Unlock assets using Lobster Basket Policy
        success, unlocked_assets, reason = self.vault_manager.unlock_assets(
            user_id=user_id,
            asset=asset,
            amount=amount,
            credit_inventory=credit_inventory
        )
        
        if not success:
            raise ValidationError(f"Failed to unlock assets: {reason}")
        
        return unlocked_assets
    
    def get_vault_asset(self, vault_id: str) -> Optional[VaultAsset]:
        """
        Get a vault asset by ID.
        
        Args:
            vault_id: Vault identifier
            
        Returns:
            Vault asset if found, None otherwise
        """
        return self.vault_manager.get_asset(vault_id)
    
    def get_vault_assets(
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
        return self.vault_manager.get_assets(user_id, asset, status)
    
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
        return self.vault_manager.get_locked_assets(user_id, asset)
    
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
        return self.vault_manager.get_unlocked_assets(user_id, asset)
    
    def get_total_locked_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total locked amount for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total locked amount
        """
        return self.vault_manager.get_total_locked_amount(user_id, asset)
    
    def get_total_unlocked_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total unlocked amount for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total unlocked amount
        """
        return self.vault_manager.get_total_unlocked_amount(user_id, asset)
    
    def get_total_amount(self, user_id: str, asset: str) -> float:
        """
        Get the total amount (locked + unlocked) for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Total amount
        """
        return self.vault_manager.get_total_amount(user_id, asset)