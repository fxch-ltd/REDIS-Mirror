"""
Redis Mirror CE Exchange Client

This module provides the main client interface for Exchange integration with Redis Mirror CE,
serving as a unified API for all Exchange operations including credit management, settlement
processing, and account integration.
"""

import logging
from typing import Dict, Any, Optional, List, Callable, Union, Tuple

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    Configuration,
    RedisMirrorError,
    ValidationError,
    TimeoutError
)
from .credit import CreditInventory, CreditValidator, CreditInventoryManager, CreditManager
from .settlement import SettlementReport, SettlementConfirmation, SettlementManager

# Configure logging
logger = logging.getLogger(__name__)


class ExchangeClient:
    """
    Main client for Exchange integration with Redis Mirror CE.
    
    This class provides a unified API for all Exchange operations, including
    credit management, settlement processing, and account integration.
    
    Attributes:
        connection_manager: Redis connection manager
        config: Configuration object
        credit_manager: Credit manager for processing credit requests
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        config: Configuration,
        approved_custodians: Optional[List[str]] = None,
        approved_assets: Optional[List[str]] = None,
        auto_start_processors: bool = True,
        **kwargs
    ):
        """
        Initialize the Exchange client.
        
        Args:
            connection_manager: Redis connection manager
            config: Configuration object
            approved_custodians: Optional list of approved custodian IDs
            approved_assets: Optional list of approved asset IDs
            auto_start_processors: Whether to automatically start stream processors
            **kwargs: Additional arguments to pass to component managers
        """
        # Initialize components
        self.connection_manager = connection_manager
        self.config = config
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        self.replica_client = connection_manager.get_replica_client()
        
        # Create inventory manager
        self.inventory_manager = CreditInventoryManager(self.wsp_client)
        
        # Create validator
        self.validator = CreditValidator(
            approved_custodians=approved_custodians,
            approved_assets=approved_assets
        )
        
        # Create credit manager
        self.credit_manager = CreditManager(
            connection_manager,
            inventory_manager=self.inventory_manager,
            validator=self.validator,
            approved_custodians=approved_custodians,
            approved_assets=approved_assets,
            auto_start_processor=auto_start_processors
        )
        
        # Create settlement manager
        self.settlement_manager = SettlementManager(
            connection_manager,
            inventory_manager=self.inventory_manager,
            auto_start_processor=auto_start_processors
        )
        
        # These will be implemented later
        self.inventory_processor = None
        self.account_integration = None
        
        logger.info("Exchange Client initialized")
    
    def close(self):
        """
        Close all connections and stop all processors.
        
        This method should be called when the client is no longer needed
        to ensure proper cleanup of resources.
        """
        logger.info("Closing Exchange Client...")
        
        # Stop credit manager processor
        if self.credit_manager:
            self.credit_manager.stop_request_processor()
        
        # Stop settlement manager processor
        if self.settlement_manager:
            self.settlement_manager.stop_confirmation_processor()
        
        logger.info("Exchange Client closed")
    
    #
    # Credit Operations
    #
    
    def start_credit_request_processor(self):
        """
        Start processing credit requests.
        
        This method starts the credit request processor, which listens for
        credit requests from WSPs and processes them.
        """
        logger.info("Starting credit request processor")
        self.credit_manager.start_request_processor()
    
    def stop_credit_request_processor(self):
        """
        Stop processing credit requests.
        
        This method stops the credit request processor.
        """
        logger.info("Stopping credit request processor")
        self.credit_manager.stop_request_processor()
    
    def add_credit_request_handler(
        self,
        callback: Callable[[Dict[str, str], Dict[str, str]], None]
    ):
        """
        Add a callback for handling credit requests.
        
        The callback will be called with the request and response dictionaries
        when a credit request is processed.
        
        Args:
            callback: Callback function that takes request and response dictionaries
        """
        self.credit_manager.add_request_callback(callback)
        logger.debug("Credit request callback added")
    
    def process_credit_request(
        self,
        request: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Process a credit request manually.
        
        This method allows for manual processing of credit requests,
        bypassing the automatic processor.
        
        Args:
            request: Credit request dictionary
            
        Returns:
            Credit response dictionary
        """
        logger.info(f"Manually processing credit request: {request}")
        return self.credit_manager.process_credit_request(request)
    
    def get_credit_inventory(
        self,
        user_id: str,
        asset: str
    ) -> Optional[CreditInventory]:
        """
        Get a credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            
        Returns:
            Credit inventory if found, None otherwise
        """
        return self.inventory_manager.get_inventory(user_id, asset)
    
    def update_credit_inventory(
        self,
        user_id: str,
        asset: str,
        amount: float,
        is_increase: bool = True
    ) -> Optional[CreditInventory]:
        """
        Update a credit inventory for a user and asset.
        
        Args:
            user_id: User identifier
            asset: Asset identifier
            amount: Amount to update by (positive value)
            is_increase: Whether this is an increase (True) or decrease (False)
            
        Returns:
            Updated credit inventory if successful, None otherwise
        """
        logger.info(f"Updating credit inventory for user {user_id}, asset {asset}, amount {amount}, increase: {is_increase}")
        
        if amount < 0:
            raise ValidationError("Amount must be positive, use is_increase=False for decreases")
        
        if is_increase:
            return self.inventory_manager.increase_inventory(user_id, asset, amount)
        else:
            return self.inventory_manager.decrease_inventory(user_id, asset, amount)
    
    def validate_credit_request(
        self,
        custodian: str,
        user_id: str,
        asset: str,
        amount: float,
        ci: float,
        chain: Optional[str] = None,
        address: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Validate a credit request without processing it.
        
        Args:
            custodian: Custodian identifier
            user_id: User identifier
            asset: Asset identifier
            amount: Credit change amount (positive for increase, negative for decrease)
            ci: Credit inventory amount after change
            chain: Blockchain chain (required for increases)
            address: Blockchain address (required for increases)
            
        Returns:
            Tuple of (is_valid, reject_reason)
        """
        logger.info(f"Validating credit request for user {user_id}, asset {asset}, amount {amount}")
        
        if amount > 0:
            return self.validator.validate_increase(
                custodian=custodian,
                user_id=user_id,
                asset=asset,
                amount=amount,
                ci=ci,
                chain=chain,
                address=address,
                inventory_manager=self.inventory_manager
            )
        else:
            return self.validator.validate_decrease(
                custodian=custodian,
                user_id=user_id,
                asset=asset,
                amount=amount,
                ci=ci,
                inventory_manager=self.inventory_manager
            )
    
    #
    # Settlement Operations
    #
    
    def add_settlement_confirmation_callback(self, callback: Callable[[SettlementConfirmation], None]):
        """
        Add a callback for settlement confirmations.
        
        The callback will be called with the settlement confirmation when a confirmation is received.
        
        Args:
            callback: Callback function that takes a SettlementConfirmation as its argument
        """
        self.settlement_manager.add_confirmation_callback(callback)
        logger.debug("Settlement confirmation callback added")
    
    def generate_settlement_report(
        self,
        user_id: str,
        assets_bought: List[Dict[str, Any]],
        assets_sold: List[Dict[str, Any]],
        net_position: Dict[str, str],
        settlement_instructions: Dict[str, str],
        eod_time: Optional[str] = None,
        publish: bool = True
    ) -> SettlementReport:
        """
        Generate a settlement report for a user.
        
        Args:
            user_id: User identifier
            assets_bought: List of assets bought
            assets_sold: List of assets sold
            net_position: Net position for each asset
            settlement_instructions: Settlement instructions
            eod_time: Optional end-of-day time (defaults to current time in ISO format)
            publish: Whether to publish the report to the settlement stream
            
        Returns:
            Generated settlement report
        """
        logger.info(f"Generating settlement report for user: {user_id}")
        return self.settlement_manager.generate_settlement_report(
            user_id=user_id,
            assets_bought=assets_bought,
            assets_sold=assets_sold,
            net_position=net_position,
            settlement_instructions=settlement_instructions,
            eod_time=eod_time,
            publish=publish
        )
    
    def generate_settlement_reports(
        self,
        user_data: Dict[str, Dict[str, Any]],
        eod_time: Optional[str] = None,
        publish: bool = True
    ) -> List[SettlementReport]:
        """
        Generate settlement reports for multiple users.
        
        Args:
            user_data: Dictionary mapping user IDs to settlement data
            eod_time: Optional end-of-day time (defaults to current time in ISO format)
            publish: Whether to publish the reports to the settlement stream
            
        Returns:
            List of generated settlement reports
        """
        logger.info(f"Generating settlement reports for {len(user_data)} users")
        return self.settlement_manager.generate_settlement_reports(
            user_data=user_data,
            eod_time=eod_time,
            publish=publish
        )
    
    def process_settlement_confirmations(
        self,
        callback: Optional[Callable[[SettlementConfirmation], None]] = None,
        run_once: bool = False
    ):
        """
        Process settlement confirmations from WSPs.
        
        Args:
            callback: Optional callback function for settlement confirmations
            run_once: If True, process one batch and return; if False, run continuously
        """
        logger.info("Processing settlement confirmations")
        self.settlement_manager.process_settlement_confirmations(
            callback=callback,
            run_once=run_once
        )
    
    def get_settlement_report(self, report_id: str) -> Optional[SettlementReport]:
        """
        Get a settlement report by ID.
        
        Args:
            report_id: Report identifier
            
        Returns:
            Settlement report if found, None otherwise
        """
        return self.settlement_manager.get_settlement_report(report_id)
    
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
        return self.settlement_manager.get_settlement_reports(user_id, status)
    
    def get_settlement_confirmation(self, settlement_id: str) -> Optional[SettlementConfirmation]:
        """
        Get a settlement confirmation by ID.
        
        Args:
            settlement_id: Settlement identifier
            
        Returns:
            Settlement confirmation if found, None otherwise
        """
        return self.settlement_manager.get_settlement_confirmation(settlement_id)
    
    def get_settlement_confirmations(
        self,
        user_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[SettlementConfirmation]:
        """
        Get settlement confirmations.
        
        Args:
            user_id: Optional user ID to filter by
            status: Optional status to filter by
            
        Returns:
            List of settlement confirmations
        """
        return self.settlement_manager.get_settlement_confirmations(user_id, status)
    
    #
    # Account Operations (to be implemented)
    #
    
    def update_account(
        self,
        user_id: str,
        asset: str,
        amount: float,
        transaction_type: str,
        **kwargs
    ):
        """
        Update a user account balance.
        
        Args:
            user_id: User ID to update
            asset: Asset to update
            amount: Amount change (positive or negative)
            transaction_type: Type of transaction (trade, settlement, etc.)
            **kwargs: Additional arguments for the account update
            
        Raises:
            NotImplementedError: This method is not yet implemented
        """
        logger.warning("Account updates are not yet implemented")
        raise NotImplementedError("Account updates are not yet implemented")
    
    def get_account_balance(self, user_id: str, asset: str):
        """
        Get a user account balance.
        
        Args:
            user_id: User ID to get balance for
            asset: Asset to get balance for
            
        Raises:
            NotImplementedError: This method is not yet implemented
        """
        logger.warning("Account balance retrieval is not yet implemented")
        raise NotImplementedError("Account balance retrieval is not yet implemented")