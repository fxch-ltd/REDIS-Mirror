"""
Redis Mirror CE WSP Settlement Client

This module provides functionality for WSP to process settlement reports from the Exchange,
manage the settlement process, and confirm settlement completion.
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


class SettlementReport:
    """
    Represents a settlement report from the Exchange.
    
    This class encapsulates the data and state of a settlement report.
    """
    
    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    
    def __init__(
        self,
        report_id: str,
        user_id: str,
        timestamp: int,
        eod_time: str,
        assets_bought: List[Dict[str, Any]],
        assets_sold: List[Dict[str, Any]],
        net_position: Dict[str, str],
        settlement_instructions: Dict[str, str],
        raw_data: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a settlement report.
        
        Args:
            report_id: Settlement report identifier
            user_id: User identifier
            timestamp: Report timestamp
            eod_time: End-of-day time
            assets_bought: List of assets bought
            assets_sold: List of assets sold
            net_position: Net position for each asset
            settlement_instructions: Settlement instructions
            raw_data: Optional raw data from the settlement report
        """
        self.report_id = report_id
        self.user_id = user_id
        self.timestamp = timestamp
        self.eod_time = eod_time
        self.assets_bought = assets_bought
        self.assets_sold = assets_sold
        self.net_position = net_position
        self.settlement_instructions = settlement_instructions
        self.raw_data = raw_data or {}
        self.status = self.STATUS_PENDING
        self.processing_timestamp = None
        self.completion_timestamp = None
        self.failure_reason = None
    
    @classmethod
    def from_stream_data(cls, data: Dict[str, str]) -> 'SettlementReport':
        """
        Create a settlement report from stream data.
        
        Args:
            data: Stream data dictionary
            
        Returns:
            Settlement report instance
            
        Raises:
            ValidationError: If the data is invalid
        """
        try:
            # Extract basic fields
            report_id = data.get("report_id")
            timestamp = int(data.get("timestamp", str(int(time.time()))))
            
            # Parse the settlement data
            settlement_data = json.loads(data.get("data", "{}"))
            
            # Extract required fields from settlement data
            user_id = settlement_data.get("user_id")
            eod_time = settlement_data.get("eod_time")
            assets = settlement_data.get("assets", {})
            assets_bought = assets.get("bought", [])
            assets_sold = assets.get("sold", [])
            net_position = settlement_data.get("net_position", {})
            settlement_instructions = settlement_data.get("settlement_instructions", {})
            
            # Validate required fields
            if not report_id:
                raise ValidationError("Settlement report is missing report_id")
            
            if not user_id:
                raise ValidationError("Settlement report is missing user_id")
            
            if not eod_time:
                raise ValidationError("Settlement report is missing eod_time")
            
            # Create settlement report
            return cls(
                report_id=report_id,
                user_id=user_id,
                timestamp=timestamp,
                eod_time=eod_time,
                assets_bought=assets_bought,
                assets_sold=assets_sold,
                net_position=net_position,
                settlement_instructions=settlement_instructions,
                raw_data=settlement_data
            )
        except (json.JSONDecodeError, ValueError) as e:
            raise ValidationError(f"Invalid settlement report data: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the settlement report to a dictionary.
        
        Returns:
            Dictionary representation of the settlement report
        """
        return {
            "report_id": self.report_id,
            "user_id": self.user_id,
            "timestamp": self.timestamp,
            "eod_time": self.eod_time,
            "assets_bought": self.assets_bought,
            "assets_sold": self.assets_sold,
            "net_position": self.net_position,
            "settlement_instructions": self.settlement_instructions,
            "status": self.status,
            "processing_timestamp": self.processing_timestamp,
            "completion_timestamp": self.completion_timestamp,
            "failure_reason": self.failure_reason
        }
    
    def mark_processing(self):
        """Mark the settlement report as processing."""
        self.status = self.STATUS_PROCESSING
        self.processing_timestamp = int(time.time())
    
    def mark_completed(self):
        """Mark the settlement report as completed."""
        self.status = self.STATUS_COMPLETED
        self.completion_timestamp = int(time.time())
    
    def mark_failed(self, reason: str):
        """
        Mark the settlement report as failed.
        
        Args:
            reason: Failure reason
        """
        self.status = self.STATUS_FAILED
        self.failure_reason = reason
        self.completion_timestamp = int(time.time())
    
    def is_pending(self) -> bool:
        """
        Check if the settlement report is pending.
        
        Returns:
            True if the settlement report is pending, False otherwise
        """
        return self.status == self.STATUS_PENDING
    
    def is_processing(self) -> bool:
        """
        Check if the settlement report is processing.
        
        Returns:
            True if the settlement report is processing, False otherwise
        """
        return self.status == self.STATUS_PROCESSING
    
    def is_completed(self) -> bool:
        """
        Check if the settlement report is completed.
        
        Returns:
            True if the settlement report is completed, False otherwise
        """
        return self.status == self.STATUS_COMPLETED
    
    def is_failed(self) -> bool:
        """
        Check if the settlement report is failed.
        
        Returns:
            True if the settlement report is failed, False otherwise
        """
        return self.status == self.STATUS_FAILED
    
    def __str__(self) -> str:
        """String representation of the settlement report."""
        return (f"SettlementReport(id={self.report_id}, user={self.user_id}, "
                f"status={self.status})")


class SettlementClient:
    """
    Handles settlement operations for WSPs.
    
    This class processes settlement reports from the Exchange and manages
    the settlement process from the WSP perspective.
    """
    
    def __init__(
        self,
        connection_manager: RedisConnectionManager,
        custodian_id: str,
        auto_start_processor: bool = True
    ):
        """
        Initialize the settlement client.
        
        Args:
            connection_manager: Redis connection manager
            custodian_id: Custodian identifier
            auto_start_processor: Whether to automatically start the settlement processor
        """
        self.connection_manager = connection_manager
        self.custodian_id = custodian_id
        
        # Get Redis clients
        self.wsp_client = connection_manager.get_wsp_client()
        self.replica_client = connection_manager.get_replica_client()
        
        # Create stream processor for settlement reports
        self.settlement_stream = KeyManager.settlement_report_stream()
        self.processor = StreamProcessor(
            self.wsp_client,
            self.settlement_stream,
            f"settlement-client-{custodian_id}",
            auto_create_group=True
        )
        
        # Create stream publisher for settlement confirmations
        self.confirmation_stream = KeyManager.settlement_confirmation_stream()
        self.publisher = StreamPublisher(self.replica_client, self.confirmation_stream)
        
        # Settlement reports dictionary (report_id -> SettlementReport)
        self.settlement_reports = {}
        
        # Settlement callbacks
        self.settlement_callbacks = []
        
        # Start settlement processor if auto_start is enabled
        self._processor_running = False
        if auto_start_processor:
            self.start_settlement_processor()
        
        logger.info(f"Settlement Client initialized for custodian: {custodian_id}")
    
    def start_settlement_processor(self):
        """Start the settlement processor."""
        if self._processor_running:
            return
        
        self._processor_running = True
        
        # Start processing in a separate thread
        import threading
        thread = threading.Thread(
            target=self._process_settlement_reports,
            daemon=True
        )
        thread.start()
        
        logger.info("Settlement processor started")
    
    def stop_settlement_processor(self):
        """Stop the settlement processor."""
        self._processor_running = False
        logger.info("Settlement processor stopped")
    
    def add_settlement_callback(self, callback: Callable[[SettlementReport], None]):
        """
        Add a callback for settlement reports.
        
        The callback will be called with the settlement report when a report is received.
        
        Args:
            callback: Callback function that takes a SettlementReport as its argument
        """
        self.settlement_callbacks.append(callback)
        logger.debug("Settlement callback added")
    
    def process_settlement_reports(
        self,
        callback: Optional[Callable[[SettlementReport], None]] = None,
        run_once: bool = False
    ):
        """
        Process settlement reports from the Exchange.
        
        This method can be used to manually process settlement reports instead of
        using the automatic processor.
        
        Args:
            callback: Optional callback function for settlement reports
            run_once: If True, process one batch and return; if False, run continuously
        """
        if callback:
            self.add_settlement_callback(callback)
        
        def settlement_handler(message):
            try:
                # Check if this is a settlement report
                message_type = message.get("type")
                if message_type != "settlement":
                    logger.debug(f"Ignoring non-settlement message: {message_type}")
                    return True
                
                # Parse settlement report
                report = SettlementReport.from_stream_data(message)
                
                # Store settlement report
                self.settlement_reports[report.report_id] = report
                
                # Call callbacks
                for callback_fn in self.settlement_callbacks:
                    try:
                        callback_fn(report)
                    except Exception as e:
                        logger.error(f"Error in settlement callback: {e}")
                
                return True
            except Exception as e:
                logger.error(f"Error processing settlement report: {e}")
                return False
        
        # Process settlement reports
        if run_once:
            self.processor._process_batch(settlement_handler, ">")
        else:
            self._processor_running = True
            while self._processor_running:
                self.processor._process_batch(settlement_handler, ">")
                time.sleep(0.1)
    
    def _process_settlement_reports(self):
        """Process settlement reports from the Exchange."""
        logger.info("Starting settlement report processor")
        
        def settlement_handler(message):
            try:
                # Check if this is a settlement report
                message_type = message.get("type")
                if message_type != "settlement":
                    logger.debug(f"Ignoring non-settlement message: {message_type}")
                    return True
                
                # Parse settlement report
                report = SettlementReport.from_stream_data(message)
                
                # Store settlement report
                self.settlement_reports[report.report_id] = report
                
                # Call callbacks
                for callback_fn in self.settlement_callbacks:
                    try:
                        callback_fn(report)
                    except Exception as e:
                        logger.error(f"Error in settlement callback: {e}")
                
                return True
            except Exception as e:
                logger.error(f"Error processing settlement report: {e}")
                return False
        
        # Process settlement reports
        self.processor.process_messages(settlement_handler)
    
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
        
        # Get settlement report
        report = self.settlement_reports.get(settlement_id)
        if not report:
            logger.warning(f"Settlement report not found: {settlement_id}")
            return False
        
        # Update settlement report status
        if status == "completed":
            report.mark_completed()
        elif status == "failed":
            failure_reason = details.get("failure_reason", "Unknown failure") if details else "Unknown failure"
            report.mark_failed(failure_reason)
        
        # Create confirmation message
        confirmation = {
            "type": "confirmation",
            "settlement_id": settlement_id,
            "user_id": report.user_id,
            "timestamp": str(int(time.time())),
            "status": status
        }
        
        # Add details if provided
        if details:
            confirmation["details"] = json.dumps(details)
        
        # Publish confirmation
        message_id = self.publisher.publish(confirmation)
        if not message_id:
            logger.error(f"Failed to publish settlement confirmation: {settlement_id}")
            return False
        
        logger.info(f"Settlement confirmation published: {settlement_id}")
        return True
    
    def get_settlement_report(self, report_id: str) -> Optional[SettlementReport]:
        """
        Get a settlement report by ID.
        
        Args:
            report_id: Report identifier
            
        Returns:
            Settlement report if found, None otherwise
        """
        return self.settlement_reports.get(report_id)
    
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
        reports = list(self.settlement_reports.values())
        
        # Filter by user ID
        if user_id:
            reports = [r for r in reports if r.user_id == user_id]
        
        # Filter by status
        if status:
            reports = [r for r in reports if r.status == status]
        
        return reports
    
    def get_pending_settlement_reports(self, user_id: Optional[str] = None) -> List[SettlementReport]:
        """
        Get pending settlement reports.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of pending settlement reports
        """
        return self.get_settlement_reports(user_id, SettlementReport.STATUS_PENDING)
    
    def get_processing_settlement_reports(self, user_id: Optional[str] = None) -> List[SettlementReport]:
        """
        Get processing settlement reports.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of processing settlement reports
        """
        return self.get_settlement_reports(user_id, SettlementReport.STATUS_PROCESSING)
    
    def get_completed_settlement_reports(self, user_id: Optional[str] = None) -> List[SettlementReport]:
        """
        Get completed settlement reports.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of completed settlement reports
        """
        return self.get_settlement_reports(user_id, SettlementReport.STATUS_COMPLETED)
    
    def get_failed_settlement_reports(self, user_id: Optional[str] = None) -> List[SettlementReport]:
        """
        Get failed settlement reports.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of failed settlement reports
        """
        return self.get_settlement_reports(user_id, SettlementReport.STATUS_FAILED)