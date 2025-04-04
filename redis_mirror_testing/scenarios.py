"""
Redis Mirror CE Testing Scenarios

This module provides test scenarios for testing the Redis Mirror CE SDK,
including scenarios for credit requests, settlement processing, and other
workflows.
"""

import time
import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union, Tuple, Set

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    Configuration,
    ValidationError
)
from sdk_ce.redis_mirror_wsp import (
    WSPClient
)
from sdk_ce.redis_mirror_exchange import (
    ExchangeClient
)
from .generators import (
    CreditRequestGenerator,
    SettlementGenerator,
    EventGenerator,
    AccountGenerator
)
from .mock_redis import MockRedisClient

# Configure logging
logger = logging.getLogger(__name__)


class TestScenario:
    """
    Base class for test scenarios.
    
    This class provides common functionality for test scenarios.
    """
    
    def __init__(
        self,
        wsp_client: Optional[WSPClient] = None,
        exchange_client: Optional[ExchangeClient] = None,
        connection_manager: Optional[RedisConnectionManager] = None,
        config: Optional[Configuration] = None,
        use_mock: bool = True
    ):
        """
        Initialize a test scenario.
        
        Args:
            wsp_client: WSP client (created if None)
            exchange_client: Exchange client (created if None)
            connection_manager: Redis connection manager (created if None)
            config: Configuration (created if None)
            use_mock: Whether to use mock Redis clients
        """
        self.use_mock = use_mock
        
        # Create configuration if not provided
        if config is None:
            self.config = Configuration({
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
        else:
            self.config = config
        
        # Create connection manager if not provided
        if connection_manager is None:
            if use_mock:
                # Create mock Redis clients
                wsp_redis = MockRedisClient()
                replica_redis = MockRedisClient()
                
                # Create connection manager with mock clients
                self.connection_manager = RedisConnectionManager(
                    wsp_config=self.config.get("wsp_redis"),
                    replica_config=self.config.get("replica_redis"),
                    wsp_client=wsp_redis,
                    replica_client=replica_redis
                )
            else:
                # Create connection manager with real clients
                self.connection_manager = RedisConnectionManager(
                    wsp_config=self.config.get("wsp_redis"),
                    replica_config=self.config.get("replica_redis")
                )
        else:
            self.connection_manager = connection_manager
        
        # Create WSP client if not provided
        if wsp_client is None:
            self.wsp_client = WSPClient(
                connection_manager=self.connection_manager,
                config=self.config,
                custodian_id="test-custodian",
                auto_start_processors=False
            )
        else:
            self.wsp_client = wsp_client
        
        # Create Exchange client if not provided
        if exchange_client is None:
            self.exchange_client = ExchangeClient(
                connection_manager=self.connection_manager,
                config=self.config,
                auto_start_processors=False
            )
        else:
            self.exchange_client = exchange_client
        
        # Initialize test data
        self.test_data = {}
        
        logger.info("Test scenario initialized")
    
    def setup(self):
        """
        Set up the test scenario.
        
        This method should be overridden by subclasses to set up the test scenario.
        """
        logger.info("Setting up test scenario")
    
    def run(self):
        """
        Run the test scenario.
        
        This method should be overridden by subclasses to run the test scenario.
        """
        logger.info("Running test scenario")
    
    def teardown(self):
        """
        Tear down the test scenario.
        
        This method should be overridden by subclasses to tear down the test scenario.
        """
        logger.info("Tearing down test scenario")
        
        # Close clients
        self.wsp_client.close()
        self.exchange_client.close()
    
    def execute(self):
        """
        Execute the test scenario.
        
        This method sets up, runs, and tears down the test scenario.
        
        Returns:
            Test results
        """
        logger.info("Executing test scenario")
        
        try:
            # Set up the test scenario
            self.setup()
            
            # Run the test scenario
            results = self.run()
            
            # Return results
            return results
        finally:
            # Tear down the test scenario
            self.teardown()


class CreditRequestScenario(TestScenario):
    """
    Test scenario for credit requests.
    
    This class provides a test scenario for credit requests.
    """
    
    def setup(self):
        """
        Set up the credit request test scenario.
        """
        super().setup()
        
        # Generate test data
        self.test_data["user_id"] = "test-user"
        self.test_data["asset"] = "BTC"
        self.test_data["amount"] = 1.0
        self.test_data["custodian_id"] = "test-custodian"
        self.test_data["chain"] = "Bitcoin"
        self.test_data["address"] = "bc1q..."
        
        # Start processors
        self.wsp_client.start_credit_response_processor()
        self.exchange_client.start_credit_request_processor()
        
        logger.info("Credit request test scenario set up")
    
    def run(self):
        """
        Run the credit request test scenario.
        
        Returns:
            Test results
        """
        super().run()
        
        results = {
            "success": True,
            "credit_request": None,
            "credit_response": None,
            "errors": []
        }
        
        try:
            # Create a credit request
            logger.info("Creating credit request")
            
            credit_request = self.wsp_client.request_credit_increase(
                user_id=self.test_data["user_id"],
                asset=self.test_data["asset"],
                amount=self.test_data["amount"],
                custodian_id=self.test_data["custodian_id"],
                chain=self.test_data["chain"],
                address=self.test_data["address"],
                wait_for_response=True,
                timeout=5
            )
            
            results["credit_request"] = credit_request
            
            # Check if the request was successful
            if credit_request is None:
                results["success"] = False
                results["errors"].append("Credit request failed")
                return results
            
            logger.info(f"Credit request created: {credit_request.request_id}")
            
            # Wait for the response
            logger.info("Waiting for credit response")
            
            # In a real scenario, we would wait for the response
            # For this test, we'll simulate a response
            
            # Get the response
            response = credit_request.response
            results["credit_response"] = response
            
            # Check if the response was received
            if response is None:
                results["success"] = False
                results["errors"].append("Credit response not received")
                return results
            
            logger.info(f"Credit response received: {response.status}")
            
            # Check if the response was approved
            if not response.is_approved():
                results["success"] = False
                results["errors"].append(f"Credit request rejected: {response.reject_reason}")
                return results
            
            logger.info("Credit request approved")
            
            # Return results
            return results
        except Exception as e:
            logger.error(f"Error in credit request test scenario: {e}")
            results["success"] = False
            results["errors"].append(str(e))
            return results
    
    def teardown(self):
        """
        Tear down the credit request test scenario.
        """
        # Stop processors
        self.wsp_client.stop_credit_response_processor()
        self.exchange_client.stop_credit_request_processor()
        
        super().teardown()
        
        logger.info("Credit request test scenario torn down")


class SettlementScenario(TestScenario):
    """
    Test scenario for settlement processing.
    
    This class provides a test scenario for settlement processing.
    """
    
    def setup(self):
        """
        Set up the settlement test scenario.
        """
        super().setup()
        
        # Generate test data
        self.test_data["user_id"] = "test-user"
        self.test_data["settlement_id"] = "test-settlement"
        self.test_data["trades"] = [
            {
                "trade_id": "test-trade-1",
                "user_id": "test-user",
                "buy_asset": "BTC",
                "sell_asset": "USD",
                "buy_amount": 1.0,
                "sell_amount": 50000.0,
                "timestamp": int(time.time())
            },
            {
                "trade_id": "test-trade-2",
                "user_id": "test-user",
                "buy_asset": "ETH",
                "sell_asset": "BTC",
                "buy_amount": 10.0,
                "sell_amount": 0.5,
                "timestamp": int(time.time())
            }
        ]
        
        # Calculate net positions
        net_positions = {}
        for trade in self.test_data["trades"]:
            buy_asset = trade["buy_asset"]
            sell_asset = trade["sell_asset"]
            buy_amount = trade["buy_amount"]
            sell_amount = trade["sell_amount"]
            
            if buy_asset not in net_positions:
                net_positions[buy_asset] = 0
            if sell_asset not in net_positions:
                net_positions[sell_asset] = 0
            
            net_positions[buy_asset] += buy_amount
            net_positions[sell_asset] -= sell_amount
        
        # Convert net positions to list
        self.test_data["positions"] = []
        for asset, amount in net_positions.items():
            self.test_data["positions"].append({
                "asset": asset,
                "amount": amount
            })
        
        # Start processors
        self.wsp_client.start_settlement_processor()
        
        logger.info("Settlement test scenario set up")
    
    def run(self):
        """
        Run the settlement test scenario.
        
        Returns:
            Test results
        """
        super().run()
        
        results = {
            "success": True,
            "settlement_report": None,
            "settlement_confirmation": None,
            "errors": []
        }
        
        try:
            # Create a settlement report
            logger.info("Creating settlement report")
            
            settlement_report = self.exchange_client.create_settlement_report(
                user_id=self.test_data["user_id"],
                settlement_id=self.test_data["settlement_id"],
                trades=self.test_data["trades"],
                positions=self.test_data["positions"]
            )
            
            results["settlement_report"] = settlement_report
            
            # Check if the report was created
            if settlement_report is None:
                results["success"] = False
                results["errors"].append("Settlement report creation failed")
                return results
            
            logger.info(f"Settlement report created: {settlement_report.settlement_id}")
            
            # Publish the settlement report
            logger.info("Publishing settlement report")
            
            self.exchange_client.publish_settlement_report(settlement_report)
            
            # Wait for the WSP to process the report
            logger.info("Waiting for WSP to process the report")
            
            # In a real scenario, we would wait for the WSP to process the report
            # For this test, we'll simulate processing
            
            # Create a settlement confirmation
            logger.info("Creating settlement confirmation")
            
            settlement_confirmation = self.wsp_client.confirm_settlement(
                settlement_id=settlement_report.settlement_id,
                user_id=settlement_report.user_id,
                status="confirmed"
            )
            
            results["settlement_confirmation"] = settlement_confirmation
            
            # Check if the confirmation was created
            if settlement_confirmation is None:
                results["success"] = False
                results["errors"].append("Settlement confirmation creation failed")
                return results
            
            logger.info(f"Settlement confirmation created: {settlement_confirmation.confirmation_id}")
            
            # Return results
            return results
        except Exception as e:
            logger.error(f"Error in settlement test scenario: {e}")
            results["success"] = False
            results["errors"].append(str(e))
            return results
    
    def teardown(self):
        """
        Tear down the settlement test scenario.
        """
        # Stop processors
        self.wsp_client.stop_settlement_processor()
        
        super().teardown()
        
        logger.info("Settlement test scenario torn down")


class EventHandlingScenario(TestScenario):
    """
    Test scenario for event handling.
    
    This class provides a test scenario for event handling.
    """
    
    def setup(self):
        """
        Set up the event handling test scenario.
        """
        super().setup()
        
        # Generate test data
        self.test_data["events"] = EventGenerator.generate_events_batch(10)
        
        # Set up event callbacks
        self.event_callbacks = {
            "credit:request": self._handle_credit_request,
            "credit:response": self._handle_credit_response,
            "settlement:report": self._handle_settlement_report,
            "settlement:confirmation": self._handle_settlement_confirmation,
            "system:error": self._handle_system_error
        }
        
        # Initialize event counts
        self.event_counts = {event_type: 0 for event_type in self.event_callbacks.keys()}
        
        logger.info("Event handling test scenario set up")
    
    def _handle_credit_request(self, event):
        """Handle credit request event."""
        logger.info(f"Handling credit request event: {event['event_id']}")
        self.event_counts["credit:request"] += 1
    
    def _handle_credit_response(self, event):
        """Handle credit response event."""
        logger.info(f"Handling credit response event: {event['event_id']}")
        self.event_counts["credit:response"] += 1
    
    def _handle_settlement_report(self, event):
        """Handle settlement report event."""
        logger.info(f"Handling settlement report event: {event['event_id']}")
        self.event_counts["settlement:report"] += 1
    
    def _handle_settlement_confirmation(self, event):
        """Handle settlement confirmation event."""
        logger.info(f"Handling settlement confirmation event: {event['event_id']}")
        self.event_counts["settlement:confirmation"] += 1
    
    def _handle_system_error(self, event):
        """Handle system error event."""
        logger.info(f"Handling system error event: {event['event_id']}")
        self.event_counts["system:error"] += 1
    
    def run(self):
        """
        Run the event handling test scenario.
        
        Returns:
            Test results
        """
        super().run()
        
        results = {
            "success": True,
            "events_published": 0,
            "events_processed": 0,
            "event_counts": {},
            "errors": []
        }
        
        try:
            # Create event router
            logger.info("Creating event router")
            
            event_router = self.wsp_client.create_event_router()
            
            # Register event handlers
            for event_type, callback in self.event_callbacks.items():
                event_router.register_handler(event_type, callback)
            
            # Publish events
            logger.info("Publishing events")
            
            for event in self.test_data["events"]:
                # Publish event
                self.wsp_client.publish_event(event)
                results["events_published"] += 1
            
            # Process events
            logger.info("Processing events")
            
            # In a real scenario, we would wait for the events to be processed
            # For this test, we'll simulate processing by calling the handlers directly
            
            for event in self.test_data["events"]:
                event_type = event["event_type"]
                if event_type in self.event_callbacks:
                    self.event_callbacks[event_type](event)
                    results["events_processed"] += 1
            
            # Record event counts
            results["event_counts"] = self.event_counts
            
            # Check if all events were processed
            if results["events_processed"] != results["events_published"]:
                results["success"] = False
                results["errors"].append(
                    f"Not all events were processed: {results['events_processed']} / {results['events_published']}"
                )
            
            # Return results
            return results
        except Exception as e:
            logger.error(f"Error in event handling test scenario: {e}")
            results["success"] = False
            results["errors"].append(str(e))
            return results
    
    def teardown(self):
        """
        Tear down the event handling test scenario.
        """
        super().teardown()
        
        logger.info("Event handling test scenario torn down")


class IntegrationScenario(TestScenario):
    """
    Test scenario for integration testing.
    
    This class provides a test scenario for integration testing.
    """
    
    def setup(self):
        """
        Set up the integration test scenario.
        """
        super().setup()
        
        # Generate test data
        self.test_data["user_id"] = "test-user"
        self.test_data["asset"] = "BTC"
        self.test_data["amount"] = 1.0
        self.test_data["custodian_id"] = "test-custodian"
        self.test_data["chain"] = "Bitcoin"
        self.test_data["address"] = "bc1q..."
        
        # Start processors
        self.wsp_client.start_credit_response_processor()
        self.wsp_client.start_settlement_processor()
        self.exchange_client.start_credit_request_processor()
        
        logger.info("Integration test scenario set up")
    
    def run(self):
        """
        Run the integration test scenario.
        
        Returns:
            Test results
        """
        super().run()
        
        results = {
            "success": True,
            "credit_request": None,
            "credit_response": None,
            "settlement_report": None,
            "settlement_confirmation": None,
            "errors": []
        }
        
        try:
            # Step 1: Create a credit request
            logger.info("Step 1: Creating credit request")
            
            credit_request = self.wsp_client.request_credit_increase(
                user_id=self.test_data["user_id"],
                asset=self.test_data["asset"],
                amount=self.test_data["amount"],
                custodian_id=self.test_data["custodian_id"],
                chain=self.test_data["chain"],
                address=self.test_data["address"],
                wait_for_response=True,
                timeout=5
            )
            
            results["credit_request"] = credit_request
            
            # Check if the request was successful
            if credit_request is None:
                results["success"] = False
                results["errors"].append("Credit request failed")
                return results
            
            logger.info(f"Credit request created: {credit_request.request_id}")
            
            # Get the response
            response = credit_request.response
            results["credit_response"] = response
            
            # Check if the response was received
            if response is None:
                results["success"] = False
                results["errors"].append("Credit response not received")
                return results
            
            logger.info(f"Credit response received: {response.status}")
            
            # Check if the response was approved
            if not response.is_approved():
                results["success"] = False
                results["errors"].append(f"Credit request rejected: {response.reject_reason}")
                return results
            
            logger.info("Credit request approved")
            
            # Step 2: Create a settlement report
            logger.info("Step 2: Creating settlement report")
            
            # Generate trades
            trades = [
                {
                    "trade_id": "test-trade-1",
                    "user_id": self.test_data["user_id"],
                    "buy_asset": self.test_data["asset"],
                    "sell_asset": "USD",
                    "buy_amount": self.test_data["amount"] / 2,
                    "sell_amount": 25000.0,
                    "timestamp": int(time.time())
                },
                {
                    "trade_id": "test-trade-2",
                    "user_id": self.test_data["user_id"],
                    "buy_asset": "ETH",
                    "sell_asset": self.test_data["asset"],
                    "buy_amount": 10.0,
                    "sell_amount": self.test_data["amount"] / 2,
                    "timestamp": int(time.time())
                }
            ]
            
            # Calculate net positions
            net_positions = {}
            for trade in trades:
                buy_asset = trade["buy_asset"]
                sell_asset = trade["sell_asset"]
                buy_amount = trade["buy_amount"]
                sell_amount = trade["sell_amount"]
                
                if buy_asset not in net_positions:
                    net_positions[buy_asset] = 0
                if sell_asset not in net_positions:
                    net_positions[sell_asset] = 0
                
                net_positions[buy_asset] += buy_amount
                net_positions[sell_asset] -= sell_amount
            
            # Convert net positions to list
            positions = []
            for asset, amount in net_positions.items():
                positions.append({
                    "asset": asset,
                    "amount": amount
                })
            
            # Create settlement report
            settlement_report = self.exchange_client.create_settlement_report(
                user_id=self.test_data["user_id"],
                settlement_id="test-settlement",
                trades=trades,
                positions=positions
            )
            
            results["settlement_report"] = settlement_report
            
            # Check if the report was created
            if settlement_report is None:
                results["success"] = False
                results["errors"].append("Settlement report creation failed")
                return results
            
            logger.info(f"Settlement report created: {settlement_report.settlement_id}")
            
            # Publish the settlement report
            logger.info("Publishing settlement report")
            
            self.exchange_client.publish_settlement_report(settlement_report)
            
            # Step 3: Create a settlement confirmation
            logger.info("Step 3: Creating settlement confirmation")
            
            settlement_confirmation = self.wsp_client.confirm_settlement(
                settlement_id=settlement_report.settlement_id,
                user_id=settlement_report.user_id,
                status="confirmed"
            )
            
            results["settlement_confirmation"] = settlement_confirmation
            
            # Check if the confirmation was created
            if settlement_confirmation is None:
                results["success"] = False
                results["errors"].append("Settlement confirmation creation failed")
                return results
            
            logger.info(f"Settlement confirmation created: {settlement_confirmation.confirmation_id}")
            
            # Return results
            return results
        except Exception as e:
            logger.error(f"Error in integration test scenario: {e}")
            results["success"] = False
            results["errors"].append(str(e))
            return results
    
    def teardown(self):
        """
        Tear down the integration test scenario.
        """
        # Stop processors
        self.wsp_client.stop_credit_response_processor()
        self.wsp_client.stop_settlement_processor()
        self.exchange_client.stop_credit_request_processor()
        
        super().teardown()
        
        logger.info("Integration test scenario torn down")