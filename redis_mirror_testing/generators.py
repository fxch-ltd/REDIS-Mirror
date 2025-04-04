"""
Redis Mirror CE Testing Data Generators

This module provides data generators for testing the Redis Mirror CE SDK,
including generators for credit requests, settlement reports, and other data
needed for testing.
"""

import time
import uuid
import random
import string
import logging
from typing import Dict, Any, Optional, List, Callable, Union, Tuple, Set

from sdk_ce.redis_mirror_core import (
    KeyManager,
    ValidationError
)

# Configure logging
logger = logging.getLogger(__name__)


class DataGenerator:
    """
    Base class for data generators.
    
    This class provides common functionality for generating test data.
    """
    
    @staticmethod
    def generate_id(prefix: str = "") -> str:
        """
        Generate a unique ID.
        
        Args:
            prefix: Optional prefix for the ID
            
        Returns:
            Unique ID
        """
        return f"{prefix}{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def generate_timestamp() -> int:
        """
        Generate a current timestamp.
        
        Returns:
            Current timestamp in milliseconds
        """
        return int(time.time() * 1000)
    
    @staticmethod
    def generate_string(length: int = 10) -> str:
        """
        Generate a random string.
        
        Args:
            length: Length of the string
            
        Returns:
            Random string
        """
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    @staticmethod
    def generate_amount(min_value: float = 0.1, max_value: float = 100.0) -> float:
        """
        Generate a random amount.
        
        Args:
            min_value: Minimum value
            max_value: Maximum value
            
        Returns:
            Random amount
        """
        return round(random.uniform(min_value, max_value), 8)
    
    @staticmethod
    def generate_asset() -> str:
        """
        Generate a random asset.
        
        Returns:
            Random asset
        """
        assets = ["BTC", "ETH", "XRP", "LTC", "BCH", "ADA", "DOT", "LINK", "XLM", "DOGE"]
        return random.choice(assets)
    
    @staticmethod
    def generate_user_id() -> str:
        """
        Generate a random user ID.
        
        Returns:
            Random user ID
        """
        return f"user-{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def generate_address() -> str:
        """
        Generate a random blockchain address.
        
        Returns:
            Random blockchain address
        """
        return f"0x{uuid.uuid4().hex}"
    
    @staticmethod
    def generate_chain() -> str:
        """
        Generate a random blockchain chain.
        
        Returns:
            Random blockchain chain
        """
        chains = ["Bitcoin", "Ethereum", "Ripple", "Litecoin", "Bitcoin Cash", "Cardano", "Polkadot", "Chainlink", "Stellar", "Dogecoin"]
        return random.choice(chains)
    
    @staticmethod
    def generate_custodian_id() -> str:
        """
        Generate a random custodian ID.
        
        Returns:
            Random custodian ID
        """
        custodians = ["ledger", "fireblocks", "bitgo", "coinbase", "gemini", "kraken", "binance", "celsius", "nexo", "blockfi"]
        return random.choice(custodians)
    
    @staticmethod
    def generate_status() -> str:
        """
        Generate a random status.
        
        Returns:
            Random status
        """
        statuses = ["pending", "approved", "rejected", "completed", "failed", "processing"]
        return random.choice(statuses)


class CreditRequestGenerator(DataGenerator):
    """
    Generator for credit requests.
    
    This class provides methods for generating credit request data for testing.
    """
    
    @classmethod
    def generate_credit_request(
        cls,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        amount: Optional[float] = None,
        custodian_id: Optional[str] = None,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        request_id: Optional[str] = None,
        is_increase: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Generate a credit request.
        
        Args:
            user_id: User ID (generated if None)
            asset: Asset (generated if None)
            amount: Amount (generated if None)
            custodian_id: Custodian ID (generated if None)
            chain: Blockchain chain (generated if None)
            address: Blockchain address (generated if None)
            request_id: Request ID (generated if None)
            is_increase: Whether the request is for an increase (random if None)
            
        Returns:
            Credit request data
        """
        user_id = user_id or cls.generate_user_id()
        asset = asset or cls.generate_asset()
        amount = amount or cls.generate_amount()
        custodian_id = custodian_id or cls.generate_custodian_id()
        chain = chain or cls.generate_chain()
        address = address or cls.generate_address()
        request_id = request_id or cls.generate_id("cr-")
        is_increase = is_increase if is_increase is not None else random.choice([True, False])
        
        return {
            "request_id": request_id,
            "user_id": user_id,
            "asset": asset,
            "amount": amount,
            "custodian_id": custodian_id,
            "chain": chain,
            "address": address,
            "timestamp": cls.generate_timestamp(),
            "is_increase": is_increase,
            "status": "pending"
        }
    
    @classmethod
    def generate_credit_response(
        cls,
        request: Dict[str, Any],
        status: Optional[str] = None,
        vault_id: Optional[str] = None,
        reject_reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a credit response for a request.
        
        Args:
            request: Credit request data
            status: Response status (random if None)
            vault_id: Vault ID (generated if None and status is approved)
            reject_reason: Reject reason (generated if None and status is rejected)
            
        Returns:
            Credit response data
        """
        status = status or random.choice(["approved", "rejected"])
        
        response = {
            "request_id": request["request_id"],
            "user_id": request["user_id"],
            "asset": request["asset"],
            "amount": request["amount"],
            "timestamp": cls.generate_timestamp(),
            "status": status
        }
        
        if status == "approved":
            response["vault_id"] = vault_id or cls.generate_id("vault-")
        elif status == "rejected":
            response["reject_reason"] = reject_reason or "Insufficient funds"
        
        return response
    
    @classmethod
    def generate_credit_requests_batch(
        cls,
        count: int,
        user_ids: Optional[List[str]] = None,
        assets: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate a batch of credit requests.
        
        Args:
            count: Number of requests to generate
            user_ids: List of user IDs to choose from (generated if None)
            assets: List of assets to choose from (generated if None)
            
        Returns:
            List of credit request data
        """
        user_ids = user_ids or [cls.generate_user_id() for _ in range(max(1, count // 3))]
        assets = assets or [cls.generate_asset() for _ in range(max(1, count // 5))]
        
        requests = []
        for _ in range(count):
            user_id = random.choice(user_ids)
            asset = random.choice(assets)
            requests.append(cls.generate_credit_request(user_id=user_id, asset=asset))
        
        return requests


class SettlementGenerator(DataGenerator):
    """
    Generator for settlement data.
    
    This class provides methods for generating settlement data for testing.
    """
    
    @classmethod
    def generate_trade(
        cls,
        user_id: Optional[str] = None,
        buy_asset: Optional[str] = None,
        sell_asset: Optional[str] = None,
        buy_amount: Optional[float] = None,
        sell_amount: Optional[float] = None,
        trade_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a trade.
        
        Args:
            user_id: User ID (generated if None)
            buy_asset: Buy asset (generated if None)
            sell_asset: Sell asset (generated if None)
            buy_amount: Buy amount (generated if None)
            sell_amount: Sell amount (generated if None)
            trade_id: Trade ID (generated if None)
            
        Returns:
            Trade data
        """
        user_id = user_id or cls.generate_user_id()
        
        # Ensure buy and sell assets are different
        if buy_asset is None and sell_asset is None:
            assets = [cls.generate_asset(), cls.generate_asset()]
            while assets[0] == assets[1]:
                assets[1] = cls.generate_asset()
            buy_asset, sell_asset = assets
        elif buy_asset is None:
            buy_asset = cls.generate_asset()
            while buy_asset == sell_asset:
                buy_asset = cls.generate_asset()
        elif sell_asset is None:
            sell_asset = cls.generate_asset()
            while sell_asset == buy_asset:
                sell_asset = cls.generate_asset()
        
        buy_amount = buy_amount or cls.generate_amount()
        sell_amount = sell_amount or cls.generate_amount()
        trade_id = trade_id or cls.generate_id("trade-")
        
        return {
            "trade_id": trade_id,
            "user_id": user_id,
            "buy_asset": buy_asset,
            "sell_asset": sell_asset,
            "buy_amount": buy_amount,
            "sell_amount": sell_amount,
            "timestamp": cls.generate_timestamp()
        }
    
    @classmethod
    def generate_settlement_report(
        cls,
        user_id: Optional[str] = None,
        trades: Optional[List[Dict[str, Any]]] = None,
        settlement_id: Optional[str] = None,
        settlement_date: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Generate a settlement report.
        
        Args:
            user_id: User ID (generated if None)
            trades: List of trades (generated if None)
            settlement_id: Settlement ID (generated if None)
            settlement_date: Settlement date (generated if None)
            
        Returns:
            Settlement report data
        """
        user_id = user_id or cls.generate_user_id()
        settlement_id = settlement_id or cls.generate_id("settlement-")
        settlement_date = settlement_date or cls.generate_timestamp()
        
        if trades is None:
            # Generate 1-5 trades
            trade_count = random.randint(1, 5)
            trades = [cls.generate_trade(user_id=user_id) for _ in range(trade_count)]
        
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
        
        return {
            "settlement_id": settlement_id,
            "user_id": user_id,
            "settlement_date": settlement_date,
            "trades": trades,
            "positions": positions,
            "status": "pending"
        }
    
    @classmethod
    def generate_settlement_confirmation(
        cls,
        settlement_report: Dict[str, Any],
        confirmation_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a settlement confirmation.
        
        Args:
            settlement_report: Settlement report data
            confirmation_id: Confirmation ID (generated if None)
            status: Confirmation status (random if None)
            
        Returns:
            Settlement confirmation data
        """
        confirmation_id = confirmation_id or cls.generate_id("confirmation-")
        status = status or random.choice(["confirmed", "rejected"])
        
        return {
            "confirmation_id": confirmation_id,
            "settlement_id": settlement_report["settlement_id"],
            "user_id": settlement_report["user_id"],
            "timestamp": cls.generate_timestamp(),
            "status": status
        }
    
    @classmethod
    def generate_settlement_reports_batch(
        cls,
        count: int,
        user_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate a batch of settlement reports.
        
        Args:
            count: Number of reports to generate
            user_ids: List of user IDs to choose from (generated if None)
            
        Returns:
            List of settlement report data
        """
        user_ids = user_ids or [cls.generate_user_id() for _ in range(max(1, count // 3))]
        
        reports = []
        for _ in range(count):
            user_id = random.choice(user_ids)
            reports.append(cls.generate_settlement_report(user_id=user_id))
        
        return reports


class EventGenerator(DataGenerator):
    """
    Generator for event data.
    
    This class provides methods for generating event data for testing.
    """
    
    @classmethod
    def generate_event(
        cls,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate an event.
        
        Args:
            event_type: Event type (random if None)
            source: Event source (random if None)
            data: Event data (generated if None)
            event_id: Event ID (generated if None)
            
        Returns:
            Event data
        """
        event_types = [
            "credit:request", "credit:response", "credit:update",
            "settlement:report", "settlement:confirmation", "settlement:completion",
            "system:startup", "system:shutdown", "system:error"
        ]
        sources = ["wsp", "exchange", "system"]
        
        event_type = event_type or random.choice(event_types)
        source = source or random.choice(sources)
        event_id = event_id or cls.generate_id("event-")
        
        if data is None:
            # Generate data based on event type
            if event_type.startswith("credit:"):
                if event_type == "credit:request":
                    data = CreditRequestGenerator.generate_credit_request()
                elif event_type == "credit:response":
                    request = CreditRequestGenerator.generate_credit_request()
                    data = CreditRequestGenerator.generate_credit_response(request)
                else:  # credit:update
                    data = {
                        "user_id": cls.generate_user_id(),
                        "asset": cls.generate_asset(),
                        "amount": cls.generate_amount(),
                        "transaction_id": cls.generate_id("tx-")
                    }
            elif event_type.startswith("settlement:"):
                if event_type == "settlement:report":
                    data = SettlementGenerator.generate_settlement_report()
                elif event_type == "settlement:confirmation":
                    report = SettlementGenerator.generate_settlement_report()
                    data = SettlementGenerator.generate_settlement_confirmation(report)
                else:  # settlement:completion
                    data = {
                        "settlement_id": cls.generate_id("settlement-"),
                        "user_id": cls.generate_user_id(),
                        "status": "completed",
                        "timestamp": cls.generate_timestamp()
                    }
            else:  # system events
                data = {
                    "message": f"System {event_type.split(':')[1]} event",
                    "timestamp": cls.generate_timestamp()
                }
        
        return {
            "event_id": event_id,
            "event_type": event_type,
            "source": source,
            "timestamp": cls.generate_timestamp(),
            "data": data
        }
    
    @classmethod
    def generate_events_batch(
        cls,
        count: int,
        event_types: Optional[List[str]] = None,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate a batch of events.
        
        Args:
            count: Number of events to generate
            event_types: List of event types to choose from (random if None)
            sources: List of sources to choose from (random if None)
            
        Returns:
            List of event data
        """
        event_types = event_types or [
            "credit:request", "credit:response", "credit:update",
            "settlement:report", "settlement:confirmation", "settlement:completion",
            "system:startup", "system:shutdown", "system:error"
        ]
        sources = sources or ["wsp", "exchange", "system"]
        
        events = []
        for _ in range(count):
            event_type = random.choice(event_types)
            source = random.choice(sources)
            events.append(cls.generate_event(event_type=event_type, source=source))
        
        return events


class AccountGenerator(DataGenerator):
    """
    Generator for account data.
    
    This class provides methods for generating account data for testing.
    """
    
    @classmethod
    def generate_account_entry(
        cls,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        balance: Optional[float] = None,
        available: Optional[float] = None,
        reserved: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Generate an account entry.
        
        Args:
            user_id: User ID (generated if None)
            asset: Asset (generated if None)
            balance: Balance (generated if None)
            available: Available balance (calculated if None)
            reserved: Reserved balance (calculated if None)
            
        Returns:
            Account entry data
        """
        user_id = user_id or cls.generate_user_id()
        asset = asset or cls.generate_asset()
        balance = balance or cls.generate_amount(min_value=1.0, max_value=1000.0)
        
        if reserved is None:
            # Generate a random reserved amount (0-20% of balance)
            reserved = round(balance * random.uniform(0, 0.2), 8)
        
        if available is None:
            # Calculate available balance
            available = balance - reserved
        
        return {
            "user_id": user_id,
            "asset": asset,
            "balance": balance,
            "available": available,
            "reserved": reserved,
            "timestamp": cls.generate_timestamp(),
            "transaction_id": cls.generate_id("tx-")
        }
    
    @classmethod
    def generate_credit_inventory_entry(
        cls,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        amount: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Generate a credit inventory entry.
        
        Args:
            user_id: User ID (generated if None)
            asset: Asset (generated if None)
            amount: Amount (generated if None)
            
        Returns:
            Credit inventory entry data
        """
        user_id = user_id or cls.generate_user_id()
        asset = asset or cls.generate_asset()
        amount = amount or cls.generate_amount(min_value=1.0, max_value=1000.0)
        
        return {
            "user_id": user_id,
            "asset": asset,
            "amount": amount,
            "timestamp": cls.generate_timestamp(),
            "transaction_id": cls.generate_id("tx-")
        }
    
    @classmethod
    def generate_vault_asset(
        cls,
        user_id: Optional[str] = None,
        asset: Optional[str] = None,
        amount: Optional[float] = None,
        status: Optional[str] = None,
        chain: Optional[str] = None,
        address: Optional[str] = None,
        vault_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a vault asset.
        
        Args:
            user_id: User ID (generated if None)
            asset: Asset (generated if None)
            amount: Amount (generated if None)
            status: Status (random if None)
            chain: Blockchain chain (generated if None)
            address: Blockchain address (generated if None)
            vault_id: Vault ID (generated if None)
            
        Returns:
            Vault asset data
        """
        user_id = user_id or cls.generate_user_id()
        asset = asset or cls.generate_asset()
        amount = amount or cls.generate_amount(min_value=0.1, max_value=100.0)
        status = status or random.choice(["locked", "unlocked", "pending"])
        chain = chain or cls.generate_chain()
        address = address or cls.generate_address()
        vault_id = vault_id or cls.generate_id("vault-")
        
        return {
            "vault_id": vault_id,
            "user_id": user_id,
            "asset": asset,
            "amount": amount,
            "status": status,
            "chain": chain,
            "address": address,
            "timestamp": cls.generate_timestamp(),
            "pledge_request_id": cls.generate_id("pr-") if status == "locked" else None
        }