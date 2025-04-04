"""
Redis Mirror Community Edition SDK - Exchange Module

This module provides components for Exchange integration with the Redis Mirror
Community Edition system, including credit management, settlement processing,
and account integration.
"""

__version__ = '0.1.0'

# Import components for easier access
from .credit import CreditInventory, CreditValidator, CreditInventoryManager, CreditManager
from .client import ExchangeClient
from .settlement import SettlementReport, SettlementConfirmation, SettlementManager

# Import implemented components
from .inventory import CreditInventoryEntry, CreditInventoryProcessor
from .account import AccountEntry, AccountIntegration