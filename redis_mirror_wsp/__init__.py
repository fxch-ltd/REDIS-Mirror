"""
Redis Mirror Community Edition SDK - WSP Module

This module provides components for Wallet Service Provider (WSP) integration with
the Redis Mirror Community Edition system, including credit request management,
settlement processing, and pledge management.
"""

__version__ = '0.1.0'

# Import components for easier access
from .credit import CreditRequest, CreditResponse, CreditRequestManager
from .client import WSPClient
from .settlement import SettlementReport, SettlementConfirmation, SettlementClient
from .pledge import PledgeRequest, PledgeResponse, PledgeManager
from .vault import VaultAsset, LobsterBasketPolicy, VaultManager