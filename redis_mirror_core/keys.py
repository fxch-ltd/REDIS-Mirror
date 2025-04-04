"""
Redis Mirror CE Key Management Module

This module provides utilities for generating and managing Redis keys
according to the Redis Mirror Community Edition standards.
"""

import logging
from typing import Optional, Tuple, Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


def generate_credit_inventory_key(user_id: str, asset: str) -> str:
    """
    Generate a Credit Inventory (CI) key following the Redis Mirror CE standard.
    
    Credit Inventory keys represent the double-entry bookkeeping mechanism for
    tracking pledged assets and obligations.
    
    Format: CI:{user_id}:{asset}
    
    Args:
        user_id: User identifier
        asset: Asset identifier (e.g., BTC, ETH, USDT)
        
    Returns:
        Formatted Credit Inventory key
    """
    return f"CI:{user_id}:{asset}"


def generate_exchange_stream_key(stream_type: str, identifier: str) -> str:
    """
    Generate an Exchange stream key following the Redis Mirror CE standard.
    
    Exchange stream keys are used for communication between the Exchange and WSP.
    
    Format: exchange:stream:{stream_type}:{identifier}
    
    Args:
        stream_type: Type of stream (e.g., "pledge", "settlement", "notification")
        identifier: Unique identifier for the specific stream
        
    Returns:
        Formatted Exchange stream key
    """
    return f"exchange:stream:{stream_type}:{identifier}"


def generate_custodian_stream_key(
    custodian_id: str, 
    user_id: str, 
    stream_type: str
) -> str:
    """
    Generate a Custodian stream key following the Redis Mirror CE standard.
    
    Custodian stream keys are used for communication from the WSP to the Exchange.
    
    Format: custodian:{custodian_id}:{user_id}:{stream_type}
    
    Args:
        custodian_id: Custodian identifier
        user_id: User identifier
        stream_type: Type of stream (e.g., "pledge", "settlement")
        
    Returns:
        Formatted Custodian stream key
    """
    return f"custodian:{custodian_id}:{user_id}:{stream_type}"


def generate_pledge_request_key(
    custodian_id: str, 
    user_id: str, 
    asset_id: str
) -> str:
    """
    Generate a pledge request key following the Redis Mirror CE standard.
    
    Format: custodian:{custodian_id}:{user_id}:{asset_id}:pledge
    
    Args:
        custodian_id: Custodian identifier
        user_id: User identifier
        asset_id: Asset identifier
        
    Returns:
        Formatted pledge request key
    """
    return f"custodian:{custodian_id}:{user_id}:{asset_id}:pledge"


def generate_pledge_response_key(
    custodian_id: str, 
    user_id: str
) -> str:
    """
    Generate a pledge response key following the Redis Mirror CE standard.
    
    Format: exchange:{custodian_id}:{user_id}:pledge_response
    
    Args:
        custodian_id: Custodian identifier
        user_id: User identifier
        
    Returns:
        Formatted pledge response key
    """
    return f"exchange:{custodian_id}:{user_id}:pledge_response"


def generate_settlement_report_key(custodian_id: str, user_id: str = None) -> str:
    """
    Generate a settlement report key following the Redis Mirror CE standard.
    
    Format: settlement:{user_id} if user_id is provided
    Format: exchange:{custodian_id}:settlement_report if user_id is not provided
    
    Args:
        custodian_id: Custodian identifier
        user_id: Optional user identifier for user-specific settlement reports
        
    Returns:
        Formatted settlement report key
    """
    if user_id:
        return f"settlement:{user_id}"
    return f"exchange:{custodian_id}:settlement_report"


def generate_settlement_completion_key(custodian_id: str, user_id: str = None) -> str:
    """
    Generate a settlement completion key following the Redis Mirror CE standard.
    
    Format: settlement:{user_id} if user_id is provided
    Format: exchange:{custodian_id}:settlement_completion if user_id is not provided
    
    Args:
        custodian_id: Custodian identifier
        user_id: Optional user identifier for user-specific settlement completions
        
    Returns:
        Formatted settlement completion key
    """
    if user_id:
        return f"settlement:{user_id}"
    return f"exchange:{custodian_id}:settlement_completion"


def generate_credit_request_stream_key() -> str:
    """
    Generate the credit request stream key.
    
    Format: mirror:credit:request
    
    Returns:
        Formatted credit request stream key
    """
    return "mirror:credit:request"


def generate_credit_response_stream_key() -> str:
    """
    Generate the credit response stream key.
    
    Format: mirror:credit:response
    
    Returns:
        Formatted credit response stream key
    """
    return "mirror:credit:response"


def generate_settlement_individual_response_key() -> str:
    """
    Generate the individual settlement response stream key.
    
    Format: mirror:settlement:individual:response
    
    Returns:
        Formatted individual settlement response stream key
    """
    return "mirror:settlement:individual:response"


def generate_settlement_batch_response_key() -> str:
    """
    Generate the batch settlement response stream key.
    
    Format: mirror:settlement:batch:response
    
    Returns:
        Formatted batch settlement response stream key
    """
    return "mirror:settlement:batch:response"


def parse_credit_inventory_key(key: str) -> Optional[Tuple[str, str]]:
    """
    Parse a Credit Inventory key to extract its components.
    
    Args:
        key: Credit Inventory key to parse
        
    Returns:
        Tuple of (user_id, asset) if the key is valid, None otherwise
    """
    parts = key.split(':')
    if len(parts) == 3 and parts[0] == 'CI':
        return (parts[1], parts[2])
    return None


def parse_exchange_stream_key(key: str) -> Optional[Tuple[str, str]]:
    """
    Parse an Exchange stream key to extract its components.
    
    Args:
        key: Exchange stream key to parse
        
    Returns:
        Tuple of (stream_type, identifier) if the key is valid, None otherwise
    """
    parts = key.split(':')
    if len(parts) >= 4 and parts[0] == 'exchange' and parts[1] == 'stream':
        return (parts[2], parts[3])
    return None


def parse_custodian_stream_key(key: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse a Custodian stream key to extract its components.
    
    Args:
        key: Custodian stream key to parse
        
    Returns:
        Tuple of (custodian_id, user_id, stream_type) if the key is valid, None otherwise
    """
    parts = key.split(':')
    if len(parts) >= 4 and parts[0] == 'custodian':
        return (parts[1], parts[2], parts[3])
    return None


def parse_settlement_key(key: str) -> Dict[str, Any]:
    """
    Parse a settlement key to extract its components.
    
    Args:
        key: Settlement key to parse
        
    Returns:
        Dictionary with parsed components
    """
    parts = key.split(':')
    
    if len(parts) == 2 and parts[0] == 'settlement':
        return {
            'type': 'user_settlement',
            'user_id': parts[1]
        }
    elif len(parts) == 3 and parts[0] == 'exchange' and 'settlement' in parts[2]:
        return {
            'type': 'custodian_settlement',
            'custodian_id': parts[1],
            'settlement_type': parts[2]
        }
    
    return {'type': 'unknown', 'key': key}


class KeyManager:
    """
    Manages Redis keys for the Redis Mirror CE system.
    
    This class provides a unified interface for generating and parsing
    Redis keys according to the Redis Mirror CE standards.
    """
    
    @staticmethod
    def credit_inventory(user_id: str, asset: str) -> str:
        """Generate a Credit Inventory key"""
        return generate_credit_inventory_key(user_id, asset)
    
    @staticmethod
    def exchange_stream(stream_type: str, identifier: str) -> str:
        """Generate an Exchange stream key"""
        return generate_exchange_stream_key(stream_type, identifier)
    
    @staticmethod
    def custodian_stream(custodian_id: str, user_id: str, stream_type: str) -> str:
        """Generate a Custodian stream key"""
        return generate_custodian_stream_key(custodian_id, user_id, stream_type)
    
    @staticmethod
    def pledge_request(custodian_id: str, user_id: str, asset_id: str) -> str:
        """Generate a pledge request key"""
        return generate_pledge_request_key(custodian_id, user_id, asset_id)
    
    @staticmethod
    def pledge_response(custodian_id: str, user_id: str) -> str:
        """Generate a pledge response key"""
        return generate_pledge_response_key(custodian_id, user_id)
    
    @staticmethod
    def settlement_report(custodian_id: str, user_id: str = None) -> str:
        """Generate a settlement report key"""
        return generate_settlement_report_key(custodian_id, user_id)
    
    @staticmethod
    def settlement_completion(custodian_id: str, user_id: str = None) -> str:
        """Generate a settlement completion key"""
        return generate_settlement_completion_key(custodian_id, user_id)
    
    @staticmethod
    def credit_request_stream() -> str:
        """Generate the credit request stream key"""
        return generate_credit_request_stream_key()
    
    @staticmethod
    def credit_response_stream() -> str:
        """Generate the credit response stream key"""
        return generate_credit_response_stream_key()
    
    @staticmethod
    def settlement_individual_response() -> str:
        """Generate the individual settlement response stream key"""
        return generate_settlement_individual_response_key()
    
    @staticmethod
    def settlement_batch_response() -> str:
        """Generate the batch settlement response stream key"""
        return generate_settlement_batch_response_key()
    
    @staticmethod
    def parse_key(key: str) -> Dict[str, Any]:
        """
        Parse a Redis key to extract its components.
        
        Args:
            key: Redis key to parse
            
        Returns:
            Dictionary with parsed components
        """
        if key.startswith('CI:'):
            result = parse_credit_inventory_key(key)
            if result:
                return {
                    'type': 'credit_inventory',
                    'user_id': result[0],
                    'asset': result[1]
                }
        elif key.startswith('exchange:stream:'):
            result = parse_exchange_stream_key(key)
            if result:
                return {
                    'type': 'exchange_stream',
                    'stream_type': result[0],
                    'identifier': result[1]
                }
        elif key.startswith('custodian:'):
            result = parse_custodian_stream_key(key)
            if result:
                return {
                    'type': 'custodian_stream',
                    'custodian_id': result[0],
                    'user_id': result[1],
                    'stream_type': result[2]
                }
        elif key.startswith('settlement:') or (key.startswith('exchange:') and 'settlement' in key):
            return parse_settlement_key(key)
        elif key == 'mirror:credit:request':
            return {'type': 'credit_request_stream'}
        elif key == 'mirror:credit:response':
            return {'type': 'credit_response_stream'}
        elif key == 'mirror:settlement:individual:response':
            return {'type': 'settlement_individual_response'}
        elif key == 'mirror:settlement:batch:response':
            return {'type': 'settlement_batch_response'}
        
        return {'type': 'unknown', 'key': key}