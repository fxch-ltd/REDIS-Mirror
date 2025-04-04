"""
Redis Mirror CE Testing Response Validators

This module provides validators for testing the Redis Mirror CE SDK,
including validators for credit requests, settlement reports, and other
responses.
"""

import time
import logging
import json
import re
from typing import Dict, Any, Optional, List, Callable, Union, Tuple, Set

from sdk_ce.redis_mirror_core import (
    ValidationError
)

# Configure logging
logger = logging.getLogger(__name__)


class Validator:
    """
    Base class for validators.
    
    This class provides common functionality for validators.
    """
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> List[str]:
        """
        Validate that required fields are present in the data.
        
        Args:
            data: Data to validate
            required_fields: List of required field names
            
        Returns:
            List of missing field names
        """
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        return missing_fields
    
    @staticmethod
    def validate_field_type(data: Dict[str, Any], field: str, expected_type: type) -> bool:
        """
        Validate that a field has the expected type.
        
        Args:
            data: Data to validate
            field: Field name
            expected_type: Expected type
            
        Returns:
            True if the field has the expected type, False otherwise
        """
        if field not in data:
            return False
        
        value = data[field]
        
        # Handle special case for numeric types
        if expected_type == float and isinstance(value, (int, float)):
            return True
        
        return isinstance(value, expected_type)
    
    @staticmethod
    def validate_field_regex(data: Dict[str, Any], field: str, pattern: str) -> bool:
        """
        Validate that a field matches a regex pattern.
        
        Args:
            data: Data to validate
            field: Field name
            pattern: Regex pattern
            
        Returns:
            True if the field matches the pattern, False otherwise
        """
        if field not in data:
            return False
        
        value = data[field]
        
        if not isinstance(value, str):
            return False
        
        return bool(re.match(pattern, value))
    
    @staticmethod
    def validate_field_range(
        data: Dict[str, Any],
        field: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> bool:
        """
        Validate that a numeric field is within a range.
        
        Args:
            data: Data to validate
            field: Field name
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            
        Returns:
            True if the field is within the range, False otherwise
        """
        if field not in data:
            return False
        
        value = data[field]
        
        if not isinstance(value, (int, float)):
            return False
        
        if min_value is not None and value < min_value:
            return False
        
        if max_value is not None and value > max_value:
            return False
        
        return True
    
    @staticmethod
    def validate_field_in_list(data: Dict[str, Any], field: str, valid_values: List[Any]) -> bool:
        """
        Validate that a field's value is in a list of valid values.
        
        Args:
            data: Data to validate
            field: Field name
            valid_values: List of valid values
            
        Returns:
            True if the field's value is in the list, False otherwise
        """
        if field not in data:
            return False
        
        value = data[field]
        
        return value in valid_values


class CreditRequestValidator(Validator):
    """
    Validator for credit requests.
    
    This class provides methods for validating credit request data.
    """
    
    @classmethod
    def validate_credit_request(cls, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a credit request.
        
        Args:
            request: Credit request data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "request_id", "user_id", "asset", "amount",
            "custodian_id", "chain", "address", "timestamp"
        ]
        
        missing_fields = cls.validate_required_fields(request, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "request_id": str,
            "user_id": str,
            "asset": str,
            "amount": float,
            "custodian_id": str,
            "chain": str,
            "address": str,
            "timestamp": int,
            "is_increase": bool,
            "status": str
        }
        
        for field, expected_type in field_types.items():
            if field in request and not cls.validate_field_type(request, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate amount is positive
        if "amount" in request and isinstance(request["amount"], (int, float)):
            if request["amount"] <= 0:
                result["success"] = False
                result["errors"].append("Amount must be positive")
        
        # Validate status is valid
        if "status" in request:
            valid_statuses = ["pending", "approved", "rejected"]
            if not cls.validate_field_in_list(request, "status", valid_statuses):
                result["success"] = False
                result["errors"].append(f"Invalid status: {request['status']}")
        
        return result
    
    @classmethod
    def validate_credit_response(cls, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a credit response.
        
        Args:
            response: Credit response data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "request_id", "user_id", "asset", "amount",
            "timestamp", "status"
        ]
        
        missing_fields = cls.validate_required_fields(response, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "request_id": str,
            "user_id": str,
            "asset": str,
            "amount": float,
            "timestamp": int,
            "status": str,
            "vault_id": str,
            "reject_reason": str
        }
        
        for field, expected_type in field_types.items():
            if field in response and not cls.validate_field_type(response, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate status is valid
        if "status" in response:
            valid_statuses = ["approved", "rejected"]
            if not cls.validate_field_in_list(response, "status", valid_statuses):
                result["success"] = False
                result["errors"].append(f"Invalid status: {response['status']}")
        
        # Validate approved response has vault_id
        if "status" in response and response["status"] == "approved":
            if "vault_id" not in response or not response["vault_id"]:
                result["success"] = False
                result["errors"].append("Approved response must have vault_id")
        
        # Validate rejected response has reject_reason
        if "status" in response and response["status"] == "rejected":
            if "reject_reason" not in response or not response["reject_reason"]:
                result["success"] = False
                result["errors"].append("Rejected response must have reject_reason")
        
        return result


class SettlementValidator(Validator):
    """
    Validator for settlement data.
    
    This class provides methods for validating settlement data.
    """
    
    @classmethod
    def validate_settlement_report(cls, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a settlement report.
        
        Args:
            report: Settlement report data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "settlement_id", "user_id", "settlement_date",
            "trades", "positions", "status"
        ]
        
        missing_fields = cls.validate_required_fields(report, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "settlement_id": str,
            "user_id": str,
            "settlement_date": int,
            "trades": list,
            "positions": list,
            "status": str
        }
        
        for field, expected_type in field_types.items():
            if field in report and not cls.validate_field_type(report, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate status is valid
        if "status" in report:
            valid_statuses = ["pending", "confirmed", "rejected", "completed"]
            if not cls.validate_field_in_list(report, "status", valid_statuses):
                result["success"] = False
                result["errors"].append(f"Invalid status: {report['status']}")
        
        # Validate trades
        if "trades" in report and isinstance(report["trades"], list):
            for i, trade in enumerate(report["trades"]):
                trade_result = cls.validate_trade(trade)
                if not trade_result["success"]:
                    result["success"] = False
                    for error in trade_result["errors"]:
                        result["errors"].append(f"Trade {i}: {error}")
        
        # Validate positions
        if "positions" in report and isinstance(report["positions"], list):
            for i, position in enumerate(report["positions"]):
                position_result = cls.validate_position(position)
                if not position_result["success"]:
                    result["success"] = False
                    for error in position_result["errors"]:
                        result["errors"].append(f"Position {i}: {error}")
        
        return result
    
    @classmethod
    def validate_trade(cls, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a trade.
        
        Args:
            trade: Trade data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "trade_id", "user_id", "buy_asset", "sell_asset",
            "buy_amount", "sell_amount", "timestamp"
        ]
        
        missing_fields = cls.validate_required_fields(trade, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "trade_id": str,
            "user_id": str,
            "buy_asset": str,
            "sell_asset": str,
            "buy_amount": float,
            "sell_amount": float,
            "timestamp": int
        }
        
        for field, expected_type in field_types.items():
            if field in trade and not cls.validate_field_type(trade, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate amounts are positive
        if "buy_amount" in trade and isinstance(trade["buy_amount"], (int, float)):
            if trade["buy_amount"] <= 0:
                result["success"] = False
                result["errors"].append("Buy amount must be positive")
        
        if "sell_amount" in trade and isinstance(trade["sell_amount"], (int, float)):
            if trade["sell_amount"] <= 0:
                result["success"] = False
                result["errors"].append("Sell amount must be positive")
        
        return result
    
    @classmethod
    def validate_position(cls, position: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a position.
        
        Args:
            position: Position data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = ["asset", "amount"]
        
        missing_fields = cls.validate_required_fields(position, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "asset": str,
            "amount": float
        }
        
        for field, expected_type in field_types.items():
            if field in position and not cls.validate_field_type(position, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        return result
    
    @classmethod
    def validate_settlement_confirmation(cls, confirmation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a settlement confirmation.
        
        Args:
            confirmation: Settlement confirmation data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "confirmation_id", "settlement_id", "user_id",
            "timestamp", "status"
        ]
        
        missing_fields = cls.validate_required_fields(confirmation, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "confirmation_id": str,
            "settlement_id": str,
            "user_id": str,
            "timestamp": int,
            "status": str
        }
        
        for field, expected_type in field_types.items():
            if field in confirmation and not cls.validate_field_type(confirmation, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate status is valid
        if "status" in confirmation:
            valid_statuses = ["confirmed", "rejected"]
            if not cls.validate_field_in_list(confirmation, "status", valid_statuses):
                result["success"] = False
                result["errors"].append(f"Invalid status: {confirmation['status']}")
        
        return result


class EventValidator(Validator):
    """
    Validator for event data.
    
    This class provides methods for validating event data.
    """
    
    @classmethod
    def validate_event(cls, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate an event.
        
        Args:
            event: Event data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "event_id", "event_type", "source", "timestamp", "data"
        ]
        
        missing_fields = cls.validate_required_fields(event, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "event_id": str,
            "event_type": str,
            "source": str,
            "timestamp": int,
            "data": dict
        }
        
        for field, expected_type in field_types.items():
            if field in event and not cls.validate_field_type(event, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate event_type format
        if "event_type" in event and isinstance(event["event_type"], str):
            if not cls.validate_field_regex(event, "event_type", r"^[a-z]+:[a-z]+$"):
                result["success"] = False
                result["errors"].append("Event type must be in format 'category:name'")
        
        # Validate source is valid
        if "source" in event:
            valid_sources = ["wsp", "exchange", "system"]
            if not cls.validate_field_in_list(event, "source", valid_sources):
                result["success"] = False
                result["errors"].append(f"Invalid source: {event['source']}")
        
        # Validate data based on event_type
        if "event_type" in event and "data" in event:
            event_type = event["event_type"]
            data = event["data"]
            
            if event_type.startswith("credit:"):
                if event_type == "credit:request":
                    data_result = CreditRequestValidator.validate_credit_request(data)
                    if not data_result["success"]:
                        result["success"] = False
                        for error in data_result["errors"]:
                            result["errors"].append(f"Data: {error}")
                elif event_type == "credit:response":
                    data_result = CreditRequestValidator.validate_credit_response(data)
                    if not data_result["success"]:
                        result["success"] = False
                        for error in data_result["errors"]:
                            result["errors"].append(f"Data: {error}")
            elif event_type.startswith("settlement:"):
                if event_type == "settlement:report":
                    data_result = SettlementValidator.validate_settlement_report(data)
                    if not data_result["success"]:
                        result["success"] = False
                        for error in data_result["errors"]:
                            result["errors"].append(f"Data: {error}")
                elif event_type == "settlement:confirmation":
                    data_result = SettlementValidator.validate_settlement_confirmation(data)
                    if not data_result["success"]:
                        result["success"] = False
                        for error in data_result["errors"]:
                            result["errors"].append(f"Data: {error}")
        
        return result


class AccountValidator(Validator):
    """
    Validator for account data.
    
    This class provides methods for validating account data.
    """
    
    @classmethod
    def validate_account_entry(cls, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate an account entry.
        
        Args:
            entry: Account entry data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "user_id", "asset", "balance", "available", "reserved"
        ]
        
        missing_fields = cls.validate_required_fields(entry, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "user_id": str,
            "asset": str,
            "balance": float,
            "available": float,
            "reserved": float,
            "timestamp": int,
            "transaction_id": str
        }
        
        for field, expected_type in field_types.items():
            if field in entry and not cls.validate_field_type(entry, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate balance is non-negative
        if "balance" in entry and isinstance(entry["balance"], (int, float)):
            if entry["balance"] < 0:
                result["success"] = False
                result["errors"].append("Balance must be non-negative")
        
        # Validate available is non-negative
        if "available" in entry and isinstance(entry["available"], (int, float)):
            if entry["available"] < 0:
                result["success"] = False
                result["errors"].append("Available balance must be non-negative")
        
        # Validate reserved is non-negative
        if "reserved" in entry and isinstance(entry["reserved"], (int, float)):
            if entry["reserved"] < 0:
                result["success"] = False
                result["errors"].append("Reserved balance must be non-negative")
        
        # Validate balance = available + reserved
        if all(field in entry and isinstance(entry[field], (int, float)) 
               for field in ["balance", "available", "reserved"]):
            if abs(entry["balance"] - (entry["available"] + entry["reserved"])) > 1e-8:
                result["success"] = False
                result["errors"].append("Balance must equal available + reserved")
        
        return result
    
    @classmethod
    def validate_credit_inventory_entry(cls, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a credit inventory entry.
        
        Args:
            entry: Credit inventory entry data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = ["user_id", "asset", "amount"]
        
        missing_fields = cls.validate_required_fields(entry, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "user_id": str,
            "asset": str,
            "amount": float,
            "timestamp": int,
            "transaction_id": str
        }
        
        for field, expected_type in field_types.items():
            if field in entry and not cls.validate_field_type(entry, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate amount is non-negative
        if "amount" in entry and isinstance(entry["amount"], (int, float)):
            if entry["amount"] < 0:
                result["success"] = False
                result["errors"].append("Amount must be non-negative")
        
        return result
    
    @classmethod
    def validate_vault_asset(cls, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a vault asset.
        
        Args:
            asset: Vault asset data
            
        Returns:
            Validation result with success flag and errors
        """
        result = {
            "success": True,
            "errors": []
        }
        
        # Validate required fields
        required_fields = [
            "vault_id", "user_id", "asset", "amount", "status",
            "chain", "address"
        ]
        
        missing_fields = cls.validate_required_fields(asset, required_fields)
        if missing_fields:
            result["success"] = False
            result["errors"].append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate field types
        field_types = {
            "vault_id": str,
            "user_id": str,
            "asset": str,
            "amount": float,
            "status": str,
            "chain": str,
            "address": str,
            "timestamp": int,
            "pledge_request_id": str
        }
        
        for field, expected_type in field_types.items():
            if field in asset and not cls.validate_field_type(asset, field, expected_type):
                result["success"] = False
                result["errors"].append(f"Field '{field}' has invalid type")
        
        # Validate amount is positive
        if "amount" in asset and isinstance(asset["amount"], (int, float)):
            if asset["amount"] <= 0:
                result["success"] = False
                result["errors"].append("Amount must be positive")
        
        # Validate status is valid
        if "status" in asset:
            valid_statuses = ["locked", "unlocked", "pending"]
            if not cls.validate_field_in_list(asset, "status", valid_statuses):
                result["success"] = False
                result["errors"].append(f"Invalid status: {asset['status']}")
        
        return result