"""
Redis Mirror CE SDK - Testing Example

This example demonstrates how to use the Testing module components of the
Redis Mirror CE SDK for testing integrations.
"""

import time
import logging
import json
from typing import Dict, Any

from sdk_ce.redis_mirror_core import (
    RedisConnectionManager,
    Configuration
)
from sdk_ce.redis_mirror_testing import (
    MockRedisClient,
    DataGenerator,
    CreditRequestGenerator,
    SettlementGenerator,
    EventGenerator,
    AccountGenerator,
    TestScenario,
    CreditRequestScenario,
    SettlementScenario,
    EventHandlingScenario,
    IntegrationScenario,
    Validator,
    CreditRequestValidator,
    SettlementValidator,
    EventValidator,
    AccountValidator
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def mock_redis_example():
    """Example of using the MockRedisClient."""
    logger.info("Starting MockRedisClient example")
    
    # Create a mock Redis client
    mock_redis = MockRedisClient()
    
    # Set a key
    mock_redis.set("test-key", "test-value")
    
    # Get the key
    value = mock_redis.get("test-key")
    logger.info(f"Retrieved value: {value}")
    
    # Set a hash
    mock_redis.hset("test-hash", "field1", "value1")
    mock_redis.hset("test-hash", "field2", "value2")
    
    # Get the hash
    hash_value = mock_redis.hgetall("test-hash")
    logger.info(f"Retrieved hash: {hash_value}")
    
    # Create a stream
    mock_redis.xadd("test-stream", {"key1": "value1", "key2": "value2"})
    mock_redis.xadd("test-stream", {"key1": "value3", "key2": "value4"})
    
    # Read from the stream
    stream_data = mock_redis.xread({"test-stream": "0-0"}, count=10)
    logger.info(f"Retrieved stream data: {stream_data}")
    
    # Create a consumer group
    mock_redis.xgroup_create("test-stream", "test-group", "0-0", mkstream=True)
    
    # Read from the consumer group
    group_data = mock_redis.xreadgroup("test-group", "test-consumer", {"test-stream": ">"}, count=10)
    logger.info(f"Retrieved consumer group data: {group_data}")
    
    logger.info("MockRedisClient example completed")


def data_generator_example():
    """Example of using the data generators."""
    logger.info("Starting data generator example")
    
    # Generate a credit request
    credit_request = CreditRequestGenerator.generate_credit_request(
        user_id="user123",
        asset="BTC",
        amount=1.0,
        custodian_id="ledger",
        chain="Bitcoin",
        address="bc1q..."
    )
    
    logger.info(f"Generated credit request: {credit_request}")
    
    # Generate a credit response
    credit_response = CreditRequestGenerator.generate_credit_response(
        request=credit_request,
        status="approved",
        vault_id="vault-123"
    )
    
    logger.info(f"Generated credit response: {credit_response}")
    
    # Generate a batch of credit requests
    credit_requests = CreditRequestGenerator.generate_credit_requests_batch(5)
    logger.info(f"Generated {len(credit_requests)} credit requests")
    
    # Generate a trade
    trade = SettlementGenerator.generate_trade(
        user_id="user123",
        buy_asset="BTC",
        sell_asset="USD",
        buy_amount=1.0,
        sell_amount=50000.0
    )
    
    logger.info(f"Generated trade: {trade}")
    
    # Generate a settlement report
    settlement_report = SettlementGenerator.generate_settlement_report(
        user_id="user123",
        trades=[trade]
    )
    
    logger.info(f"Generated settlement report: {settlement_report}")
    
    # Generate a settlement confirmation
    settlement_confirmation = SettlementGenerator.generate_settlement_confirmation(
        settlement_report=settlement_report,
        status="confirmed"
    )
    
    logger.info(f"Generated settlement confirmation: {settlement_confirmation}")
    
    # Generate an event
    event = EventGenerator.generate_event(
        event_type="credit:request",
        source="wsp",
        data=credit_request
    )
    
    logger.info(f"Generated event: {event}")
    
    # Generate a batch of events
    events = EventGenerator.generate_events_batch(5)
    logger.info(f"Generated {len(events)} events")
    
    # Generate an account entry
    account_entry = AccountGenerator.generate_account_entry(
        user_id="user123",
        asset="BTC",
        balance=10.0,
        available=8.0,
        reserved=2.0
    )
    
    logger.info(f"Generated account entry: {account_entry}")
    
    # Generate a credit inventory entry
    ci_entry = AccountGenerator.generate_credit_inventory_entry(
        user_id="user123",
        asset="BTC",
        amount=10.0
    )
    
    logger.info(f"Generated credit inventory entry: {ci_entry}")
    
    # Generate a vault asset
    vault_asset = AccountGenerator.generate_vault_asset(
        user_id="user123",
        asset="BTC",
        amount=1.0,
        status="locked",
        chain="Bitcoin",
        address="bc1q..."
    )
    
    logger.info(f"Generated vault asset: {vault_asset}")
    
    logger.info("Data generator example completed")


def validator_example():
    """Example of using the validators."""
    logger.info("Starting validator example")
    
    # Generate a credit request
    credit_request = CreditRequestGenerator.generate_credit_request()
    
    # Validate the credit request
    validation_result = CreditRequestValidator.validate_credit_request(credit_request)
    logger.info(f"Credit request validation result: {validation_result}")
    
    # Generate a credit response
    credit_response = CreditRequestGenerator.generate_credit_response(
        request=credit_request,
        status="approved",
        vault_id="vault-123"
    )
    
    # Validate the credit response
    validation_result = CreditRequestValidator.validate_credit_response(credit_response)
    logger.info(f"Credit response validation result: {validation_result}")
    
    # Generate a settlement report
    settlement_report = SettlementGenerator.generate_settlement_report()
    
    # Validate the settlement report
    validation_result = SettlementValidator.validate_settlement_report(settlement_report)
    logger.info(f"Settlement report validation result: {validation_result}")
    
    # Generate a settlement confirmation
    settlement_confirmation = SettlementGenerator.generate_settlement_confirmation(
        settlement_report=settlement_report,
        status="confirmed"
    )
    
    # Validate the settlement confirmation
    validation_result = SettlementValidator.validate_settlement_confirmation(settlement_confirmation)
    logger.info(f"Settlement confirmation validation result: {validation_result}")
    
    # Generate an event
    event = EventGenerator.generate_event()
    
    # Validate the event
    validation_result = EventValidator.validate_event(event)
    logger.info(f"Event validation result: {validation_result}")
    
    # Generate an account entry
    account_entry = AccountGenerator.generate_account_entry()
    
    # Validate the account entry
    validation_result = AccountValidator.validate_account_entry(account_entry)
    logger.info(f"Account entry validation result: {validation_result}")
    
    # Generate a credit inventory entry
    ci_entry = AccountGenerator.generate_credit_inventory_entry()
    
    # Validate the credit inventory entry
    validation_result = AccountValidator.validate_credit_inventory_entry(ci_entry)
    logger.info(f"Credit inventory entry validation result: {validation_result}")
    
    # Generate a vault asset
    vault_asset = AccountGenerator.generate_vault_asset()
    
    # Validate the vault asset
    validation_result = AccountValidator.validate_vault_asset(vault_asset)
    logger.info(f"Vault asset validation result: {validation_result}")
    
    logger.info("Validator example completed")


def test_scenario_example():
    """Example of using the test scenarios."""
    logger.info("Starting test scenario example")
    
    # Create a configuration
    config = Configuration({
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
    
    # Run a credit request scenario
    logger.info("Running credit request scenario")
    credit_scenario = CreditRequestScenario(use_mock=True)
    credit_results = credit_scenario.execute()
    logger.info(f"Credit request scenario results: {credit_results}")
    
    # Run a settlement scenario
    logger.info("Running settlement scenario")
    settlement_scenario = SettlementScenario(use_mock=True)
    settlement_results = settlement_scenario.execute()
    logger.info(f"Settlement scenario results: {settlement_results}")
    
    # Run an event handling scenario
    logger.info("Running event handling scenario")
    event_scenario = EventHandlingScenario(use_mock=True)
    event_results = event_scenario.execute()
    logger.info(f"Event handling scenario results: {event_results}")
    
    # Run an integration scenario
    logger.info("Running integration scenario")
    integration_scenario = IntegrationScenario(use_mock=True)
    integration_results = integration_scenario.execute()
    logger.info(f"Integration scenario results: {integration_results}")
    
    logger.info("Test scenario example completed")


def main():
    """Run all examples."""
    logger.info("Starting Testing module examples")
    
    # Run mock Redis example
    mock_redis_example()
    
    # Run data generator example
    data_generator_example()
    
    # Run validator example
    validator_example()
    
    # Run test scenario example
    test_scenario_example()
    
    logger.info("All Testing module examples completed")


if __name__ == "__main__":
    main()