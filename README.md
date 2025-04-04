# Redis Mirror Community Edition SDK

<img src="assets/redis_mirror_logo.jpeg" alt="Redis Mirror Logo" width="1000"/>

A comprehensive Python SDK for integrating with the Redis Mirror Community Edition system, providing tools for both Wallet Service Providers (WSPs) and Exchanges. Created by [FXCH Ltd.](https://fxclr.com)

## Overview

The Redis Mirror Community Edition SDK simplifies integration with the Redis Mirror system by providing a standardized approach for "off-Exchange" integration between cryptocurrency exchanges and custodians. It leverages Redis distributed cache technology to allow exchanges to "mirror" user asset balances held at Wallet Service Providers (WSPs) and custodians without requiring direct asset transfers.

This standard enables users to trade on exchanges using assets that remain securely held at their custodian of choice, addressing key challenges in the cryptocurrency trading ecosystem.

## Architecture

Redis Mirror operates on a unique architecture that enables secure, real-time communication between WSPs and Exchanges:

```mermaid
flowchart LR
    subgraph "WSP Environment"
        WSP[WSP Application]
        WSP_REDIS[WSP Redis]
    end
    
    subgraph "Exchange Environment"
        EXCHANGE[Exchange Application]
        REPLICA[Stream-Writeable Replica]
    end
    
    WSP -->|"Read/Write"| WSP_REDIS
    WSP_REDIS -->|"Replicate"| REPLICA
    EXCHANGE -->|"Write to Streams\nRead CI Values"| REPLICA
    WSP -->|"Subscribe"| REPLICA
```

The Redis Mirror Community Edition uses a specific Redis architecture:

1. A main Redis instance at the WSP
2. A replica Redis instance with a special ACL configuration that allows the Exchange to:
   - Write specifically to stream keys
   - Read Credit Inventory (CI) values

This architecture ensures data integrity while enabling real-time communication between parties.

## Key Features

- **Standardized Integration**: One standard that works with multiple custodians and exchanges
- **Connection Management**: Simplified Redis connection handling for both WSP and Exchange environments
- **Key Management**: Standardized Redis key patterns for consistent data storage
- **ACL Management**: Secure access control for different components
- **Stream Processing**: Real-time event handling for pledges, settlements, and notifications
- **Credit Management**: Tools for managing credit requests, approvals, and inventory
- **Configuration Utilities**: Flexible configuration options for different environments
- **Testing Tools**: Comprehensive testing utilities for integration validation

## Installation

```bash
# Not yet available on PyPI
# pip install redis-mirror-ce

# For now, clone the repository and install locally
git clone <repository-url>
cd redis-mirror-ce
pip install -e .
```

### Prerequisites

- Python 3.7+
- Redis 5.0+ (Redis 6.0+ recommended for enhanced security features)
- redis-py client library

## Comprehensive Usage Guide

### WSP Integration

The SDK provides a complete toolkit for WSP integration with the Redis Mirror system:

```python
from sdk_ce.redis_mirror_core import Configuration, RedisConnectionManager
from sdk_ce.redis_mirror_wsp import WSPClient

# Initialize configuration
config = Configuration.from_file('config.json')
# Or use a predefined profile
# config = ConfigurationProfile.development()

# Create connection manager
connection_manager = RedisConnectionManager(
    wsp_config=config.get_wsp_redis_config(),
    replica_config=config.get_replica_redis_config()
)

# Create WSP client
wsp_client = WSPClient(connection_manager, config)

# Request credit increase
response = wsp_client.request_credit_increase(
    user_id="user123",
    asset="BTC",
    amount=10.0,
    custodian_id="ledger",
    chain="Bitcoin",
    address="bc1q..."
)

# Check response status
if response.status == "accepted":
    print(f"Credit increase accepted, new balance: {response.new_balance}")
else:
    print(f"Credit increase rejected: {response.reject_reason}")

# Process settlement report
settlement_reports = wsp_client.process_settlement_reports()
for report in settlement_reports:
    print(f"Settlement report for {report.user_id}: {report.asset} {report.amount}")

# Monitor credit events
wsp_client.start_credit_event_listener(callback=lambda event: print(f"Credit event: {event}"))

# Clean up when done
connection_manager.close()
```

### Exchange Integration

The SDK also provides tools for Exchange integration:

```python
from sdk_ce.redis_mirror_core import Configuration, RedisConnectionManager
from sdk_ce.redis_mirror_exchange import ExchangeClient

# Initialize configuration
config = Configuration.from_file('config.json')

# Create connection manager
connection_manager = RedisConnectionManager(
    wsp_config=config.get_wsp_redis_config(),
    replica_config=config.get_replica_redis_config()
)

# Create Exchange client
exchange_client = ExchangeClient(connection_manager, config)

# Process credit requests
def handle_credit_request(request):
    user_id = request.user_id
    asset = request.asset
    amount = request.amount
    
    # Implement your business logic here
    approved = True
    reject_reason = ""
    
    if approved:
        # Update user's credit inventory
        exchange_client.update_credit_inventory(user_id, asset, amount)
        return exchange_client.approve_credit_request(request)
    else:
        return exchange_client.reject_credit_request(request, reject_reason)

# Start processing credit requests
exchange_client.start_credit_request_processor(callback=handle_credit_request)

# Read Credit Inventory values
ci_value = exchange_client.get_credit_inventory("user123", "BTC")
print(f"Credit Inventory for user123 BTC: {ci_value}")

# Generate settlement reports
settlement_reports = exchange_client.generate_settlement_reports()
for report in settlement_reports:
    print(f"Generated settlement report: {report}")

# Clean up when done
exchange_client.stop_credit_request_processor()
connection_manager.close()
```

### Event-Driven Architecture

Redis Mirror uses Redis Streams for event-driven communication:

```python
from sdk_ce.redis_mirror_core import StreamProcessor, KeyManager

# Create a stream processor
stream_processor = StreamProcessor(redis_client, consumer_group="my-app", consumer_name="consumer-1")

# Define a message handler
def handle_message(message):
    print(f"Received message: {message}")
    # Process the message
    return True  # Return True to acknowledge the message

# Start processing messages
stream_processor.process_stream(
    stream_key=KeyManager.credit_request_stream(),
    handler=handle_message,
    batch_size=10,
    block_ms=2000
)
```

## Core Components

### Connection Management

The SDK provides utilities for managing Redis connections:

```python
from sdk_ce.redis_mirror_core import RedisConnectionManager

# Create connection manager
connection_manager = RedisConnectionManager(
    wsp_config={
        "host": "localhost",
        "port": 6379,
        "password": "",
        "db": 0
    },
    replica_config={
        "host": "replica.example.com",
        "port": 6379,
        "password": "password",
        "db": 0
    }
)

# Get clients
wsp_client = connection_manager.get_wsp_client()
replica_client = connection_manager.get_replica_client()

# Use with context manager
with RedisConnectionManager(wsp_config, replica_config) as manager:
    # Connections are automatically closed when exiting the context
    wsp_client = manager.get_wsp_client()
```

### Key Management

The SDK provides utilities for generating and parsing Redis keys:

```python
from sdk_ce.redis_mirror_core import KeyManager

# Generate keys
ci_key = KeyManager.credit_inventory("user123", "BTC")
stream_key = KeyManager.exchange_stream("pledge", "user123")

# Parse keys
key_info = KeyManager.parse_key("CI:user123:BTC")
```

### ACL Management

The SDK provides utilities for setting up and managing Redis ACLs:

```python
from sdk_ce.redis_mirror_core import ACLManager, RedisConnectionManager

# Create connection manager with admin privileges
connection_manager = RedisConnectionManager(wsp_config)
admin_client = connection_manager.get_wsp_client()

# Create ACL manager
acl_manager = ACLManager(admin_client)

# Set up ACLs
acl_manager.setup_exchange_user("exchange", "password")
acl_manager.setup_wsp_user("wsp", "password")

# Test ACLs
exchange_client = connection_manager.get_replica_client()
acl_manager.test_exchange_permissions(exchange_client, "exchange:stream:test")
```

### Configuration

The SDK provides utilities for managing configuration:

```python
from sdk_ce.redis_mirror_core import Configuration, ConfigurationProfile

# Create configuration from file
config = Configuration.from_file('config.json')

# Create configuration from environment variables
config = Configuration.from_env(prefix="REDIS_MIRROR_")

# Use predefined profiles
dev_config = ConfigurationProfile.development()
prod_config = ConfigurationProfile.production()
test_config = ConfigurationProfile.testing()

# Get configuration values
wsp_host = config.get("wsp_redis.host")
log_level = config.get("logging.level")

# Set configuration values
config.set("wsp_redis.host", "new-host.example.com")
```

## WSP Components

The WSP module provides components for WSP integration:

### Credit Request Manager

```python
from sdk_ce.redis_mirror_wsp import CreditRequestManager

# Create credit request manager
credit_manager = CreditRequestManager(connection_manager, config)

# Request credit increase
response = credit_manager.request_credit_increase(
    user_id="user123",
    asset="BTC",
    amount=10.0,
    custodian_id="ledger",
    chain="Bitcoin",
    address="bc1q..."
)
```

### Settlement Client

```python
from sdk_ce.redis_mirror_wsp import SettlementClient

# Create settlement client
settlement_client = SettlementClient(connection_manager, config)

# Process settlement reports
reports = settlement_client.process_settlement_reports()
```

### Pledge Manager

```python
from sdk_ce.redis_mirror_wsp import PledgeManager

# Create pledge manager
pledge_manager = PledgeManager(connection_manager, config)

# Create a pledge
pledge = pledge_manager.create_pledge(
    user_id="user123",
    asset="BTC",
    amount=5.0,
    custodian_id="ledger",
    chain="Bitcoin",
    address="bc1q..."
)
```

### Vault Manager

```python
from sdk_ce.redis_mirror_wsp import VaultManager

# Create vault manager
vault_manager = VaultManager(connection_manager, config)

# Get vault balance
balance = vault_manager.get_balance("BTC")
```

## Exchange Components

The Exchange module provides components for Exchange integration:

### Credit Manager

```python
from sdk_ce.redis_mirror_exchange import CreditManager

# Create credit manager
credit_manager = CreditManager(connection_manager, config)

# Process credit requests
credit_manager.start_processor(callback=lambda request: print(f"Processing request: {request}"))

# Read Credit Inventory
ci_value = credit_manager.get_credit_inventory("user123", "BTC")
```

### Settlement Manager

```python
from sdk_ce.redis_mirror_exchange import SettlementManager

# Create settlement manager
settlement_manager = SettlementManager(connection_manager, config)

# Generate settlement reports
reports = settlement_manager.generate_settlement_reports()
```

### Credit Inventory Processor

```python
from sdk_ce.redis_mirror_exchange import CreditInventoryProcessor

# Create credit inventory processor
inventory_processor = CreditInventoryProcessor(connection_manager, config)

# Update credit inventory
inventory_processor.update_inventory("user123", "BTC", 10.0)

# Read credit inventory
ci_value = inventory_processor.get_inventory("user123", "BTC")
```

### Account Integration

```python
from sdk_ce.redis_mirror_exchange import AccountIntegration

# Create account integration
account_integration = AccountIntegration(connection_manager, config)

# Apply credit to user account
account_integration.apply_credit("user123", "BTC", 10.0)
```

## Testing Components

The Testing module provides components for testing:

### Mock Redis Client

```python
from sdk_ce.redis_mirror_testing import MockRedisClient

# Create mock Redis client
mock_client = MockRedisClient()

# Set up test data
mock_client.set("test:key", "value")
mock_client.xadd("test:stream", {"field": "value"})

# Use in tests
assert mock_client.get("test:key") == "value"
```

### Data Generators

```python
from sdk_ce.redis_mirror_testing import DataGenerators

# Generate test data
user_id = DataGenerators.generate_user_id()
credit_request = DataGenerators.generate_credit_request(user_id, "BTC", 10.0)
```

### Test Scenarios

```python
from sdk_ce.redis_mirror_testing import TestScenarios

# Run a test scenario
scenario = TestScenarios.credit_request_approval_scenario()
scenario.run()
```

### Response Validators

```python
from sdk_ce.redis_mirror_testing import ResponseValidators

# Validate a response
credit_response = {"status": "accepted", "ci": "10.0"}
assert ResponseValidators.validate_credit_response(credit_response)
```

## Redis as the Backbone

<img src="assets/redis_mirror_logo.jpeg" alt="Redis Mirror Architecture" width="300" align="right"/>

Redis is the backbone of the Redis Mirror solution, providing the critical infrastructure for real-time communication between WSPs and Exchanges. The Redis Mirror Community Edition leverages several key Redis features:

### Redis Streams

Redis Streams are used for event-driven communication between WSPs and Exchanges. This provides:

- Real-time event processing
- Reliable message delivery
- Consumer groups for load balancing
- Message acknowledgment and recovery

### Redis ACLs

Access Control Lists (ACLs) in Redis 6.0+ provide fine-grained security controls:

- Restricted write access for Exchanges (limited to stream keys)
- Restricted read access for Exchanges (limited to CI keys)
- Pattern-based key restrictions
- Command-level permissions
- User-based authentication

### Redis Replication

The Redis Mirror architecture uses Redis replication with a special configuration:

- WSP Redis instance as the master
- Stream-Writeable Replica at the Exchange
- Special ACL configuration to allow limited writes to the replica and reading of CI values

This unique configuration enables secure, real-time communication while maintaining data integrity.

## License and Contribution

### License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) - see the LICENSE file for details.

### Contribution

Contributions to the Redis Mirror Community Edition SDK are welcome. Please follow these steps to contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

### Acknowledgments

Redis Mirror Community Edition is built on Redis, an open-source, in-memory data structure store used as a database, cache, and message broker. We extend our gratitude to the Redis community for creating and maintaining this powerful tool that serves as the backbone of our solution.

---

<img src="assets/fxch_logo.png" alt="FXCH Logo" width="150"/>

*Redis Mirror Community Edition SDK is created and maintained by [FXCH Ltd.](https://fxclr.com)*
