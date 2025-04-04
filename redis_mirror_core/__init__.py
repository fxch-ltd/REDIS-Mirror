"""
Redis Mirror Community Edition SDK - Core Module

This module provides the foundational components for interacting with the Redis Mirror
Community Edition system, including connection management, key management, stream processing,
ACL management, event handling, and error handling.
"""

__version__ = '0.1.0'

# Import core components for easier access
from .connection import (
    create_redis_client,
    create_stream_writeable_replica_client,
    RedisConnectionManager,
    check_connection,
    get_connection_info
)

from .keys import (
    generate_credit_inventory_key,
    generate_exchange_stream_key,
    generate_custodian_stream_key,
    generate_pledge_request_key,
    generate_pledge_response_key,
    generate_settlement_report_key,
    generate_settlement_completion_key,
    generate_credit_request_stream_key,
    generate_credit_response_stream_key,
    generate_settlement_individual_response_key,
    generate_settlement_batch_response_key,
    KeyManager
)

from .acl import (
    setup_exchange_acl,
    setup_wsp_acl,
    disable_default_user,
    setup_basic_acls,
    test_exchange_acl,
    test_wsp_acl,
    get_acl_list,
    ACLManager
)

from .errors import (
    RedisMirrorError,
    ConnectionError,
    ACLError,
    StreamError,
    KeyError,
    ValidationError,
    CreditRequestError,
    SettlementError,
    ConfigurationError,
    TimeoutError,
    handle_redis_error
)

from .configuration import (
    Configuration,
    ConfigurationProfile
)

from .streams import (
    create_consumer_group,
    add_message_to_stream,
    read_messages,
    read_messages_from_group,
    acknowledge_message,
    get_pending_messages,
    claim_pending_messages,
    trim_stream,
    delete_message,
    get_stream_info,
    get_consumer_group_info,
    StreamProcessor,
    StreamPublisher,
    publish_event
)

from .events import (
    EventType,
    Event,
    EventPublisher,
    EventSubscriber,
    EventRouter,
    EventFilter,
    create_event_publisher,
    create_event_subscriber,
    create_event_router,
    publish_event as publish_event_to_stream
)