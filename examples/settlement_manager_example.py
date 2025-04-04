"""
Redis Mirror CE SDK - Settlement Manager Example

This example demonstrates how to use the Exchange Settlement Manager to generate settlement reports,
process settlement confirmations from WSPs, and manage the settlement process.
"""

import time
import logging
import json
from sdk_ce.redis_mirror_core import (
    Configuration, 
    ConfigurationProfile,
    RedisConnectionManager,
    StreamPublisher
)
from sdk_ce.redis_mirror_exchange import (
    ExchangeClient,
    SettlementReport,
    SettlementConfirmation
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def settlement_confirmation_callback(confirmation: SettlementConfirmation):
    """
    Callback function for settlement confirmations.
    
    Args:
        confirmation: Settlement confirmation
    """
    logger.info(f"Received settlement confirmation: {confirmation}")
    logger.info(f"  Settlement ID: {confirmation.settlement_id}")
    logger.info(f"  User ID: {confirmation.user_id}")
    logger.info(f"  Status: {confirmation.status}")
    logger.info(f"  Timestamp: {confirmation.timestamp}")
    
    if confirmation.details:
        logger.info(f"  Details: {confirmation.details}")
    
    # In a real implementation, you would process the settlement confirmation here
    # For example, you might:
    # 1. Update the settlement status in your database
    # 2. Process any asset transfers
    # 3. Update credit inventories


def settlement_manager_example():
    """Example of using the Settlement Manager."""
    logger.info("Starting Settlement Manager example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create Exchange client
        client = ExchangeClient(
            connection_manager,
            config,
            approved_custodians=["ledger", "exchange"],
            approved_assets=["BTC", "ETH", "USDT"],
            auto_start_processors=True
        )
        
        # Add settlement confirmation callback
        client.add_settlement_confirmation_callback(settlement_confirmation_callback)
        
        # Example 1: Generate a settlement report for a single user
        logger.info("Example 1: Generating a settlement report for a single user")
        
        user_id = "user123"
        assets_bought = [
            {"asset_id": "BTC", "amount": "0.5", "price": "65000.00"},
            {"asset_id": "ETH", "amount": "10.0", "price": "3500.00"}
        ]
        assets_sold = [
            {"asset_id": "USDT", "amount": "67500.00", "price": "1.00"}
        ]
        net_position = {
            "BTC": "+0.5",
            "ETH": "+10.0",
            "USDT": "-67500.00"
        }
        settlement_instructions = {
            "deposit_address": "0x1234...",
            "withdrawal_address": "0xabcd..."
        }
        
        report = client.generate_settlement_report(
            user_id=user_id,
            assets_bought=assets_bought,
            assets_sold=assets_sold,
            net_position=net_position,
            settlement_instructions=settlement_instructions
        )
        
        logger.info(f"Generated settlement report: {report}")
        logger.info(f"  Report ID: {report.report_id}")
        logger.info(f"  Status: {report.status}")
        
        # Example 2: Generate settlement reports for multiple users
        logger.info("Example 2: Generating settlement reports for multiple users")
        
        # Create user data for multiple users
        user_data = {
            "user456": {
                "assets_bought": [
                    {"asset_id": "ETH", "amount": "5.0", "price": "3500.00"}
                ],
                "assets_sold": [
                    {"asset_id": "BTC", "amount": "0.25", "price": "65000.00"}
                ],
                "net_position": {
                    "ETH": "+5.0",
                    "BTC": "-0.25"
                },
                "settlement_instructions": {
                    "deposit_address": "0x5678...",
                    "withdrawal_address": "0xefgh..."
                }
            },
            "user789": {
                "assets_bought": [
                    {"asset_id": "USDT", "amount": "50000.00", "price": "1.00"}
                ],
                "assets_sold": [
                    {"asset_id": "ETH", "amount": "15.0", "price": "3500.00"}
                ],
                "net_position": {
                    "USDT": "+50000.00",
                    "ETH": "-15.0"
                },
                "settlement_instructions": {
                    "deposit_address": "0x9012...",
                    "withdrawal_address": "0xijkl..."
                }
            }
        }
        
        reports = client.generate_settlement_reports(user_data)
        logger.info(f"Generated {len(reports)} settlement reports")
        
        for report in reports:
            logger.info(f"  Report ID: {report.report_id}, User ID: {report.user_id}, Status: {report.status}")
        
        # Example 3: Manually publish a settlement confirmation for testing
        # In a real scenario, these would come from WSPs
        logger.info("Example 3: Publishing a test settlement confirmation")
        
        # Get the confirmation stream key
        from sdk_ce.redis_mirror_core import KeyManager
        confirmation_stream = KeyManager.settlement_confirmation_stream()
        
        # Create a publisher
        replica_client = connection_manager.get_replica_client()
        publisher = StreamPublisher(replica_client, confirmation_stream)
        
        # Create a test confirmation
        settlement_id = report.report_id  # Use the first report's ID
        timestamp = int(time.time())
        
        # Confirmation data
        confirmation_data = {
            "type": "confirmation",
            "settlement_id": settlement_id,
            "user_id": user_id,
            "timestamp": str(timestamp),
            "status": "completed",
            "details": json.dumps({
                "assets_transferred": [
                    {"asset_id": "BTC", "amount": "0.5"},
                    {"asset_id": "ETH", "amount": "10.0"},
                    {"asset_id": "USDT", "amount": "67500.00"}
                ],
                "timestamp": timestamp
            })
        }
        
        # Publish the confirmation
        logger.info(f"Publishing settlement confirmation: {settlement_id}")
        publisher.publish(confirmation_data)
        
        # Example 4: Process settlement confirmations
        logger.info("Example 4: Processing settlement confirmations")
        
        # Process settlement confirmations
        client.process_settlement_confirmations(run_once=True)
        
        # Wait a bit for the callback to be called
        logger.info("Waiting for settlement confirmations to be processed...")
        time.sleep(2)
        
        # Example 5: Get settlement reports and confirmations
        logger.info("Example 5: Getting settlement reports and confirmations")
        
        # Get all settlement reports
        all_reports = client.get_settlement_reports()
        logger.info(f"Found {len(all_reports)} settlement reports")
        
        # Get reports by status
        completed_reports = client.get_settlement_reports(status=SettlementReport.STATUS_COMPLETED)
        logger.info(f"Found {len(completed_reports)} completed reports")
        
        # Get all settlement confirmations
        confirmations = client.get_settlement_confirmations()
        logger.info(f"Found {len(confirmations)} settlement confirmations")
        
        # Get confirmations by status
        completed_confirmations = client.get_settlement_confirmations(status="completed")
        logger.info(f"Found {len(completed_confirmations)} completed confirmations")
        
    finally:
        # Close the client
        if 'client' in locals():
            client.close()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the example."""
    settlement_manager_example()


if __name__ == "__main__":
    main()