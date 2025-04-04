"""
Redis Mirror CE SDK - Settlement Client Example

This example demonstrates how to use the Settlement Client to process settlement reports
from the Exchange, confirm settlements, and manage the settlement process.
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
from sdk_ce.redis_mirror_wsp import (
    WSPClient,
    SettlementReport
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def settlement_report_callback(report: SettlementReport):
    """
    Callback function for settlement reports.
    
    Args:
        report: Settlement report
    """
    logger.info(f"Received settlement report: {report}")
    logger.info(f"  Report ID: {report.report_id}")
    logger.info(f"  User ID: {report.user_id}")
    logger.info(f"  Status: {report.status}")
    logger.info(f"  Assets bought: {len(report.assets_bought)}")
    logger.info(f"  Assets sold: {len(report.assets_sold)}")
    
    # In a real implementation, you would process the settlement report here
    # For example, you might:
    # 1. Transfer bought assets to the user's wallet
    # 2. Transfer sold assets to the exchange
    # 3. Confirm the settlement
    
    # For this example, we'll just mark the report as processing
    report.mark_processing()
    logger.info(f"Marked report as processing: {report.status}")


def settlement_client_example():
    """Example of using the Settlement Client."""
    logger.info("Starting Settlement Client example")
    
    # Load configuration
    config = ConfigurationProfile.development()
    
    # Create connection manager
    connection_manager = RedisConnectionManager(
        wsp_config=config.get_wsp_redis_config(),
        replica_config=config.get_replica_config()
    )
    
    try:
        # Create WSP client
        custodian_id = "ledger"
        client = WSPClient(
            connection_manager,
            config,
            custodian_id,
            auto_start_processors=True
        )
        
        # Add settlement callback
        client.add_settlement_callback(settlement_report_callback)
        
        # Example 1: Manually publish a settlement report for testing
        # In a real scenario, these would come from the Exchange
        logger.info("Example 1: Publishing a test settlement report")
        
        # Get the settlement stream key
        from sdk_ce.redis_mirror_core import KeyManager
        settlement_stream = KeyManager.settlement_report_stream()
        
        # Create a publisher
        wsp_client = connection_manager.get_wsp_client()
        publisher = StreamPublisher(wsp_client, settlement_stream)
        
        # Create a test settlement report
        user_id = "user123"
        report_id = f"settlement-{int(time.time())}-{user_id}"
        timestamp = int(time.time())
        
        # Settlement data
        settlement_data = {
            "user_id": user_id,
            "eod_time": "2025-04-04T17:00:00Z",
            "assets": {
                "bought": [
                    {"asset_id": "BTC", "amount": "0.5", "price": "65000.00"},
                    {"asset_id": "ETH", "amount": "10.0", "price": "3500.00"}
                ],
                "sold": [
                    {"asset_id": "USDT", "amount": "67500.00", "price": "1.00"}
                ]
            },
            "net_position": {
                "BTC": "+0.5",
                "ETH": "+10.0",
                "USDT": "-67500.00"
            },
            "settlement_instructions": {
                "deposit_address": "0x1234...",
                "withdrawal_address": "0xabcd..."
            }
        }
        
        # Create the message
        message = {
            "type": "settlement",
            "report_id": report_id,
            "timestamp": str(timestamp),
            "data": json.dumps(settlement_data)
        }
        
        # Publish the message
        logger.info(f"Publishing settlement report: {report_id}")
        publisher.publish(message)
        
        # Example 2: Process settlement reports
        logger.info("Example 2: Processing settlement reports")
        
        # Process settlement reports
        client.process_settlement_reports()
        
        # Wait a bit for the callback to be called
        logger.info("Waiting for settlement reports to be processed...")
        time.sleep(2)
        
        # Example 3: Get settlement reports
        logger.info("Example 3: Getting settlement reports")
        
        # Get all settlement reports
        reports = client.get_settlement_reports()
        logger.info(f"Found {len(reports)} settlement reports")
        
        # Get reports by status
        processing_reports = client.settlement_client.get_processing_settlement_reports()
        logger.info(f"Found {len(processing_reports)} processing reports")
        
        # Example 4: Confirm settlement
        logger.info("Example 4: Confirming settlement")
        
        # Get the first report
        if reports:
            report = reports[0]
            
            # Confirm the settlement
            details = {
                "assets_transferred": [
                    {"asset_id": "BTC", "amount": "0.5"},
                    {"asset_id": "ETH", "amount": "10.0"},
                    {"asset_id": "USDT", "amount": "67500.00"}
                ],
                "timestamp": int(time.time())
            }
            
            success = client.confirm_settlement(
                report.report_id,
                status="completed",
                details=details
            )
            
            logger.info(f"Settlement confirmation {'succeeded' if success else 'failed'}")
            
            # Get the updated report
            updated_report = client.get_settlement_report(report.report_id)
            logger.info(f"Updated report status: {updated_report.status}")
        
    finally:
        # Close the client
        if 'client' in locals():
            client.close()
        
        # Close connections
        connection_manager.close()
        logger.info("Connections closed")


def main():
    """Run the example."""
    settlement_client_example()


if __name__ == "__main__":
    main()