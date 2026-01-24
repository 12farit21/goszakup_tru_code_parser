"""
Test script to verify the parser works with a single URL
"""

import json
from pathlib import Path

from config import INPUT_JSON_FILE
from database import Database
from http_client import GoszakupHTTPClient
from parsers import extract_lot_ids, parse_lot_table, determine_parse_status, extract_announce_id
from utils import setup_logging

# Setup logging
logger = setup_logging('test_single')

def test_single_url():
    """Test with a single URL from the JSON file"""

    # Load first URL from JSON
    logger.info(f"Loading URL from {INPUT_JSON_FILE}")
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    urls = data.get('links', [])
    if not urls:
        logger.error("No URLs found in JSON file")
        return

    # Test with first URL
    test_url = urls[0]
    logger.info(f"Testing with URL: {test_url}")

    # Extract announce ID
    announce_id = extract_announce_id(test_url)
    logger.info(f"Announce ID: {announce_id}")

    # Initialize components
    db = Database()
    http_client = GoszakupHTTPClient()

    try:
        # Step 1: Fetch lot IDs
        logger.info("Step 1: Fetching lot IDs...")
        html_content = http_client.get_lot_ids_page(test_url)

        if not html_content:
            logger.error("Failed to fetch lot IDs page")
            return

        logger.info(f"Received HTML content: {len(html_content)} bytes")

        # Extract lot IDs
        lot_ids = extract_lot_ids(html_content, test_url)
        logger.info(f"Found {len(lot_ids)} lot IDs: {lot_ids[:5]}...")

        if not lot_ids:
            logger.warning("No lot IDs found")
            return

        # Step 2: Test with first lot ID
        test_lot_id = lot_ids[0]
        logger.info(f"\nStep 2: Fetching details for lot ID: {test_lot_id}")

        lot_html = http_client.get_lot_detail(announce_id, test_lot_id)

        if not lot_html:
            logger.error("Failed to fetch lot detail")
            return

        logger.info(f"Received lot HTML: {len(lot_html)} bytes")

        # Parse lot table
        parsed_data = parse_lot_table(lot_html, test_lot_id)
        parse_status, error_message = determine_parse_status(parsed_data)

        logger.info(f"\nParsed data (status: {parse_status}):")
        for field, value in parsed_data.items():
            if value:
                logger.info(f"  {field}: {value[:100] if len(value) > 100 else value}")

        if error_message:
            logger.warning(f"Parse error: {error_message}")

        # Step 3: Save to database
        logger.info("\nStep 3: Saving to database...")
        success = db.insert_lot_detail(
            lot_url=test_url,
            data_lot_id=test_lot_id,
            announce_id=announce_id,
            parsed_data=parsed_data,
            parse_status=parse_status,
            error_message=error_message
        )

        if success:
            logger.info("Successfully saved to database")
        else:
            logger.warning("Record already exists (duplicate)")

        # Get statistics
        stats = db.get_statistics()
        logger.info(f"\nDatabase statistics:")
        logger.info(f"  Total lots: {stats['total_lots']}")
        logger.info(f"  Parse status: {stats['parse_status']}")

    finally:
        http_client.close()

    logger.info("\nTest completed successfully!")

if __name__ == '__main__':
    test_single_url()
