"""
Main script for Goszakup Tender Data Parser

Reads announcement URLs from JSON file, fetches lot details, and saves to SQLite database.
"""

import argparse
import json
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not installed
    tqdm = None

from config import INPUT_JSON_FILE, REQUEST_DELAY, CHECKPOINT_INTERVAL
from database import Database
from http_client import GoszakupHTTPClient
from parsers import (
    extract_lot_ids,
    parse_lot_table,
    determine_parse_status,
    extract_announce_id
)
from utils import setup_logging, format_statistics, format_duration


class TenderParser:
    """Main parser orchestrator"""

    def __init__(self, json_file: Path, max_urls: int = None, resume: bool = True):
        """
        Initialize parser

        Args:
            json_file: Path to JSON file with announcement URLs
            max_urls: Maximum number of URLs to process (None = all)
            resume: Resume from previous run if True
        """
        self.json_file = json_file
        self.max_urls = max_urls
        self.resume = resume

        # Setup logging
        self.logger = setup_logging()

        # Initialize components
        self.db = Database()
        self.http_client = GoszakupHTTPClient()

        # Statistics
        self.stats = {
            'urls_processed': 0,
            'lots_found': 0,
            'lots_saved': 0,
            'lots_failed': 0,
            'start_time': None,
        }

        # Graceful shutdown
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)

    def _request_shutdown(self, signum, frame):
        """Handle shutdown signal"""
        self.logger.warning("\nShutdown requested, completing current task...")
        self.shutdown_requested = True

    def load_urls(self) -> list:
        """
        Load announcement URLs from JSON file

        Returns:
            List of URLs
        """
        self.logger.info(f"Loading URLs from {self.json_file}")

        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            urls = data.get('links', [])
            self.logger.info(f"Loaded {len(urls)} URLs from JSON")

            # Filter by resume status if enabled
            if self.resume:
                pending = set(self.db.get_pending_urls())
                if pending:
                    # Keep only URLs that are pending or not yet in progress table
                    urls = [url for url in urls if url in pending or url not in pending]
                    self.logger.info(f"Resume mode: {len(pending)} URLs are pending/failed")

            # Apply max_urls limit
            if self.max_urls and len(urls) > self.max_urls:
                urls = urls[:self.max_urls]
                self.logger.info(f"Limited to {self.max_urls} URLs")

            return urls

        except FileNotFoundError:
            self.logger.error(f"JSON file not found: {self.json_file}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON file: {e}")
            sys.exit(1)

    def process_url(self, announce_url: str):
        """
        Process a single announcement URL

        Args:
            announce_url: Announcement URL to process
        """
        # Extract announce ID
        announce_id = extract_announce_id(announce_url)
        if not announce_id:
            self.logger.error(f"Could not extract announce_id from {announce_url}")
            return

        # Get or create progress record
        progress = self.db.get_or_create_progress(announce_url, announce_id)

        # Skip if already completed (unless resume mode)
        if progress['status'] == 'completed' and self.resume:
            self.logger.debug(f"Skipping completed URL: {announce_url}")
            return

        # Update status to processing
        self.db.update_progress(announce_url, status='processing')

        try:
            # Step 1: Fetch page and extract lot IDs
            self.logger.info(f"Processing {announce_url}")
            html_content = self.http_client.get_lot_ids_page(announce_url)

            if not html_content:
                self.logger.error(f"Failed to fetch lot IDs page: {announce_url}")
                self.db.update_progress(
                    announce_url,
                    status='failed',
                    last_error='Failed to fetch lot IDs page'
                )
                return

            lot_ids = extract_lot_ids(html_content, announce_url)

            if not lot_ids:
                self.logger.warning(f"No lot IDs found for {announce_url}")
                self.db.update_progress(
                    announce_url,
                    status='completed',
                    lot_ids_found=0,
                    lot_ids_processed=0
                )
                return

            self.logger.info(f"Found {len(lot_ids)} lot IDs for {announce_url}")
            self.db.update_progress(announce_url, lot_ids_found=len(lot_ids))
            self.stats['lots_found'] += len(lot_ids)

            # Step 2: Process each lot ID
            for lot_id in lot_ids:
                if self.shutdown_requested:
                    self.logger.warning("Shutdown requested, stopping lot processing")
                    break

                self.process_lot(announce_url, announce_id, lot_id)

                # Rate limiting
                time.sleep(REQUEST_DELAY)

            # Mark as completed
            self.db.update_progress(announce_url, status='completed')
            self.stats['urls_processed'] += 1

        except Exception as e:
            self.logger.error(f"Error processing {announce_url}: {e}", exc_info=True)
            self.db.update_progress(
                announce_url,
                status='failed',
                last_error=str(e)
            )

    def process_lot(self, announce_url: str, announce_id: str, lot_id: str):
        """
        Process a single lot

        Args:
            announce_url: Announcement URL
            announce_id: Announcement ID
            lot_id: Lot ID to process
        """
        try:
            # Fetch lot detail HTML
            html_content = self.http_client.get_lot_detail(announce_id, lot_id)

            if not html_content:
                self.logger.error(f"Failed to fetch lot detail: {lot_id}")
                self.stats['lots_failed'] += 1
                return

            # Parse lot table
            parsed_data = parse_lot_table(html_content, lot_id)

            # Determine parse status
            parse_status, error_message = determine_parse_status(parsed_data)

            # Save to database
            success = self.db.insert_lot_detail(
                lot_url=announce_url,
                data_lot_id=lot_id,
                announce_id=announce_id,
                parsed_data=parsed_data,
                parse_status=parse_status,
                error_message=error_message
            )

            if success:
                self.stats['lots_saved'] += 1
                self.db.increment_processed(announce_url)
                self.logger.debug(
                    f"Saved lot {lot_id} (status: {parse_status})"
                )
            else:
                self.logger.debug(f"Skipped duplicate lot: {lot_id}")

        except Exception as e:
            self.logger.error(f"Error processing lot {lot_id}: {e}", exc_info=True)
            self.stats['lots_failed'] += 1

    def run(self):
        """Main execution loop"""
        self.stats['start_time'] = time.time()

        try:
            # Load URLs
            urls = self.load_urls()

            if not urls:
                self.logger.warning("No URLs to process")
                return

            self.logger.info(f"Starting to process {len(urls)} URLs")

            # Process URLs with progress bar
            if tqdm:
                url_iterator = tqdm(urls, desc="Processing URLs", unit="url")
            else:
                url_iterator = urls

            for i, url in enumerate(url_iterator, 1):
                if self.shutdown_requested:
                    self.logger.warning("Shutdown requested, stopping")
                    break

                self.process_url(url)

                # Checkpoint
                if i % CHECKPOINT_INTERVAL == 0:
                    self.logger.info(f"Checkpoint: {i}/{len(urls)} URLs processed")

        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)

        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup and print statistics"""
        # Close HTTP client
        self.http_client.close()

        # Calculate duration
        elapsed = time.time() - self.stats['start_time']

        # Get database statistics
        db_stats = self.db.get_statistics()

        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"URLs processed: {self.stats['urls_processed']}")
        self.logger.info(f"Lots found: {self.stats['lots_found']}")
        self.logger.info(f"Lots saved: {self.stats['lots_saved']}")
        self.logger.info(f"Lots failed: {self.stats['lots_failed']}")
        self.logger.info(f"Elapsed time: {format_duration(elapsed)}")
        self.logger.info("")
        self.logger.info(format_statistics(db_stats))


def main():
    """Entry point"""
    parser = argparse.ArgumentParser(
        description='Goszakup Tender Data Parser',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --max-urls 10
  python main.py --no-resume
  python main.py --json-file output/custom.json
        """
    )

    parser.add_argument(
        '--json-file',
        type=Path,
        default=INPUT_JSON_FILE,
        help='Path to JSON file with announcement URLs'
    )

    parser.add_argument(
        '--max-urls',
        type=int,
        default=None,
        help='Maximum number of URLs to process (default: all)'
    )

    parser.add_argument(
        '--no-resume',
        action='store_false',
        dest='resume',
        help='Do not resume from previous run'
    )

    args = parser.parse_args()

    # Create and run parser
    tender_parser = TenderParser(
        json_file=args.json_file,
        max_urls=args.max_urls,
        resume=args.resume
    )
    tender_parser.run()


if __name__ == '__main__':
    main()
