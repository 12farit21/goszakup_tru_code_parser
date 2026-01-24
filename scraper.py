"""
Goszakup Tender Links Scraper
A Playwright-based web scraper for extracting tender links from goszakup.gov.kz
"""

import asyncio
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set, Optional
from urllib.parse import urljoin

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, Browser, BrowserContext


class GoszakupScraper:
    """Main scraper class for goszakup.gov.kz tender links"""

    def __init__(self, config: dict):
        """Initialize scraper with configuration"""
        self.config = config
        self.all_links: Set[str] = set()
        self.pages_scraped = 0
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None

        # Setup logging
        self.setup_logging()

        # Ensure output directories exist
        Path(self.config['OUTPUT_DIR']).mkdir(parents=True, exist_ok=True)
        Path(self.config['LOG_DIR']).mkdir(parents=True, exist_ok=True)

        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_filename = f"{self.config['OUTPUT_FILENAME_PREFIX']}_{timestamp}.json"
        self.output_filepath = os.path.join(self.config['OUTPUT_DIR'], self.output_filename)

        self.logger.info(f"Scraper initialized with max_links={config.get('max_links', 'unlimited')}")

    def setup_logging(self):
        """Setup logging configuration"""
        # Create logger
        self.logger = logging.getLogger('GoszakupScraper')
        self.logger.setLevel(getattr(logging, self.config['LOG_LEVEL']))

        # Clear existing handlers
        self.logger.handlers.clear()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.config['LOG_LEVEL']))
        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # File handler
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filepath = os.path.join(self.config['LOG_DIR'], f"scraper_{timestamp}.log")
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

        self.logger.info(f"Logging initialized. Log file: {log_filepath}")

    async def setup_browser(self):
        """Launch Playwright browser and create page"""
        self.logger.info(f"Launching {self.config['BROWSER_TYPE']} browser (headless={self.config['HEADLESS_MODE']})")

        self.playwright = await async_playwright().start()

        # Launch browser based on type
        if self.config['BROWSER_TYPE'] == 'firefox':
            self.browser = await self.playwright.firefox.launch(headless=self.config['HEADLESS_MODE'])
        elif self.config['BROWSER_TYPE'] == 'webkit':
            self.browser = await self.playwright.webkit.launch(headless=self.config['HEADLESS_MODE'])
        else:  # chromium (default)
            self.browser = await self.playwright.chromium.launch(headless=self.config['HEADLESS_MODE'])

        # Create browser context with custom settings
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            locale='ru-RU',
        )

        # Set default timeout
        self.context.set_default_timeout(self.config['PAGE_TIMEOUT'])

        # Create new page
        self.page = await self.context.new_page()

        self.logger.info("Browser launched successfully")

    async def set_records_per_page(self, count: int = 2000):
        """
        Set the number of records to display per page via dropdown

        Args:
            count: Number of records (default: 2000)
        """
        self.logger.info(f"Setting records per page to {count}")

        try:
            # Find the select element
            select_selector = 'select.form-control.m-b-sm'
            await self.page.wait_for_selector(select_selector, timeout=10000)

            # Select the value and wait for navigation (AJAX reload)
            async with self.page.expect_navigation(wait_until='networkidle', timeout=30000):
                await self.page.select_option(select_selector, value=str(count))

            self.logger.info(f"Successfully set records per page to {count}")

        except Exception as e:
            self.logger.error(f"Failed to set records per page: {e}")
            # Take screenshot for debugging
            screenshot_path = os.path.join(self.config['LOG_DIR'], f"error_dropdown_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            await self.page.screenshot(path=screenshot_path)
            self.logger.error(f"Screenshot saved to {screenshot_path}")
            raise

    async def extract_links(self, max_links: Optional[int] = None) -> List[str]:
        """
        Extract all tender links from the current page

        Args:
            max_links: Maximum total links to collect (stops early if reached)

        Returns:
            List of absolute URLs
        """
        self.logger.debug("Extracting links from page")

        try:
            # CSS selector for target links
            selector = 'a[target="_blank"][style="font-size: 13px"]'

            # Wait for links to be present
            await self.page.wait_for_selector(selector, timeout=10000)

            # Get all matching elements
            elements = await self.page.query_selector_all(selector)

            new_links = []
            for element in elements:
                # Check if we've reached max_links
                if max_links and len(self.all_links) >= max_links:
                    self.logger.debug(f"Reached max_links ({max_links}) during extraction, stopping early")
                    break

                href = await element.get_attribute('href')
                if href:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(self.config['BASE_URL'], href)

                    # Add only if not already in our set
                    if absolute_url not in self.all_links:
                        self.all_links.add(absolute_url)
                        new_links.append(absolute_url)

                        # Check again after adding
                        if max_links and len(self.all_links) >= max_links:
                            self.logger.debug(f"Reached max_links ({max_links}) after adding link")
                            break

            self.logger.debug(f"Extracted {len(new_links)} new links ({len(elements)} total elements)")
            return new_links

        except Exception as e:
            self.logger.warning(f"Failed to extract links: {e}")
            return []

    async def scrape_page(self, page_num: int, is_first_page: bool = False, max_links: Optional[int] = None) -> int:
        """
        Scrape a single page

        Args:
            page_num: Page number to scrape
            is_first_page: Whether this is the first page (needs dropdown selection)
            max_links: Maximum total links to collect

        Returns:
            Number of new links found
        """
        # Construct URL
        url = f"{self.config['BASE_URL']}?{self.config['URL_FILTERS']}&count_record={self.config['RECORDS_PER_PAGE']}&page={page_num}"

        self.logger.info(f"Scraping page {page_num}...")
        self.logger.debug(f"URL: {url}")

        try:
            # Navigate to page
            response = await self.page.goto(url, wait_until='networkidle', timeout=self.config['PAGE_TIMEOUT'])

            # Check response status
            if response and response.status == 404:
                self.logger.warning(f"Page {page_num} returned 404 - likely end of results")
                return 0

            if response and response.status >= 400:
                self.logger.error(f"Page {page_num} returned status {response.status}")
                return 0

            # Set dropdown only on first page
            if is_first_page:
                await self.set_records_per_page(self.config['RECORDS_PER_PAGE'])

            # Extract links (with max_links limit)
            new_links = await self.extract_links(max_links=max_links)

            if len(new_links) == 0:
                self.logger.warning(f"Page {page_num}: No new links found")
            else:
                self.logger.info(f"Page {page_num}: Extracted {len(new_links)} new links (total: {len(self.all_links)})")

            self.pages_scraped += 1
            return len(new_links)

        except Exception as e:
            self.logger.error(f"Error scraping page {page_num}: {e}")

            # Take screenshot for debugging
            screenshot_path = os.path.join(self.config['LOG_DIR'], f"error_page{page_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            try:
                await self.page.screenshot(path=screenshot_path)
                self.logger.error(f"Screenshot saved to {screenshot_path}")
            except:
                pass

            return 0

    async def save_checkpoint(self, final: bool = False):
        """
        Save current progress to JSON file

        Args:
            final: Whether this is the final save
        """
        save_type = "Final" if final else "Checkpoint"
        self.logger.info(f"{save_type} save: {len(self.all_links)} links")

        try:
            # Prepare data
            data = {
                "metadata": {
                    "scrape_date": datetime.now().isoformat(),
                    "total_links": len(self.all_links),
                    "pages_scraped": self.pages_scraped,
                    "records_per_page": self.config['RECORDS_PER_PAGE'],
                    "base_url": self.config['BASE_URL'],
                    "filters": self.config['URL_FILTERS']
                },
                "links": sorted(list(self.all_links))
            }

            # Atomic write (temp file + rename)
            temp_filepath = self.output_filepath + '.tmp'

            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # Rename to final filename
            os.replace(temp_filepath, self.output_filepath)

            self.logger.info(f"{save_type} saved to {self.output_filepath}")

        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    async def run(self, max_links: Optional[int] = None, start_page: int = 1):
        """
        Main scraping loop

        Args:
            max_links: Maximum number of links to scrape (None = unlimited)
            start_page: Starting page number
        """
        self.logger.info(f"Starting scraper (max_links={max_links or 'unlimited'}, start_page={start_page})")

        try:
            # Setup browser
            await self.setup_browser()

            page_num = start_page
            consecutive_empty_pages = 0
            max_consecutive_empty = 3

            while True:
                # Check if we've reached max_links
                if max_links and len(self.all_links) >= max_links:
                    self.logger.info(f"Reached max_links limit ({max_links}). Stopping.")
                    break

                # Scrape page
                is_first_page = (page_num == start_page)
                new_links_count = await self.scrape_page(page_num, is_first_page, max_links=max_links)

                # Check for empty pages
                if new_links_count == 0:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.info(f"Found {consecutive_empty_pages} consecutive empty pages. Stopping.")
                        break
                else:
                    consecutive_empty_pages = 0

                # Save checkpoint if needed
                if self.pages_scraped % self.config['CHECKPOINT_INTERVAL'] == 0:
                    await self.save_checkpoint()

                # Check max_links again (we might have exceeded it)
                if max_links and len(self.all_links) >= max_links:
                    self.logger.info(f"Reached max_links limit ({max_links}). Stopping.")
                    break

                # Delay between pages
                if self.config['PAGE_DELAY'] > 0:
                    self.logger.debug(f"Waiting {self.config['PAGE_DELAY']} seconds before next page")
                    await asyncio.sleep(self.config['PAGE_DELAY'])

                # Move to next page
                page_num += 1

            # Final save
            await self.save_checkpoint(final=True)

            # Summary
            self.logger.info("=" * 60)
            self.logger.info("Scraping completed successfully!")
            self.logger.info(f"Total links collected: {len(self.all_links)}")
            self.logger.info(f"Pages scraped: {self.pages_scraped}")
            self.logger.info(f"Output file: {self.output_filepath}")
            self.logger.info("=" * 60)

        except KeyboardInterrupt:
            self.logger.warning("Scraping interrupted by user (Ctrl+C)")
            await self.save_checkpoint(final=True)
            self.logger.info(f"Progress saved. Total links: {len(self.all_links)}")

        except Exception as e:
            self.logger.error(f"Fatal error during scraping: {e}", exc_info=True)
            await self.save_checkpoint(final=True)
            raise

        finally:
            await self.cleanup()

    async def cleanup(self):
        """Close browser and cleanup resources"""
        self.logger.info("Cleaning up resources...")

        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()

            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def load_config(args) -> dict:
    """
    Load configuration from .env file and CLI arguments

    Args:
        args: Parsed command-line arguments

    Returns:
        Configuration dictionary
    """
    # Load .env file
    load_dotenv()

    # Build configuration
    config = {
        'BASE_URL': os.getenv('BASE_URL', 'https://goszakup.gov.kz/ru/search/lots'),
        'URL_FILTERS': os.getenv('URL_FILTERS', 'filter%5Bamount_from%5D=5000000&filter%5Btrade_type%5D=g'),
        'RECORDS_PER_PAGE': int(os.getenv('RECORDS_PER_PAGE', '2000')),
        'START_PAGE': int(os.getenv('START_PAGE', '1')),
        'DEFAULT_MAX_LINKS': int(os.getenv('DEFAULT_MAX_LINKS', '10000')),
        'HEADLESS_MODE': os.getenv('HEADLESS_MODE', 'true').lower() == 'true',
        'BROWSER_TYPE': os.getenv('BROWSER_TYPE', 'chromium'),
        'PAGE_TIMEOUT': int(os.getenv('PAGE_TIMEOUT', '60000')),
        'CHECKPOINT_INTERVAL': int(os.getenv('CHECKPOINT_INTERVAL', '10')),
        'MAX_RETRIES': int(os.getenv('MAX_RETRIES', '3')),
        'RETRY_DELAY': float(os.getenv('RETRY_DELAY', '5')),
        'PAGE_DELAY': float(os.getenv('PAGE_DELAY', '2.5')),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'LOG_DIR': os.getenv('LOG_DIR', 'logs'),
        'OUTPUT_FILENAME_PREFIX': os.getenv('OUTPUT_FILENAME_PREFIX', 'goszakup_links'),
        'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
    }

    # Override with CLI arguments
    if args.max_links is not None:
        config['max_links'] = args.max_links
    else:
        config['max_links'] = config['DEFAULT_MAX_LINKS']

    if args.start_page is not None:
        config['START_PAGE'] = args.start_page

    if args.headless is not None:
        config['HEADLESS_MODE'] = args.headless

    if args.verbose:
        config['LOG_LEVEL'] = 'DEBUG'

    return config


def main():
    """Main entry point"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Goszakup Tender Links Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py --max-links 2500
  python scraper.py --start-page 5 --max-links 1000
  python scraper.py --max-links 100 --no-headless --verbose
        """
    )

    parser.add_argument(
        '--max-links',
        type=int,
        default=None,
        help='Maximum number of links to scrape (default: from .env or 10000)'
    )

    parser.add_argument(
        '--start-page',
        type=int,
        default=None,
        help='Starting page number (default: from .env or 1)'
    )

    parser.add_argument(
        '--headless',
        action='store_true',
        dest='headless',
        default=None,
        help='Run browser in headless mode'
    )

    parser.add_argument(
        '--no-headless',
        action='store_false',
        dest='headless',
        help='Run browser in visible mode'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args)

    # Create and run scraper
    scraper = GoszakupScraper(config)
    asyncio.run(scraper.run(
        max_links=config.get('max_links'),
        start_page=config['START_PAGE']
    ))


if __name__ == '__main__':
    main()
