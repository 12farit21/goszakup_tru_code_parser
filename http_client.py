"""
HTTP client with retry logic for Goszakup requests
"""

import time
import logging
from typing import Optional
import requests

from config import (
    HTTP_HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    RATE_LIMIT_DELAY
)

logger = logging.getLogger(__name__)


class GoszakupHTTPClient:
    """HTTP client with retry and rate limiting"""

    def __init__(
        self,
        max_retries: int = MAX_RETRIES,
        base_delay: float = RETRY_BASE_DELAY,
        rate_limit_delay: float = RATE_LIMIT_DELAY,
        timeout: int = REQUEST_TIMEOUT
    ):
        """
        Initialize HTTP client

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff (seconds)
            rate_limit_delay: Delay after receiving 429 error (seconds)
            timeout: Request timeout (seconds)
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout

        # Create session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(HTTP_HEADERS)

    def post_with_retry(
        self,
        url: str,
        data: Optional[dict] = None,
        **kwargs
    ) -> Optional[requests.Response]:
        """
        Make POST request with retry logic

        Args:
            url: Target URL
            data: POST data (form-encoded)
            **kwargs: Additional arguments for requests.post()

        Returns:
            Response object, or None if all retries failed
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    url,
                    data=data,
                    timeout=kwargs.get('timeout', self.timeout),
                    **{k: v for k, v in kwargs.items() if k != 'timeout'}
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    wait_time = self.rate_limit_delay * attempt
                    logger.warning(
                        f"Rate limited (429) for {url}, waiting {wait_time}s "
                        f"(attempt {attempt}/{self.max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                # Handle server errors (5xx) with retry
                if 500 <= response.status_code < 600:
                    if attempt < self.max_retries:
                        wait_time = self.base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Server error {response.status_code} for {url}, "
                            f"retry {attempt}/{self.max_retries} after {wait_time}s"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"Server error {response.status_code} for {url} "
                            f"after {self.max_retries} retries"
                        )
                        return None

                # Raise exception for 4xx errors (except 429)
                response.raise_for_status()

                # Success
                logger.debug(f"POST {url} - Status {response.status_code}")
                return response

            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    wait_time = self.base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"Timeout for {url}, retry {attempt}/{self.max_retries} "
                        f"after {wait_time}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Timeout for {url} after {self.max_retries} retries")
                    return None

            except requests.exceptions.ConnectionError as e:
                if attempt < self.max_retries:
                    wait_time = self.base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"Connection error for {url}: {e}, "
                        f"retry {attempt}/{self.max_retries} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Connection error for {url} after {self.max_retries} retries: {e}"
                    )
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = self.base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"Request error for {url}: {e}, "
                        f"retry {attempt}/{self.max_retries} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Request failed for {url} after {self.max_retries} retries: {e}"
                    )
                    return None

        # All retries exhausted
        logger.error(f"All {self.max_retries} retries exhausted for {url}")
        return None

    def get_lot_ids_page(self, announce_url: str) -> Optional[str]:
        """
        Fetch announcement page to extract lot IDs (step 1, based on 2.py)

        Args:
            announce_url: Announcement URL (e.g., https://goszakup.gov.kz/ru/announce/index/16099116)

        Returns:
            HTML content, or None if request failed
        """
        # Add query parameter as in 2.py
        url = f"{announce_url}?tab=lots#"

        logger.debug(f"Fetching lot IDs page: {url}")
        response = self.post_with_retry(url)

        if response and response.status_code == 200:
            return response.text
        else:
            logger.error(f"Failed to fetch lot IDs page: {url}")
            return None

    def get_lot_detail(self, announce_id: str, data_lot_id: str) -> Optional[str]:
        """
        Fetch lot detail HTML via AJAX (step 2, based on 3.py)

        Args:
            announce_id: Announcement ID (extracted from URL)
            data_lot_id: Lot ID (from step 1)

        Returns:
            HTML content, or None if request failed
        """
        # Construct URL as in 3.py
        from config import AJAX_LOAD_LOT_URL
        url = AJAX_LOAD_LOT_URL.format(announce_id=announce_id)

        # Payload as in 3.py
        payload = {"id": data_lot_id}

        logger.debug(f"Fetching lot detail: {url} with id={data_lot_id}")
        response = self.post_with_retry(url, data=payload)

        if response and response.status_code == 200:
            return response.text
        else:
            logger.error(f"Failed to fetch lot detail: {url} (id={data_lot_id})")
            return None

    def close(self):
        """Close the session"""
        self.session.close()
        logger.debug("HTTP client session closed")
