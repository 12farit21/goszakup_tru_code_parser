"""
HTML parsing functions for Goszakup tender data
"""

import logging
from typing import List, Dict, Optional
from lxml import html

from config import FIELD_MAPPING

logger = logging.getLogger(__name__)


def extract_lot_ids(html_content: str, announce_url: str) -> List[str]:
    """
    Extract data-lot-id values from announcement page.

    Based on 2.py logic.
    URL pattern: {lot_url}?tab=lots#
    XPath: //a[@data-lot-id]/@data-lot-id

    Args:
        html_content: HTML content of the announcement page
        announce_url: URL of the announcement (for logging)

    Returns:
        List of unique lot IDs (empty list if parsing fails)
    """
    try:
        tree = html.fromstring(html_content)
        lot_ids = tree.xpath('//a[@data-lot-id]/@data-lot-id')

        # Remove duplicates while preserving order
        seen = set()
        unique_lot_ids = []
        for lot_id in lot_ids:
            if lot_id and lot_id not in seen:
                seen.add(lot_id)
                unique_lot_ids.append(lot_id)

        logger.debug(f"Extracted {len(unique_lot_ids)} lot IDs from {announce_url}")
        return unique_lot_ids

    except Exception as e:
        logger.error(f"Failed to extract lot IDs from {announce_url}: {e}")
        return []


def parse_lot_table(html_content: str, data_lot_id: str) -> Dict[str, Optional[str]]:
    """
    Parse lot details from HTML table.

    Based on 3.py logic.
    Expected HTML structure:
        <tbody>
            <tr>
                <th>Лот №</th>
                <td>Value</td>
            </tr>
            ...
        </tbody>

    Args:
        html_content: HTML content from AJAX response
        data_lot_id: Lot ID (for logging)

    Returns:
        Dictionary with parsed fields (all 12 fields, None if not found)
    """
    try:
        tree = html.fromstring(html_content)

        # Initialize result with all fields as None
        result = {field: None for field in FIELD_MAPPING.values()}

        # Extract all table rows (try multiple strategies)
        # Strategy 1: Direct table rows (most common)
        rows = tree.xpath('//table[@class="table table-bordered table-hover"]//tr')

        # Strategy 2: Fallback to any table rows if first strategy fails
        if not rows:
            rows = tree.xpath('//table//tr')

        # Strategy 3: Fallback to tbody/tr if present
        if not rows:
            rows = tree.xpath('//tbody/tr')

        if not rows:
            logger.warning(f"No table rows found for lot {data_lot_id}")
            return result

        # Parse each row
        for row in rows:
            # Get th and td elements
            th_elements = row.xpath('.//th')
            td_elements = row.xpath('.//td')

            if not th_elements or not td_elements:
                continue

            # Get text content
            field_name = th_elements[0].text_content().strip()
            field_value = td_elements[0].text_content().strip()

            # Map to English field name
            if field_name in FIELD_MAPPING:
                english_name = FIELD_MAPPING[field_name]
                result[english_name] = field_value if field_value else None

        # Count how many fields were found
        filled_fields = sum(1 for v in result.values() if v is not None)
        logger.debug(f"Parsed {filled_fields}/{len(FIELD_MAPPING)} fields for lot {data_lot_id}")

        return result

    except Exception as e:
        logger.error(f"Failed to parse lot table for {data_lot_id}: {e}")
        return {field: None for field in FIELD_MAPPING.values()}


def determine_parse_status(parsed_data: Dict[str, Optional[str]]) -> tuple[str, Optional[str]]:
    """
    Determine parse status based on how many fields were successfully parsed.

    Args:
        parsed_data: Dictionary of parsed fields

    Returns:
        Tuple of (status, error_message)
        - status: 'success', 'partial', or 'failed'
        - error_message: Description of what went wrong (or None)
    """
    filled_fields = sum(1 for v in parsed_data.values() if v is not None)
    total_fields = len(FIELD_MAPPING)

    if filled_fields == 0:
        return 'failed', 'No fields could be parsed'
    elif filled_fields < total_fields:
        missing = total_fields - filled_fields
        return 'partial', f'Only {filled_fields}/{total_fields} fields parsed ({missing} missing)'
    else:
        return 'success', None


def extract_announce_id(url: str) -> Optional[str]:
    """
    Extract announce ID from URL.

    Examples:
        https://goszakup.gov.kz/ru/announce/index/16099116 → 16099116
        https://goszakup.gov.kz/ru/announce/index/15908669 → 15908669

    Args:
        url: Announcement URL

    Returns:
        Announce ID as string, or None if extraction fails
    """
    try:
        # URL pattern: .../announce/index/<ID>
        parts = url.rstrip('/').split('/')
        if 'index' in parts:
            idx = parts.index('index')
            if idx + 1 < len(parts):
                return parts[idx + 1]

        logger.warning(f"Could not extract announce_id from URL: {url}")
        return None

    except Exception as e:
        logger.error(f"Error extracting announce_id from {url}: {e}")
        return None
