"""
Database operations for Goszakup Tender Data Parser
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from config import DB_PATH

logger = logging.getLogger(__name__)


class Database:
    """Database manager for lot details and scraping progress"""

    def __init__(self, db_path: Path = DB_PATH):
        """Initialize database connection"""
        self.db_path = db_path

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create schema if needed
        self.create_schema()

        logger.info(f"Database initialized at {self.db_path}")

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_schema(self):
        """Create database schema if it doesn't exist"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Table: lot_details
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lot_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,

                    -- Source tracking
                    lot_url TEXT NOT NULL,
                    data_lot_id TEXT NOT NULL,
                    announce_id TEXT,

                    -- 12 required fields from table
                    lot_number TEXT,
                    lot_status TEXT,
                    customer_bin TEXT,
                    customer_name TEXT,
                    tru_code TEXT,
                    tru_name TEXT,
                    brief_description TEXT,
                    additional_description TEXT,
                    price_per_unit TEXT,
                    unit_of_measurement TEXT,
                    quantity TEXT,
                    delivery_location_kato TEXT,

                    -- Metadata
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    parse_status TEXT,
                    error_message TEXT,

                    UNIQUE(lot_url, data_lot_id)
                )
            """)

            # Table: scraping_progress
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraping_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lot_url TEXT UNIQUE NOT NULL,
                    announce_id TEXT,
                    status TEXT NOT NULL,
                    lot_ids_found INTEGER DEFAULT 0,
                    lot_ids_processed INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_count INTEGER DEFAULT 0,
                    last_error TEXT
                )
            """)

            # Indexes for performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_announce_id
                ON lot_details(announce_id)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_lot_id
                ON lot_details(data_lot_id)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tru_code
                ON lot_details(tru_code)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_customer_bin
                ON lot_details(customer_bin)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_parse_status
                ON lot_details(parse_status)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_progress_status
                ON scraping_progress(status)
            """)

            logger.debug("Database schema created/verified")

    def insert_lot_detail(
        self,
        lot_url: str,
        data_lot_id: str,
        announce_id: str,
        parsed_data: Dict[str, Optional[str]],
        parse_status: str = 'success',
        error_message: Optional[str] = None
    ) -> bool:
        """
        Insert lot detail record

        Returns:
            True if inserted, False if duplicate (UNIQUE constraint)
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO lot_details (
                        lot_url, data_lot_id, announce_id,
                        lot_number, lot_status, customer_bin, customer_name,
                        tru_code, tru_name, brief_description, additional_description,
                        price_per_unit, unit_of_measurement, quantity, delivery_location_kato,
                        parse_status, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    lot_url, data_lot_id, announce_id,
                    parsed_data.get('lot_number'),
                    parsed_data.get('lot_status'),
                    parsed_data.get('customer_bin'),
                    parsed_data.get('customer_name'),
                    parsed_data.get('tru_code'),
                    parsed_data.get('tru_name'),
                    parsed_data.get('brief_description'),
                    parsed_data.get('additional_description'),
                    parsed_data.get('price_per_unit'),
                    parsed_data.get('unit_of_measurement'),
                    parsed_data.get('quantity'),
                    parsed_data.get('delivery_location_kato'),
                    parse_status,
                    error_message
                ))
                return True
        except sqlite3.IntegrityError:
            logger.debug(f"Duplicate lot: {lot_url}/{data_lot_id}")
            return False
        except Exception as e:
            logger.error(f"Error inserting lot detail: {e}")
            raise

    def get_or_create_progress(self, lot_url: str, announce_id: str) -> Dict:
        """Get or create progress record for a URL"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Try to get existing record
            cursor.execute("""
                SELECT * FROM scraping_progress WHERE lot_url = ?
            """, (lot_url,))

            row = cursor.fetchone()

            if row:
                return dict(row)

            # Create new record
            cursor.execute("""
                INSERT INTO scraping_progress (
                    lot_url, announce_id, status, started_at
                ) VALUES (?, ?, ?, ?)
            """, (lot_url, announce_id, 'pending', datetime.now()))

            # Fetch the newly created record
            cursor.execute("""
                SELECT * FROM scraping_progress WHERE lot_url = ?
            """, (lot_url,))

            return dict(cursor.fetchone())

    def update_progress(
        self,
        lot_url: str,
        status: Optional[str] = None,
        lot_ids_found: Optional[int] = None,
        lot_ids_processed: Optional[int] = None,
        error_count: Optional[int] = None,
        last_error: Optional[str] = None
    ):
        """Update progress record"""
        updates = []
        params = []

        if status:
            updates.append("status = ?")
            params.append(status)

            if status == 'processing' and lot_ids_found is None:
                updates.append("started_at = ?")
                params.append(datetime.now())
            elif status == 'completed':
                updates.append("completed_at = ?")
                params.append(datetime.now())

        if lot_ids_found is not None:
            updates.append("lot_ids_found = ?")
            params.append(lot_ids_found)

        if lot_ids_processed is not None:
            updates.append("lot_ids_processed = ?")
            params.append(lot_ids_processed)

        if error_count is not None:
            updates.append("error_count = ?")
            params.append(error_count)

        if last_error is not None:
            updates.append("last_error = ?")
            params.append(last_error)

        if not updates:
            return

        params.append(lot_url)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE scraping_progress
                SET {', '.join(updates)}
                WHERE lot_url = ?
            """, params)

    def get_pending_urls(self) -> List[str]:
        """Get URLs that are pending or failed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT lot_url FROM scraping_progress
                WHERE status IN ('pending', 'failed')
                ORDER BY id
            """)
            return [row[0] for row in cursor.fetchall()]

    def get_statistics(self) -> Dict:
        """Get scraping statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Total lots
            cursor.execute("SELECT COUNT(*) FROM lot_details")
            total_lots = cursor.fetchone()[0]

            # By parse status
            cursor.execute("""
                SELECT parse_status, COUNT(*)
                FROM lot_details
                GROUP BY parse_status
            """)
            by_status = dict(cursor.fetchall())

            # Progress stats
            cursor.execute("""
                SELECT status, COUNT(*)
                FROM scraping_progress
                GROUP BY status
            """)
            progress_status = dict(cursor.fetchall())

            # Total URLs
            cursor.execute("SELECT COUNT(*) FROM scraping_progress")
            total_urls = cursor.fetchone()[0]

            return {
                'total_lots': total_lots,
                'total_urls': total_urls,
                'parse_status': by_status,
                'progress_status': progress_status
            }

    def increment_processed(self, lot_url: str):
        """Increment lot_ids_processed counter"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scraping_progress
                SET lot_ids_processed = lot_ids_processed + 1
                WHERE lot_url = ?
            """, (lot_url,))
