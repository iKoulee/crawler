import re
import logging
import sqlite3
from typing import Dict, List, Any, Optional

from advert import Advertisement


class KeywordManager:
    """
    Manages keyword operations including fetching, compiling, and matching advertisements
    against keywords stored in the database.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the KeywordManager.

        Args:
            logger: Optional logger instance. If not provided, a new logger will be created.
        """
        self.logger = logger or logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )

    def create_keyword_tables(self, connection: sqlite3.Connection) -> None:
        """
        Create the database tables for keywords if they don't exist.

        Args:
            connection: SQLite database connection
        """
        cursor = connection.cursor()

        # Create keywords table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                title TEXT,
                search TEXT NOT NULL UNIQUE,
                case_sensitive BOOLEAN NOT NULL DEFAULT 0 CHECK (case_sensitive IN (0, 1))
            )
            """
        )

        # Create many-to-many relationship table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_advertisement (
                keyword_id INTEGER,
                advertisement_id INTEGER,
                FOREIGN KEY (keyword_id) REFERENCES keywords(id),
                FOREIGN KEY (advertisement_id) REFERENCES advertisements(id),
                PRIMARY KEY (keyword_id, advertisement_id)
            )
            """
        )

        connection.commit()

    def insert_keyword(
        self, connection: sqlite3.Connection, keyword: Dict[str, Any]
    ) -> None:
        """
        Insert a keyword into the database if it doesn't already exist.

        Args:
            connection: SQLite database connection
            keyword: Dictionary containing keyword data (title, search, case_sensitive)
        """
        cursor = connection.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO keywords (title, search, case_sensitive) VALUES (?, ?, ?)",
            (keyword["title"], keyword["search"], keyword["case_sensitive"]),
        )
        connection.commit()

    def fetch_keywords(self, connection: sqlite3.Connection) -> Dict[int, re.Pattern]:
        """
        Fetch and compile keywords from the database.

        Args:
            connection: SQLite database connection

        Returns:
            Dictionary mapping keyword IDs to compiled regex patterns
        """
        cursor = connection.cursor()
        cursor.execute("SELECT id, search, case_sensitive FROM keywords")
        result = cursor.fetchall()

        if result:
            self.logger.debug(f"Fetched {len(result)} keywords from database")
            return dict(
                [
                    (
                        row[0],
                        self._compile_keyword(search=row[1], case_sensitive=row[2]),
                    )
                    for row in result
                ]
            )

        self.logger.warning("No keywords found in database")
        return {}

    @staticmethod
    def _compile_keyword(search: str, case_sensitive: bool) -> re.Pattern:
        """
        Compiles a keyword into a regex pattern.

        Args:
            search: Search pattern string
            case_sensitive: Whether the pattern is case sensitive

        Returns:
            Compiled regular expression pattern
        """
        if case_sensitive:
            return re.compile(search)
        return re.compile(search, re.IGNORECASE)

    def match_keywords(
        self, advert: Advertisement, regexes: Dict[int, re.Pattern]
    ) -> List[int]:
        """
        Matches the advertisement against the keywords.

        Args:
            advert: Advertisement to match against keywords
            regexes: Dictionary mapping keyword IDs to compiled regex patterns

        Returns:
            List of keyword IDs that match the advertisement
        """
        result = []

        # Extract the text from the advertisement
        description = advert.get_description()

        # If no description is available, fall back to the raw source
        if description is None:
            description = advert.source

        # Ensure we have a string to search
        if description is None:
            self.logger.warning("No text available to match keywords for advertisement")
            return result

        # Search for each keyword in the description
        for id, regex in regexes.items():
            if regex.search(description):
                # If the keyword matches, add it to the result
                result.append(id)

        return result

    def store_keyword_matches(
        self,
        connection: sqlite3.Connection,
        advertisement_id: int,
        matched_keywords: List[int],
    ) -> None:
        """
        Store the matches between an advertisement and keywords.

        Args:
            connection: SQLite database connection
            advertisement_id: ID of the advertisement
            matched_keywords: List of keyword IDs that matched the advertisement
        """
        if not matched_keywords:
            self.logger.debug(
                f"No keyword matches to store for advertisement {advertisement_id}"
            )
            return

        cursor = connection.cursor()
        try:
            for keyword_id in matched_keywords:
                cursor.execute(
                    "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
                    (keyword_id, advertisement_id),
                )
            self.logger.debug(
                f"Stored {len(matched_keywords)} keyword matches for advertisement {advertisement_id}"
            )
        except sqlite3.Error as e:
            self.logger.error(f"Error storing keyword matches: {e}")
            connection.rollback()
            raise

        connection.commit()
