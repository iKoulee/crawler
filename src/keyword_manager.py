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
        self,
        advert: Advertisement,
        regexes: Dict[int, re.Pattern],
        title_only: bool = True,
    ) -> List[int]:
        """
        Matches the advertisement against the keywords.

        By default, matches against the job title only for more precise matching.
        When title_only is False, it will match against the description or fall back to raw HTML.

        Args:
            advert: Advertisement to match against keywords
            regexes: Dictionary mapping keyword IDs to compiled regex patterns
            title_only: If True, only match against the job title. If False, match against
                        description or fall back to raw HTML source if needed.

        Returns:
            List of keyword IDs that match the advertisement
        """
        result = []

        # Extract the title and description from the advertisement
        title = advert.get_title()
        description = advert.get_description()

        # Determine what to search against based on title_only parameter
        if title_only:
            # When title_only is True, we only search in the title
            if title is None:
                self.logger.debug(
                    "Title is None and title_only=True, no matches possible"
                )
                return result  # Empty list if title is None and we're only matching against title
            search_text = title
            search_field = "title"
        else:
            # When title_only is False, we search in both title and description
            # If both are available, we concatenate them with a space in between
            if title and description:
                search_text = f"{title} {description}"
                search_field = "title+description"
            elif title:
                search_text = title
                search_field = "title"
            elif description:
                search_text = description
                search_field = "description"
            else:
                # If both title and description are None, fall back to raw HTML
                search_text = advert.source
                search_field = "raw HTML source"
                self.logger.debug(
                    "Title and description are None, using raw HTML source"
                )

        # Ensure we have a string to search
        if search_text is None:
            self.logger.warning("No text available to match keywords for advertisement")
            return result

        # Search for each keyword in the search text
        for keyword_id, regex in regexes.items():
            if regex.search(search_text):
                # If the keyword matches, add it to the result
                result.append(keyword_id)

        self.logger.debug(f"Found {len(result)} keyword matches in {search_field}")
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
