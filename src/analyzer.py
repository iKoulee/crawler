import sqlite3
import yaml
import logging
import os
from typing import Dict, Any, List, Optional, Tuple, Pattern
import re

from advert import AdFactory, Advertisement
from keyword_manager import KeywordManager


class AdvertAnalyzer:
    """Class for analyzing advertisements and matching them against keywords."""

    def __init__(self, db_path: str, config_path: Optional[str] = None) -> None:
        """
        Initialize the AdvertAnalyzer.

        Args:
            db_path: Path to the SQLite database file
            config_path: Path to the configuration file with keywords
        """
        self.db_path: str = db_path
        self.config_path: Optional[str] = config_path
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.compiled_keywords: Dict[int, re.Pattern] = {}
        self._connection: Optional[sqlite3.Connection] = None
        self.keyword_manager = KeywordManager(self.logger)

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection, creating a new one if needed.

        Returns:
            SQLite database connection
        """
        if self._connection is None:
            self._connection = sqlite3.connect(self.db_path)
        return self._connection

    def _close_connection(self) -> None:
        """Close the database connection if it exists."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def load_keywords_from_config(self) -> int:
        """
        Load and insert keywords from configuration file into the database.

        Returns:
            Number of keywords inserted
        """
        if not self.config_path:
            self.logger.warning("No configuration file path provided")
            return 0

        try:
            with open(self.config_path, "r") as config_file:
                config: Dict[str, Any] = yaml.safe_load(config_file)

            if "keywords" not in config or not config["keywords"]:
                self.logger.warning("No keywords found in configuration")
                return 0

            connection = self._get_connection()
            count = 0

            self.logger.info(
                "Inserting %d keywords from configuration", len(config["keywords"])
            )

            for keyword in config["keywords"]:
                try:
                    # Use KeywordManager to insert keywords
                    self.keyword_manager.insert_keyword(connection, keyword)
                    count += 1
                    self.logger.debug(
                        "Inserted keyword: %s", keyword.get("title", "Unnamed")
                    )
                except Exception as e:
                    self.logger.warning("Failed to insert keyword: %s", e)

            connection.commit()

            self.logger.info("Inserted %d keywords successfully", count)
            return count

        except FileNotFoundError:
            self.logger.error("Config file '%s' not found", self.config_path)
            return 0
        except yaml.YAMLError:
            self.logger.error("Invalid YAML in config file '%s'", self.config_path)
            return 0

    def reset_keyword_tables(self) -> None:
        """
        Reset the keywords and keyword_advertisement tables.

        This method truncates the keyword_advertisement and keywords tables,
        removing all existing keywords and their associations.
        """
        connection = self._get_connection()
        cursor = connection.cursor()

        # Truncate keyword_advertisement first due to foreign key constraints
        self.logger.info("Truncating keyword_advertisement table")
        cursor.execute("DELETE FROM keyword_advertisement")

        # Then truncate keywords table
        self.logger.info("Truncating keywords table")
        cursor.execute("DELETE FROM keywords")

        # Commit changes
        connection.commit()
        self.logger.debug("Keyword tables reset successfully")

    def _insert_keyword(
        self, connection: sqlite3.Connection, keyword: Dict[str, Any]
    ) -> None:
        """
        Insert a keyword into the database.

        Args:
            connection: SQLite database connection
            keyword: Dictionary containing keyword attributes
        """
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO keywords (title, search, case_sensitive) 
            VALUES (?, ?, ?)
            """,
            (keyword["title"], keyword["search"], keyword["case_sensitive"]),
        )

    def _compile_keyword_patterns(self) -> Dict[int, re.Pattern]:
        """
        Fetch and compile keywords from the database.

        Returns:
            Dictionary mapping keyword IDs to compiled regex patterns
        """
        connection = self._get_connection()
        # Use KeywordManager to fetch and compile keywords
        self.compiled_keywords = self.keyword_manager.fetch_keywords(connection)
        self.logger.debug("Compiled %d keyword patterns", len(self.compiled_keywords))
        return self.compiled_keywords

    def match_keywords_for_ad(self, ad: Advertisement) -> List[int]:
        """
        Match an advertisement against keywords and return matching keyword IDs.

        Args:
            ad: Advertisement instance to check

        Returns:
            List of keyword IDs that match the advertisement
        """
        # Ensure keywords are compiled
        if not self.compiled_keywords:
            self._compile_keyword_patterns()

        # Delegate keyword matching to KeywordManager
        return self.keyword_manager.match_keywords(ad, self.compiled_keywords)

    def update_advertisement_keywords(self, ad_id: int, keyword_ids: List[int]) -> None:
        """
        Update the keyword associations for an advertisement.

        Args:
            ad_id: ID of the advertisement
            keyword_ids: List of keyword IDs that match the advertisement
        """
        if ad_id is None:
            self.logger.warning("Cannot update keywords for advertisement with None ID")
            return

        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            # First delete any existing keyword associations for this ad
            cursor.execute(
                "DELETE FROM keyword_advertisement WHERE advertisement_id = ?", (ad_id,)
            )

            # Use KeywordManager to store keyword matches
            self.keyword_manager.store_keyword_matches(connection, ad_id, keyword_ids)

            # Commit changes immediately to ensure they're visible to other connections
            connection.commit()

            self.logger.debug(
                "Updated advertisement ID %d with %d keyword associations",
                ad_id,
                len(keyword_ids),
            )

        except sqlite3.Error as e:
            self.logger.error("Error updating keywords for ad %d: %s", ad_id, e)
            connection.rollback()
            raise

    def process_advertisements(
        self,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
        batch_size: int = 100,
    ) -> int:
        """
        Process advertisements to match them with keywords.

        Args:
            min_id: Minimum advertisement ID to process (inclusive)
            max_id: Maximum advertisement ID to process (inclusive)
            batch_size: Number of advertisements to process in each batch

        Returns:
            Number of advertisements processed
        """
        # Ensure keywords are compiled
        if not self.compiled_keywords:
            self._compile_keyword_patterns()

        if not self.compiled_keywords:
            self.logger.warning("No keywords found for matching")
            return 0

        self.logger.info(
            "Using %d keywords for matching advertisements", len(self.compiled_keywords)
        )

        # Get a connection to pass to AdFactory
        connection = self._get_connection()

        # Build condition for filtering by ID range if provided
        condition = ""
        params = []

        if min_id is not None or max_id is not None:
            conditions = []

            if min_id is not None:
                conditions.append("id >= ?")
                params.append(min_id)

            if max_id is not None:
                conditions.append("id <= ?")
                params.append(max_id)

            condition = " AND ".join(conditions)

            self.logger.info(
                "Filtering advertisements: %s",
                condition.replace("?", "{}").format(*params),
            )

        # Use AdFactory to get advertisements in batches, passing our connection
        ads_iterator = AdFactory.fetch_by_condition(
            condition=condition,
            params=params,
            batch_size=batch_size,
            connection=connection,  # Pass our existing connection
        )

        processed_count = 0

        try:
            # Process each advertisement
            for ad in ads_iterator:
                # Match keywords for this advertisement
                matched_keyword_ids = self.match_keywords_for_ad(ad)

                # Update keyword matches in the database
                self.update_advertisement_keywords(ad.id, matched_keyword_ids)

                processed_count += 1
                if processed_count % 100 == 0:
                    self.logger.info("Processed %d advertisements", processed_count)
                    connection.commit()  # Commit periodically

            # Final commit
            connection.commit()
            self.logger.info("Processed %d advertisements", processed_count)

        except Exception as e:
            self.logger.error("Error processing advertisements: %s", e)
            connection.rollback()
            raise

        return processed_count

    def run_analysis(
        self,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
        batch_size: int = 100,
        reset_tables: bool = True,
    ) -> int:
        """
        Run the complete analysis process on advertisements.

        This is the main entry point that runs the full analysis workflow:
        1. Optionally reset keyword tables based on reset_tables flag
        2. Load keywords from config
        3. Process advertisements within the given ID range

        Args:
            min_id: Minimum advertisement ID to analyze (None for no lower bound)
            max_id: Maximum advertisement ID to analyze (None for no upper bound)
            batch_size: Number of advertisements to process in each batch
            reset_tables: Whether to reset the keyword tables before analysis

        Returns:
            Number of advertisements processed
        """
        try:
            self.logger.info("Starting advertisement analysis")

            # Reset keyword tables if requested
            if reset_tables:
                self.reset_keyword_tables()

                # Insert keywords from config
                keyword_count = self.load_keywords_from_config()
                self.logger.info("Loaded %d keywords from configuration", keyword_count)
            else:
                self.logger.info("Skipping table reset as requested (--no-reset flag)")
                # Make sure keywords are compiled even if tables weren't reset
                self._compile_keyword_patterns()
                keyword_count = len(self.compiled_keywords)
                self.logger.info(
                    "Using %d existing keywords from database", keyword_count
                )

            # Process advertisements with the specified parameters
            ad_count = self.process_advertisements(
                min_id=min_id, max_id=max_id, batch_size=batch_size
            )

            self.logger.info(
                "Analysis complete: processed %d advertisements with %d keywords",
                ad_count,
                keyword_count,
            )

            return ad_count

        except Exception as e:
            self.logger.error("Analysis failed: %s", e)
            raise
        finally:
            # Always close the connection when done
            self._close_connection()
