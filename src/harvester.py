from functools import cached_property
import sqlite3
from urllib.parse import urlparse
import requests
import logging
from abc import abstractmethod
from time import sleep, time
from datetime import datetime
from protego import Protego
from typing import Dict, List, Any, Iterator, Optional, Type, Tuple, Pattern
import os
import yaml
import csv
import io
from pathlib import Path
import re
import xml.etree.ElementTree as ET

from advert import (
    AdFactory,
    Advertisement,
    KarriereAdvertisement,
    StepstoneAdvertisement,
)
from keyword_manager import KeywordManager


class Harvester:
    AGENT = "Crawler"
    _url: Optional[str] = None
    _headers: Dict[str, str] = {
        "User-Agent": AGENT,
        "Connection": "keep-alive",
        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9",
    }
    _cookies: Optional[requests.cookies.RequestsCookieJar] = None
    _referer: Optional[str] = None
    _last_request: float = time()
    _robot_parser: Optional[Protego] = None

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url: str = config["url"]
        self.requests_per_minute: int = config.get("requests_per_minute", 1)
        # Add configurable retry_timeout in minutes (default: 5 minutes)
        self.retry_timeout: int = config.get("retry_timeout", 5)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.keyword_manager = KeywordManager(self.logger)

    @staticmethod
    def create_schema(connection: sqlite3.Connection) -> None:
        """
        Create the database schema if it doesn't exist.

        This method creates the tables needed for storing advertisements.
        Keyword-related tables are created by the KeywordManager class.

        Args:
            connection: SQLite database connection
        """
        cursor = connection.cursor()

        # Create advertisements table with the updated schema
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS advertisements (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                title TEXT,
                description TEXT,
                company TEXT,
                location TEXT,
                url TEXT NOT NULL UNIQUE,
                html_body TEXT NOT NULL,
                http_status INTEGER NOT NULL,
                ad_type TEXT NOT NULL,
                filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create indexes for better performance
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_advertisements_url ON advertisements(url)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_advertisements_ad_type ON advertisements(ad_type)"
        )

        # Create keyword-related tables using KeywordManager
        keyword_manager = KeywordManager()
        keyword_manager.create_keyword_tables(connection)

        connection.commit()

    @staticmethod
    def insert_keyword(connection: sqlite3.Connection, keyword: Dict[str, Any]) -> None:
        """
        Insert a keyword into the database if it doesn't already exist.

        Note: This static method provides backward compatibility with existing code.
        The recommended approach is to use KeywordManager.insert_keyword directly.

        Args:
            connection: SQLite database connection
            keyword: Dictionary containing keyword data (title, search, case_sensitive)
        """
        keyword_manager = KeywordManager()
        keyword_manager.insert_keyword(connection, keyword)

    @staticmethod
    def fetch_keywords(connection: sqlite3.Connection) -> Dict[int, re.Pattern]:
        """
        Fetch and compile keywords from the database.

        Note: This static method provides backward compatibility with existing code.
        The recommended approach is to use KeywordManager.fetch_keywords directly.

        Args:
            connection: SQLite database connection

        Returns:
            Dictionary mapping keyword IDs to compiled regex patterns
        """
        keyword_manager = KeywordManager()
        return keyword_manager.fetch_keywords(connection)

    @abstractmethod
    def get_next_link(self) -> Iterator[str]:
        raise NotImplementedError(
            f"{self.__class__.__name__}.get_next_link() is not implemented yet."
        )

    def advertisement_exists(self, db_file_name: str, url: str) -> bool:
        """
        Check if an advertisement already exists in the database.

        This method verifies if an advertisement with the given URL exists,
        has a successful HTTP status code (200), and contains some HTML content.

        Args:
            db_file_name: Path to the SQLite database file
            url: URL of the advertisement to check

        Returns:
            True if the advertisement exists and is valid, False otherwise
        """
        connection = sqlite3.connect(db_file_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT url, http_status, html_body 
            FROM advertisements 
            WHERE url = ?
            """,
            (url,),
        )

        result = cursor.fetchone()
        connection.close()

        if not result:
            return False

        _, status_code, html_body = result

        return status_code == 200 and html_body and len(html_body.strip()) > 0

    def get_next_advert(self, db_file_name: str) -> Iterator[Advertisement]:
        """
        Retrieves and yields the next advertisement from the sitemap.
        """
        for link in self.get_next_link():
            if self.advertisement_exists(db_file_name, link):
                self.logger.info(
                    "Advertisement %s already exists in the database.", link
                )
                continue
            response = self._get(link, headers=self._headers, cookies=self.cookies)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            yield Advertisement(
                status=response.status_code,
                link=link,
                source=response.text,
            )

    def harvest(self, db_file_name: str) -> None:
        """
        Harvest advertisements and store them in the database.

        This method fetches advertisements from the source, processes them,
        and stores them in the database along with their keyword matches.

        Args:
            db_file_name: Path to the SQLite database file
        """
        self.logger.info("Starting harvest process for %s", self.__class__.__name__)
        start_time = time()
        ads_processed = 0
        ads_stored = 0
        errors = 0

        try:
            # Initialize database and fetch keywords
            try:
                connection = sqlite3.connect(db_file_name)
                regexes = self.fetch_keywords(connection)

                if not regexes:
                    self.logger.warning(
                        "No keywords found in database, harvesting will store ads but not match keywords"
                    )

                connection.commit()
                connection.close()
            except sqlite3.Error as e:
                self.logger.error("Database error during initialization: %s", str(e))
                return
            except Exception as e:
                self.logger.error("Unexpected error during initialization: %s", str(e))
                return

            # Process each advertisement
            for advert in self.get_next_advert(db_file_name):
                ads_processed += 1

                try:
                    # Connect to database for each advertisement (to avoid long-running connections)
                    connection = sqlite3.connect(db_file_name)
                    cursor = connection.cursor()

                    try:
                        # Check if the advertisement already exists in the database
                        cursor.execute(
                            "SELECT id FROM advertisements WHERE url = ?",
                            (advert.link,),
                        )
                        if cursor.fetchone():
                            self.logger.debug(
                                "Advertisement %s already exists in the database.",
                                advert.link,
                            )
                            connection.close()
                            continue

                        # Extract advertisement data safely
                        try:
                            title = advert.get_title() or ""
                            company = advert.get_company() or ""
                            location = advert.get_location() or ""
                            description = advert.get_description() or ""
                        except Exception as e:
                            self.logger.warning(
                                "Error extracting data from advertisement %s: %s. Using empty values where needed.",
                                advert.link,
                                str(e),
                            )
                            title = getattr(advert, "title", "") or ""
                            company = getattr(advert, "company", "") or ""
                            location = getattr(advert, "location", "") or ""
                            description = getattr(advert, "description", "") or ""

                        # Insert the advertisement into the database
                        cursor.execute(
                            """
                            INSERT INTO advertisements 
                            (title, company, location, description, html_body, http_status, url, ad_type, filename) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                title,
                                company,
                                location,
                                description,
                                advert.source,
                                advert.status,
                                advert.link,
                                advert.__class__.__name__,
                                None,  # Default filename to None initially
                            ),
                        )

                        advert_id = cursor.lastrowid
                        if not advert_id:
                            self.logger.error(
                                "Failed to get ID for inserted advertisement %s",
                                advert.link,
                            )
                            connection.rollback()
                            connection.close()
                            errors += 1
                            continue

                        # Match and store keywords
                        try:
                            matched_keywords = self.match_keywords(advert, regexes)

                            self.logger.debug(
                                "Advertisement %s matched %d keywords",
                                advert.link,
                                len(matched_keywords),
                            )

                            self.store_keyword_matches(
                                connection, advert_id, matched_keywords
                            )
                        except Exception as e:
                            self.logger.warning(
                                "Error matching keywords for advertisement %s: %s. Continuing without keyword matches.",
                                advert.link,
                                str(e),
                            )

                        # Commit changes for this advertisement
                        connection.commit()
                        ads_stored += 1

                        # Log progress periodically
                        if ads_stored % 10 == 0:
                            elapsed = time() - start_time
                            self.logger.info(
                                "Processed %d advertisements, stored %d new ones (%.2f per minute)",
                                ads_processed,
                                ads_stored,
                                (ads_stored / (elapsed / 60)) if elapsed > 0 else 0,
                            )

                    except sqlite3.Error as e:
                        self.logger.error(
                            "Database error processing advertisement %s: %s",
                            advert.link,
                            str(e),
                        )
                        connection.rollback()
                        errors += 1
                    except Exception as e:
                        self.logger.error(
                            "Unexpected error processing advertisement %s: %s",
                            advert.link,
                            str(e),
                        )
                        connection.rollback()
                        errors += 1
                    finally:
                        connection.close()

                except Exception as e:
                    self.logger.error(
                        "Critical error processing advertisement: %s", str(e)
                    )
                    errors += 1

        except Exception as e:
            self.logger.error("Critical error in harvest process: %s", str(e))
            errors += 1

        # Log summary
        elapsed_time = time() - start_time
        self.logger.info(
            "Harvest process completed for %s: Processed %d advertisements, stored %d new ones, encountered %d errors in %.2f seconds",
            self.__class__.__name__,
            ads_processed,
            ads_stored,
            errors,
            elapsed_time,
        )

    def match_keywords(
        self, advert: Advertisement, regexes: Dict[int, re.Pattern]
    ) -> List[int]:
        """
        Matches the advertisement against the keywords.

        By default, matches against both the title and description.
        This method delegates to KeywordManager's match_keywords method.

        Args:
            advert: Advertisement to match against keywords
            regexes: Dictionary mapping keyword IDs to compiled regex patterns

        Returns:
            List of keyword IDs that match the advertisement
        """
        # Set title_only to False to match against title and description for maximum coverage
        return self.keyword_manager.match_keywords(advert, regexes, title_only=False)

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
        self.keyword_manager.store_keyword_matches(
            connection, advertisement_id, matched_keywords
        )

    def _get_robot_parser(self, refresh: bool = False) -> Protego:
        if not self._robot_parser or refresh:
            self._robot_parser = Protego.parse(
                requests.get(f"{self.url}/robots.txt", headers=self._headers).text
            )
        return self._robot_parser

    @property
    def cookies(self) -> requests.cookies.RequestsCookieJar:
        if not self._cookies:
            response = self._get(self.url, headers=self._headers)
            self._cookies = response.cookies
            self._referer = response.url
        return self._cookies

    @cached_property
    def crawl_delay(self) -> float:
        robots_value: int = self._get_robot_parser().crawl_delay(self.AGENT) or 0
        return max((60 / self.requests_per_minute), robots_value)

    def can_fetch(self, uri: str) -> bool:
        return self._get_robot_parser().can_fetch(uri, self.AGENT)

    def _get(self, *args: Any, **kwargs: Any) -> requests.Response:
        now = time()
        if self._last_request + self.crawl_delay > now:
            delay = self._last_request + self.crawl_delay - now
            self.logger.debug("Respecting crawl delay, waiting %.2f seconds", delay)
            sleep(delay)
        self._last_request = time()
        self.logger.debug(
            "Sending GET request to %s",
            args[0] if args else kwargs.get("url", "unknown"),
        )
        response = requests.get(*args, **kwargs)
        if response.cookies:
            self._cookies = response.cookies
        return response

    @staticmethod
    def fetch_advertisements_by_id_range(
        connection: sqlite3.Connection,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch advertisements from the database within a specific ID range.

        Args:
            connection: SQLite database connection
            min_id: Minimum advertisement ID to fetch (inclusive)
            max_id: Maximum advertisement ID to fetch (inclusive)

        Returns:
            List of dictionaries containing advertisement data with related keywords
        """
        logger = logging.getLogger(f"{__name__}.fetch_advertisements_by_id_range")
        cursor = connection.cursor()

        # Build the query with optional ID filters
        query = """
            SELECT a.id, a.title, a.company, a.location, a.ad_type, a.html_body, 
                   a.url, a.created_at, a.filename
            FROM advertisements a
            WHERE EXISTS (
                SELECT 1 FROM keyword_advertisement ka
                WHERE ka.advertisement_id = a.id
            )
        """

        params = []
        if min_id is not None:
            query += " AND a.id >= ?"
            params.append(min_id)
        if max_id is not None:
            query += " AND a.id <= ?"
            params.append(max_id)

        query += " ORDER BY a.id ASC"

        # Execute the query
        cursor.execute(query, params)
        dataset = cursor.fetchall()

        logger.info(
            "Fetched %d advertisements from database (min_id=%s, max_id=%s)",
            len(dataset),
            min_id if min_id is not None else "None",
            max_id if max_id is not None else "None",
        )

        # Fetch related keywords for each advertisement
        result = []
        for data in dataset:
            ad_id = data[0]
            title = data[1] or ""
            company = data[2] or ""
            location = data[3] or ""
            ad_type = data[4]
            html_body = data[5]
            url = data[6] or ""
            created_at = data[7] or ""
            filename = data[8] or ""

            # Create Advertisement instance to use get_* methods only if fields are missing
            if not title or not company or not location:
                ad = AdFactory.create(ad_type, html_body, url)
                title = title or ad.get_title() or ""
                company = company or ad.get_company() or ""
                location = location or ad.get_location() or ""
                logger.debug(
                    "Extracted missing fields for ad %d: title=%s, company=%s, location=%s",
                    ad_id,
                    title[:20] + "..." if len(title) > 20 else title,
                    company[:20] + "..." if len(company) > 20 else company,
                    location[:20] + "..." if len(location) > 20 else location,
                )

            # Get related keywords
            cursor.execute(
                """
                SELECT k.title
                FROM keywords k
                JOIN keyword_advertisement ka ON k.id = ka.keyword_id
                WHERE ka.advertisement_id = ?
                """,
                (ad_id,),
            )

            keyword_titles = [row[0] for row in cursor.fetchall() if row[0]]
            logger.debug(
                "Advertisement ID %d has %d related keywords",
                ad_id,
                len(keyword_titles),
            )

            # Build advertisement data with keywords
            ad_data = {
                "id": ad_id,
                "title": title,
                "company": company,
                "location": location,
                "date": created_at,
                "url": url,
                "portal": urlparse(url).netloc,
                "keywords": keyword_titles,
                "filename": filename,
            }

            result.append(ad_data)

        return result

    @staticmethod
    def export_to_csv(
        connection: sqlite3.Connection,
        output_file: str,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> int:
        """
        Export advertisements to CSV file.

        Args:
            connection: SQLite database connection
            output_file: Path to the output CSV file
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)

        Returns:
            Number of exported advertisements
        """
        logger = logging.getLogger(f"{__name__}.export_to_csv")

        # Fetch advertisements from database
        advertisements = Harvester.fetch_advertisements_by_id_range(
            connection, min_id, max_id
        )

        # Define CSV headers
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        logger.info(
            "Exporting %d advertisements to CSV file: %s",
            len(advertisements),
            output_file,
        )

        # Write to CSV
        try:
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for ad in advertisements:
                    writer.writerow(
                        {
                            "job_title": ad["title"],
                            "company_name": ad["company"],
                            "location": ad["location"],
                            "harvest_date": ad["date"],
                            "url": ad["url"],
                            "portal": ad["portal"],
                            "related_keywords": "; ".join(ad["keywords"]),
                            "filename": ad["filename"] or "",
                        }
                    )
                logger.info("CSV export completed successfully")
        except IOError as e:
            logger.error("Failed to write CSV file: %s", e)
            raise

        return len(advertisements)

    @staticmethod
    def export_to_csv_string(
        connection: sqlite3.Connection,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> str:
        """
        Export advertisements to CSV string.

        Args:
            connection: SQLite database connection
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)

        Returns:
            CSV formatted string containing the exported data
        """
        logger = logging.getLogger(f"{__name__}.export_to_csv_string")

        # Fetch advertisements from database
        advertisements = Harvester.fetch_advertisements_by_id_range(
            connection, min_id, max_id
        )

        logger.info("Generating CSV string for %d advertisements", len(advertisements))

        # Define CSV headers
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        # Write to CSV string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for ad in advertisements:
            writer.writerow(
                {
                    "job_title": ad["title"],
                    "company_name": ad["company"],
                    "location": ad["location"],
                    "harvest_date": ad["date"],
                    "url": ad["url"],
                    "portal": ad["portal"],
                    "related_keywords": "; ".join(ad["keywords"]),
                    "filename": ad["filename"] or "",
                }
            )

        logger.debug(
            "CSV string generation completed with %d rows", len(advertisements)
        )
        return output.getvalue()

    @staticmethod
    def export_html_bodies(
        connection: sqlite3.Connection,
        output_dir: str,
        config_path: str,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
        create_csv_files: bool = False,
    ) -> Tuple[int, Dict[str, int]]:
        """
        Export advertisement HTML bodies to files with nested directory structure.

        This method extracts HTML content from advertisements in the database and
        saves each one to a file within a nested directory structure based on
        filter matches defined in the configuration.

        Args:
            connection: SQLite database connection
            output_dir: Base directory for exported HTML files
            config_path: Path to the configuration file with filter definitions
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)
            create_csv_files: Whether to create CSV files in each directory

        Returns:
            Tuple containing (total_exported, category_counts)
            where category_counts is a dictionary of category:count pairs
        """
        logger = logging.getLogger(f"{__name__}.export_html_bodies")
        logger.info("Starting HTML body export process")

        # Load filter configuration
        filters = Harvester._load_filter_configuration(config_path)
        if not filters:
            logger.error("No valid filters found in configuration. Aborting export.")
            return (0, {})

        # Compile regular expressions for each filter
        compiled_filters = Harvester._compile_filters(filters)
        logger.debug(
            "Compiled %d filter categories with %d total filters",
            len(compiled_filters),
            sum(
                len(category_filters) for category_filters in compiled_filters.values()
            ),
        )

        # Ensure output directory exists
        base_path = Path(output_dir)
        base_path.mkdir(parents=True, exist_ok=True)

        # Retrieve advertisements from database
        cursor = connection.cursor()
        query = """
            SELECT a.id, a.html_body, a.url, a.ad_type, a.title, a.company, a.location, a.created_at 
            FROM advertisements a
            WHERE EXISTS (SELECT 1 FROM keyword_advertisement ka WHERE ka.advertisement_id = a.id)
        """

        params = []
        if min_id is not None:
            query += " AND a.id >= ?"
            params.append(min_id)
        if max_id is not None:
            query += " AND a.id <= ?"
            params.append(max_id)

        query += " ORDER BY a.id ASC"

        cursor.execute(query, params)

        # Process results and export files
        total_exported = 0
        category_counts = {category: 0 for category in compiled_filters.keys()}
        batch_size = 100

        # Dictionary to store CSV data for each directory
        # Key: directory path, Value: list of ad data dictionaries
        directory_csv_data = {} if create_csv_files else None

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                ad_id, html_body, url, ad_type, title, company, location, created_at = (
                    row
                )

                # Determine portal name from ad_type or URL
                portal_name = Harvester._extract_portal_name(ad_type, url)

                # Create the file path based on filter matches
                rel_path_parts = Harvester._determine_path_from_filters(
                    html_body, compiled_filters, category_counts
                )

                # Skip if no filters matched at all
                if not rel_path_parts:
                    logger.warning(
                        "Advertisement ID %d did not match any filters and will not be exported",
                        ad_id,
                    )
                    continue

                # Format file name: portal_00001.html
                file_name = f"{portal_name}_{ad_id:05d}.html"

                # Build full path
                full_path = base_path.joinpath(*rel_path_parts, file_name)

                # Ensure directory exists
                full_path.parent.mkdir(parents=True, exist_ok=True)

                # Write HTML to file
                try:
                    with open(full_path, "w", encoding="utf-8") as f:
                        f.write(html_body)

                    cursor_inner = connection.cursor()

                    # Update the filename in the database
                    rel_file_path = str(full_path.relative_to(base_path))
                    cursor_inner.execute(
                        "UPDATE advertisements SET filename = ? WHERE id = ?",
                        (rel_file_path, ad_id),
                    )

                    # If CSV files are requested, collect data for each directory
                    if create_csv_files:
                        # Get keywords for this advertisement
                        keywords_cursor = connection.cursor()
                        keywords_cursor.execute(
                            """
                            SELECT k.title
                            FROM keywords k
                            JOIN keyword_advertisement ka ON k.id = ka.keyword_id
                            WHERE ka.advertisement_id = ?
                            """,
                            (ad_id,),
                        )
                        keywords = [row[0] for row in keywords_cursor.fetchall()]

                        # Create ad data dictionary
                        ad_data = {
                            "job_title": title or "",
                            "company_name": company or "",
                            "location": location or "",
                            "harvest_date": created_at or "",
                            "url": url or "",
                            "portal": urlparse(url).netloc if url else "",
                            "related_keywords": "; ".join(keywords),
                            "filename": rel_file_path,
                        }

                        # Add data to all parent directories in the path
                        current_dir = base_path
                        # Add to root directory
                        dir_path_str = str(current_dir)
                        if dir_path_str not in directory_csv_data:
                            directory_csv_data[dir_path_str] = []
                        directory_csv_data[dir_path_str].append(ad_data)

                        # Add to each subdirectory
                        for part in rel_path_parts:
                            current_dir = current_dir / part
                            dir_path_str = str(current_dir)
                            if dir_path_str not in directory_csv_data:
                                directory_csv_data[dir_path_str] = []
                            directory_csv_data[dir_path_str].append(ad_data)

                    total_exported += 1
                    if total_exported % 100 == 0:
                        logger.info("Exported %d advertisement files", total_exported)
                        connection.commit()  # Commit periodically

                except IOError as e:
                    logger.error("Failed to write file %s: %s", full_path, e)

        # Final commit
        connection.commit()

        # If CSV files are requested, create them in each directory
        if create_csv_files and directory_csv_data:
            Harvester._create_directory_csv_files(directory_csv_data, logger)

        logger.info(
            "Export completed. Exported %d advertisement files to %s",
            total_exported,
            base_path,
        )

        return (total_exported, category_counts)

    @staticmethod
    def _create_directory_csv_files(
        directory_csv_data: Dict[str, List[Dict[str, str]]],
        logger: logging.Logger,
    ) -> None:
        """
        Create CSV files in each directory containing information about advertisements.

        Args:
            directory_csv_data: Dictionary mapping directory paths to lists of ad data dictionaries
            logger: Logger instance
        """
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        csv_count = 0
        for dir_path, ads_data in directory_csv_data.items():
            try:
                csv_path = Path(dir_path) / "advertisements.csv"
                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for ad_data in ads_data:
                        writer.writerow(ad_data)
                csv_count += 1

                if csv_count % 10 == 0:
                    logger.debug("Created %d CSV files in directories", csv_count)
            except IOError as e:
                logger.error(
                    "Failed to create CSV file in directory %s: %s", dir_path, e
                )

        logger.info("Created %d CSV files in directories", csv_count)

    @staticmethod
    def _extract_portal_name(ad_type: str, url: str) -> str:
        """
        Extract a clean portal name from ad_type or URL.

        Args:
            ad_type: Advertisement type class name
            url: URL of the advertisement

        Returns:
            Cleaned portal name suitable for filename
        """
        if ad_type:
            # Remove "Advertisement" suffix if present
            portal_name = ad_type.lower().replace("advertisement", "")
            if portal_name:
                return portal_name

        # Fallback to extracting from URL
        try:
            netloc = urlparse(url).netloc
            parts = netloc.split(".")
            if len(parts) >= 2:
                return parts[
                    -2
                ]  # Get the main domain name (e.g. "karriere" from "www.karriere.at")
        except (ValueError, IndexError):
            pass

        return "unknown"

    @staticmethod
    def _determine_path_from_filters(
        html_body: str,
        compiled_filters: Dict[str, Dict[str, Tuple[Pattern, bool]]],
        category_counts: Dict[str, int],
    ) -> List[str]:
        """
        Determine the path components based on filter matches.

        Args:
            html_body: HTML content to match against filters
            compiled_filters: Dictionary of compiled regex patterns
            category_counts: Dictionary to track match counts by category

        Returns:
            List of path components to form the directory structure
        """
        rel_path_parts = []
        for category, category_filters in compiled_filters.items():
            # Find the first matching filter in this category
            matched = False
            for filter_name, (pattern, is_catch_all) in category_filters.items():
                if is_catch_all:
                    continue  # Skip catch-all filters on first pass

                if pattern.search(html_body):
                    rel_path_parts.append(filter_name)
                    category_counts[category] += 1
                    matched = True
                    break

            if not matched:
                # Look for catch-all filter if no match was found
                for filter_name, (pattern, is_catch_all) in category_filters.items():
                    if is_catch_all:
                        rel_path_parts.append(filter_name)
                        category_counts[category] += 1
                        break

        return rel_path_parts

    @staticmethod
    def _load_filter_configuration(
        config_path: str,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Load filter configuration from YAML file.

        Args:
            config_path: Path to the configuration file

        Returns:
            Dictionary containing filter categories and their configurations
        """
        logger = logging.getLogger(f"{__name__}._load_filter_configuration")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Extract filter configuration
            if not config or not isinstance(config, dict) or "filters" not in config:
                logger.error("Invalid configuration format: missing 'filters' section")
                return {}

            return config["filters"]

        except (yaml.YAMLError, IOError) as e:
            logger.error("Failed to load configuration file: %s", e)
            return {}

    @staticmethod
    def _compile_filters(
        filters: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, Dict[str, Tuple[Pattern, bool]]]:
        """
        Compile regular expressions for each filter.

        Args:
            filters: Filter configuration dictionary

        Returns:
            Dictionary of compiled regex patterns with their catch_all status
        """
        logger = logging.getLogger(f"{__name__}._compile_filters")
        compiled_filters = {}

        for category, category_filters in filters.items():
            compiled_filters[category] = {}

            for filter_name, filter_config in category_filters.items():
                pattern = filter_config.get("pattern", "")
                is_catch_all = filter_config.get("catch_all", False)
                case_sensitive = filter_config.get("case_sensitive", False)

                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    compiled_pattern = re.compile(pattern, flags)
                    compiled_filters[category][filter_name] = (
                        compiled_pattern,
                        is_catch_all,
                    )
                except re.error as e:
                    logger.error(
                        "Invalid regular expression in filter %s.%s: %s",
                        category,
                        filter_name,
                        e,
                    )

        return compiled_filters


class StepStoneHarvester(Harvester):

    def get_next_advert(self, db_file_name: str) -> Iterator[StepstoneAdvertisement]:
        """
        Retrieves and yields the next advertisement from the sitemap.
        """
        for link in self.get_next_link():
            if self.advertisement_exists(db_file_name, link):
                self.logger.debug(
                    "Advertisement %s already exists in the database.", link
                )
                continue

            response = self._get(link, headers=self._headers, cookies=self.cookies)

            if response.status_code in (500, 502, 503, 504):
                self.logger.warning(
                    "Server error fetching %s: %d, retrying in 5 minutes",
                    link,
                    response.status_code,
                )
                sleep(
                    self.retry_timeout * 60
                )  # Wait for retry_timeout minutes before retrying
                response = self._get(link, headers=self._headers, cookies=self.cookies)

            if response.status_code == 410:
                self.logger.warning(
                    "Advertisement %s is no longer available (410 Gone)", link
                )
                continue
            if response.status_code != 200:
                self.logger.error(
                    "Failed to fetch %s: HTTP %d", link, response.status_code
                )
                continue

            response.encoding = response.apparent_encoding
            self.logger.debug("Successfully retrieved advertisement from %s", link)

            yield StepstoneAdvertisement(
                status=response.status_code,
                link=link,
                source=response.text,
            )

    def get_next_link(self) -> Iterator[str]:
        """
        Retrieves and yields links from the sitemap and nested sitemaps.
        """
        # Fetch the main sitemap
        sitemap_url = f"{self.url}/sitemap.xml"
        self.logger.info("Fetching main sitemap from %s", sitemap_url)

        response = self._get(sitemap_url, headers=self._headers)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        main_sitemap = ET.fromstring(response.text)

        # Iterate through all <loc> elements in the main sitemap
        for link in main_sitemap.findall(
            ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
        ):
            if re.match(r".*listings-[0-9]+.*", link.text):
                self.logger.info("Found nested sitemap: %s", link.text)
                # Fetch the nested sitemap
                nested_sitemap_link = link.text
                nested_response = self._get(
                    nested_sitemap_link, headers=self._headers, cookies=self.cookies
                )
                nested_response.raise_for_status()
                nested_response.encoding = nested_response.apparent_encoding
                nested_sitemap = ET.fromstring(nested_response.text)

                # Yield all <loc> elements from the nested sitemap
                link_count = 0
                for nested_link in nested_sitemap.findall(
                    ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                ):
                    link_count += 1
                    yield nested_link.text

                self.logger.info("Extracted %d links from nested sitemap", link_count)


class KarriereHarvester(Harvester):

    def get_next_advert(self, db_file_name: str) -> Iterator[KarriereAdvertisement]:
        """
        Retrieves and yields the next advertisement from the sitemap.
        """
        for link in self.get_next_link():
            if self.advertisement_exists(db_file_name, link):
                self.logger.debug(
                    "Advertisement %s already exists in the database.", link
                )
                continue

            response = self._get(link, headers=self._headers, cookies=self.cookies)

            if response.status_code in (500, 502, 503, 504):
                self.logger.warning(
                    "Server error fetching %s: %d, retrying in 5 minutes",
                    link,
                    response.status_code,
                )
                sleep(
                    self.retry_timeout * 60
                )  # Wait for retry_timeout minutes before retrying
                response = self._get(link, headers=self._headers, cookies=self.cookies)

            if response.status_code == 410:
                self.logger.warning(
                    "Advertisement %s is no longer available (410 Gone)", link
                )
                continue

            if response.status_code != 200:
                self.logger.error(
                    "Failed to fetch %s: HTTP %d", link, response.status_code
                )
                continue

            response.raise_for_status()
            response.encoding = response.apparent_encoding

            self.logger.debug("Successfully retrieved advertisement from %s", link)

            yield KarriereAdvertisement(
                status=response.status_code,
                link=link,
                source=response.text,
            )

    def get_next_link(self) -> Iterator[str]:
        """
        Fetch and yield links from the job sitemaps.

        Yields:
            URLs of job advertisements from the sitemaps
        """
        self.logger.info("Fetching sitemap links from robots.txt")
        sitemap_count = 0
        link_count = 0

        # Safely get robot parser and sitemaps
        try:
            robot_parser = self._get_robot_parser()
            if not robot_parser:
                self.logger.error("Failed to get robot parser")
                return

            # Get sitemaps from robots.txt
            sitemaps = robot_parser.sitemaps
            if not sitemaps:
                self.logger.warning("No sitemaps found in robots.txt")
                return
        except Exception as e:
            self.logger.error("Error retrieving sitemaps from robots.txt: %s", str(e))
            return

        # Process each sitemap
        for sitemap_link in sitemaps:
            # Skip None or non-string sitemap links
            if sitemap_link is None:
                self.logger.warning("Found None sitemap link in robots.txt, skipping")
                continue

            if not isinstance(sitemap_link, str):
                self.logger.warning(
                    "Sitemap link is not a string. Type: %s, skipping",
                    type(sitemap_link),
                )
                continue

            # Process job-related sitemaps
            try:
                # Skip non-job sitemaps
                if not re.search(r".*sitemap-jobs.*", sitemap_link):
                    continue

                sitemap_count += 1
                self.logger.info("Processing jobs sitemap: %s", sitemap_link)

                try:
                    # Get sitemap content
                    response = self._get(sitemap_link, headers=self._headers)

                    # Check response validity
                    if response.status_code != 200:
                        self.logger.warning(
                            "Failed to fetch sitemap %s: HTTP %d",
                            sitemap_link,
                            response.status_code,
                        )
                        continue

                    response.encoding = response.apparent_encoding
                    sitemap_text = response.text

                    # Validate sitemap content
                    if not sitemap_text or not sitemap_text.strip():
                        self.logger.warning(
                            "Empty sitemap response from %s", sitemap_link
                        )
                        continue

                    # Parse XML safely
                    try:
                        sitemap = ET.fromstring(sitemap_text)
                    except ET.ParseError as parse_error:
                        self.logger.error(
                            "XML parse error in sitemap %s: %s",
                            sitemap_link,
                            str(parse_error),
                        )
                        continue

                    # Extract links
                    sitemap_link_count = 0

                    # Using namespace dictionary for more robust XML parsing
                    namespaces = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

                    # Try with namespaces first, then without if none found
                    loc_elements = sitemap.findall(".//sm:loc", namespaces)
                    if not loc_elements:
                        loc_elements = sitemap.findall(".//loc")

                    for link in loc_elements:
                        link_text = link.text

                        # Skip empty links
                        if link_text is None or not link_text.strip():
                            self.logger.warning("Found empty link in sitemap, skipping")
                            continue

                        sitemap_link_count += 1
                        link_count += 1
                        yield link_text.strip()

                    self.logger.info(
                        "Extracted %d links from sitemap %s",
                        sitemap_link_count,
                        sitemap_link,
                    )

                except requests.RequestException as e:
                    self.logger.error(
                        "Error fetching sitemap %s: %s", sitemap_link, str(e)
                    )
                    continue
                except Exception as e:
                    self.logger.error(
                        "Unexpected error processing sitemap %s: %s",
                        sitemap_link,
                        str(e),
                    )
                    continue

            except re.error as e:
                self.logger.error(
                    "Regex error matching sitemap pattern: %s. Link: %s",
                    str(e),
                    sitemap_link,
                )
                continue
            except Exception as e:
                self.logger.error(
                    "Unexpected error processing sitemap link %s: %s",
                    sitemap_link,
                    str(e),
                )
                continue

        self.logger.info(
            "Processed %d job sitemaps, found %d total links", sitemap_count, link_count
        )


class MonsterHarvester(Harvester):
    pass


class IndeedHarvester(Harvester):
    pass


class HarvesterFactory:
    engines: Dict[str, Type[Harvester]] = {
        StepStoneHarvester.__name__: StepStoneHarvester,
        KarriereHarvester.__name__: KarriereHarvester,
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_next_harvester(self) -> Iterator[Harvester]:
        self.logger.info("Initializing harvesters from configuration")
        harvester_count = 0

        for portal in self.config["portals"]:
            engine_name = portal["engine"]
            if engine_name in self.engines:
                self.logger.info(
                    "Creating harvester: %s for URL: %s", engine_name, portal["url"]
                )
                harvester_count += 1
                yield self.engines[engine_name](portal)
            else:
                error_msg = f"Harvester {engine_name} not found"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        self.logger.info("Created %d harvesters", harvester_count)
