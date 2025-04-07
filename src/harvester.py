from functools import cached_property
import re
import sqlite3
from urllib.parse import urlparse
import requests
import csv
import io
import logging
from abc import abstractmethod
from time import sleep, time
from datetime import datetime
from protego import Protego
from typing import Dict, List, Any, Iterator, Optional, Type, Tuple, Set

from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from advert import (
    AdFactory,
    Advertisement,
    KarriereAdvertisement,
    StepstoneAdvertisement,
)


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
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @staticmethod
    def create_schema(connection: sqlite3.Connection) -> None:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS advertisements (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                title TEXT,
                description TEXT,
                company TEXT,
                location TEXT,
                harvest_date DATE,
                url TEXT NOT NULL UNIQUE,
                html_body TEXT NOT NULL,
                html_status INTEGER NOT NULL,
                ad_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
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
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_advertisement (
                keyword_id INTEGER,
                advertisement_id INTEGER,
                FOREIGN KEY (keyword_id) REFERENCES keywords(id),
                FOREIGN KEY (advertisement_id) REFERENCES advertisements(id)
                PRIMARY KEY (keyword_id, advertisement_id)
            )
            """
        )
        connection.commit()

    @staticmethod
    def insert_keyword(connection: sqlite3.Connection, keyword: Dict[str, Any]) -> None:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO keywords (title, search, case_sensitive) VALUES (?, ?, ?)",
            (keyword["title"], keyword["search"], keyword["case_sensitive"]),
        )
        connection.commit()

    def fetch_keywords(self, connection: sqlite3.Connection) -> Dict[int, re.Pattern]:
        cursor = connection.cursor()
        cursor.execute("SELECT id, search, case_sensitive FROM keywords")
        result = cursor.fetchall()
        if result:
            return dict(
                [
                    (
                        row[0],
                        Harvester._compile_keyword(
                            search=row[1], case_sensitive=row[2]
                        ),
                    )
                    for row in result
                ]
            )
        return {}

    @staticmethod
    def _compile_keyword(search: str, case_sensitive: bool) -> re.Pattern:
        """
        Compiles a keyword into a regex pattern.
        """
        if case_sensitive:
            return re.compile(search)
        return re.compile(search, re.IGNORECASE)

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
            connection: SQLite database connection
            url: URL of the advertisement to check

        Returns:
            True if the advertisement exists and is valid, False otherwise
        """
        connection = sqlite3.connect(db_file_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT url, html_status, html_body 
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
        connection = sqlite3.connect(db_file_name)
        regexes = self.fetch_keywords(connection)
        connection.commit()
        connection.close()

        self.logger.info("Starting harvest process for %s", self.__class__.__name__)

        for advert in self.get_next_advert(db_file_name):
            connection = sqlite3.connect(db_file_name)
            cursor = connection.cursor()
            # Check if the advertisement already exists in the database
            cursor.execute(
                "SELECT id FROM advertisements WHERE url = ?",
                (advert.link,),
            )
            if cursor.fetchone():
                self.logger.info(
                    "Advertisement %s already exists in the database.", advert.link
                )
                continue
            # Insert the advertisement into the database
            cursor.execute(
                "INSERT INTO advertisements (title, company, location, description, html_body, html_status, url, ad_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    advert.get_title(),
                    advert.get_company(),
                    advert.get_location(),
                    advert.get_description(),
                    advert.source,
                    advert.status,
                    advert.link,
                    advert.__class__.__name__,
                ),
            )
            advert_id = cursor.lastrowid
            matched_keywords = self.match_keywords(advert, regexes)
            self.logger.debug(
                "Advertisement %s matched %d keywords",
                advert.link,
                len(matched_keywords),
            )

            for keyword_id in matched_keywords:
                cursor.execute(
                    "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
                    (keyword_id, advert_id),
                )
            connection.commit()
            connection.close()

        self.logger.info("Harvest process completed for %s", self.__class__.__name__)

    def match_keywords(
        self, advert: Advertisement, regexes: Dict[int, re.Pattern]
    ) -> List[int]:
        """
        Matches the advertisement against the keywords.
        """
        result = []
        # Extract the text from the advertisement
        for id, regex in regexes.items():
            match = regex.search(advert.get_description())
            if match:
                # If the keyword matches, add it to the result
                result.append(id)
        return result

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
            SELECT a.id, a.ad_type, a.html_body, a.url, a.created_at
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
            ad = AdFactory.create(data[1], data[2], data[3])
            title = ad.get_title() or ""
            company = ad.get_company() or ""
            location = ad.get_location() or ""
            harvest_date = data[4] or ""
            url = data[3] or ""

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
                "harvest_date": harvest_date,
                "url": url,
                "portal": urlparse(url).netloc,
                "keywords": keyword_titles,
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
                            "harvest_date": ad["harvest_date"],
                            "url": ad["url"],
                            "portal": ad["portal"],
                            "related_keywords": "; ".join(ad["keywords"]),
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
                    "harvest_date": ad["harvest_date"],
                    "url": ad["url"],
                    "portal": ad["portal"],
                    "related_keywords": "; ".join(ad["keywords"]),
                }
            )

        logger.debug(
            "CSV string generation completed with %d rows", len(advertisements)
        )
        return output.getvalue()


class StepStoneHarvester(Harvester):

    def get_next_advert(self, db_file_name: str) -> Iterator[StepstoneAdvertisement]:
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

            if response.status_code in (500, 502, 503, 504):
                self.logger.warning(
                    "Server error fetching %s: %d, retrying in 5 minutes",
                    link,
                    response.status_code,
                )
                sleep(5 * 60)  # Wait for 5 minutes before retrying
                response = self._get(link, headers=self._headers, cookies=self.cookies)

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
                self.logger.info(
                    "Advertisement %s already exists in the database.", link
                )
                continue

            response = self._get(link, headers=self._headers, cookies=self.cookies)
            response.raise_for_status()
            response.encoding = response.apparent_encoding

            self.logger.debug("Successfully retrieved advertisement from %s", link)

            yield KarriereAdvertisement(
                status=response.status_code,
                link=link,
                source=response.text,
            )

    def get_next_link(self) -> Iterator[str]:
        self.logger.info("Fetching sitemap links from robots.txt")
        sitemap_count = 0
        link_count = 0

        for sitemap_link in self._get_robot_parser().sitemaps:
            if re.match(r".*sitemap-jobs.*", sitemap_link):
                sitemap_count += 1
                self.logger.info("Processing jobs sitemap: %s", sitemap_link)

                response = self._get(sitemap_link, headers=self._headers)
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                sitemap = ET.fromstring(response.text)

                sitemap_link_count = 0
                for link in sitemap.findall(
                    ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                ):
                    sitemap_link_count += 1
                    link_count += 1
                    yield link.text

                self.logger.info(
                    "Extracted %d links from sitemap %s",
                    sitemap_link_count,
                    sitemap_link,
                )

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
