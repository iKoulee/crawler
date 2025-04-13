import sqlite3
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional, List, Type, Union, TypeVar, Iterator


class Advertisement:
    """Base class for job advertisements."""

    id: Optional[int] = None

    def __init__(
        self, source: str, link: Optional[str] = None, status: Optional[int] = None
    ) -> None:
        """
        Initialize an Advertisement instance.

        Args:
            source: HTML content of the advertisement
            link: URL of the advertisement
            status: HTTP status code of the response
        """
        self.source: str = source
        self.link: Optional[str] = link
        self.status: Optional[int] = status
        # Parse HTML only once and store the BeautifulSoup object
        self.soup: BeautifulSoup = BeautifulSoup(self.source, "html.parser")

    def get_title(self) -> Optional[str]:
        """
        Extract the job title from the advertisement.

        Returns:
            Job title or None if not found
        """
        return None

    def get_company(self) -> Optional[str]:
        """
        Extract the company name from the advertisement.

        Returns:
            Company name or None if not found
        """
        return None

    def get_location(self) -> Optional[str]:
        """
        Extract the job location from the advertisement.

        Returns:
            Job location or None if not found
        """
        return None

    def get_description(self) -> Optional[str]:
        """
        Extract the job description from the advertisement.

        Returns:
            Job description or None if not found
        """
        return None

    def get_date(self) -> Optional[str]:
        """
        Extract the posting date from the advertisement.

        Returns:
            Posting date or None if not found
        """
        return None

    def save(self, db_path: str) -> int:
        """
        Save the advertisement to the database. Updates if ID exists, otherwise inserts.

        This method connects to the database specified by db_path and either:
        - Updates an existing row if self.id is set and exists in the database
        - Inserts a new row if self.id is None or not found in the database

        Args:
            db_path: Path to the SQLite database file

        Returns:
            The ID of the saved advertisement

        Raises:
            sqlite3.Error: If a database error occurs during save operation
        """
        import sqlite3

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        try:
            # Get data from advertisement
            ad_data = self.to_dict()
            ad_type = self.__class__.__name__

            if self.id is not None:
                # Check if record exists with this ID
                cursor.execute("SELECT id FROM advertisements WHERE id = ?", (self.id,))

                if cursor.fetchone():
                    # Update existing record
                    cursor.execute(
                        """
                        UPDATE advertisements 
                        SET title = ?, company = ?, location = ?, description = ?,
                            html_body = ?, http_status = ?, url = ?, ad_type = ?
                        WHERE id = ?
                        """,
                        (
                            ad_data["title"],
                            ad_data["company"],
                            ad_data["location"],
                            ad_data["description"],
                            self.source,
                            self.status,
                            self.link,
                            ad_type,
                            self.id,
                        ),
                    )
                    connection.commit()
                    return self.id

            # If no ID or ID not found, insert new record
            cursor.execute(
                """
                INSERT INTO advertisements 
                (title, company, location, description, html_body, http_status, url, ad_type, filename)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ad_data["title"],
                    ad_data["company"],
                    ad_data["location"],
                    ad_data["description"],
                    self.source,
                    self.status,
                    self.link,
                    ad_type,
                    None,  # Default filename to None initially
                ),
            )

            # Get the ID of the newly inserted record
            self.id = cursor.lastrowid
            connection.commit()
            return self.id

        except sqlite3.Error as e:
            connection.rollback()
            raise e
        finally:
            connection.close()

    def debug(self) -> None:
        """Print advertisement information for debugging purposes."""
        print(f"Title: {self.get_title()}")
        print(f"Company: {self.get_company()}")
        print(f"Location: {self.get_location()}")
        print(f"Description: {self.get_description()}")
        print(f"Date: {self.get_date()}")
        print(f"Link: {self.link}")
        print(f"Status: {self.status}")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the advertisement to a dictionary.

        Returns:
            Dictionary representation of the advertisement
        """
        return {
            "title": self.get_title(),
            "company": self.get_company(),
            "location": self.get_location(),
            "description": self.get_description(),
            "date": self.get_date(),
            "link": self.link,
            "source": self.source,
            "status": self.status,
        }


class KarriereAdvertisement(Advertisement):
    """Class for parsing karriere.at job advertisements."""

    def get_title(self) -> Optional[str]:
        """
        Extract the job title from a karriere.at advertisement.

        Returns:
            Job title or None if not found
        """
        title_element = self.soup.select_one("h1.m-jobHeader__jobTitle")
        return title_element.text.strip() if title_element else None

    def get_company(self) -> Optional[str]:
        """
        Extract the company name from a karriere.at advertisement.

        Returns:
            Company name or None if not found
        """
        company_element = self.soup.select_one("a[aria-label^='Employer Page von']")

        if not company_element:
            company_element = self.soup.select_one("a.m-keyfactBox__companyName")

        if not company_element:
            company_element = self.soup.select_one("div.m-keyfactBox__companyName")

        return company_element.text.strip() if company_element else None

    def get_location(self) -> Optional[str]:
        """
        Extract the job location from a karriere.at advertisement.

        Returns:
            Job location or None if not found
        """
        location_element = self.soup.select_one(".m-keyfactBox__jobLocations")
        return location_element.text.strip() if location_element else None

    def get_description(self) -> Optional[str]:
        """
        Extract the job description from a karriere.at advertisement.

        Returns:
            Job description or None if not found
        """
        description_element = self.soup.select_one(".m-jobContent__jobDetail")
        return description_element.text.strip() if description_element else None

    def get_date(self) -> Optional[str]:
        """
        Extract the posting date from a karriere.at advertisement.

        Returns:
            Posting date or None if not found
        """
        date_element = self.soup.select_one(".m-jobHeader__jobDateShort")
        return date_element.text.strip() if date_element else None


class StepstoneAdvertisement(Advertisement):
    """Class for parsing stepstone.at job advertisements."""

    def get_title(self) -> Optional[str]:
        """
        Extract the job title from a stepstone.at advertisement.

        Returns:
            Job title or empty string if not found
        """
        title_element = self.soup.find("h1", {"data-at": "header-job-title"})
        return title_element.text.strip() if title_element else None

    def get_company(self) -> Optional[str]:
        """
        Extract the company name from a stepstone.at advertisement.

        Returns:
            Company name or empty string if not found
        """
        company_element = self.soup.find("a", {"data-at": "metadata-company-name"})
        if not company_element:
            company_element = self.soup.find(
                "span", {"data-at": "metadata-company-name"}
            )
        return company_element.text.strip() if company_element else None

    def get_location(self) -> Optional[str]:
        """
        Extract the job location from a stepstone.at advertisement.

        Returns:
            Job location or empty string if not found
        """
        location_element = self.soup.find("a", {"data-at": "metadata-location"})
        return location_element.text.strip() if location_element else None

    def get_description(self) -> Optional[str]:
        """
        Extract the job description from a stepstone.at advertisement.

        Returns:
            Job description or empty string if not found
        """
        description_elements = self.soup.find_all("article")
        description: str = ""
        for element in description_elements:
            description += element.text.strip()
        return description if description else None

    def get_date(self) -> Optional[str]:
        """
        Extract the posting date from a stepstone.at advertisement.

        Returns:
            Posting date or empty string if not found
        """
        date_element = self.soup.select_one("time")
        return date_element.text.strip() if date_element else None


T = TypeVar("T", bound=Advertisement)


class AdFactory:
    """Factory for creating advertisement instances."""

    _registry: Dict[str, Type[Advertisement]] = {}

    @classmethod
    def register(cls, ad_type: str, advertisement_class: Type[T]) -> None:
        """
        Register an advertisement class.

        Args:
            ad_type: Type identifier for the advertisement
            advertisement_class: Advertisement class to register
        """
        cls._registry[ad_type] = advertisement_class

    @classmethod
    def create(
        cls,
        ad_type: str,
        source: str,
        link: Optional[str] = None,
        status: Optional[int] = None,
        id: Optional[int] = None,
    ) -> Advertisement:
        """
        Create an advertisement instance of the specified type.

        Args:
            ad_type: Type identifier for the advertisement
            source: HTML content of the advertisement
            link: URL of the advertisement
            status: HTTP status code of the response
            id: Database ID of the advertisement (optional)

        Returns:
            Advertisement instance

        Raises:
            ValueError: If the ad_type is not registered
        """
        if ad_type not in cls._registry:
            raise ValueError(f"Unknown advertisement type: {ad_type}")
        ad = cls._registry[ad_type](source=source, link=link, status=status)

        # Set the ID if provided
        if id is not None:
            ad.id = id

        return ad

    @classmethod
    def get_registered_types(cls) -> List[str]:
        """
        Get a list of registered advertisement types.

        Returns:
            List of registered advertisement types
        """
        return list(cls._registry.keys())

    @classmethod
    def fetch_by_condition(
        cls,
        db_path: Optional[str] = None,
        condition: str = "",
        params: Optional[List[Any]] = None,
        batch_size: int = 100,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Iterator[Advertisement]:
        """
        Fetch advertisements from the database using SQL condition and return as an iterator.

        This method efficiently retrieves advertisements in batches to minimize memory usage.
        Either db_path or connection must be provided, but not both.

        Args:
            db_path: Path to the SQLite database file (mutually exclusive with connection)
            condition: SQL WHERE clause condition (without the "WHERE" keyword)
            params: Parameters for the SQL query placeholders
            batch_size: Number of records to fetch in each batch
            connection: Optional existing SQLite connection (mutually exclusive with db_path)

        Returns:
            Iterator of Advertisement objects

        Raises:
            ValueError: If both db_path and connection are provided or if neither is provided
            sqlite3.Error: If a database error occurs during fetch operation
        """
        import sqlite3
        import logging

        logger = logging.getLogger(f"{__name__}.AdFactory.fetch_by_condition")

        # Validate arguments
        if db_path is not None and connection is not None:
            error_msg = (
                "Both db_path and connection were provided. Please provide only one."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        if db_path is None and connection is None:
            error_msg = (
                "Neither db_path nor connection was provided. Please provide one."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Determine if we should close the connection when done
        should_close_connection = db_path is not None

        # Use provided connection or create a new one from db_path
        if connection is None:
            connection = sqlite3.connect(db_path)

        cursor = connection.cursor()

        # Build query with optional condition
        query = "SELECT id, ad_type, html_body, url, http_status FROM advertisements"
        if condition:
            query += f" WHERE {condition}"

        # Default to empty list if params is None
        params = params or []

        try:
            cursor.execute(query, params)
            logger.debug(f"Executing query: {query} with params: {params}")

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                logger.debug(f"Fetched batch of {len(rows)} advertisements")

                for row in rows:
                    ad_id, ad_type, html_body, url, status = row

                    # Create advertisement instance using the factory
                    yield cls.create(
                        ad_type=ad_type,
                        source=html_body,
                        link=url,
                        status=status,
                        id=ad_id,
                    )

        except sqlite3.Error as e:
            logger.error(f"Database error while fetching advertisements: {e}")
            # Only close the connection if we created it and an error occurred
            if should_close_connection:
                connection.close()
            raise
        finally:
            # Only close the connection if we created it
            if should_close_connection and connection is not None:
                connection.close()


# Register advertisement classes
AdFactory.register(KarriereAdvertisement.__name__, KarriereAdvertisement)
AdFactory.register(StepstoneAdvertisement.__name__, StepstoneAdvertisement)
