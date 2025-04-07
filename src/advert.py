from bs4 import BeautifulSoup
from typing import Dict, Any, Optional, List, Type, Union, TypeVar


class Advertisement:
    """Base class for job advertisements."""

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
    ) -> Advertisement:
        """
        Create an advertisement instance of the specified type.

        Args:
            ad_type: Type identifier for the advertisement
            source: HTML content of the advertisement
            link: URL of the advertisement
            status: HTTP status code of the response

        Returns:
            Advertisement instance

        Raises:
            ValueError: If the ad_type is not registered
        """
        if ad_type not in cls._registry:
            raise ValueError(f"Unknown advertisement type: {ad_type}")
        return cls._registry[ad_type](source=source, link=link, status=status)

    @classmethod
    def get_registered_types(cls) -> List[str]:
        """
        Get a list of registered advertisement types.

        Returns:
            List of registered advertisement types
        """
        return list(cls._registry.keys())


# Register advertisement classes
AdFactory.register(KarriereAdvertisement.__name__, KarriereAdvertisement)
AdFactory.register(StepstoneAdvertisement.__name__, StepstoneAdvertisement)
