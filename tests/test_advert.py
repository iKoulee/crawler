import sys
import unittest
import os
import logging
from bs4 import BeautifulSoup
from unittest.mock import patch, MagicMock, mock_open
from typing import Dict, Any

# Configure logging at DEBUG level
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)
from advert import KarriereAdvertisement, StepstoneAdvertisement, Advertisement


class TestKarriereAdvertisement(unittest.TestCase):
    def test_get_title_with_valid_html(self):
        html = """
        <html>
            <body>
                <h1 class="m-jobHeader__jobTitle">Software Engineer</h1>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertEqual(ad.get_title(), "Software Engineer")

    def test_get_title_with_missing_title(self):
        html = """
        <html>
            <body>
                <div class="m-jobHeader__jobTitle">No title here</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_title())

    def test_get_title_with_empty_source(self):
        html = ""
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_title())

    def test_get_title_with_no_matching_class(self):
        html = """
        <html>
            <body>
                <h1 class="other-class">Some Title</h1>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_title())

    def test_get_title_from_karriere_at_html(self):
        # Load the actual HTML file
        file_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "test_data",
                "karriere_at.html",
            )
        )
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        ad = KarriereAdvertisement(source=html_content)
        title = ad.get_title()
        self.assertIsNotNone(title)
        self.assertIsInstance(title, str)
        self.assertTrue(len(title) > 0)

    def test_get_company(self):
        html = """
        <html>
            <body>
                <div class="m-keyfactBox__companyName">Test Company GmbH</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertEqual(ad.get_company(), "Test Company GmbH")

    def test_get_company_with_missing_company(self):
        html = """
        <html>
            <body>
                <div class="wrong-class">Some Company</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_company())

    def test_get_description(self):
        html = """
        <html>
            <body>
                <div class="m-jobContent__jobDetail">Job description here</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertEqual(ad.get_description(), "Job description here")

    def test_get_description_with_missing_description(self):
        html = """
        <html>
            <body>
                <div class="wrong-class">Some description</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_description())

    def test_get_location(self):
        html = """
        <html>
            <body>
                <div class="m-keyfactBox__jobLocations">Vienna, Austria</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertEqual(ad.get_location(), "Vienna, Austria")

    def test_get_location_with_missing_location(self):
        html = """
        <html>
            <body>
                <div class="wrong-class">Some Location</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_location())

    def test_get_location_with_empty_source(self):
        html = ""
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_location())

    def test_get_location_with_no_matching_class(self):
        html = """
        <html>
            <body>
                <div class="other-class">Another Location</div>
            </body>
        </html>
        """
        ad = KarriereAdvertisement(source=html)
        self.assertIsNone(ad.get_location())

    def test_get_location_from_karriere_at_html(self):
        # Load the actual HTML file
        file_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "test_data",
                "karriere_at.html",
            )
        )
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        ad = KarriereAdvertisement(source=html_content)
        location = ad.get_location()
        self.assertIsNotNone(location)
        self.assertIsInstance(location, str)
        self.assertTrue(len(location) > 0)


class TestStepstoneAdvertisement(unittest.TestCase):
    """Test cases for the StepstoneAdvertisement class."""

    def setUp(self) -> None:
        """Set up test data for each test."""
        # Sample HTML content for testing
        self.sample_html = """
        <html>
            <head>
                <title>Software Developer - ABC Company - Vienna, Austria</title>
                <meta name="description" content="Job description here">
            </head>
            <body>
                <div class="listing-content">
                    <h1 data-at="header-job-title">Software Developer</h1>
                    <a data-at="metadata-company-name">ABC Company</a>
                    <a data-at="metadata-location">Vienna, Austria</a>
                    <article class="job-description">
                        <p>We are seeking a Python developer with 3+ years experience.</p>
                        <p>Skills required: Python, Django, SQL</p>
                    </article>
                    <time datetime="2023-07-15">July 15, 2023</time>
                </div>
            </body>
        </html>
        """

        # Create a StepstoneAdvertisement instance for testing
        self.advert = StepstoneAdvertisement(
            status=200,
            link="https://www.stepstone.at/job/12345",
            source=self.sample_html,
        )

    def test_init(self) -> None:
        """Test that the StepstoneAdvertisement initializes correctly."""
        self.assertEqual(self.advert.status, 200)
        self.assertEqual(self.advert.link, "https://www.stepstone.at/job/12345")
        self.assertEqual(self.advert.source, self.sample_html)
        self.assertIsInstance(self.advert.soup, BeautifulSoup)

    def test_get_title(self) -> None:
        """Test that get_title extracts the title correctly."""
        title = self.advert.get_title()
        self.assertEqual(title, "Software Developer")

    def test_get_company(self) -> None:
        """Test that get_company extracts the company name correctly."""
        company = self.advert.get_company()
        self.assertEqual(company, "ABC Company")

    def test_get_location(self) -> None:
        """Test that get_location extracts the location correctly."""
        location = self.advert.get_location()
        self.assertEqual(location, "Vienna, Austria")

    def test_get_description(self) -> None:
        """Test that get_description extracts the job description correctly."""
        description = self.advert.get_description()
        self.assertIn("We are seeking a Python developer", description)
        self.assertIn("Skills required: Python, Django, SQL", description)

    def test_get_date(self) -> None:
        """Test that get_date extracts the posting date correctly."""
        date = self.advert.get_date()
        self.assertEqual(date, "July 15, 2023")

    def test_to_dict(self) -> None:
        """Test that to_dict returns the correct dictionary representation."""
        result = self.advert.to_dict()
        expected = {
            "title": "Software Developer",
            "company": "ABC Company",
            "location": "Vienna, Austria",
            "description": "We are seeking a Python developer with 3+ years experience.\nSkills required: Python, Django, SQL",
            "date": "July 15, 2023",
            "link": "https://www.stepstone.at/job/12345",
            "source": self.sample_html,
            "status": 200,
        }
        self.assertDictEqual(result, expected)

    def test_missing_elements(self) -> None:
        """Test behavior when elements are missing from the HTML."""
        incomplete_html = """
        <html>
            <body>
                <div class="listing-content">
                    <h1 data-at="header-job-title">Software Developer</h1>
                </div>
            </body>
        </html>
        """

        advert = StepstoneAdvertisement(
            status=200,
            link="https://www.stepstone.at/job/12345",
            source=incomplete_html,
        )

        # Test methods return empty strings or None for missing elements
        self.assertEqual(advert.get_title(), "Software Developer")
        self.assertEqual(advert.get_company(), None)
        self.assertEqual(advert.get_location(), None)
        self.assertEqual(advert.get_description(), None)
        self.assertEqual(advert.get_date(), None)

    def test_invalid_html(self) -> None:
        """Test behavior with invalid HTML."""
        invalid_html = "<invalid>This is not valid HTML"

        advert = StepstoneAdvertisement(
            status=200, link="https://www.stepstone.at/job/12345", source=invalid_html
        )

        # Even with invalid HTML, BeautifulSoup should create a valid object
        # and methods should return empty strings rather than raising exceptions
        self.assertEqual(advert.get_title(), None)
        self.assertEqual(advert.get_company(), None)
        self.assertEqual(advert.get_description(), None)

    @patch("builtins.print")
    def test_debug_method(self, mock_print: MagicMock) -> None:
        """Test the debug method for outputting information."""
        self.advert.debug()
        # Verify that print was called with the expected arguments
        mock_print.assert_called()
        # Check that title was part of the debug output
        title_call = any(
            "Software Developer" in call[0][0] for call in mock_print.call_args_list
        )
        self.assertTrue(title_call)


if __name__ == "__main__":
    unittest.main()
