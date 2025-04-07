import logging
import sqlite3
import tempfile
import unittest
from unittest.mock import call, patch
from time import time, sleep
import sys
import os
import re


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)
from harvester import (
    Harvester,
    IndeedHarvester,
    KarriereHarvester,
    MonsterHarvester,
    StepStoneHarvester,
)

KEYWORDS = [
    {"title": "Manager", "search": r"manager", "case_sensitive": False},
    {"title": "Controller", "search": r"controll", "case_sensitive": False},
    {"title": "Analyst", "search": r"analyst", "case_sensitive": False},
]

logger = logging.getLogger(__name__)


def mocked_request_get(*args, **kwargs):
    class MockResponse:
        cookies = {"session": "12345"}
        status_code = 200
        apparent_encoding = "utf-8"

        def __init__(self, file, status_code):
            self.file = file
            self.status_code = status_code

        @property
        def text(self):
            if not self.file:
                raise ValueError(f"Test data for '{args[0]}' does not found!")
            with open(self.file, "r", encoding="utf-8") as file:
                return file.read()

        @property
        def url(self):
            return args[0]

        def raise_for_status(self):
            pass

        def read(self):
            return self.file.read()

    if re.search(r"example.com/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "example_robots.txt",
                )
            ),
            200,
        )
    if re.search(r"/jobs/manager", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone.html",
                )
            ),
            200,
        )
    if re.search(r"/jobs\?keywords=manager", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_at.html",
                )
            ),
            200,
        )
    if re.search(r"stepstone.at/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"stepstone.at/sitemap.xml", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_sitemap.xml",
                ),
            ),
            200,
        )
    if re.search(r"stepstone.at/.*/sitemaps/.*/listings-[0-9]+.xml", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_listings.xml",
                ),
            ),
            200,
        )
    if re.search(
        r"stepstone.at.*/stellenangebote--.*\.html$",
        args[0],
    ):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_jobs_OTR_Manager.txt.iconv_cleaned_utf8",
                )
            ),
            200,
        )
    if re.search(r"karriere.at/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"karriere.at/static/sitemaps", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_sitemap_jobs.xml",
                ),
            ),
            200,
        )
    if re.search(r"monster.de/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"indeed.com/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(
        r"/jobs/[0-9]+$",
        args[0],
    ):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_job.html",
                )
            ),
            200,
        )
    return MockResponse(None, 404)


class TestHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    @patch("harvester.time")
    @patch("harvester.sleep")
    def test_get(self, mock_sleep, mock_time, mock_requests_get):
        config = {"url": "http://example.com", "requests_per_minute": 30}
        harvester = Harvester(config)
        harvester._last_request = 0

        mock_time.return_value = 5

        response = harvester._get("http://example.com")

        self.assertEqual(response.status_code, 404)
        mock_sleep.assert_called_once()
        self.assertEqual(harvester._last_request, 5)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "http://example.com/robots.txt",
                    headers={
                        "User-Agent": "Crawler",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9",
                    },
                ),
                call("http://example.com"),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_cookies(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 30}
        harvester = Harvester(config)

        cookies = harvester.cookies

        self.assertEqual(cookies, {"session": "12345"})
        self.assertEqual(harvester._referer, config["url"])
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={
                        "User-Agent": "Crawler",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9",
                    },
                ),
                call(
                    "https://www.stepstone.at",
                    headers={
                        "User-Agent": "Crawler",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9",
                    },
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_robot_uri_check(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 60}
        harvester = Harvester(config)

        self.assertTrue(harvester.can_fetch("/jobs/manager"))
        mock_requests_get.assert_called_once_with(
            "https://www.stepstone.at/robots.txt", headers=harvester._headers
        )


class TestStepstoneHarvester(unittest.TestCase):

    def setUp(self):
        self.temp_db_file = tempfile.mkstemp(suffix=".db")[1]
        self.connection = sqlite3.connect(self.temp_db_file)
        # self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_get_next_link(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 6000}
        harvester = StepStoneHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        links = []
        for link in harvester.get_next_link():
            self.assertIsNotNone(link)
            links.append(link)

        self.assertEqual(len(links), 11017)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call(
                    "https://www.stepstone.at/sitemap.xml",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call(
                    "https://www.stepstone.at/5/sitemaps/at/de/listings-1.xml",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )

    @patch("harvester.StepStoneHarvester.get_next_link")
    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get, mock_get_next_link):
        mock_get_next_link.return_value = iter(
            [
                "https://www.stepstone.at/stellenangebote--FruehstueckskellnerIn-m-w-d-Bad-Ischl-Alpin-Family-GmbH--888366-inline.html",
                "https://www.stepstone.at/stellenangebote--Berater-fuer-digitale-Zeitmanagementsysteme-m-w-d-Full-Time-Kufstein-Oesterreich-SELSYS-GmbH--895653-inline.html",
                "https://www.stepstone.at/stellenangebote--Elektrischer-Instandhaltungstechniker-Oberpullendorf-ISG-Personalmanagement-GmbH--892240-inline.html",
            ]
        )
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 6000}
        for keyword in KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)
        harvester = StepStoneHarvester(config)
        harvester._headers = {"User-Agent": Harvester.AGENT}

        harvester.harvest(self.temp_db_file)

        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={"User-Agent": "Crawler"},
                ),
                call("https://www.stepstone.at", headers={"User-Agent": "Crawler"}),
                call(
                    "https://www.stepstone.at/stellenangebote--FruehstueckskellnerIn-m-w-d-Bad-Ischl-Alpin-Family-GmbH--888366-inline.html",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Berater-fuer-digitale-Zeitmanagementsysteme-m-w-d-Full-Time-Kufstein-Oesterreich-SELSYS-GmbH--895653-inline.html",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Elektrischer-Instandhaltungstechniker-Oberpullendorf-ISG-Personalmanagement-GmbH--892240-inline.html",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
            ]
        )
        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM advertisements")
        result = cursor.fetchone()
        self.assertEqual(result[0], 3)
        cursor.execute("SELECT count(*) FROM keyword_advertisement")
        result = cursor.fetchone()
        self.assertEqual(result[0], 6)


class TestKarriereAtHarvester(unittest.TestCase):

    def setUp(self):
        self.temp_db_file = tempfile.mkstemp(suffix=".db")[1]
        self.connection = sqlite3.connect(self.temp_db_file)
        # self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_get_next_link(self, mock_requests_get):
        config = {"url": "https://www.karriere.at", "requests_per_minute": 6000}
        harvester = KarriereHarvester(config)
        harvester._headers = {"User-Agent": KarriereHarvester.AGENT}

        links = []
        for link in harvester.get_next_link():
            self.assertIsNotNone(link)
            links.append(link)

        self.assertEqual(len(links), 18549)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.karriere.at/robots.txt",
                    headers={"User-Agent": "Crawler"},
                ),
                call(
                    "https://www.karriere.at/static/sitemaps/sitemap-jobs-https.xml",
                    headers={"User-Agent": "Crawler"},
                ),
            ]
        )

    @patch("harvester.KarriereHarvester.get_next_link")
    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get, mock_get_next_link):
        mock_get_next_link.return_value = iter(
            [
                "https://www.karriere.at/jobs/7473235",
                "https://www.karriere.at/jobs/7482247",
                "https://www.karriere.at/jobs/7440898",
            ]
        )
        config = {"url": "https://www.karriere.at", "requests_per_minute": 6000}
        for keyword in KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)
        harvester = KarriereHarvester(config)
        harvester._headers = {"User-Agent": KarriereHarvester.AGENT}

        harvester.harvest(self.temp_db_file)

        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.karriere.at/robots.txt",
                    headers={"User-Agent": "Crawler"},
                ),
                call("https://www.karriere.at", headers={"User-Agent": "Crawler"}),
                call(
                    "https://www.karriere.at/jobs/7473235",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.at/jobs/7482247",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.at/jobs/7440898",
                    headers={"User-Agent": "Crawler"},
                    cookies={"session": "12345"},
                ),
            ]
        )

        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM advertisements")
        result = cursor.fetchone()
        self.assertEqual(result[0], 3)
        cursor.execute("SELECT * FROM keyword_advertisement")
        result = cursor.fetchall()
        self.assertEqual(len(result), 3)


class TestMonsterHarvester(unittest.TestCase):

    def setUp(self):
        self.temp_db_file = tempfile.mkstemp(suffix=".db")[1]
        self.connection = sqlite3.connect(self.temp_db_file)
        # self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    def test_get_next_link(self):
        config = {"url": "https://www.monster.de", "requests_per_minute": 60}
        harvester = MonsterHarvester(config)

        with self.assertRaises(NotImplementedError) as context:
            list(harvester.get_next_link())

        self.assertEqual(
            str(context.exception),
            "MonsterHarvester.get_next_link() is not implemented yet.",
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://www.monster.de", "requests_per_minute": 60}
        for keyword in KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)
        harvester = MonsterHarvester(config)
        harvester._headers = {"User-Agent": MonsterHarvester.AGENT}

        with self.assertRaises(NotImplementedError) as context:
            harvester.harvest(self.temp_db_file)

        self.assertEqual(
            str(context.exception),
            "MonsterHarvester.get_next_link() is not implemented yet.",
        )

        self.assertFalse(mock_requests_get.called)

        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM advertisements")
        result = cursor.fetchone()
        self.assertEqual(result[0], 0)
        cursor.execute("SELECT * FROM keyword_advertisement")
        result = cursor.fetchall()
        self.assertEqual(len(result), 0)


class TestIneedHarvester(unittest.TestCase):

    def setUp(self):
        # Set up an in-memory SQLite database for testing
        self.temp_db_file = tempfile.mkstemp(suffix=".db")[1]
        self.connection = sqlite3.connect(self.temp_db_file)
        # self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    def test_get_next_link(self):
        config = {"url": "https://www.monster.de", "requests_per_minute": 60}
        harvester = IndeedHarvester(config)

        with self.assertRaises(NotImplementedError) as context:
            list(harvester.get_next_link())

        self.assertEqual(
            str(context.exception),
            "IndeedHarvester.get_next_link() is not implemented yet.",
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://www.monster.de", "requests_per_minute": 60}
        for keyword in KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)
        harvester = IndeedHarvester(config)
        harvester._headers = {"User-Agent": MonsterHarvester.AGENT}

        with self.assertRaises(NotImplementedError) as context:
            harvester.harvest(self.temp_db_file)

        self.assertEqual(
            str(context.exception),
            "IndeedHarvester.get_next_link() is not implemented yet.",
        )

        self.assertFalse(mock_requests_get.called)

        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM advertisements")
        result = cursor.fetchone()
        self.assertEqual(result[0], 0)
        cursor.execute("SELECT * FROM keyword_advertisement")
        result = cursor.fetchall()
        self.assertEqual(len(result), 0)


class TestInsertKeyword(unittest.TestCase):

    def setUp(self):
        # Set up an in-memory SQLite database for testing
        self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    def test_insert_keyword_new(self):
        # Test inserting a new keyword
        logger.info("Inserting keyword:", KEYWORDS[0])
        Harvester.insert_keyword(self.connection, KEYWORDS[0])

        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT search FROM keywords WHERE title = ?", (KEYWORDS[0]["title"],)
        )
        result = cursor.fetchone()

        logger.info("Retrieved regex:", result)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], KEYWORDS[0]["search"])

    def test_insert_keyword_duplicate(self):
        Harvester.insert_keyword(self.connection, KEYWORDS[0])
        Harvester.insert_keyword(self.connection, KEYWORDS[0])

        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM keywords WHERE search = ?", (KEYWORDS[0]["search"],)
        )
        result = cursor.fetchone()

        self.assertEqual(result[0], 1)  # Ensure no duplicate entries


class TestFetchKeywords(unittest.TestCase):

    def setUp(self):
        # Set up an in-memory SQLite database for testing
        self.connection = sqlite3.connect(":memory:")
        Harvester.create_schema(self.connection)

    def tearDown(self):
        # Close the database connection after each test
        self.connection.close()

    def test_fetch_keywords_empty(self):
        # Test fetch_keywords when no keywords are present in the database
        harvester = Harvester({"url": "http://example.com"})
        keywords = harvester.fetch_keywords(self.connection)

        self.assertEqual(keywords, {})  # Should return an empty dictionary

    def test_fetch_keywords_single(self):
        # Test fetch_keywords with a single keyword in the database
        Harvester.insert_keyword(
            self.connection,
            {"title": "Manager", "search": r"\w*manager\w*", "case_sensitive": False},
        )
        harvester = Harvester({"url": "http://example.com"})
        keywords = harvester.fetch_keywords(self.connection)

        self.assertEqual(len(keywords), 1)
        self.assertEqual(keywords[1].pattern, r"\w*manager\w*")

    def test_fetch_keywords_multiple(self):
        # Test fetch_keywords with multiple keywords in the database
        Harvester.insert_keyword(
            self.connection,
            {"title": "Manager", "search": r"manager", "case_sensitive": False},
        )
        Harvester.insert_keyword(
            self.connection,
            {"title": "Controller", "search": r"controll", "case_sensitive": False},
        )
        harvester = Harvester({"url": "http://example.com"})
        keywords = harvester.fetch_keywords(self.connection)

        self.assertEqual(len(keywords), 2)
        self.assertTrue(1 in keywords)
        self.assertTrue(2 in keywords)
        self.assertEqual(keywords[1].pattern, r"manager")
        self.assertEqual(keywords[2].pattern, r"controll")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG, format="%(name)s %(levelname)s %(message)s"
    )
    unittest.main()
