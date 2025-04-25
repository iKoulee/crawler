import unittest
import os
import sys
import sqlite3
import tempfile
import re
import shutil
import yaml
from unittest.mock import patch, MagicMock, PropertyMock, mock_open
from typing import Dict, Any, List, Optional, Tuple
from time import sleep

# Configure logging
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Add src directory to path for importing analyzer module
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from analyzer import AdvertAnalyzer
from advert import (
    Advertisement,
    AdFactory,
    StepstoneAdvertisement,
    KarriereAdvertisement,
)


class TestAdvertAnalyzer(unittest.TestCase):
    """Test cases for AdvertAnalyzer class."""

    def setUp(self) -> None:
        """Set up test environment with a temporary database."""
        # Create a temporary database file
        self.temp_db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(self.temp_db_fd)

        # Create a temporary config file
        self.config_content = {
            "keywords": [
                {"title": "Python", "search": "python", "case_sensitive": False},
                {"title": "Java", "search": "\\bjava\\b", "case_sensitive": False},
                {"title": "SQL", "search": "SQL", "case_sensitive": True},
            ]
        }
        self.config_fd, self.config_path = tempfile.mkstemp(suffix=".yml")
        with os.fdopen(self.config_fd, "w") as f:
            yaml.dump(self.config_content, f)

        # Create the database schema
        self.connection = sqlite3.connect(self.db_path)
        self._create_test_schema()

        # Set up the analyzer instance
        self.analyzer = AdvertAnalyzer(
            db_path=self.db_path, config_path=self.config_path
        )

        # Register advertisement classes with AdFactory
        AdFactory.register(KarriereAdvertisement.__name__, KarriereAdvertisement)
        AdFactory.register(StepstoneAdvertisement.__name__, StepstoneAdvertisement)

        # Insert test data
        self._insert_test_data()

    def tearDown(self) -> None:
        """Clean up test environment."""
        # Ensure analyzer connection is closed
        if hasattr(self, "analyzer"):
            self.analyzer._close_connection()

        # Close the test connection if it exists
        if hasattr(self, "connection") and self.connection:
            self.connection.close()

        # Add a short delay to ensure connections are fully released
        # (especially important on Windows)
        sleep(0.1)

        # Now delete the temporary files
        if os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except PermissionError:
                # On Windows, sometimes we need a bit more time for file locks to be released
                logging.warning(
                    "Failed to delete temporary database on first attempt, retrying..."
                )
                sleep(0.5)
                os.unlink(self.db_path)

        os.unlink(self.config_path)

    def _create_test_schema(self) -> None:
        """Create a test database schema."""
        cursor = self.connection.cursor()

        # Create advertisements table
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

        self.connection.commit()

    def _insert_test_data(self) -> None:
        """Insert test data into the database."""
        cursor = self.connection.cursor()

        # Insert test advertisements
        test_ads = [
            {
                "title": "Python Developer",
                "description": "We are looking for a Python developer with Django experience.",
                "company": "Tech Company",
                "location": "Berlin",
                "url": "https://example.com/python-job",
                "html_body": "<html><body><h1>Python Developer</h1><p>Looking for Python skills</p></body></html>",
                "http_status": 200,
                "ad_type": "StepstoneAdvertisement",
                "filename": None,
            },
            {
                "title": "Java Engineer",
                "description": "Java backend engineer needed for enterprise applications.",
                "company": "Enterprise Corp",
                "location": "Vienna",
                "url": "https://example.com/java-job",
                "html_body": "<html><body><h1>Java Engineer</h1><p>Java and SQL required</p></body></html>",
                "http_status": 200,
                "ad_type": "KarriereAdvertisement",
                "filename": None,
            },
            {
                "title": "Full Stack Developer",
                "description": "Full stack developer for web applications using JavaScript.",
                "company": "Web Solutions",
                "location": "Remote",
                "url": "https://example.com/fullstack-job",
                "html_body": "<html><body><h1>Full Stack Developer</h1><p>JavaScript, HTML, CSS</p></body></html>",
                "http_status": 200,
                "ad_type": "StepstoneAdvertisement",
                "filename": None,
            },
        ]

        for ad in test_ads:
            cursor.execute(
                """
                INSERT INTO advertisements 
                (title, description, company, location, url, html_body, http_status, ad_type, filename) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ad["title"],
                    ad["description"],
                    ad["company"],
                    ad["location"],
                    ad["url"],
                    ad["html_body"],
                    ad["http_status"],
                    ad["ad_type"],
                    ad["filename"],
                ),
            )

        self.connection.commit()

    def test_initialization(self) -> None:
        """Test the initialization of AdvertAnalyzer."""
        analyzer = AdvertAnalyzer(db_path=self.db_path, config_path=self.config_path)

        self.assertEqual(analyzer.db_path, self.db_path)
        self.assertEqual(analyzer.config_path, self.config_path)
        self.assertIsNone(analyzer._connection)
        self.assertEqual(analyzer.compiled_keywords, {})

    def test_get_connection(self) -> None:
        """Test the _get_connection method."""
        # Initial state should be None
        self.assertIsNone(self.analyzer._connection)

        # Get connection should create a connection
        connection = self.analyzer._get_connection()
        self.assertIsNotNone(connection)
        self.assertIsInstance(connection, sqlite3.Connection)

        # Second call should return the same connection
        second_connection = self.analyzer._get_connection()
        self.assertIs(connection, second_connection)

    def test_close_connection(self) -> None:
        """Test the _close_connection method."""
        # Get a connection first
        connection = self.analyzer._get_connection()
        self.assertIsNotNone(self.analyzer._connection)

        # Close the connection
        self.analyzer._close_connection()
        self.assertIsNone(self.analyzer._connection)

        # Closing when no connection exists should not raise an error
        self.analyzer._close_connection()

    def test_load_keywords_from_config(self) -> None:
        """Test loading keywords from configuration."""
        # Reset keyword tables to ensure clean state
        self.analyzer.reset_keyword_tables()

        # Load keywords
        count = self.analyzer.load_keywords_from_config()
        self.assertEqual(count, 3)  # Should load all 3 test keywords

        # Verify keywords were inserted into the database
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM keywords")
        db_count = cursor.fetchone()[0]
        self.assertEqual(db_count, 3)

        # Verify specific keywords
        cursor.execute(
            "SELECT title, search, case_sensitive FROM keywords ORDER BY title"
        )
        keywords = cursor.fetchall()

        # Check Java keyword
        java_keyword = next((kw for kw in keywords if kw[0] == "Java"), None)
        self.assertIsNotNone(java_keyword)
        self.assertEqual(java_keyword[1], "\\bjava\\b")
        self.assertEqual(java_keyword[2], 0)  # case_sensitive=False

        # Check SQL keyword
        sql_keyword = next((kw for kw in keywords if kw[0] == "SQL"), None)
        self.assertIsNotNone(sql_keyword)
        self.assertEqual(sql_keyword[1], "SQL")
        self.assertEqual(sql_keyword[2], 1)  # case_sensitive=True

    def test_reset_keyword_tables(self) -> None:
        """Test resetting the keyword tables."""
        # Load keywords to have data to reset
        self.analyzer.load_keywords_from_config()

        # Verify keywords exist
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM keywords")
        count_before = cursor.fetchone()[0]
        self.assertGreater(count_before, 0)

        # Reset tables
        self.analyzer.reset_keyword_tables()

        # Verify tables are empty
        cursor.execute("SELECT COUNT(*) FROM keywords")
        keywords_count = cursor.fetchone()[0]
        self.assertEqual(keywords_count, 0)

        cursor.execute("SELECT COUNT(*) FROM keyword_advertisement")
        assoc_count = cursor.fetchone()[0]
        self.assertEqual(assoc_count, 0)

    def test_compile_keyword_patterns(self) -> None:
        """Test compiling keyword patterns from the database."""
        # Load keywords
        self.analyzer.load_keywords_from_config()

        # Compile patterns
        patterns = self.analyzer._compile_keyword_patterns()

        # Verify patterns were compiled
        self.assertEqual(len(patterns), 3)

        # Check the types of compiled patterns
        for keyword_id, pattern in patterns.items():
            self.assertIsInstance(pattern, re.Pattern)

        # Test a pattern match
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM keywords WHERE title = 'Python'")
        python_id = cursor.fetchone()[0]

        python_pattern = patterns[python_id]
        self.assertTrue(python_pattern.search("Python is a programming language"))
        self.assertTrue(python_pattern.search("python is case-insensitive"))

        # Test case-sensitive pattern
        cursor.execute("SELECT id FROM keywords WHERE title = 'SQL'")
        sql_id = cursor.fetchone()[0]

        sql_pattern = patterns[sql_id]
        self.assertTrue(sql_pattern.search("SQL is a query language"))
        self.assertFalse(sql_pattern.search("sql is lowercase"))

    @patch("advert.AdFactory.fetch_by_condition")
    def test_process_advertisements(self, mock_fetch: MagicMock) -> None:
        """Test processing advertisements."""
        # Set up mock advertisements
        ad1 = MagicMock(spec=Advertisement)
        ad1.id = 1
        ad1.source = "<html><body>Python developer</body></html>"
        ad1.get_title = MagicMock(return_value="Python Developer")
        ad1.get_description = MagicMock(return_value="Python developer position")

        ad2 = MagicMock(spec=Advertisement)
        ad2.id = 2
        ad2.source = "<html><body>Java and SQL developer</body></html>"
        ad2.get_title = MagicMock(return_value="Java Developer")
        ad2.get_description = MagicMock(return_value="Java and SQL developer position")

        # Set up the mock to return our test advertisements
        mock_fetch.return_value = [ad1, ad2]

        # Load keywords and compile patterns
        self.analyzer.load_keywords_from_config()
        self.analyzer._compile_keyword_patterns()

        # Process advertisements with default settings (title_only matching)
        count = self.analyzer.process_advertisements()

        # Verify the correct number of advertisements were processed
        self.assertEqual(count, 2)

        # Verify fetch_by_condition was called with the right parameters
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args[1]
        self.assertEqual(call_kwargs["batch_size"], 100)

        # Verify that both get_title and get_description were called
        ad1.get_title.assert_called()
        ad2.get_title.assert_called()

        # Reset mocks and try with include_description=True
        mock_fetch.reset_mock()
        ad1.get_title.reset_mock()
        ad1.get_description.reset_mock()
        ad2.get_title.reset_mock()
        ad2.get_description.reset_mock()
        
        # Set up the mock to return our test advertisements again
        mock_fetch.return_value = [ad1, ad2]
        
        # Process with include_description=True
        count = self.analyzer.process_advertisements(include_description=True)
        
        # Verify both title and description were used
        ad1.get_title.assert_called()
        ad1.get_description.assert_called()
        ad2.get_title.assert_called()
        ad2.get_description.assert_called()

    def test_match_keywords_for_ad(self) -> None:
        """Test matching keywords for an advertisement."""
        # Load keywords and compile patterns
        self.analyzer.load_keywords_from_config()
        self.analyzer._compile_keyword_patterns()

        # Create a test advertisement with proper mocking for title and description
        ad = MagicMock(spec=Advertisement)
        ad.source = "<html><body>Python and Java developer with SQL knowledge</body></html>"
        ad.get_title = MagicMock(return_value="Python and Java Developer")
        ad.get_description = MagicMock(return_value="Developer with SQL knowledge")

        # Match keywords using title only (default)
        matched_ids = self.analyzer.match_keywords_for_ad(ad)
        
        # Verify Python and Java keywords matched in title
        self.assertEqual(len(matched_ids), 2)
        
        # Now match with include_description=True which should also find SQL
        matched_ids = self.analyzer.match_keywords_for_ad(ad, include_description=True)
        
        # Verify all three keywords matched
        self.assertEqual(len(matched_ids), 3)

        # Try another advertisement with no matches in title but one in description
        ad.get_title = MagicMock(return_value="JavaScript Developer")
        ad.get_description = MagicMock(return_value="Knowledge of Python required")
        ad.source = "<html><body>JavaScript developer with Python knowledge</body></html>"

        # With title only, shouldn't match any keywords
        matched_ids = self.analyzer.match_keywords_for_ad(ad)
        self.assertEqual(len(matched_ids), 0)
        
        # With description included, should match Python
        matched_ids = self.analyzer.match_keywords_for_ad(ad, include_description=True)
        self.assertEqual(len(matched_ids), 1)

        # Test with a description that contains only SQL in lowercase
        ad.get_title = MagicMock(return_value="Database Developer")
        ad.get_description = MagicMock(return_value="sql developer")
        ad.source = "<html><body>sql developer</body></html>"

        # SQL is case-sensitive, so shouldn't match with lowercase
        matched_ids = self.analyzer.match_keywords_for_ad(ad, include_description=True)
        self.assertEqual(len(matched_ids), 0)

    def test_update_advertisement_keywords(self) -> None:
        """Test updating advertisement keywords associations."""
        # Insert some test keywords
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO keywords (title, search, case_sensitive) VALUES (?, ?, ?)",
            ("Python", "python", 0),
        )
        python_id = cursor.lastrowid

        cursor.execute(
            "INSERT INTO keywords (title, search, case_sensitive) VALUES (?, ?, ?)",
            ("Java", "java", 0),
        )
        java_id = cursor.lastrowid

        self.connection.commit()

        # Get the test advertisement ID
        cursor.execute("SELECT id FROM advertisements LIMIT 1")
        ad_id = cursor.fetchone()[0]

        # Test with initial keywords
        self.analyzer.update_advertisement_keywords(ad_id, [python_id])

        # Commit the changes explicitly to ensure they're visible in the next query
        self.connection.commit()

        cursor.execute(
            "SELECT keyword_id FROM keyword_advertisement WHERE advertisement_id = ?",
            (ad_id,),
        )
        keyword_ids = [row[0] for row in cursor.fetchall()]
        self.assertEqual(
            len(keyword_ids),
            1,
            f"Expected 1 keyword, found {len(keyword_ids)} for ad_id={ad_id}, python_id={python_id}",
        )
        self.assertIn(python_id, keyword_ids)

        # Update with a different set of keywords
        self.analyzer.update_advertisement_keywords(ad_id, [java_id])

        # Commit again
        self.connection.commit()

        cursor.execute(
            "SELECT keyword_id FROM keyword_advertisement WHERE advertisement_id = ?",
            (ad_id,),
        )
        keyword_ids = [row[0] for row in cursor.fetchall()]
        self.assertEqual(len(keyword_ids), 1)
        self.assertIn(java_id, keyword_ids)
        self.assertNotIn(python_id, keyword_ids)

        # Update with multiple keywords
        self.analyzer.update_advertisement_keywords(ad_id, [python_id, java_id])

        # Commit again
        self.connection.commit()

        cursor.execute(
            "SELECT keyword_id FROM keyword_advertisement WHERE advertisement_id = ?",
            (ad_id,),
        )
        keyword_ids = [row[0] for row in cursor.fetchall()]
        self.assertEqual(len(keyword_ids), 2)
        self.assertIn(python_id, keyword_ids)
        self.assertIn(java_id, keyword_ids)

        # Update with empty list should clear associations
        self.analyzer.update_advertisement_keywords(ad_id, [])

        # Commit again
        self.connection.commit()

        cursor.execute(
            "SELECT COUNT(*) FROM keyword_advertisement WHERE advertisement_id = ?",
            (ad_id,),
        )
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)

    @patch("advert.AdFactory.fetch_by_condition")
    def test_process_advertisements_with_id_range(self, mock_fetch: MagicMock) -> None:
        """Test processing advertisements with ID range filters."""
        # Setup the mock to return an empty list so the method executes fully
        mock_fetch.return_value = []

        # Make sure the analyzer has compiled keywords to avoid early returns
        self.analyzer.compiled_keywords = {1: re.compile("test")}

        # Process with min_id
        self.analyzer.process_advertisements(min_id=2)

        # Verify fetch_by_condition was called with the correct condition
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        self.assertEqual(call_kwargs["condition"], "id >= ?")
        self.assertEqual(call_kwargs["params"], [2])

        # Reset mock and test with max_id
        mock_fetch.reset_mock()
        self.analyzer.process_advertisements(max_id=5)

        # Verify fetch_by_condition was called with the correct condition
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        self.assertEqual(call_kwargs["condition"], "id <= ?")
        self.assertEqual(call_kwargs["params"], [5])

        # Reset mock and test with both min_id and max_id
        mock_fetch.reset_mock()
        self.analyzer.process_advertisements(min_id=2, max_id=5)

        # Verify fetch_by_condition was called with the correct condition
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        self.assertEqual(call_kwargs["condition"], "id >= ? AND id <= ?")
        self.assertEqual(call_kwargs["params"], [2, 5])

    @patch("analyzer.AdvertAnalyzer.process_advertisements")
    def test_run_analysis(self, mock_process: MagicMock) -> None:
        """Test the run_analysis method."""
        mock_process.return_value = 3

        # Test normal execution
        count = self.analyzer.run_analysis(batch_size=50)
        self.assertEqual(count, 3)

        # Verify process_advertisements was called with right parameters including include_description=False
        mock_process.assert_called_with(min_id=None, max_id=None, batch_size=50, include_description=False)

        # Test with custom parameters
        mock_process.reset_mock()
        count = self.analyzer.run_analysis(min_id=10, max_id=20, batch_size=200)

        mock_process.assert_called_with(min_id=10, max_id=20, batch_size=200, include_description=False)

        # Test with reset_tables=False
        mock_process.reset_mock()
        with patch("analyzer.AdvertAnalyzer.reset_keyword_tables") as mock_reset:
            with patch(
                "analyzer.AdvertAnalyzer.load_keywords_from_config"
            ) as mock_load:
                self.analyzer.run_analysis(reset_tables=False)

                mock_reset.assert_not_called()
                mock_load.assert_not_called()
                
        # Test with include_description=True
        mock_process.reset_mock()
        count = self.analyzer.run_analysis(include_description=True)
        
        mock_process.assert_called_with(min_id=None, max_id=None, batch_size=100, include_description=True)


if __name__ == "__main__":
    unittest.main()
