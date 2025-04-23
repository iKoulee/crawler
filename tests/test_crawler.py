import unittest
import os
import sys
import logging
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock, mock_open
from typing import Dict, Any, List

# Configure logging at DEBUG level
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Add src directory to path for importing crawler module
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import crawler
from harvester import Harvester, HarvesterFactory


class TestSetupLogging(unittest.TestCase):
    """Test cases for the setup_logging function."""

    @patch("logging.basicConfig")
    def test_valid_log_level(self, mock_basic_config: MagicMock) -> None:
        """Test setup_logging with valid log level."""
        # Test with valid log levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            crawler.setup_logging(level)
            mock_basic_config.assert_called()
            # Reset mock for next iteration
            mock_basic_config.reset_mock()

    def test_invalid_log_level(self) -> None:
        """Test setup_logging with invalid log level."""
        with self.assertRaises(ValueError):
            crawler.setup_logging("INVALID_LEVEL")


class TestMainFunction(unittest.TestCase):
    """Test cases for the main function."""

    @patch("argparse.ArgumentParser.parse_args")
    @patch("crawler.setup_logging")
    @patch("logging.getLogger")
    def test_argument_parsing(
        self,
        mock_get_logger: MagicMock,
        mock_setup_logging: MagicMock,
        mock_parse_args: MagicMock,
    ) -> None:
        """Test command line argument parsing."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.command = "harvest"
        mock_args.config = "config.yml"
        mock_args.database = "test.db"
        mock_args.loglevel = "INFO"
        mock_parse_args.return_value = mock_args

        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock the rest of the function to isolate argument parsing test
        with patch("builtins.open", mock_open(read_data="keywords: []")):
            with patch("yaml.safe_load", return_value={"keywords": []}):
                with patch("sqlite3.connect"):
                    with patch("crawler.Harvester"):
                        with patch("crawler.HarvesterFactory"):
                            # Call the main function
                            crawler.main()

        # Verify setup_logging was called with the right log level
        mock_setup_logging.assert_called_once_with(mock_args.loglevel)

    @patch("argparse.ArgumentParser.parse_args")
    @patch("crawler.setup_logging")
    @patch("logging.getLogger")
    def test_file_not_found_error(
        self,
        mock_get_logger: MagicMock,
        mock_setup_logging: MagicMock,
        mock_parse_args: MagicMock,
    ) -> None:
        """Test handling of FileNotFoundError."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.command = "harvest"  # Add the command attribute
        mock_args.config = "nonexistent.yml"
        mock_args.database = "test.db"
        mock_args.loglevel = "INFO"
        mock_parse_args.return_value = mock_args

        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock open to raise FileNotFoundError
        with patch("builtins.open", side_effect=FileNotFoundError()):
            crawler.main()

        # Verify error was logged
        mock_logger.error.assert_called_once_with(
            "Config file '%s' not found.", mock_args.config
        )


class TestThreadManagement(unittest.TestCase):
    """Test cases for thread management."""

    @patch("argparse.ArgumentParser.parse_args")
    @patch("crawler.setup_logging")
    @patch("logging.getLogger")
    @patch("threading.Thread")
    def test_thread_creation_and_execution(
        self,
        mock_thread: MagicMock,
        mock_get_logger: MagicMock,
        mock_setup_logging: MagicMock,
        mock_parse_args: MagicMock,
    ) -> None:
        """Test creation and management of threads."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.command = "harvest"
        mock_args.config = "config.yml"
        mock_args.database = "test.db"
        mock_args.loglevel = "INFO"
        mock_parse_args.return_value = mock_args

        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create mock harvesters
        mock_harvester1 = MagicMock()
        mock_harvester1.__class__.__name__ = "MockHarvester1"
        mock_harvester2 = MagicMock()
        mock_harvester2.__class__.__name__ = "MockHarvester2"

        # Mock thread instances
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        # Mock configuration and connections
        with patch("builtins.open", mock_open()):
            with patch(
                "yaml.safe_load",
                return_value={
                    "keywords": [],
                    "portals": [
                        {"engine": "MockHarvester1"},
                        {"engine": "MockHarvester2"},
                    ],
                },
            ):
                with patch("sqlite3.connect"):
                    with patch.object(crawler, "HarvesterFactory") as mock_factory:
                        # Setup mock harvester factory to return our mock harvesters
                        mock_factory_instance = MagicMock()
                        mock_factory.return_value = mock_factory_instance
                        mock_factory_instance.get_next_harvester.return_value = [
                            mock_harvester1,
                            mock_harvester2,
                        ]

                        # Run the main function
                        crawler.main()

        # Verify threads were created and started
        self.assertEqual(
            mock_thread.call_count,
            2,
            "Thread constructor should be called twice (once for each harvester)",
        )
        self.assertEqual(
            mock_thread_instance.start.call_count,
            2,
            "Thread.start() should be called twice",
        )
        self.assertEqual(
            mock_thread_instance.join.call_count,
            2,
            "Thread.join() should be called twice",
        )


class TestDatabaseOperations(unittest.TestCase):
    """Test cases for database operations."""

    def setUp(self) -> None:
        """Set up test environment with a temporary database."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()

        # Create connection and schema
        self.connection = sqlite3.connect(self.db_path)
        Harvester.create_schema(self.connection)

    def tearDown(self) -> None:
        """Clean up test environment."""
        self.connection.close()
        os.unlink(self.db_path)

    def test_database_schema(self) -> None:
        """Test that the database schema is created correctly."""
        cursor = self.connection.cursor()

        # Check advertisements table
        cursor.execute("PRAGMA table_info(advertisements)")
        columns = {row[1]: row for row in cursor.fetchall()}

        # Verify required columns exist
        self.assertIn("id", columns)
        self.assertIn("title", columns)
        self.assertIn("url", columns)
        self.assertIn("http_status", columns)  # Updated from html_status
        self.assertIn("filename", columns)  # New column

        # Verify harvest_date column no longer exists
        self.assertNotIn("harvest_date", columns)

        # Check keyword_advertisement table
        cursor.execute("PRAGMA foreign_key_list(keyword_advertisement)")
        foreign_keys = cursor.fetchall()

        # Verify foreign keys are set up correctly
        self.assertTrue(any(fk[2] == "advertisements" for fk in foreign_keys))
        self.assertTrue(any(fk[2] == "keywords" for fk in foreign_keys))

    def test_export_to_csv(self) -> None:
        """Test exporting advertisements to CSV."""
        cursor = self.connection.cursor()

        # Insert test data
        cursor.execute(
            """
            INSERT INTO advertisements 
            (title, company, location, url, html_body, http_status, ad_type, filename) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Test Job",
                "Test Company",
                "Test Location",
                "http://example.com",
                "<html><body><h1>Test Job</h1><p>Test Company</p></body></html>",
                200,
                "KarriereAdvertisement",
                "test_file.html",
            ),
        )
        ad_id = cursor.lastrowid

        # Insert test keyword
        cursor.execute(
            "INSERT INTO keywords (title, search, case_sensitive) VALUES (?, ?, ?)",
            ("Python", "python", 0),
        )
        keyword_id = cursor.lastrowid

        # Link keyword to advertisement
        cursor.execute(
            "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
            (keyword_id, ad_id),
        )

        self.connection.commit()

        # Create a mock for AdFactory.create to return our test data
        with patch("advert.AdFactory.create") as mock_factory_create:
            # Create a mock advertisement that returns our test values
            mock_ad = MagicMock()
            mock_ad.get_title.return_value = "Test Job"
            mock_ad.get_company.return_value = "Test Company"
            mock_ad.get_location.return_value = "Test Location"
            mock_factory_create.return_value = mock_ad

            # Test CSV export
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_csv:
                csv_path = temp_csv.name

            try:
                record_count = Harvester.export_to_csv(self.connection, csv_path)
                self.assertEqual(record_count, 1)

                # Check CSV content
                with open(csv_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    self.assertIn("Test Job", content)
                    self.assertIn("Test Company", content)
                    self.assertIn("Test Location", content)
                    self.assertIn("test_file.html", content)  # New filename field
                    self.assertIn("Python", content)  # Check related keywords
            finally:
                os.unlink(csv_path)


if __name__ == "__main__":
    unittest.main()
