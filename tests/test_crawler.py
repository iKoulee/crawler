import unittest
import os
import sys
import logging
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock, mock_open
from typing import Dict, Any, List

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
            with patch("yaml.safe_load", return_value={"keywords": []}):
                with patch("sqlite3.connect"):
                    with patch("crawler.Harvester"):
                        with patch("crawler.HarvesterFactory") as mock_factory:
                            # Setup mock harvester factory to return our mock harvesters
                            mock_factory.return_value.get_next_harvester.return_value = [
                                mock_harvester1,
                                mock_harvester2,
                            ]

                            # Run the main function
                            crawler.main()

        # Verify threads were created and started
        self.assertEqual(mock_thread.call_count, 2)
        self.assertEqual(mock_thread_instance.start.call_count, 2)
        self.assertEqual(mock_thread_instance.join.call_count, 2)


if __name__ == "__main__":
    unittest.main()
