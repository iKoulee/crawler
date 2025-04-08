import unittest
import os
import sys
import tempfile
import sqlite3
import yaml
import logging
from pathlib import Path

# Configure logging at DEBUG level
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Add src directory to path for importing modules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from harvester import Harvester


class TestHtmlExport(unittest.TestCase):
    """Test cases for the HTML export functionality."""

    def setUp(self) -> None:
        """Set up test environment with a temporary database."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.temp_db.name
        self.temp_db.close()

        # Create a temporary directory for output
        self.temp_output_dir = tempfile.mkdtemp()

        # Create a temporary config file
        self.temp_config = tempfile.NamedTemporaryFile(delete=False, suffix=".yml")
        self.config_path = self.temp_config.name
        self.temp_config.close()  # Explicitly close the file

        # Write test configuration to the file
        test_config = {
            "filters": {
                "education_level": {
                    "higher_education": {
                        "pattern": "university|college|bachelor|master|phd|degree",
                        "case_sensitive": False,
                    },
                    "vocational": {
                        "pattern": "vocational|apprentice|trainee|ausbildung",
                        "case_sensitive": False,
                    },
                    "other_education": {"pattern": ".*", "catch_all": True},
                },
                "job_type": {
                    "full_time": {
                        "pattern": "full[ -]time|vollzeit|permanent",
                        "case_sensitive": False,
                    },
                    "part_time": {
                        "pattern": "part[ -]time|teilzeit",
                        "case_sensitive": False,
                    },
                    "other_job_type": {"pattern": ".*", "catch_all": True},
                },
            }
        }

        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(test_config, f)

        # Create connection and schema
        self.connection = sqlite3.connect(self.db_path)
        Harvester.create_schema(self.connection)

        # Insert test data
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO keywords (title, search, case_sensitive)
            VALUES (?, ?, ?)
            """,
            ("Job title", r"pattern", False),
        )
        keyword_id = cursor.lastrowid

        # Higher education + Full-time advertisement
        cursor.execute(
            """
            INSERT INTO advertisements 
            (title, html_body, http_status, url, ad_type) 
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "University Professor",
                "<html><body><h1>University Professor</h1><p>We seek a full-time professor.</p></body></html>",
                200,
                "https://karriere.at/jobs/1",
                "KarriereAdvertisement",
            ),
        )

        last_id = cursor.lastrowid
        cursor.execute(
            """
            INSERT INTO keyword_advertisement (keyword_id, advertisement_id) 
            VALUES (?, ?)
            """,
            (keyword_id, last_id),
        )

        # Vocational + Part-time advertisement
        cursor.execute(
            """
            INSERT INTO advertisements 
            (title, html_body, http_status, url, ad_type) 
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Apprentice Developer",
                "<html><body><h1>Apprentice Developer</h1><p>Part-time position for vocational training.</p></body></html>",
                200,
                "https://stepstone.at/jobs/2",
                "StepstoneAdvertisement",
            ),
        )

        last_id = cursor.lastrowid
        cursor.execute(
            """
            INSERT INTO keyword_advertisement (keyword_id, advertisement_id) 
            VALUES (?, ?)
            """,
            (keyword_id, last_id),
        )

        # Other education + Other job type advertisement
        cursor.execute(
            """
            INSERT INTO advertisements 
            (title, html_body, http_status, url, ad_type) 
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Cook",
                "<html><body><h1>Cook</h1><p>Experienced cook needed.</p></body></html>",
                200,
                "https://indeed.com/jobs/3",
                "IndeedAdvertisement",
            ),
        )

        last_id = cursor.lastrowid
        cursor.execute(
            """
            INSERT INTO keyword_advertisement (keyword_id, advertisement_id) 
            VALUES (?, ?)
            """,
            (keyword_id, last_id),
        )

        self.connection.commit()

    def tearDown(self) -> None:
        """Clean up test environment."""
        self.connection.close()
        os.unlink(self.db_path)
        os.unlink(self.config_path)

        # Remove all files in temp output directory
        for root, dirs, files in os.walk(self.temp_output_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_output_dir)

    def test_export_html_bodies(self) -> None:
        """Test export_html_bodies with multi-category filters."""
        total_exported, category_counts = Harvester.export_html_bodies(
            self.connection, self.temp_output_dir, self.config_path
        )

        # Check that all 3 advertisements were exported
        self.assertEqual(total_exported, 3)

        # Check that category counts are correct
        self.assertEqual(category_counts["education_level"], 3)
        self.assertEqual(category_counts["job_type"], 3)

        # Check that files were created in the right directories
        university_file_path = os.path.join(
            self.temp_output_dir, "higher_education", "full_time", "karriere_00001.html"
        )
        apprentice_file_path = os.path.join(
            self.temp_output_dir, "vocational", "part_time", "stepstone_00002.html"
        )
        other_file_path = os.path.join(
            self.temp_output_dir,
            "other_education",
            "other_job_type",
            "indeed_00003.html",
        )

        self.assertTrue(os.path.exists(university_file_path))
        self.assertTrue(os.path.exists(apprentice_file_path))
        self.assertTrue(os.path.exists(other_file_path))

        # Check file content
        with open(university_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("University Professor", content)
            self.assertIn("full-time professor", content)

        with open(apprentice_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("Apprentice Developer", content)
            self.assertIn("Part-time", content)
            self.assertIn("vocational training", content)

        with open(other_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("Cook", content)

        # Check database was updated with filenames
        cursor = self.connection.cursor()

        cursor.execute("SELECT filename FROM advertisements WHERE id = 1")
        result = cursor.fetchone()
        self.assertEqual(
            result[0],
            os.path.join("higher_education", "full_time", "karriere_00001.html"),
        )

        cursor.execute("SELECT filename FROM advertisements WHERE id = 2")
        result = cursor.fetchone()
        self.assertEqual(
            result[0], os.path.join("vocational", "part_time", "stepstone_00002.html")
        )

        cursor.execute("SELECT filename FROM advertisements WHERE id = 3")
        result = cursor.fetchone()
        self.assertEqual(
            result[0],
            os.path.join("other_education", "other_job_type", "indeed_00003.html"),
        )


if __name__ == "__main__":
    unittest.main()
