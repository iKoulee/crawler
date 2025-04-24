import logging
import sqlite3
import tempfile
import unittest
import os
import sys
import re
from typing import Dict, List, Any, Pattern

# Add the src directory to the path so we can import from there
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)
from harvester import Harvester
from keyword_manager import KeywordManager
from advert import Advertisement, StepstoneAdvertisement

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# Sample test data
SAMPLE_KEYWORDS = [
    {
        "title": "Python Developer",
        "search": r"python\s+developer",
        "case_sensitive": False,
    },
    {"title": "Senior Position", "search": r"senior", "case_sensitive": False},
    {
        "title": "Data Scientist",
        "search": r"data\s+scien(ce|tist)",
        "case_sensitive": False,
    },
    {
        "title": "JavaScript",
        "search": r"JavaScript",
        "case_sensitive": True,
    },  # Case sensitive
    {"title": "Management", "search": r"manag(er|ement)", "case_sensitive": False},
]

# Sample HTML content for testing
SAMPLE_HTML = """
<html>
<body>
<h1>Python Developer Position</h1>
<div class="company">TechCorp Inc.</div>
<div class="location">Vienna, Austria</div>
<div class="description">
We are looking for a skilled Python Developer with 3+ years of experience.
The ideal candidate will have experience with web frameworks like Django or Flask,
and knowledge of JavaScript, HTML, and CSS.
</div>
</body>
</html>
"""

MANAGEMENT_HTML = """
<html>
<body>
<h1>Senior Management Position</h1>
<div class="company">Management Consulting Group</div>
<div class="location">Graz, Austria</div>
<div class="description">
We are hiring a senior manager to lead our consulting team.
Responsibilities include team management, client relations, and project oversight.
</div>
</body>
</html>
"""


class TestKeywordFunctionality(unittest.TestCase):
    """Characterization tests for the keyword functionality in the Harvester class."""

    def setUp(self) -> None:
        """Set up a temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()

        # Create connection and schema
        self.connection = sqlite3.connect(self.db_path)
        Harvester.create_schema(self.connection)

        # Create a basic harvester instance for testing
        self.harvester = Harvester({"url": "https://example.com"})

    def tearDown(self) -> None:
        """Clean up the temporary database."""
        self.connection.close()
        os.unlink(self.db_path)

    def test_insert_and_fetch_keywords(self) -> None:
        """Characterize the behavior of inserting and fetching keywords."""
        # Insert test keywords
        for keyword in SAMPLE_KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)

        # Fetch the keywords
        regexes = Harvester.fetch_keywords(self.connection)

        # Verify the number of keywords
        self.assertEqual(len(regexes), len(SAMPLE_KEYWORDS))

        # Verify that each keyword was properly compiled with the right case sensitivity
        for i, keyword in enumerate(SAMPLE_KEYWORDS, 1):
            # Check the regex exists in the results
            self.assertIn(i, regexes)

            # Create the expected pattern for comparison
            expected_pattern = keyword["search"]
            actual_pattern = regexes[i].pattern

            # Check the pattern matches
            self.assertEqual(actual_pattern, expected_pattern)

            # Check the case sensitivity flag
            if not keyword["case_sensitive"]:
                self.assertTrue(regexes[i].flags & re.IGNORECASE)
            else:
                self.assertFalse(regexes[i].flags & re.IGNORECASE)

    def test_compile_keyword(self) -> None:
        """Characterize the _compile_keyword method behavior."""
        # Create a KeywordManager instance
        keyword_manager = KeywordManager()
        
        # Test with case sensitive = True
        pattern1 = keyword_manager._compile_keyword(search="Python", case_sensitive=True)
        self.assertEqual(pattern1.pattern, "Python")
        self.assertFalse(pattern1.flags & re.IGNORECASE)

        # Test with case sensitive = False
        pattern2 = keyword_manager._compile_keyword(search="Python", case_sensitive=False)
        self.assertEqual(pattern2.pattern, "Python")
        self.assertTrue(pattern2.flags & re.IGNORECASE)

        # Test with a more complex regex
        pattern3 = keyword_manager._compile_keyword(
            search=r"data\s+scien(ce|tist)", case_sensitive=False
        )
        self.assertEqual(pattern3.pattern, r"data\s+scien(ce|tist)")
        self.assertTrue(pattern3.flags & re.IGNORECASE)

    def test_match_keywords(self) -> None:
        """Characterize the match_keywords method behavior."""
        # Insert test keywords
        for keyword in SAMPLE_KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)

        # Fetch the keywords
        regexes = Harvester.fetch_keywords(self.connection)

        # Create a test advertisement that should match "Python Developer" and "JavaScript"
        python_ad = StepstoneAdvertisement(source=SAMPLE_HTML)

        # Match the keywords
        matched_keywords = self.harvester.match_keywords(python_ad, regexes)

        # Verify that the correct keywords matched
        # 1 = Python Developer, 4 = JavaScript
        expected_matches = [1, 4]
        self.assertEqual(sorted(matched_keywords), sorted(expected_matches))

        # Test with a different advertisement that should match "Senior Position" and "Management"
        management_ad = StepstoneAdvertisement(source=MANAGEMENT_HTML)

        # Match the keywords
        matched_keywords = self.harvester.match_keywords(management_ad, regexes)

        # Verify that the correct keywords matched
        # 2 = Senior Position, 5 = Management
        expected_matches = [2, 5]
        self.assertEqual(sorted(matched_keywords), sorted(expected_matches))

    def test_keyword_case_sensitivity(self) -> None:
        """Characterize the case sensitivity behavior of keyword matching."""
        # Define case sensitivity test keywords
        case_test_keywords = [
            {"title": "JavaScript", "search": r"JavaScript", "case_sensitive": True},
            {"title": "javascript", "search": r"javascript", "case_sensitive": False},
        ]

        # Insert test keywords
        for keyword in case_test_keywords:
            Harvester.insert_keyword(self.connection, keyword)

        # Fetch the keywords
        regexes = Harvester.fetch_keywords(self.connection)

        # Test HTML with mixed case JavaScript
        js_mixed_case_html = """
        <html>
        <body>
        <h1>JavaScript Developer</h1>
        <div class="description">Experience with JavaScript is required.</div>
        </body>
        </html>
        """

        # Test HTML with lowercase javascript
        js_lower_case_html = """
        <html>
        <body>
        <h1>javascript Developer</h1>
        <div class="description">Experience with javascript is required.</div>
        </body>
        </html>
        """

        # Create test advertisements
        mixed_case_ad = StepstoneAdvertisement(source=js_mixed_case_html)
        lower_case_ad = StepstoneAdvertisement(source=js_lower_case_html)

        # Match keywords for mixed case ad
        mixed_case_matches = self.harvester.match_keywords(mixed_case_ad, regexes)

        # Should match both keywords (id 1 and 2)
        # ID 1: JavaScript (case sensitive) should match "JavaScript"
        # ID 2: javascript (case insensitive) should match "JavaScript"
        self.assertEqual(sorted(mixed_case_matches), [1, 2])

        # Match keywords for lowercase ad
        lower_case_matches = self.harvester.match_keywords(lower_case_ad, regexes)

        # Should only match keyword (id 2)
        # ID 1: JavaScript (case sensitive) should NOT match "javascript"
        # ID 2: javascript (case insensitive) should match "javascript"
        self.assertEqual(sorted(lower_case_matches), [2])

    def test_empty_and_edge_cases(self) -> None:
        """Characterize behavior with empty or edge case inputs."""
        # Test with empty database
        empty_regexes = Harvester.fetch_keywords(self.connection)
        self.assertEqual(empty_regexes, {})

        # Insert keywords then test with empty ad
        for keyword in SAMPLE_KEYWORDS:
            Harvester.insert_keyword(self.connection, keyword)

        regexes = Harvester.fetch_keywords(self.connection)

        empty_ad = StepstoneAdvertisement(source="<html><body></body></html>")
        empty_matches = self.harvester.match_keywords(empty_ad, regexes)
        self.assertEqual(empty_matches, [])

        # Test with None description
        class NoneDescriptionAd(Advertisement):
            def get_description(self):
                return None

        none_ad = NoneDescriptionAd(source="<html><body><h1>Title</h1></body></html>")
        none_matches = self.harvester.match_keywords(none_ad, regexes)

        # Should still try to match against the source since description is None
        self.assertGreaterEqual(len(none_matches), 0)

        # Test with weird characters
        weird_html = """
        <html>
        <body>
        <h1>Special Ch@r@cters: !!!</h1>
        <div class="description">
        Test with special characters: áéíóú ñ €$£¥ ©®™
        </div>
        </body>
        </html>
        """

        # Add a keyword with special characters
        special_keyword = {
            "title": "Special",
            "search": r"special\s+characters",
            "case_sensitive": False,
        }
        Harvester.insert_keyword(self.connection, special_keyword)

        # Re-fetch keywords
        updated_regexes = Harvester.fetch_keywords(self.connection)

        weird_ad = StepstoneAdvertisement(source=weird_html)
        weird_matches = self.harvester.match_keywords(weird_ad, updated_regexes)

        # Should match the special characters keyword
        self.assertIn(len(SAMPLE_KEYWORDS) + 1, weird_matches)

    def test_regex_pattern_behavior(self) -> None:
        """Characterize how different regex patterns behave in keyword matching."""
        # Define test patterns
        patterns = [
            {
                "title": "Word Boundary",
                "search": r"\bpython\b",
                "case_sensitive": False,
            },
            {"title": "No Boundary", "search": r"python", "case_sensitive": False},
            {
                "title": "Optional Space",
                "search": r"data\s*science",
                "case_sensitive": False,
            },
            {
                "title": "Capturing Group",
                "search": r"(front|back)end",
                "case_sensitive": False,
            },
            {
                "title": "Complex Pattern",
                "search": r"[0-9]+ years? experience",
                "case_sensitive": False,
            },
        ]

        # Insert patterns
        for pattern in patterns:
            Harvester.insert_keyword(self.connection, pattern)

        # Fetch patterns
        regexes = Harvester.fetch_keywords(self.connection)

        # Test HTML with various patterns
        pattern_test_html = """
        <html>
        <body>
        <h1>Python Developer Position</h1>
        <div class="description">
        We are looking for a python developer with at least 5 years experience.
        Skills needed: python programming, datascience, and frontend/backend development.
        Subpython is not a word. Python3 is the version we use.
        </div>
        </body>
        </html>
        """

        pattern_ad = StepstoneAdvertisement(source=pattern_test_html)
        matched_patterns = self.harvester.match_keywords(pattern_ad, regexes)

        # Expected matches:
        # - "Word Boundary" should match "python" (when standalone)
        # - "No Boundary" should match all occurrences of "python"
        # - "Optional Space" should match "datascience"
        # - "Capturing Group" should match "frontend" and "backend"
        # - "Complex Pattern" should match "5 years experience"

        # All patterns should match something in our test HTML
        self.assertEqual(len(matched_patterns), len(patterns))
        self.assertEqual(set(matched_patterns), set(range(1, len(patterns) + 1)))


if __name__ == "__main__":
    unittest.main()
