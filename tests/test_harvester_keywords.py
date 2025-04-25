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
        pattern1 = keyword_manager._compile_keyword(
            search="Python", case_sensitive=True
        )
        self.assertEqual(pattern1.pattern, "Python")
        self.assertFalse(pattern1.flags & re.IGNORECASE)

        # Test with case sensitive = False
        pattern2 = keyword_manager._compile_keyword(
            search="Python", case_sensitive=False
        )
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

    def test_title_only_matching(self) -> None:
        """Test that the title_only parameter properly controls which content is matched against."""
        # Create keyword manager for direct testing
        keyword_manager = KeywordManager(logger)

        # Test data setup
        html_with_keyword_in_title_only = """
        <html>
        <body>
        <h1>Senior Python Developer</h1>
        <div class="description">
        We are looking for a skilled developer with 3+ years of experience.
        The ideal candidate will have experience with web frameworks like Django or Flask.
        This position does not require javascript knowledge.
        </div>
        </body>
        </html>
        """

        html_with_keyword_in_description_only = """
        <html>
        <body>
        <h1>Software Developer Position</h1>
        <div class="description">
        We are looking for a skilled Python Developer with 3+ years of experience.
        The ideal candidate will have experience with web frameworks like Django or Flask.
        </div>
        </body>
        </html>
        """

        # Define test keyword that appears in different places in our test data
        python_keyword = {
            "title": "Python Developer",
            "search": r"python\s+developer",
            "case_sensitive": False,
        }

        # Insert keyword into the database
        self.connection.execute("DELETE FROM keywords")  # Clear existing keywords
        Harvester.insert_keyword(self.connection, python_keyword)

        # Fetch compiled regex
        regexes = Harvester.fetch_keywords(self.connection)
        self.assertEqual(len(regexes), 1, "Should have exactly one keyword")

        # Create test advertisements
        title_ad = Advertisement(source=html_with_keyword_in_title_only)
        title_ad.get_title = lambda: "Senior Python Developer"
        title_ad.get_description = (
            lambda: "We are looking for a skilled developer with experience."
        )

        description_ad = Advertisement(source=html_with_keyword_in_description_only)
        description_ad.get_title = lambda: "Software Developer Position"
        description_ad.get_description = (
            lambda: "We are looking for a skilled Python Developer with experience."
        )

        # Test 1: Using title_only=True
        # Should match when the keyword is in the title, but not when it's only in description
        title_only_matches_title = keyword_manager.match_keywords(
            title_ad, regexes, title_only=True
        )
        self.assertEqual(
            len(title_only_matches_title),
            1,
            "Should match keyword in title when title_only=True",
        )

        title_only_matches_description = keyword_manager.match_keywords(
            description_ad, regexes, title_only=True
        )
        self.assertEqual(
            len(title_only_matches_description),
            0,
            "Should not match keyword in description when title_only=True",
        )

        # Test 2: Using title_only=False
        # Should match when the keyword is in the title or in the description
        full_matches_title = keyword_manager.match_keywords(
            title_ad, regexes, title_only=False
        )
        self.assertEqual(
            len(full_matches_title),
            1,
            "Should match keyword in title when title_only=False",
        )

        full_matches_description = keyword_manager.match_keywords(
            description_ad, regexes, title_only=False
        )
        self.assertEqual(
            len(full_matches_description),
            1,
            "Should match keyword in description when title_only=False",
        )

        # Test 3: Default behavior of Harvester.match_keywords
        # Should use title_only=False by default (matching both title and description)
        harvester_matches_title = self.harvester.match_keywords(title_ad, regexes)
        self.assertEqual(
            len(harvester_matches_title), 1, "Harvester should match keyword in title"
        )

        harvester_matches_description = self.harvester.match_keywords(
            description_ad, regexes
        )
        self.assertEqual(
            len(harvester_matches_description),
            1,
            "Harvester should match keyword in description",
        )

    def test_title_only_with_missing_fields(self) -> None:
        """Test title_only parameter with advertisements that have missing title or description."""
        # Create keyword manager for direct testing
        keyword_manager = KeywordManager(logger)

        # Create test keyword
        python_keyword = {
            "title": "Python Developer",
            "search": r"python\s+developer",
            "case_sensitive": False,
        }

        # Insert keyword into the database
        self.connection.execute("DELETE FROM keywords")  # Clear existing keywords
        Harvester.insert_keyword(self.connection, python_keyword)

        # Fetch compiled regex
        regexes = Harvester.fetch_keywords(self.connection)

        # Test HTML content
        html_with_python = """
        <html><body>
        <div>This is about a Python Developer position</div>
        </body></html>
        """

        # Create test advertisements with missing fields
        class NoTitleAd(Advertisement):
            def get_title(self):
                return None

            def get_description(self):
                return "This is about a Python Developer position"

        class NoDescriptionAd(Advertisement):
            def get_title(self):
                return "Python Developer Position"

            def get_description(self):
                return None

        class NoFieldsAd(Advertisement):
            def get_title(self):
                return None

            def get_description(self):
                return None

        # Create instances
        no_title_ad = NoTitleAd(source=html_with_python)
        no_description_ad = NoDescriptionAd(source=html_with_python)
        no_fields_ad = NoFieldsAd(source=html_with_python)

        # Test with title_only=True
        # Should not match when title is missing, even if description matches
        matches_no_title = keyword_manager.match_keywords(
            no_title_ad, regexes, title_only=True
        )
        self.assertEqual(
            len(matches_no_title),
            0,
            "Should not match when title is None and title_only=True",
        )

        # Should match when title is present and matches
        matches_no_desc = keyword_manager.match_keywords(
            no_description_ad, regexes, title_only=True
        )
        self.assertEqual(
            len(matches_no_desc),
            1,
            "Should match on title when description is None and title_only=True",
        )

        # Test with title_only=False
        # Should match on description when title is missing
        matches_no_title_full = keyword_manager.match_keywords(
            no_title_ad, regexes, title_only=False
        )
        self.assertEqual(
            len(matches_no_title_full),
            1,
            "Should match on description when title is None and title_only=False",
        )

        # Should match on title when description is missing
        matches_no_desc_full = keyword_manager.match_keywords(
            no_description_ad, regexes, title_only=False
        )
        self.assertEqual(
            len(matches_no_desc_full),
            1,
            "Should match on title when description is None and title_only=False",
        )

        # Test with all fields missing - should fall back to raw HTML source
        matches_no_fields_strict = keyword_manager.match_keywords(
            no_fields_ad, regexes, title_only=True
        )
        self.assertEqual(
            len(matches_no_fields_strict),
            0,
            "Should not match raw source when title_only=True and both fields are None",
        )

        matches_no_fields_full = keyword_manager.match_keywords(
            no_fields_ad, regexes, title_only=False
        )
        self.assertEqual(
            len(matches_no_fields_full),
            1,
            "Should match on raw source when both fields are None and title_only=False",
        )


if __name__ == "__main__":
    unittest.main()
