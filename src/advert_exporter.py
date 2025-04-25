from typing import Dict, List, Tuple, Optional, Any, Pattern
import logging
import sqlite3
import csv
import io
import re
import yaml
import os
from urllib.parse import urlparse
from pathlib import Path
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

from advert import AdFactory


class AdvertExporter:
    """
    A class for exporting advertisement data from the database to various formats.

    This class provides methods to export job advertisement data to different formats
    and structures for analysis and reporting purposes.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize an AdvertExporter instance.

        Args:
            logger: Optional logger instance. If None, a new logger will be created.
        """
        self.logger = logger or logging.getLogger(__name__)

    def fetch_advertisements_by_id_range(
        self,
        connection: sqlite3.Connection,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch advertisements from the database within a specific ID range.

        Args:
            connection: SQLite database connection
            min_id: Minimum advertisement ID to fetch (inclusive)
            max_id: Maximum advertisement ID to fetch (inclusive)

        Returns:
            List of dictionaries containing advertisement data with related keywords
        """
        cursor = connection.cursor()

        # Build the query with optional ID filters
        query = """
            SELECT a.id, a.title, a.company, a.location, a.ad_type, a.html_body, 
                   a.url, a.created_at, a.filename
            FROM advertisements a
            WHERE EXISTS (
                SELECT 1 FROM keyword_advertisement ka
                WHERE ka.advertisement_id = a.id
            )
        """

        params = []
        if min_id is not None:
            query += " AND a.id >= ?"
            params.append(min_id)
        if max_id is not None:
            query += " AND a.id <= ?"
            params.append(max_id)

        query += " ORDER BY a.id ASC"

        # Execute the query
        cursor.execute(query, params)
        dataset = cursor.fetchall()

        self.logger.info(
            "Fetched %d advertisements from database (min_id=%s, max_id=%s)",
            len(dataset),
            min_id if min_id is not None else "None",
            max_id if max_id is not None else "None",
        )

        # Fetch related keywords for each advertisement
        result = []
        for data in dataset:
            ad_id = data[0]
            title = data[1] or ""
            company = data[2] or ""
            location = data[3] or ""
            ad_type = data[4]
            html_body = data[5]
            url = data[6] or ""
            created_at = data[7] or ""
            filename = data[8] or ""

            # Create Advertisement instance to use get_* methods only if fields are missing
            if not title or not company or not location:
                ad = AdFactory.create(ad_type, html_body, url)
                title = title or ad.get_title() or ""
                company = company or ad.get_company() or ""
                location = location or ad.get_location() or ""
                self.logger.debug(
                    "Extracted missing fields for ad %d: title=%s, company=%s, location=%s",
                    ad_id,
                    title[:20] + "..." if len(title) > 20 else title,
                    company[:20] + "..." if len(company) > 20 else company,
                    location[:20] + "..." if len(location) > 20 else location,
                )

            # Get related keywords
            cursor.execute(
                """
                SELECT k.title
                FROM keywords k
                JOIN keyword_advertisement ka ON k.id = ka.keyword_id
                WHERE ka.advertisement_id = ?
                """,
                (ad_id,),
            )

            keyword_titles = [row[0] for row in cursor.fetchall() if row[0]]
            self.logger.debug(
                "Advertisement ID %d has %d related keywords",
                ad_id,
                len(keyword_titles),
            )

            # Build advertisement data with keywords
            ad_data = {
                "id": ad_id,
                "title": title,
                "company": company,
                "location": location,
                "date": created_at,
                "url": url,
                "portal": urlparse(url).netloc,
                "keywords": keyword_titles,
                "filename": filename,
            }

            result.append(ad_data)

        return result

    def export_to_csv(
        self,
        connection: sqlite3.Connection,
        output_file: str,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> int:
        """
        Export advertisements to CSV file.

        Args:
            connection: SQLite database connection
            output_file: Path to the output CSV file
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)

        Returns:
            Number of exported advertisements
        """
        # Fetch advertisements from database
        advertisements = self.fetch_advertisements_by_id_range(
            connection, min_id, max_id
        )

        # Define CSV headers
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        self.logger.info(
            "Exporting %d advertisements to CSV file: %s",
            len(advertisements),
            output_file,
        )

        # Write to CSV
        try:
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for ad in advertisements:
                    writer.writerow(
                        {
                            "job_title": ad["title"],
                            "company_name": ad["company"],
                            "location": ad["location"],
                            "harvest_date": ad["date"],
                            "url": ad["url"],
                            "portal": ad["portal"],
                            "related_keywords": "; ".join(ad["keywords"]),
                            "filename": ad["filename"] or "",
                        }
                    )
                self.logger.info("CSV export completed successfully")
        except IOError as e:
            self.logger.error("Failed to write CSV file: %s", e)
            raise

        return len(advertisements)

    def export_to_csv_string(
        self,
        connection: sqlite3.Connection,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> str:
        """
        Export advertisements to CSV string.

        Args:
            connection: SQLite database connection
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)

        Returns:
            CSV formatted string containing the exported data
        """
        # Fetch advertisements from database
        advertisements = self.fetch_advertisements_by_id_range(
            connection, min_id, max_id
        )

        self.logger.info(
            "Generating CSV string for %d advertisements", len(advertisements)
        )

        # Define CSV headers
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        # Write to CSV string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for ad in advertisements:
            writer.writerow(
                {
                    "job_title": ad["title"],
                    "company_name": ad["company"],
                    "location": ad["location"],
                    "harvest_date": ad["date"],
                    "url": ad["url"],
                    "portal": ad["portal"],
                    "related_keywords": "; ".join(ad["keywords"]),
                    "filename": ad["filename"] or "",
                }
            )

        self.logger.debug(
            "CSV string generation completed with %d rows", len(advertisements)
        )
        return output.getvalue()

    def export_html_bodies(
        self,
        connection: sqlite3.Connection,
        output_dir: str,
        config_path: str,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
        create_csv_files: bool = False,
    ) -> Tuple[int, Dict[str, int]]:
        """
        Export advertisement HTML bodies to files with nested directory structure.

        This method extracts HTML content from advertisements in the database and
        saves each one to a file within a nested directory structure based on
        filter matches defined in the configuration.

        Args:
            connection: SQLite database connection
            output_dir: Base directory for exported HTML files
            config_path: Path to the configuration file with filter definitions
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)
            create_csv_files: Whether to create CSV files in each directory

        Returns:
            Tuple containing (total_exported, category_counts)
            where category_counts is a dictionary of category:count pairs
        """
        self.logger.info("Starting HTML body export process")

        # Load filter configuration
        filters = self._load_filter_configuration(config_path)
        if not filters:
            self.logger.error(
                "No valid filters found in configuration. Aborting export."
            )
            return (0, {})

        # Compile regular expressions for each filter
        compiled_filters = self._compile_filters(filters)
        self.logger.debug(
            "Compiled %d filter categories with %d total filters",
            len(compiled_filters),
            sum(
                len(category_filters) for category_filters in compiled_filters.values()
            ),
        )

        # Ensure output directory exists
        base_path = Path(output_dir)
        base_path.mkdir(parents=True, exist_ok=True)

        # Retrieve advertisements from database
        cursor = connection.cursor()
        query = """
            SELECT a.id, a.html_body, a.url, a.ad_type, a.title, a.company, a.location, a.created_at 
            FROM advertisements a
            WHERE EXISTS (SELECT 1 FROM keyword_advertisement ka WHERE ka.advertisement_id = a.id)
        """

        params = []
        if min_id is not None:
            query += " AND a.id >= ?"
            params.append(min_id)
        if max_id is not None:
            query += " AND a.id <= ?"
            params.append(max_id)

        query += " ORDER BY a.id ASC"

        cursor.execute(query, params)

        # Process results and export files
        total_exported = 0
        category_counts = {category: 0 for category in compiled_filters.keys()}
        batch_size = 100

        # Dictionary to store CSV data for each directory
        # Key: directory path, Value: list of ad data dictionaries
        directory_csv_data = {} if create_csv_files else None

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                ad_id, html_body, url, ad_type, title, company, location, created_at = (
                    row
                )

                # Determine portal name from ad_type or URL
                portal_name = self._extract_portal_name(ad_type, url)

                # Create the file path based on filter matches
                rel_path_parts = self._determine_path_from_filters(
                    html_body, compiled_filters, category_counts
                )

                # Skip if no filters matched at all
                if not rel_path_parts:
                    self.logger.warning(
                        "Advertisement ID %d did not match any filters and will not be exported",
                        ad_id,
                    )
                    continue

                # Format file name: portal_00001.html
                file_name = f"{portal_name}_{ad_id:05d}.html"

                # Build full path
                full_path = base_path.joinpath(*rel_path_parts, file_name)

                # Ensure directory exists
                full_path.parent.mkdir(parents=True, exist_ok=True)

                # Write HTML to file
                try:
                    with open(full_path, "w", encoding="utf-8") as f:
                        f.write(html_body)

                    cursor_inner = connection.cursor()

                    # Update the filename in the database
                    rel_file_path = str(full_path.relative_to(base_path))
                    cursor_inner.execute(
                        "UPDATE advertisements SET filename = ? WHERE id = ?",
                        (rel_file_path, ad_id),
                    )

                    # If CSV files are requested, collect data for each directory
                    if create_csv_files:
                        # Get keywords for this advertisement
                        keywords_cursor = connection.cursor()
                        keywords_cursor.execute(
                            """
                            SELECT k.title
                            FROM keywords k
                            JOIN keyword_advertisement ka ON k.id = ka.keyword_id
                            WHERE ka.advertisement_id = ?
                            """,
                            (ad_id,),
                        )
                        keywords = [row[0] for row in keywords_cursor.fetchall()]

                        # Create ad data dictionary
                        ad_data = {
                            "job_title": title or "",
                            "company_name": company or "",
                            "location": location or "",
                            "harvest_date": created_at or "",
                            "url": url or "",
                            "portal": urlparse(url).netloc if url else "",
                            "related_keywords": "; ".join(keywords),
                            "filename": rel_file_path,
                        }

                        # Add data to all parent directories in the path
                        current_dir = base_path
                        # Add to root directory
                        dir_path_str = str(current_dir)
                        if dir_path_str not in directory_csv_data:
                            directory_csv_data[dir_path_str] = []
                        directory_csv_data[dir_path_str].append(ad_data)

                        # Add to each subdirectory
                        for part in rel_path_parts:
                            current_dir = current_dir / part
                            dir_path_str = str(current_dir)
                            if dir_path_str not in directory_csv_data:
                                directory_csv_data[dir_path_str] = []
                            directory_csv_data[dir_path_str].append(ad_data)

                    total_exported += 1
                    if total_exported % 100 == 0:
                        self.logger.info(
                            "Exported %d advertisement files", total_exported
                        )
                        connection.commit()  # Commit periodically

                except IOError as e:
                    self.logger.error("Failed to write file %s: %s", full_path, e)

        # Final commit
        connection.commit()

        # If CSV files are requested, create them in each directory
        if create_csv_files and directory_csv_data:
            self._create_directory_csv_files(directory_csv_data)

        self.logger.info(
            "Export completed. Exported %d advertisement files to %s",
            total_exported,
            base_path,
        )

        return (total_exported, category_counts)

    def _create_directory_csv_files(
        self,
        directory_csv_data: Dict[str, List[Dict[str, str]]],
    ) -> None:
        """
        Create CSV files in each directory containing information about advertisements.

        Args:
            directory_csv_data: Dictionary mapping directory paths to lists of ad data dictionaries
        """
        fieldnames = [
            "job_title",
            "company_name",
            "location",
            "harvest_date",
            "url",
            "portal",
            "related_keywords",
            "filename",
        ]

        csv_count = 0
        for dir_path, ads_data in directory_csv_data.items():
            try:
                csv_path = Path(dir_path) / "advertisements.csv"
                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for ad_data in ads_data:
                        writer.writerow(ad_data)
                csv_count += 1

                if csv_count % 10 == 0:
                    self.logger.debug("Created %d CSV files in directories", csv_count)
            except IOError as e:
                self.logger.error(
                    "Failed to create CSV file in directory %s: %s", dir_path, e
                )

        self.logger.info("Created %d CSV files in directories", csv_count)

    @staticmethod
    def _extract_portal_name(ad_type: str, url: str) -> str:
        """
        Extract a clean portal name from ad_type or URL.

        Args:
            ad_type: Advertisement type class name
            url: URL of the advertisement

        Returns:
            Cleaned portal name suitable for filename
        """
        if ad_type:
            # Remove "Advertisement" suffix if present
            portal_name = ad_type.lower().replace("advertisement", "")
            if portal_name:
                return portal_name

        # Fallback to extracting from URL
        try:
            netloc = urlparse(url).netloc
            parts = netloc.split(".")
            if len(parts) >= 2:
                return parts[
                    -2
                ]  # Get the main domain name (e.g. "karriere" from "www.karriere.at")
        except (ValueError, IndexError):
            pass

        return "unknown"

    def _determine_path_from_filters(
        self,
        html_body: str,
        compiled_filters: Dict[str, Dict[str, Tuple[Pattern, bool]]],
        category_counts: Dict[str, int],
    ) -> List[str]:
        """
        Determine the path components based on filter matches.

        Args:
            html_body: HTML content to match against filters
            compiled_filters: Dictionary of compiled regex patterns
            category_counts: Dictionary to track match counts by category

        Returns:
            List of path components to form the directory structure
        """
        rel_path_parts = []
        for category, category_filters in compiled_filters.items():
            # Find the first matching filter in this category
            matched = False
            for filter_name, (pattern, is_catch_all) in category_filters.items():
                if is_catch_all:
                    continue  # Skip catch-all filters on first pass

                if pattern.search(html_body):
                    rel_path_parts.append(filter_name)
                    category_counts[category] += 1
                    matched = True
                    break

            if not matched:
                # Look for catch-all filter if no match was found
                for filter_name, (pattern, is_catch_all) in category_filters.items():
                    if is_catch_all:
                        rel_path_parts.append(filter_name)
                        category_counts[category] += 1
                        break

        return rel_path_parts

    def _load_filter_configuration(
        self,
        config_path: str,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Load filter configuration from YAML file.

        Args:
            config_path: Path to the configuration file

        Returns:
            Dictionary containing filter categories and their configurations
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Extract filter configuration
            if not config or not isinstance(config, dict) or "filters" not in config:
                self.logger.error(
                    "Invalid configuration format: missing 'filters' section"
                )
                return {}

            return config["filters"]

        except (yaml.YAMLError, IOError) as e:
            self.logger.error("Failed to load configuration file: %s", e)
            return {}

    def _compile_filters(
        self,
        filters: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, Dict[str, Tuple[Pattern, bool]]]:
        """
        Compile regular expressions for each filter.

        Args:
            filters: Filter configuration dictionary

        Returns:
            Dictionary of compiled regex patterns with their catch_all status
        """
        compiled_filters = {}

        for category, category_filters in filters.items():
            compiled_filters[category] = {}

            for filter_name, filter_config in category_filters.items():
                pattern = filter_config.get("pattern", "")
                is_catch_all = filter_config.get("catch_all", False)
                case_sensitive = filter_config.get("case_sensitive", False)

                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    compiled_pattern = re.compile(pattern, flags)
                    compiled_filters[category][filter_name] = (
                        compiled_pattern,
                        is_catch_all,
                    )
                except re.error as e:
                    self.logger.error(
                        "Invalid regular expression in filter %s.%s: %s",
                        category,
                        filter_name,
                        e,
                    )

        return compiled_filters

    def export_to_xml(
        self,
        connection: sqlite3.Connection,
        output_dir: str,
        config_path: str,
        min_id: Optional[int] = None,
        max_id: Optional[int] = None,
        batch_size: int = 100,
    ) -> Tuple[int, Dict[str, int]]:
        """
        Export advertisements to individual XML files with nested directory structure.

        Each advertisement is saved as a separate XML file using the same directory
        structure as export_html_bodies. The XML format uses a <text> element for
        each advertisement with attributes for metadata and the description as content.

        Args:
            connection: SQLite database connection
            output_dir: Base directory for exported XML files
            config_path: Path to the configuration file with filter definitions
            min_id: Minimum advertisement ID to export (inclusive)
            max_id: Maximum advertisement ID to export (inclusive)
            batch_size: Number of advertisements to process in each batch

        Returns:
            Tuple containing (total_exported, category_counts)
            where category_counts is a dictionary of category:count pairs
        """
        self.logger.info("Starting XML export process")

        # Load filter configuration
        filters = self._load_filter_configuration(config_path)
        if not filters:
            self.logger.error(
                "No valid filters found in configuration. Aborting export."
            )
            return (0, {})

        # Compile regular expressions for each filter
        compiled_filters = self._compile_filters(filters)
        self.logger.debug(
            "Compiled %d filter categories with %d total filters",
            len(compiled_filters),
            sum(
                len(category_filters) for category_filters in compiled_filters.values()
            ),
        )

        # Ensure output directory exists
        base_path = Path(output_dir)
        base_path.mkdir(parents=True, exist_ok=True)

        # Retrieve advertisements from database
        cursor = connection.cursor()
        query = """
            SELECT a.id, a.title, a.company, a.location, a.description, a.url, 
                   a.created_at, a.ad_type, a.html_body 
            FROM advertisements a
            WHERE EXISTS (SELECT 1 FROM keyword_advertisement ka WHERE ka.advertisement_id = a.id)
        """

        params = []
        if min_id is not None:
            query += " AND a.id >= ?"
            params.append(min_id)
        if max_id is not None:
            query += " AND a.id <= ?"
            params.append(max_id)

        query += " ORDER BY a.id ASC"

        cursor.execute(query, params)

        # Process results and export files
        total_exported = 0
        category_counts = {category: 0 for category in compiled_filters.keys()}

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                (
                    ad_id,
                    title,
                    company,
                    location,
                    description,
                    url,
                    created_at,
                    ad_type,
                    html_body,
                ) = row

                # If description is missing, try to extract it using AdFactory
                if not description:
                    try:
                        ad = AdFactory.create(ad_type, html_body, url)
                        description = ad.get_description() or ""
                    except Exception as e:
                        self.logger.warning(
                            "Error extracting description for advertisement ID %d: %s",
                            ad_id,
                            str(e),
                        )
                        description = ""

                # Determine portal name from ad_type or URL
                portal_name = self._extract_portal_name(ad_type, url)

                # Create the file path based on filter matches
                rel_path_parts = self._determine_path_from_filters(
                    html_body, compiled_filters, category_counts
                )

                # Skip if no filters matched at all
                if not rel_path_parts:
                    self.logger.warning(
                        "Advertisement ID %d did not match any filters and will not be exported",
                        ad_id,
                    )
                    continue

                # Format file name: portal_00001.xml
                file_name = f"{portal_name}_{ad_id:05d}.xml"

                # Build full path
                full_path = base_path.joinpath(*rel_path_parts, file_name)

                # Ensure directory exists
                full_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    # Create XML document

                    text_element = ET.Element("text")

                    # Add attributes
                    text_element.set("ID", str(ad_id))
                    text_element.set("position", title or "")
                    text_element.set("company", company or "")
                    text_element.set("location", location or "")
                    text_element.set("URL", url or "")
                    text_element.set("accessed", created_at or "")

                    # Add description as content
                    text_element.text = description

                    # Use minidom to format the XML with proper indentation
                    xml_string = ET.tostring(text_element, encoding="utf-8")
                    pretty_xml = minidom.parseString(xml_string).toprettyxml(
                        indent="  ", encoding="utf-8"
                    )

                    # Write XML to file
                    with open(full_path, "wb") as f:
                        f.write(pretty_xml)

                    # Update the XML filename in the database (optional)
                    rel_file_path = str(full_path.relative_to(base_path))
                    cursor_inner = connection.cursor()
                    cursor_inner.execute(
                        "UPDATE advertisements SET filename = ? WHERE id = ?",
                        (rel_file_path, ad_id),
                    )

                    total_exported += 1
                    if total_exported % 100 == 0:
                        self.logger.info("Exported %d XML files", total_exported)
                        connection.commit()  # Commit periodically

                except IOError as e:
                    self.logger.error("Failed to write XML file %s: %s", full_path, e)
                except Exception as e:
                    self.logger.error(
                        "Error processing advertisement ID %d: %s", ad_id, e
                    )

        # Final commit
        connection.commit()

        self.logger.info(
            "XML export completed. Exported %d advertisement files to %s",
            total_exported,
            base_path,
        )

        return (total_exported, category_counts)
