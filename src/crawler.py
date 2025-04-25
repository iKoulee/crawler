# This is a sample Python script.
import sqlite3
import threading
import yaml
import argparse
import os
import logging
from typing import Dict, List, Any
import re

from advert import AdFactory, Advertisement
from harvester import Harvester, HarvesterFactory
from keyword_manager import KeywordManager
from advert_exporter import AdvertExporter


def setup_logging(log_level: str) -> None:
    """
    Configure the logging system with appropriate format and level.

    Args:
        log_level: The logging level to set (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string log level to logging module constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def harvest_command(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Execute the harvest command to collect job advertisements.

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        with open(args.config) as config_handle:
            config: Dict[str, Any] = yaml.safe_load(config_handle)
            logger.debug("Loaded configuration from %s", args.config)

        connection = sqlite3.connect(args.database)
        Harvester.create_schema(connection)
        logger.debug("Database schema created")

        # Initialize KeywordManager directly
        keyword_manager = KeywordManager(logger)

        # Insert keywords using KeywordManager instead of Harvester
        for keyword in config["keywords"]:
            keyword_manager.insert_keyword(connection, keyword)
            logger.debug("Added keyword: %s", keyword)

        harvester_factory = HarvesterFactory(config)
        logger.debug("Initialized harvester factory")

        # Dictionary to track which harvester belongs to which portal
        harvester_info: Dict[int, Dict[str, str]] = {}
        threads: List[threading.Thread] = []
        thread_id = 0

        # Create and start threads for each harvester
        for harvester in harvester_factory.get_next_harvester():
            # Store information about this harvester
            portal_name = next(
                (
                    p.get("name", "unknown")
                    for p in config["portals"]
                    if p.get("engine") == harvester.__class__.__name__
                ),
                harvester.__class__.__name__,
            )
            harvester_info[thread_id] = {
                "class": harvester.__class__.__name__,
                "portal": portal_name,
                "url": harvester.url,
            }

            # Create a wrapper function to track completion
            def harvester_wrapper(
                harvester_id: int, harvester: Harvester, db_path: str
            ) -> None:
                h_info = harvester_info[harvester_id]
                try:
                    logger.info(
                        "Starting harvester %s for portal '%s' (%s)",
                        h_info["class"],
                        h_info["portal"],
                        h_info["url"],
                    )
                    harvester.harvest(db_path)
                    logger.info(
                        "✅ Completed harvester %s for portal '%s' (%s)",
                        h_info["class"],
                        h_info["portal"],
                        h_info["url"],
                    )
                except Exception as e:
                    logger.error(
                        "❌ Error in harvester %s for portal '%s' (%s): %s",
                        h_info["class"],
                        h_info["portal"],
                        h_info["url"],
                        str(e),
                    )

            # Start thread with the wrapper
            thread = threading.Thread(
                target=harvester_wrapper, args=(thread_id, harvester, args.database)
            )
            threads.append(thread)
            thread.start()
            logger.debug(
                "Started harvester thread #%d for %s on portal '%s'",
                thread_id,
                harvester.__class__.__name__,
                portal_name,
            )
            thread_id += 1

        # Wait for all threads to complete
        for i, thread in enumerate(threads):
            thread.join()
            logger.debug("Thread #%d completed", i)

        connection.commit()
        connection.close()
        logger.info(
            "All harvesting threads finished. Harvesting completed successfully."
        )

    except FileNotFoundError:
        logger.error("Config file '%s' not found.", args.config)
    except yaml.YAMLError:
        logger.error("Invalid YAML in config file '%s'.", args.config)
    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)


def assembly_command(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Execute the assembly command to generate CSV from collected advertisements.

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        connection = sqlite3.connect(args.database)

        # Determine output filename if not provided
        output_file = args.output
        if not output_file:
            base_name = os.path.splitext(os.path.basename(args.database))[0]
            output_file = f"{base_name}_export.csv"
            logger.info("No output file specified, using: %s", output_file)

        # Create an AdvertExporter instance
        exporter = AdvertExporter(logger)

        # Export advertisements to CSV
        count = exporter.export_to_csv(
            connection, output_file, min_id=args.min_id, max_id=args.max_id
        )

        logger.info(
            "Exported %d advertisements to %s", count, os.path.abspath(output_file)
        )

        connection.close()

    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
    except IOError as e:
        logger.error("File error: %s", e)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)


def export_command(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Execute the export command to export advertisements to files with nested directory structure.

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        connection = sqlite3.connect(args.database)

        # Create output directory if it doesn't exist
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
            logger.info("Created output directory: %s", args.output_dir)

        # Create an AdvertExporter instance
        exporter = AdvertExporter(logger)

        # Export advertisements based on the specified format
        if args.format.upper() == "XML":
            logger.info("Exporting advertisements in XML format")
            total_exported, category_counts = exporter.export_to_xml(
                connection,
                args.output_dir,
                args.config,
                min_id=args.min_id,
                max_id=args.max_id,
                batch_size=args.batch_size,
            )
        else:  # Default to HTML
            logger.info("Exporting advertisements in HTML format")
            total_exported, category_counts = exporter.export_html_bodies(
                connection,
                args.output_dir,
                args.config,
                min_id=args.min_id,
                max_id=args.max_id,
                create_csv_files=args.create_csv_files,
            )

        logger.info("Exported %d advertisements", total_exported)
        for category, count in category_counts.items():
            logger.info("  Category %s: %d matches", category, count)

        connection.close()

    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
    except IOError as e:
        logger.error("File error: %s", e)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)


def analyze_command(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Execute the analyze command to refresh keyword matching on all advertisements.

    This command:
    1. Initializes an AdvertAnalyzer instance
    2. Runs the analysis process which:
       - Optionally truncates existing keywords and keyword_advertisement tables
       - Reinserts keywords from the configuration file
       - Updates advertisement keyword matches based on content

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        from analyzer import AdvertAnalyzer

        logger.info("Initializing advertisement analyzer")
        analyzer = AdvertAnalyzer(db_path=args.database, config_path=args.config)

        # Configure analyzer options based on command-line arguments
        analyzer_options = {
            "min_id": args.min_id,
            "max_id": args.max_id,
            "batch_size": args.batch_size,
            "reset_tables": not args.no_reset,
            "include_description": args.include_description,
        }

        logger.info(
            "Analysis parameters: %s",
            ", ".join(f"{k}={v}" for k, v in analyzer_options.items() if v is not None),
        )

        # Run the analysis process with options
        ad_count = analyzer.run_analysis(**analyzer_options)

        logger.info(
            "Analysis completed successfully. Processed %d advertisements.", ad_count
        )

    except ImportError as e:
        logger.error("Failed to import analyzer module: %s", e)
    except FileNotFoundError:
        logger.error("Config file '%s' not found.", args.config)
    except yaml.YAMLError:
        logger.error("Invalid YAML in config file '%s'.", args.config)
    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
    except Exception as e:
        logger.exception("Unexpected error during analysis: %s", e)


def reset_keyword_tables(
    connection: sqlite3.Connection, logger: logging.Logger
) -> None:
    """
    Truncate the keywords and keyword_advertisement tables.

    Args:
        connection: SQLite database connection
        logger: Logger instance
    """
    cursor = connection.cursor()

    # Truncate keyword_advertisement first due to foreign key constraints
    logger.info("Truncating keyword_advertisement table")
    cursor.execute("DELETE FROM keyword_advertisement")

    # Then truncate keywords table
    logger.info("Truncating keywords table")
    cursor.execute("DELETE FROM keywords")

    # Commit changes
    connection.commit()
    logger.debug("Keyword tables reset successfully")


def insert_keywords_from_config(
    connection: sqlite3.Connection, config: Dict[str, Any], logger: logging.Logger
) -> int:
    """
    Insert keywords from configuration file into the database.

    Args:
        connection: SQLite database connection
        config: Configuration dictionary containing keywords
        logger: Logger instance

    Returns:
        Number of keywords inserted
    """
    if "keywords" not in config or not config["keywords"]:
        logger.warning("No keywords found in configuration")
        return 0

    logger.info("Inserting %d keywords from configuration", len(config["keywords"]))
    count = 0

    for keyword in config["keywords"]:
        try:
            Harvester.insert_keyword(connection, keyword)
            count += 1
            logger.debug("Inserted keyword: %s", keyword.get("title", "Unnamed"))
        except Exception as e:
            logger.warning("Failed to insert keyword: %s", e)

    logger.info("Inserted %d keywords successfully", count)
    return count


def process_advertisements_with_factory(
    connection: sqlite3.Connection, db_path: str, logger: logging.Logger
) -> int:
    """
    Process all advertisements using AdFactory to match with keywords.

    Args:
        connection: SQLite database connection
        db_path: Path to the SQLite database file
        logger: Logger instance

    Returns:
        Number of advertisements processed
    """
    from advert import AdFactory

    batch_size = 100
    processed_count = 0

    # Fetch keywords using static method
    regexes = Harvester.fetch_keywords(connection)
    if not regexes:
        logger.warning("No keywords found for matching")
        return 0

    logger.info("Using %d keywords for matching", len(regexes))

    # Use the new AdFactory fetch_by_condition method to get advertisements in batches
    advertisements_iterator = AdFactory.fetch_by_condition(
        db_path=db_path, batch_size=batch_size
    )

    # Process each advertisement
    for ad in advertisements_iterator:
        # Match keywords for this advertisement
        matched_keyword_ids = match_keywords_for_ad(ad, regexes, logger)

        # Update keyword matches in the database
        update_advertisement_keywords(connection, ad.id, matched_keyword_ids, logger)

        processed_count += 1
        if processed_count % 100 == 0:
            logger.info("Processed %d advertisements", processed_count)
            connection.commit()  # Commit periodically

    # Final commit
    connection.commit()
    logger.info("Processed %d advertisements", processed_count)

    return processed_count


def match_keywords_for_ad(
    ad: Advertisement, regexes: Dict[int, re.Pattern], logger: logging.Logger
) -> List[int]:
    """
    Match an advertisement against keywords and return matching keyword IDs.

    Args:
        ad: Advertisement instance to check
        regexes: Dictionary of compiled regex patterns for keywords
        logger: Logger instance

    Returns:
        List of keyword IDs that match the advertisement
    """
    # Use KeywordManager directly for matching keywords
    keyword_manager = KeywordManager(logger)
    return keyword_manager.match_keywords(ad, regexes)


def update_advertisement_keywords(
    connection: sqlite3.Connection,
    ad_id: int,
    keyword_ids: List[int],
    logger: logging.Logger,
) -> None:
    """
    Update the keyword associations for an advertisement.

    Args:
        connection: SQLite database connection
        ad_id: ID of the advertisement
        keyword_ids: List of keyword IDs that match the advertisement
        logger: Logger instance
    """
    if ad_id is None:
        logger.warning("Cannot update keywords for advertisement with None ID")
        return

    # Use KeywordManager directly for storing keyword matches
    keyword_manager = KeywordManager(logger)

    try:
        # First delete any existing keyword associations for this ad
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM keyword_advertisement WHERE advertisement_id = ?", (ad_id,)
        )

        # Store new keyword matches using KeywordManager
        keyword_manager.store_keyword_matches(connection, ad_id, keyword_ids)

        logger.debug(
            "Updated advertisement ID %d with %d keyword associations",
            ad_id,
            len(keyword_ids),
        )

    except sqlite3.Error as e:
        logger.error("Error updating keywords for ad %d: %s", ad_id, e)
        raise


def update_command(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    Execute the update command to fill missing advertisement information.

    This command processes advertisements and updates any missing fields
    by re-parsing the HTML content. When the --force flag is used, it will
    override existing data even if not empty.

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        connection = sqlite3.connect(args.database)
        cursor = connection.cursor()

        # Build the query to select advertisements that need updating
        # If force is not enabled, only select ads with missing fields
        query = """
            SELECT id, ad_type, html_body, url, http_status, title, company, location, description
            FROM advertisements
        """

        conditions = []
        params = []

        # Add ID range filters if provided
        if args.min_id is not None:
            conditions.append("id >= ?")
            params.append(args.min_id)

        if args.max_id is not None:
            conditions.append("id <= ?")
            params.append(args.max_id)

        # If not forcing updates, only select rows with missing data
        if not args.force:
            conditions.append(
                "(title IS NULL OR company IS NULL OR location IS NULL OR description IS NULL)"
            )

        # Add conditions to query if present
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add order and limit
        query += " ORDER BY id ASC"

        logger.info("Starting update of advertisements")
        if args.force:
            logger.info(
                "Force mode enabled: updating all fields regardless of current content"
            )

        # Execute query and get count of ads to process
        cursor.execute(f"SELECT COUNT(*) FROM ({query})", params)
        total_count = cursor.fetchone()[0]
        logger.info(f"Found {total_count} advertisements to process")

        if total_count == 0:
            logger.info("No advertisements need updating. Exiting.")
            connection.close()
            return

        # Process advertisements in batches
        cursor.execute(query, params)
        processed_count = 0
        updated_count = 0
        batch_size = args.batch_size

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                (
                    ad_id,
                    ad_type,
                    html_body,
                    url,
                    status,
                    current_title,
                    current_company,
                    current_location,
                    current_description,
                ) = row

                try:
                    # Create Advertisement instance to re-parse HTML
                    ad = AdFactory.create(
                        ad_type=ad_type,
                        source=html_body,
                        link=url,
                        status=status,
                        id=ad_id,
                    )

                    # Extract data from the advertisement
                    title = ad.get_title()
                    company = ad.get_company()
                    location = ad.get_location()
                    description = ad.get_description()

                    # Determine what fields need updating
                    update_fields = []
                    update_values = []

                    # For each field, update if:
                    # 1. Force mode is enabled, OR
                    # 2. The current value is None/empty AND the new value is not None

                    if (args.force or not current_title) and title:
                        update_fields.append("title = ?")
                        update_values.append(title)

                    if (args.force or not current_company) and company:
                        update_fields.append("company = ?")
                        update_values.append(company)

                    if (args.force or not current_location) and location:
                        update_fields.append("location = ?")
                        update_values.append(location)

                    if (args.force or not current_description) and description:
                        update_fields.append("description = ?")
                        update_values.append(description)

                    # Perform update if there are fields to update
                    if update_fields:
                        update_query = f"UPDATE advertisements SET {', '.join(update_fields)} WHERE id = ?"
                        update_values.append(ad_id)

                        update_cursor = connection.cursor()
                        update_cursor.execute(update_query, update_values)

                        updated_count += 1
                        logger.debug(
                            f"Updated advertisement ID {ad_id} with {len(update_fields)} fields"
                        )

                except Exception as e:
                    logger.error(f"Error processing advertisement ID {ad_id}: {str(e)}")

                processed_count += 1
                if processed_count % 100 == 0:
                    # Commit every 100 records
                    connection.commit()
                    logger.info(
                        f"Progress: {processed_count}/{total_count} advertisements processed, {updated_count} updated"
                    )

        # Final commit and cleanup
        connection.commit()
        connection.close()

        logger.info(
            f"Update completed: {processed_count} advertisements processed, {updated_count} updated"
        )

    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")


def main() -> None:
    """
    Main function that parses command line arguments and dispatches to subcommands.
    """

    # Create the main parser
    parser = argparse.ArgumentParser(
        description="WU Advertisement Crawler and Processor"
    )

    # Add global arguments
    parser.add_argument(
        "-l",
        "--loglevel",
        required=False,
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Create the parser for the "harvest" command
    harvest_parser = subparsers.add_parser(
        "harvest", help="Harvest job advertisements from various sources"
    )
    harvest_parser.add_argument(
        "-c", "--config", required=True, type=str, help="Path to the config file"
    )
    harvest_parser.add_argument(
        "-d",
        "--database",
        required=False,
        type=str,
        default=os.path.join(os.getcwd(), "crawler.db"),
        help="Path to the database file",
    )

    # Create the parser for the "assembly" command
    assembly_parser = subparsers.add_parser(
        "assembly", help="Generate CSV file from collected advertisements"
    )
    assembly_parser.add_argument(
        "-d",
        "--database",
        required=False,
        type=str,
        default=os.path.join(os.getcwd(), "crawler.db"),
        help="Path to the database file",
    )
    assembly_parser.add_argument(
        "-o",
        "--output",
        required=False,
        type=str,
        help="Path to output CSV file",
    )
    assembly_parser.add_argument(
        "--min-id",
        required=False,
        type=int,
        help="Minimum advertisement ID to include",
    )
    assembly_parser.add_argument(
        "--max-id",
        required=False,
        type=int,
        help="Maximum advertisement ID to include",
    )

    # Create the parser for the "export" command
    export_parser = subparsers.add_parser(
        "export", help="Export advertisement HTML bodies to files with nested structure"
    )
    export_parser.add_argument(
        "-d",
        "--database",
        required=False,
        type=str,
        default=os.path.join(os.getcwd(), "crawler.db"),
        help="Path to the database file",
    )
    export_parser.add_argument(
        "-c",
        "--config",
        required=True,
        type=str,
        help="Path to the filter configuration file",
    )
    export_parser.add_argument(
        "-o",
        "--output-dir",
        required=True,
        type=str,
        help="Output directory for exported HTML files",
    )
    export_parser.add_argument(
        "--min-id",
        required=False,
        type=int,
        help="Minimum advertisement ID to include",
    )
    export_parser.add_argument(
        "--max-id",
        required=False,
        type=int,
        help="Maximum advertisement ID to include",
    )
    export_parser.add_argument(
        "--create-csv-files",
        action="store_true",
        help="Create CSV files for exported advertisements",
    )
    export_parser.add_argument(
        "--format",
        required=False,
        type=str,
        default="HTML",
        choices=["HTML", "XML"],
        help="Format for exported advertisements (HTML or XML)",
    )
    export_parser.add_argument(
        "-b",
        "--batch-size",
        required=False,
        type=int,
        default=100,
        help="Number of advertisements to process in each batch",
    )

    # Create the parser for the "analyze" command
    analyze_parser = subparsers.add_parser(
        "analyze", help="Refresh keyword matching on all advertisements"
    )
    analyze_parser.add_argument(
        "-d",
        "--database",
        required=False,
        type=str,
        default=os.path.join(os.getcwd(), "crawler.db"),
        help="Path to the database file",
    )
    analyze_parser.add_argument(
        "-c", "--config", required=True, type=str, help="Path to the config file"
    )
    analyze_parser.add_argument(
        "--min-id",
        required=False,
        type=int,
        help="Minimum advertisement ID to analyze",
    )
    analyze_parser.add_argument(
        "--max-id",
        required=False,
        type=int,
        help="Maximum advertisement ID to analyze",
    )
    analyze_parser.add_argument(
        "-b",
        "--batch-size",
        required=False,
        type=int,
        default=100,
        help="Number of advertisements to process in each batch",
    )
    analyze_parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Don't reset keyword tables before analysis (use for incremental updates)",
    )
    analyze_parser.add_argument(
        "--include-description",
        action="store_true",
        help="Match keywords in both title and description (default is title-only matching)",
    )

    # Create the parser for the "update" command
    update_parser = subparsers.add_parser(
        "update", help="Update missing information in advertisements"
    )
    update_parser.add_argument(
        "-d",
        "--database",
        required=False,
        type=str,
        default=os.path.join(os.getcwd(), "crawler.db"),
        help="Path to the database file",
    )
    update_parser.add_argument(
        "--force",
        action="store_true",
        help="Override existing data even if not empty",
    )
    update_parser.add_argument(
        "--min-id",
        required=False,
        type=int,
        help="Minimum advertisement ID to update",
    )
    update_parser.add_argument(
        "--max-id",
        required=False,
        type=int,
        help="Maximum advertisement ID to update",
    )
    update_parser.add_argument(
        "-b",
        "--batch-size",
        required=False,
        type=int,
        default=100,
        help="Number of advertisements to process in each batch",
    )

    # Parse arguments
    args = parser.parse_args()

    # Setup logging early to ensure it's available for all commands
    setup_logging(args.loglevel)
    logger = logging.getLogger(__name__)

    # Execute the appropriate command
    if args.command == "harvest":
        harvest_command(args, logger)
    elif args.command == "assembly":
        assembly_command(args, logger)
    elif args.command == "export":
        export_command(args, logger)
    elif args.command == "analyze":
        analyze_command(args, logger)
    elif args.command == "update":
        update_command(args, logger)
    else:
        logger.error(
            "No command specified. Use 'harvest', 'assembly', 'export', 'analyze', or 'update'."
        )
        parser.print_help()


if __name__ == "__main__":
    main()
