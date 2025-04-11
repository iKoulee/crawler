# This is a sample Python script.
import sqlite3
import threading
import yaml
import argparse
import os
import logging
from typing import Dict, List, Any, Optional, Tuple
import re

from harvester import Harvester, HarvesterFactory


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

        for keyword in config["keywords"]:
            Harvester.insert_keyword(connection, keyword)
            logger.debug("Added keyword: %s", keyword)

        harvester_factory = HarvesterFactory(config)
        logger.debug("Initialized harvester factory")

        threads: List[threading.Thread] = []
        for harvester in harvester_factory.get_next_harvester():
            thread = threading.Thread(target=harvester.harvest, args=(args.database,))
            threads.append(thread)
            thread.start()
            logger.debug(
                "Started harvester thread for %s", harvester.__class__.__name__
            )

        for thread in threads:
            thread.join()
            logger.debug("Thread completed")

        connection.commit()
        connection.close()
        logger.info("Finished harvesting.")

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

        # Export advertisements to CSV
        count = Harvester.export_to_csv(
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
    Execute the export command to export HTML bodies to files with nested directory structure.

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

        # Export HTML bodies
        total_exported, category_counts = Harvester.export_html_bodies(
            connection,
            args.output_dir,
            args.config,
            min_id=args.min_id,
            max_id=args.max_id,
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
    1. Truncates existing keywords and keyword_advertisement tables
    2. Reinserts keywords from the configuration file
    3. Updates advertisement keyword matches based on content

    Args:
        args: Command line arguments
        logger: Logger instance
    """
    try:
        # Load configuration
        with open(args.config) as config_handle:
            config: Dict[str, Any] = yaml.safe_load(config_handle)
            logger.debug("Loaded configuration from %s", args.config)

        # Connect to the database
        connection = sqlite3.connect(args.database)
        logger.debug("Connected to database: %s", args.database)

        # Reset keyword tables
        reset_keyword_tables(connection, logger)

        # Insert keywords from config
        insert_keywords_from_config(connection, config, logger)

        # Process advertisements
        process_advertisements(connection, logger)

        connection.close()
        logger.info("Analysis completed successfully.")

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


def process_advertisements(
    connection: sqlite3.Connection, logger: logging.Logger
) -> int:
    """
    Process all advertisements in the database to match with keywords.

    Args:
        connection: SQLite database connection
        logger: Logger instance

    Returns:
        Number of advertisements processed
    """
    cursor = connection.cursor()

    # Get all advertisements
    logger.info("Fetching advertisements from database")
    cursor.execute("SELECT id, html_body FROM advertisements")
    ads = cursor.fetchall()

    if not ads:
        logger.warning("No advertisements found in database")
        return 0

    logger.info("Processing %d advertisements for keyword matches", len(ads))

    # Fetch keywords using static method
    regexes = Harvester.fetch_keywords(connection)
    if not regexes:
        logger.warning("No keywords found for matching")
        return 0

    logger.debug("Using %d keywords for matching", len(regexes))

    # Process each advertisement
    return match_advertisements_with_keywords(connection, ads, regexes, logger)


def match_advertisements_with_keywords(
    connection: sqlite3.Connection,
    ads: List[Tuple[int, str]],
    regexes: Dict[int, re.Pattern],
    logger: logging.Logger,
) -> int:
    """
    Match advertisements with keywords and update the keyword_advertisement table.

    Args:
        connection: SQLite database connection
        ads: List of tuples containing (ad_id, html_body)
        regexes: Dictionary of compiled regex patterns for keywords
        logger: Logger instance

    Returns:
        Number of advertisement-keyword matches created
    """
    cursor = connection.cursor()
    total_matches = 0
    processed_count = 0
    batch_size = 100

    for ad_id, html_body in ads:
        # Create a temporary Advertisement instance to use matching logic
        from advert import Advertisement

        ad = Advertisement(source=html_body)

        # Match keywords
        matched_keywords = match_keywords_for_ad(ad, regexes, logger)

        # Insert matches into database
        for keyword_id in matched_keywords:
            try:
                cursor.execute(
                    "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
                    (keyword_id, ad_id),
                )
                total_matches += 1
            except sqlite3.Error as e:
                logger.warning(
                    "Error linking ad %d with keyword %d: %s", ad_id, keyword_id, e
                )

        processed_count += 1

        # Commit in batches to avoid large transactions
        if processed_count % batch_size == 0:
            connection.commit()
            logger.debug(
                "Processed %d advertisements (%d matches so far)",
                processed_count,
                total_matches,
            )

    # Final commit
    connection.commit()
    logger.info(
        "Created %d keyword matches for %d advertisements",
        total_matches,
        processed_count,
    )

    return processed_count


def match_keywords_for_ad(
    ad: "Advertisement", regexes: Dict[int, re.Pattern], logger: logging.Logger
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
    # Extract the HTML content from the advertisement
    matched_keywords = []
    description = ad.get_description()

    # If no description, check the raw source
    if not description:
        description = ad.source

    # Match against each keyword regex
    for keyword_id, regex in regexes.items():
        if regex.search(description):
            matched_keywords.append(keyword_id)

    return matched_keywords


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
    else:
        logger.error(
            "No command specified. Use 'harvest', 'assembly', 'export', or 'analyze'."
        )
        parser.print_help()


if __name__ == "__main__":
    main()
