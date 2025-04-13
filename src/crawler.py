# This is a sample Python script.
import sqlite3
import threading
import yaml
import argparse
import os
import logging
from typing import Dict, List, Any, Optional, Tuple
import re

from advert import AdFactory
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
    # Extract text content from the advertisement
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

    cursor = connection.cursor()

    try:
        # First delete any existing keyword associations for this ad
        cursor.execute(
            "DELETE FROM keyword_advertisement WHERE advertisement_id = ?", (ad_id,)
        )

        # Insert new keyword associations
        for keyword_id in keyword_ids:
            cursor.execute(
                "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
                (keyword_id, ad_id),
            )

        logger.debug(
            "Updated advertisement ID %d with %d keyword associations",
            ad_id,
            len(keyword_ids),
        )

    except sqlite3.Error as e:
        logger.error("Error updating keywords for ad %d: %s", ad_id, e)
        raise


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
