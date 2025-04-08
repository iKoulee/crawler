# This is a sample Python script.
import sqlite3
import threading
import yaml
import argparse
import os
import logging
from typing import Dict, List, Any, Optional

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
    else:
        logger.error("No command specified. Use 'harvest', 'assembly', or 'export'.")
        parser.print_help()


if __name__ == "__main__":
    main()
