#!/usr/bin/env python3
"""
A simple script to count available advertisements from job portals by parsing their sitemaps.
This script reuses the harvester code but doesn't store any advertisements in the database.
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Any, Optional
import yaml

# Add the src directory to Python path
sys.path.append(
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
)

from harvester import StepStoneHarvester, KarriereHarvester, HarvesterFactory


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


def count_advertisements(config_path: str) -> Dict[str, Dict[str, int]]:
    """
    Count the number of advertisements available on job portals.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary mapping portal names to dictionaries of URL:count pairs
    """
    logger = logging.getLogger(__name__)

    # Load configuration
    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
            logger.debug("Loaded configuration from %s", config_path)
    except (FileNotFoundError, yaml.YAMLError) as e:
        logger.error("Error loading configuration: %s", e)
        return {}

    # Initialize harvester factory
    harvester_factory = HarvesterFactory(config)
    logger.info("Initialized harvester factory")

    # Count advertisements for each portal
    counts = {}
    for harvester in harvester_factory.get_next_harvester():
        portal_name = harvester.__class__.__name__
        portal_url = harvester.url

        if portal_name not in counts:
            counts[portal_name] = {}

        logger.info("Counting advertisements for %s (%s)", portal_name, portal_url)

        # Count links from the sitemap
        link_count = 0
        try:
            for _ in harvester.get_next_link():
                link_count += 1
                if link_count % 1000 == 0:
                    logger.info(
                        "Found %d links so far for %s (%s)",
                        link_count,
                        portal_name,
                        portal_url,
                    )
        except Exception as e:
            logger.error(
                "Error counting links for %s (%s): %s", portal_name, portal_url, e
            )

        counts[portal_name][portal_url] = link_count
        logger.info(
            "Found %d total links for %s (%s)", link_count, portal_name, portal_url
        )

    return counts


def main() -> None:
    """Main function that parses arguments and runs the counter."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Count available job advertisements from portal sitemaps"
    )
    parser.add_argument(
        "-c", "--config", required=True, type=str, help="Path to the config file"
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        required=False,
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=False,
        type=str,
        help="Path to output file for results (optional)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.loglevel)
    logger = logging.getLogger(__name__)

    logger.info("Starting advertisement count")

    # Count advertisements
    counts = count_advertisements(args.config)

    # Display results
    if counts:
        print("\nAdvertisement counts by portal:")
        print("------------------------------")
        total_count = 0
        for portal, urls in counts.items():
            print(f"{portal}:")
            portal_total = sum(urls.values())
            for url, count in urls.items():
                print(f"  {url}: {count:,}")
                total_count += count
            print(f"  Total for {portal}: {portal_total:,}")
            print()
        print("------------------------------")
        print(f"Total advertisements: {total_count:,}")

        # Write to output file if specified
        if args.output:
            try:
                with open(args.output, "w", encoding="utf-8") as out_file:
                    out_file.write("Portal,URL,Count\n")
                    for portal, urls in counts.items():
                        for url, count in urls.items():
                            out_file.write(f"{portal},{url},{count}\n")
                        out_file.write(f"{portal},TOTAL,{sum(urls.values())}\n")
                    out_file.write(f"GRAND TOTAL,,{total_count}\n")
                logger.info("Results written to %s", args.output)
            except IOError as e:
                logger.error("Failed to write output file: %s", e)
    else:
        logger.error("No advertisements counted. Check the configuration and logs.")


if __name__ == "__main__":
    main()
