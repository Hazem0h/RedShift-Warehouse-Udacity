"""
This is the main script for the ETL pipeline.
It is responsible for running the table creation and ETL scripts.

"""
from redshift_project import create_tables, etl
import logging
import argparse


root_logger = logging.getLogger()


def parse_cli_args() -> argparse.Namespace:
    """Parse command line argument (duh)

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Show debug level logs")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)

    # Run the table dropping/creation script, then the ETL
    create_tables.main()
    etl.main()