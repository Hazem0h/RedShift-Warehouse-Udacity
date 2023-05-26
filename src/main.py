from redshift_project import create_tables, etl
import logging
import argparse

logging.basicConfig()
root_logger = logging.getLogger()


def parse_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)

    create_tables.main()
    etl.main()