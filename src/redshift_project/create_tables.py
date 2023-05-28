import configparser
import psycopg2
from redshift_project.sql_queries import create_table_queries, drop_table_queries
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def drop_tables(cur:psycopg2.cursor, conn:psycopg2.connection):
    """Drop Tables in Redshift (if they exist)

    Args:
        cur (psycopg2.cursor): The psycopg2 cursor
        conn (psycopg2.connection): The psycopg2 connection to Redshift
    """
    for index, query in enumerate(drop_table_queries):
        logger.debug(query)
        logger.info(f"Dropping table #{index}")

        cur.execute(query)
        conn.commit()

        logger.info("Table dropped")


def create_tables(cur: psycopg2.cursor, conn:psycopg2.connection):
    """Create Tables in Redshift (if they don't exist)

    Args:
        cur (psycopg2.cursor): The psycopg2 cursor
        conn (psycopg2.connection): The psycopg2 connection to Redshift
    """
    for index, query in enumerate(create_table_queries):
        logger.debug(query)
        logger.info(f"Creating table #{index}")

        cur.execute(query)
        conn.commit()

        logger.info("Table created")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    logger.debug("Read config data")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.debug("Connected to Redshift")

    drop_tables(cur, conn)
    logger.info("Dropped tables (if they existed)")

    create_tables(cur, conn)
    logger.info("Created tables")

    conn.close()
    logger.debug("Closed connection")


if __name__ == "__main__":
    main()