import configparser
import psycopg2
from redshift_project.sql_queries import copy_table_queries, insert_table_queries
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_staging_tables(cur: psycopg2.cursor, conn: psycopg2.connection):
    """Load Redshift staging tables from S3

    Args:
        cur (psycopg2.cursor): the psycopg2 cursor
        conn (psycopg2 connection): the psycopg2 connection to Redshift
    """
    for index, query in enumerate(copy_table_queries):
        logger.debug(query)
        logger.info(f"Loading data into staging table {index}")

        cur.execute(query)
        conn.commit()

        logger.info("Loaded data into staging table")


def insert_tables(cur:psycopg2.cursor, conn: psycopg2.connection):
    """Insert data from staging tables into the fact and dimension tables

    Args:
        cur (psycopg2.cursor): the psycopg2 cursor
        conn (psycopg2.connection): the psycopg2 connection to Redshift
    """
    for query in insert_table_queries:
        logger.debug(query)
        logger.info("inserting data into final table")

        cur.execute(query)
        conn.commit()

        logger.info("Data inserted.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    logger.debug("Read config data")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.debug("Connected to Redshift")

    load_staging_tables(cur, conn)
    logger.info("Loaded staging tables")

    insert_tables(cur, conn)
    logger.info("Inserted data into tables")
    
    conn.close()
    logger.debug("Closed connection to Redshift")


if __name__ == "__main__":
    main()
