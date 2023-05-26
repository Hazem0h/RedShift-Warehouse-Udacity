import configparser
import psycopg2
from redshift_project.sql_queries import copy_table_queries, insert_table_queries
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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