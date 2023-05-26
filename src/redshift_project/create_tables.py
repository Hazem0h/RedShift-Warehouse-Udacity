import configparser
import psycopg2
from redshift_project.sql_queries import create_table_queries, drop_table_queries
import logging

logger = logging.getLogger(__name__)

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


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