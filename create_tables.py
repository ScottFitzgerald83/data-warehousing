import configparser

import psycopg2
from sql_queries import drop_tables_query, create_table_queries


def drop_tables(cur, conn):
    """
    Executes drop table statements with SQL provided in drop_tables_query
    :param cur: psycopg cursor
    :param conn: psycopg connection object
    :return: None
    """
    cur.execute(drop_tables_query)
    conn.commit()


def create_tables(cur, conn):
    """
    Creates tables by executing SQL in create_table_queries
    :param cur: psycopg cursor
    :param conn: psycopg connection object
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
