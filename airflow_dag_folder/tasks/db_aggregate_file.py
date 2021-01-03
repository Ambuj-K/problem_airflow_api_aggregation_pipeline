import requests, psycopg2

connection = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow",
)
connection.autocommit = True


def create_aggregate_table(cursor) -> None:
    """
            Pipeline of transformations called
                - Create api table with requisite columns

            :param

            cursor : cursor object

    """
    cursor.execute("""
        DROP TABLE IF EXISTS aggregate_table;
        CREATE UNLOGGED TABLE aggregate_table (
                date_val TEXT,
                delta_recovered_agg INT
        );
    """)


def api_db_aggregate_fill():
    """
            Pipeline of transformations called
                - call db creation logic
                - aggregate and insert query into the aggregate table

    """
    with connection.cursor() as cursor:
        create_aggregate_table(cursor)
        cursor.execute("""
            INSERT INTO aggregate_table(date_val,delta_recovered_agg)
                Select date_val, delta_confirmed-total_confirmed
                                from staging_api_call
                                where delta_confirmed-total_confirmed is not 
                                null
                                order by delta_confirmed-total_confirmed asc ;
        """)


api_db_aggregate_fill()
