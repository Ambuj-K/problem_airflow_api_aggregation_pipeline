import psycopg2, requests

connection = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow",
)
connection.autocommit = True


def create_staging_table(cursor) -> None:
    """
        Pipeline of transformations called
            - Create api table with requisite columns

        :param

        cursor : cursor object

    """
    cursor.execute("""
        DROP TABLE IF EXISTS staging_api_call;
        CREATE UNLOGGED TABLE staging_api_call (
                date_val TEXT,
                total_confirmed INT,
                total_tested INT,
                total_recovered INT,
                total_deceased INT,
                delta_confirmed INT,
                delta_tested INT,
                delta_recovered INT
        );
    """)


def api_to_db():
    """
        Pipeline of transformations called
            - call db creation logic
            - call api formatting logic
            - insert returned formatted values into db

    """

    with connection.cursor() as cursor:
        # create table
        create_staging_table(cursor)

        # call api and parse the response
        ini_arr = api_formatting()

        # insert using optimized call, into the table
        cursor.executemany("""
            INSERT INTO staging_api_call VALUES (
                %(date_val)s,
                %(total_confirmed)s,
                %(total_tested)s,
                %(total_recovered)s,
                %(total_deceased)s,
                %(delta_confirmed)s,
                %(delta_tested)s,
                %(delta_recovered)s
            );
        """, ini_arr)


def api_formatting():
    """
            Pipeline of transformations for returning
                - call api logic
                - parse api response for null values
                - return values as array of dicts

    """

    url = "https://api.covid19india.org/v4/timeseries.json"
    try:
        response = requests.request("GET", url)
    except Exception:
        print("Error response")
    # print(response.json()["AN"]["dates"])

    # global array
    ini_Arr = []

    # parse json custom structure
    dates_key = response.json()["AN"]["dates"]

    # iterate over the custom json structure
    for i in dates_key:
        # print(i)
        # print(dates_key[i])
        if dates_key[i].get("delta", None):
            intr_dict = {
                "delta_confirmed": dates_key[i].get("delta", None).get(
                    "confirmed", None),
                "delta_recovered": dates_key[i].get("delta",
                                                    None).get(
                    "recovered", None),
                "delta_tested": dates_key[i].get("delta",
                                                 None).get(
                    "tested", None), "date_val": i}
        else:
            intr_dict = {"delta_confirmed": None, "delta_recovered": None,
                         "delta_tested": None, "date_val": i}

        intr_dict["total_recovered"] = dates_key[i].get("total", None).get(
            "recovered", None)
        intr_dict["total_confirmed"] = dates_key[i].get("total", None).get(
            "confirmed", None)
        intr_dict["total_deceased"] = dates_key[i].get("total", None).get(
            "deceased", None)
        intr_dict["total_tested"] = dates_key[i].get("total", None).get(
            "tested", None)

        ini_Arr.append(intr_dict)

        intr_dict = {}

    return ini_Arr

# call main function for the DAG pipeline call
api_to_db()

# if __name__ == '__main__':
#    api_to_db()
