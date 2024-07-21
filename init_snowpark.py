import json
import pandas
from snowflake.snowpark import Session


def create_session():
    connection_parameters = None
    with open("./connection.json", "r") as ifs:
        connection_parameters = json.load(ifs)

    return Session.builder.configs(connection_parameters).create()

def load_table(session, table_name, database_name=None, database_schema=None, table=None):
    table_path = table_name
    if database_schema:
        table_path = f"{database_schema}.{table_path}"
    if database_schema:
        table_path = f"{database_name}.{table_path}"

    return session.table(table_path)

