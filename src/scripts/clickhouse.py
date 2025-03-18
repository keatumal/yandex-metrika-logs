import os
import sys
import argparse
from string import Template

from dotenv import load_dotenv
from tabulate import tabulate
import clickhouse_connect

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.clickhouse.types import columns_types
from utils.utils import fprint

from config import (
    CLICKHOUSE_VISITS_FIELDS,
    CLICKHOUSE_CREATE_VISITS_TABLE,
    CLICKHOUSE_EVENTS_FIELDS,
    CLICKHOUSE_CREATE_EVENTS_TABLE,
    FIELDS_RENAMING_MAPPING,
    DEFAULT_ATTRIBUTION_MODEL,
)


def connect_to_clickhouse(host: str, port: int, user: str, password: str):
    print(f"Connecting to Clickhouse: {user}@{host}:{port}…")

    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
        )
        return client
    except Exception as e:
        print(f"Can't connect:\n\n{e}\n")
        return None


arg_parser = argparse.ArgumentParser(
    # TODO
    description=""
)
arg_parser.add_argument(
    "-s",
    "--data-source",
    choices=["visits", "hits"],
    required=True,
    help="which data to work with: visits or events (hits)",
)
arg_parser.add_argument(
    "-c",
    "--create-table",
    metavar="TABLE_NAME",
    type=str,
    help="create a new table",
)

args = arg_parser.parse_args()

load_dotenv()

ENV_VARS = [
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
]
conn_params = dict()
for env_var in ENV_VARS:
    value = os.getenv(env_var)
    if not value and env_var != "CLICKHOUSE_PASSWORD":
        print(f"Environment variable `{env_var}` is missing", file=sys.stderr)
        exit(1)
    conn_params[env_var] = value


if args.create_table:
    if args.data_source == "visits":
        ym_fields = CLICKHOUSE_VISITS_FIELDS
        query_tmpl = Template(CLICKHOUSE_CREATE_VISITS_TABLE)
    else:
        ym_fields = CLICKHOUSE_EVENTS_FIELDS
        query_tmpl = Template(CLICKHOUSE_CREATE_EVENTS_TABLE)

    table_name = args.create_table
    table_fields_str = ""
    output_table = []

    ym_fields = [f.replace("<attr>", DEFAULT_ATTRIBUTION_MODEL) for f in ym_fields]

    for field in ym_fields:
        if field in FIELDS_RENAMING_MAPPING:
            table_fields_str += f"{FIELDS_RENAMING_MAPPING[field]} "
            table_fields_str += f"{columns_types[field]},"
            output_table.append(
                {"Field": FIELDS_RENAMING_MAPPING[field], "Type": columns_types[field]}
            )
        else:
            print(
                f"Field `{field}` of Clickhouse fields mapping is not available for renaming"
            )
            exit(1)

    table_fields_str = table_fields_str.rstrip(",")
    query = query_tmpl.substitute(table_name=table_name, table_fields=table_fields_str)
    ch = connect_to_clickhouse(
        host=conn_params["CLICKHOUSE_HOST"],
        port=int(conn_params["CLICKHOUSE_PORT"]),
        user=conn_params["CLICKHOUSE_USER"],
        password=conn_params["CLICKHOUSE_PASSWORD"],
    )
    if not ch:
        exit(1)
    print(f"Creating a table `{table_name}`…")
    try:
        ch.command(query)
    except Exception as e:
        print(f"Can't create a table:\n\n{e}\n")
        exit(1)

    print(f"Table `{table_name}` has been created:\n")
    print(tabulate(output_table, headers="keys", tablefmt="pipe"))
