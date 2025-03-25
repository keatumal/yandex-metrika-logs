import os
import sys
import argparse
import math
import csv
from string import Template
from datetime import datetime

from dotenv import load_dotenv
from tabulate import tabulate
import clickhouse_connect

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.clickhouse.types import columns_types
from utils.utils import fprint

from config import (
    CLICKHOUSE_BATCH_SIZE,
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


def get_number_of_rows(client, table: str) -> int:
    result: list = client.query(f"SELECT COUNT(*) FROM {table};").result_rows
    return result[0][0]


def convert_value(value, column_type):
    """Convert value to appropriate type based on ClickHouse schema."""
    if column_type.startswith("Nullable"):
        column_type = column_type[9:-1]
    if isinstance(value, str):
        value = value.lstrip(r"\'")
        value = value.rstrip(r"\'")
    if column_type.startswith("UInt") or column_type.startswith("Int"):
        return int(value) if value else None
    elif column_type == "Float32" or column_type == "Float64":
        return float(value) if value else None
    elif column_type == "Date":
        return datetime.strptime(value, "%Y-%m-%d").date() if value else None
    elif column_type == "DateTime":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None
    elif column_type.startswith("Array"):
        inner_type = column_type[6:-1]
        if inner_type.startswith("Nullable"):
            inner_type = inner_type[9:-1]
        value = value.lstrip("[")
        value = value.rstrip("]")
        return [convert_value(x, inner_type) for x in value.split(",")] if value else []
    elif column_type == "String":
        return str(value)
    else:
        raise ValueError(f"Unsupported type: {column_type}")


arg_parser = argparse.ArgumentParser(
    # TODO
    description=""
)
arg_parser.add_argument(
    "table_name",
    type=str,
)
arg_parser.add_argument(
    "-s",
    "--data-source",
    choices=["visits", "hits"],
    required=True,
    help="which data to work with: visits or events (hits)",
)

arg_group = arg_parser.add_mutually_exclusive_group(required=True)
arg_group.add_argument(
    "-i",
    "--import-file",
    metavar="TSV_FILENAME",
    type=str,
    help="import a file into the database",
)
arg_group.add_argument(
    "-c",
    "--create-table",
    action="store_true",
    help="create a new table",
)

arg_parser.add_argument(
    "-R",
    "--renamed-fields",
    action="store_true",
    help="use renamed fields",
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

    table_name = args.table_name
    table_fields_str = ""
    output_table = []

    ym_fields = [f.replace("<attr>", DEFAULT_ATTRIBUTION_MODEL) for f in ym_fields]

    for field in ym_fields:
        if field in FIELDS_RENAMING_MAPPING:
            if args.renamed_fields:
                table_fields_str += f"{FIELDS_RENAMING_MAPPING[field]} "
            else:
                table_fields_str += f"{field} "
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

if args.import_file:
    input_fname = args.import_file

    if args.data_source == "visits":
        ym_fields = CLICKHOUSE_VISITS_FIELDS
    else:
        ym_fields = CLICKHOUSE_EVENTS_FIELDS
    ym_fields = [f.replace("<attr>", DEFAULT_ATTRIBUTION_MODEL) for f in ym_fields]

    if args.renamed_fields:
        ch_fields = []
        for field in ym_fields:
            if field in FIELDS_RENAMING_MAPPING:
                ch_fields.append(FIELDS_RENAMING_MAPPING[field])
            else:
                print(
                    f"Field `{field}` of Clickhouse fields mapping is not available for renaming"
                )
                exit(1)
    else:
        ch_fields = ym_fields

    with open(input_fname, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        file_columns = next(reader)

    columns_difference = set(file_columns).difference(set(ch_fields))
    if len(columns_difference) > 0:
        print(
            "The field names in the input file do not match the database:\n\n",
            ", ".join(columns_difference),
        )
        exit(1)

    file_columns_types = []
    for file_col in file_columns:
        if args.renamed_fields:
            ym_col = ""
            for k, v in FIELDS_RENAMING_MAPPING.items():
                if v == file_col:
                    ym_col = k
            file_columns_types.append(columns_types[ym_col])
        else:
            file_columns_types.append(columns_types[file_col])

    print("Reading the input file…")
    with open(input_fname, "r") as f:
        total_rows = sum(1 for _ in f)
    total_rows -= 1

    ch = connect_to_clickhouse(
        host=conn_params["CLICKHOUSE_HOST"],
        port=int(conn_params["CLICKHOUSE_PORT"]),
        user=conn_params["CLICKHOUSE_USER"],
        password=conn_params["CLICKHOUSE_PASSWORD"],
    )
    if not ch:
        exit(1)

    table_name = args.table_name

    batches_num = math.ceil(total_rows / CLICKHOUSE_BATCH_SIZE)
    rows_num_before = get_number_of_rows(ch, table_name)

    with open(input_fname, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)

        batch = []
        batch_num = 1
        row_num = 1
        for row in reader:
            typed_row = []
            for idx in range(0, len(row)):
                typed_val = convert_value(row[idx], file_columns_types[idx])
                typed_row.append(typed_val)
            batch.append(typed_row)

            if len(batch) == CLICKHOUSE_BATCH_SIZE:
                progress_pct = round((row_num / total_rows) * 100, 2)
                fprint(
                    f"Uploading data: {row_num}/{total_rows} rows, {batch_num}/{batches_num} batches, {progress_pct}%"
                )
                try:
                    ch.insert(table_name, batch, column_names=file_columns)
                except Exception as e:
                    print(f"\nError while uploading data:\n\n{e}\n")
                    exit(1)
                batch.clear()
                batch_num += 1
            row_num += 1
        if batch:
            progress_pct = round((row_num / total_rows) * 100, 2)
            fprint(
                f"Uploading data: {row_num-1}/{total_rows} rows, {batch_num}/{batches_num} batches, {progress_pct}%"
            )
            ch.insert(table_name, batch, column_names=ch_fields)

    rows_num_after = get_number_of_rows(ch, table_name)
    print(
        f"\nDone. Rows before: {rows_num_before}, after: {rows_num_after}, "
        f"diff: {rows_num_after - rows_num_before}"
    )
