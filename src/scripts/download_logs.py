import time
import os
import re
import sys
import argparse
import datetime as dt

import pandas as pd
from dotenv import load_dotenv
from humanize import naturaldelta, naturalsize

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    WAIT_INTERVAL,
    DEFAULT_ATTRIBUTION_MODEL,
    ATTRIBUTION_RENAMING_MAPPING,
    DOWNLOAD_FIELDS,
    DOWNLOAD_SOURCE,
    FIELDS_RENAMING_MAPPING,
)
from utils.utils import fprint
from logs_api.logs_api import LogsAPI, OperationResult


def validate_iso_date(date_str: str):
    iso_format_regex = r"^\d{4}-\d{2}-\d{2}$"
    if not re.match(iso_format_regex, date_str):
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Expected format: YYYY-MM-DD"
        )
    return date_str


def validate_args(args):
    if args.report_id is not None and (
        args.from_date is not None or args.to_date is not None
    ):
        print("Error: you cannot use -r at the same time as -f and -t")
        sys.exit(1)
    if args.report_id is None and (args.from_date is None or args.to_date is None):
        print("Error: you must specify either -r or both -f and -t.")
        sys.exit(1)


arg_parser = argparse.ArgumentParser(
    description="Downloads Yandex Metrika logs and saves them in TSV format"
)
arg_parser.add_argument(
    "-c", "--counter-id", type=int, required=True, help="YM counter ID"
)
arg_parser.add_argument(
    "-r", "--report-id", type=int, help="download a ready-made report with ID"
)
arg_parser.add_argument(
    "-f",
    "--from-date",
    type=validate_iso_date,
    help="start date in ISO format (YYYY-MM-DD)",
)
arg_parser.add_argument(
    "-t",
    "--to-date",
    type=validate_iso_date,
    help="end date in ISO format (YYYY-MM-DD)",
)
arg_parser.add_argument(
    "-R", "--dont-rename", action="store_true", help="do not rename field names"
)
arg_parser.add_argument("-o", "--output-file", type=str, help="output file name")
arg_parser.add_argument(
    "-d",
    "--dry-run",
    action="store_true",
    help="only to check if the report can be created",
)
args = arg_parser.parse_args()
validate_args(args)

load_dotenv()
AUTH_TOKEN = os.getenv("YM_AUTH_TOKEN")
if not AUTH_TOKEN:
    print("Environment variable `YM_AUTH_TOKEN` is missing", file=sys.stderr)
    exit(1)

output_fname = (
    args.output_file or f"{args.counter_id}_{args.from_date}_{args.to_date}.tsv"
)

if os.path.exists(output_fname):
    print(f"Output file already exists: {output_fname}", file=sys.stderr)
    exit(1)


# if DOWNLOAD_SOURCE == "visits":
#     renaming_map = VISITS_FIELDS_MAPPING
# elif DOWNLOAD_SOURCE == "hits":
#     renaming_map = EVENT_FIELDS_MAPPING
# else:
#     print("DOWNLOAD_SOURCE should be `visits` or `hits`", file=sys.stderr)
#     exit(1)

if DEFAULT_ATTRIBUTION_MODEL not in ATTRIBUTION_RENAMING_MAPPING.keys():
    print(
        f"`DEFAULT_ATTRIBUTION_MODEL` must be one of: {', '.join(ATTRIBUTION_RENAMING_MAPPING.keys())}",
        file=sys.stderr,
    )
    exit(1)

report_fields = [
    f.replace("<attr>", DEFAULT_ATTRIBUTION_MODEL) for f in DOWNLOAD_FIELDS
]

df_columns = []
for field in report_fields:
    if field in FIELDS_RENAMING_MAPPING:
        df_columns.append(FIELDS_RENAMING_MAPPING[field])
    else:
        print(f"Field `{field}` of DOWNLOAD_FIELDS is not available for renaming")
        exit(1)

if not args.report_id:
    ym = LogsAPI(
        fields=report_fields,
        auth_token=AUTH_TOKEN,
        counter_id=args.counter_id,
        start_date=args.from_date,
        end_date=args.to_date,
        source=DOWNLOAD_SOURCE,
    )
    if args.dry_run:
        result: OperationResult = ym.check_reporting_capability()
        if result.success:
            print("Yes, a report can be created.")
            exit(0)
        else:
            print(f"The report cannot be created. Error:\n\n{result.error}\n")
            exit(1)

    print("Ordering report…")
    request_id = ym.create_report()
else:
    ym = LogsAPI(auth_token=AUTH_TOKEN, counter_id=args.counter_id)
    request_id = args.report_id

    try:
        info = ym.get_report_info(request_id)
    except Exception as e:
        print(
            f"It appears that report #{request_id} does not exist. "
            f"An error occurred while retrieving information about it:\n\n{e}\n"
        )
        exit(1)

    start_date = info["log_request"]["date1"]
    end_date = info["log_request"]["date2"]
    output_fname = args.output_file or f"{args.counter_id}_{start_date}_{end_date}.tsv"

wait_counter: int = 0

while True:
    if ym.is_report_ready(request_id):
        break
    else:
        elapsed_time = wait_counter * WAIT_INTERVAL
        elapsed_time = dt.timedelta(seconds=elapsed_time)
        elapsed_time = naturaldelta(elapsed_time)
        fprint(f"Waiting for report. It's been {elapsed_time}…")
        wait_counter += 1
        time.sleep(WAIT_INTERVAL)

if wait_counter > 0:
    print()

report_info = ym.get_report_info(request_id)
parts = report_info["log_request"]["parts"]
parts_len = len(parts)
report_size = report_info["log_request"]["size"]
print("Number of parts in the report:", parts_len)
print("Report size:", naturalsize(report_size, binary=True))

for part_num, part_info in enumerate(parts, start=1):
    fprint(f"Part {part_num}/{parts_len}: downloading")
    part_orig_num = part_info["part_number"]
    part = ym.download_report_part(request_id, part_orig_num)
    fprint(f"Part {part_num}/{parts_len}: converting")
    part_dict = part().to_dicts()
    df = pd.DataFrame(part_dict, columns=report_fields)
    if not args.dont_rename:
        df.rename(columns=dict(zip(report_fields, df_columns)), inplace=True)
    fprint(f"Part {part_num}/{parts_len}: saving")
    if part_num == 1:
        df.to_csv(output_fname, sep="\t", index=False, header=True, mode="w")
    else:
        df.to_csv(output_fname, sep="\t", index=False, header=False, mode="a")

    fprint(f"Part {part_num}/{parts_len}: done")

print()
print(f"The report is saved in {output_fname}")
