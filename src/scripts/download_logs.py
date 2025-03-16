import time
import os
import re
import sys
import argparse
import datetime as dt

import pandas as pd
from dotenv import load_dotenv
from humanize import naturaldelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import WAIT_INTERVAL, FIELDS_MAP
from logs_api.logs_api import LogsAPI, ReportCheckResult


def validate_iso_date(date_str: str):
    iso_format_regex = r"^\d{4}-\d{2}-\d{2}$"
    if not re.match(iso_format_regex, date_str):
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected format: YYYY-MM-DD")
    return date_str

def fprint(line: str, **kwargs):
    print(' ' * 100, end='\r')
    print(line, end='\r', flush=True, **kwargs)


arg_parser = argparse.ArgumentParser(description='Downloads Yandex Metrika logs and saves them in TSV format.')
arg_parser.add_argument('-c', '--counter-id', type=int, required=True, help='YM counter ID')
arg_parser.add_argument('-f', '--from-date', type=validate_iso_date, required=True,
                        help='start date in ISO format (YYYY-MM-DD)')
arg_parser.add_argument('-t', '--to-date', type=validate_iso_date, required=True,
                        help='end date in ISO format (YYYY-MM-DD)')
arg_parser.add_argument('-o', '--output-file', type=str, help='output file name')
arg_parser.add_argument('-d', '--dry-run', action='store_true',
                        help='only to check if the report can be created')
args = arg_parser.parse_args()

load_dotenv()
AUTH_TOKEN = os.getenv('YM_AUTH_TOKEN')
if not AUTH_TOKEN:
    print('Environment variable `YM_AUTH_TOKEN` is missing', file=sys.stderr)
    exit(1)

output_fname = args.output_file or f'{args.counter_id}_{args.from_date}_{args.to_date}.tsv'

if os.path.exists(output_fname):
    print(f'Output file already exists: {output_fname}', file=sys.stderr)
    exit(1)


YM_FIELDS = ','.join(FIELDS_MAP.keys())
DF_COLUMNS = FIELDS_MAP.values()

ym = LogsAPI(
    fields=YM_FIELDS,
    auth_token=AUTH_TOKEN,
    counter_id=args.counter_id,
    start_date=args.from_date,
    end_date=args.to_date,
)

if args.dry_run:
    result: ReportCheckResult = ym.check_reporting_capability()
    if result.success:
        print('Yes, a report can be created.')
        exit(0)
    else:
        print(f'The report cannot be created. Error:\n\n{result.error}\n')
        exit(1)

print('Ordering report…')
request_id = ym.create_report()

wait_counter: int = 1

while True:
    if ym.is_report_ready(request_id):
        break
    else:
        elapsed_time = (wait_counter - 1) * WAIT_INTERVAL
        elapsed_time = dt.timedelta(seconds=elapsed_time)
        elapsed_time = naturaldelta(elapsed_time)
        fprint(f"Waiting for report. It's been {elapsed_time}…")
        wait_counter += 1
        time.sleep(WAIT_INTERVAL)

print()

report_info = ym.get_report_info(request_id)
parts = report_info['log_request']['parts']
parts_len = len(parts)
print("Number of parts in the report: ", parts_len)

for part_num, part_info in enumerate(parts, start=1):
    fprint(f'Part {part_num}/{parts_len}: downloading')
    part_orig_num = part_info['part_number']
    part = ym.download_report_part(request_id, part_orig_num)
    fprint(f'Part {part_num}/{parts_len}: converting')
    part_dict = part().to_dicts()
    df = pd.DataFrame(part_dict, columns=FIELDS_MAP.keys())
    df.rename(columns=FIELDS_MAP, inplace=True)
    fprint(f'Part {part_num}/{parts_len}: saving')
    if part_num == 1:
        df.to_csv(output_fname, sep='\t', index=False, header=True, mode='w')
    else:
        df.to_csv(output_fname, sep='\t', index=False, header=False, mode='a')

    fprint(f'Part {part_num}/{parts_len}: done')

print()
print(f'The report is saved in {output_fname}')