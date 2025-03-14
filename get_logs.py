import time
import os
import re
import sys
import argparse

import pandas as pd
from dotenv import load_dotenv
from tapi_yandex_metrika import YandexMetrikaLogsapi

from config import WAIT_INTERVAL, FIELDS_MAP

def validate_iso_date(date_str):
    iso_format_regex = r"^\d{4}-\d{2}-\d{2}$"
    if not re.match(iso_format_regex, date_str):
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected format: YYYY-MM-DD")
    return date_str

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

client = YandexMetrikaLogsapi(
    access_token=AUTH_TOKEN,
    default_url_params={'counterId': args.counter_id},
)

params = {
    "fields": YM_FIELDS,
    "source": "visits",
    "date1": args.from_date,
    "date2": args.to_date,
}

# Check the possibility of creating a report
try:
    client.evaluate().get(params=params)
except Exception as e:
    print(f"There's no way to create a report. YM says:\n\n{e}", file=sys.stderr)
    exit(1)

if args.dry_run:
    print('Yes, a report can be created')
    exit(0)


print('Ordering report…')
result = client.create().post(params=params)
request_id = result["log_request"]["request_id"]

wait_counter: int = 1

while True:
    info = client.info(requestId=request_id).get()
    if info["log_request"]["status"] == "processed":
        break
    else:
        print(f"\rWaiting for report. It's been {(wait_counter-1) * WAIT_INTERVAL} seconds…", end='', flush=True)
        wait_counter += 1
        time.sleep(WAIT_INTERVAL)

print()
parts = info["log_request"]["parts"]
parts_len = len(parts)
print("Number of parts in the report: ", parts_len)

for part_num, part_info in enumerate(parts, start=1):
    print(f'\rPart {part_num}/{parts_len}: downloading', end=' '*10, flush=True)
    part_orig_num = part_info['part_number']
    part = client.download(requestId=request_id, partNumber=part_orig_num).get()
    print(f'\rPart {part_num}/{parts_len}: converting', end=' '*10, flush=True)
    part_dict = part().to_dicts()
    df = pd.DataFrame(part_dict, columns=FIELDS_MAP.keys())
    df.rename(columns=FIELDS_MAP, inplace=True)
    print(f'\rPart {part_num}/{parts_len}: saving', end=' '*10, flush=True)
    if part_num == 1:
        df.to_csv(output_fname, sep='\t', index=False, header=True, mode='w')
    else:
        df.to_csv(output_fname, sep='\t', index=False, header=False, mode='a')
    print()

print('\nDone')