import os
import sys
import argparse

from humanize import naturalsize
from tabulate import tabulate
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logs_api.logs_api import LogsAPI, ReportDeleteResult

arg_parser = argparse.ArgumentParser(description='')
arg_parser.add_argument('-c', '--counter-id', type=int, required=True, help='YM counter ID')

arg_group = arg_parser.add_mutually_exclusive_group(required=True)
arg_group.add_argument('-l', '--list', action='store_true', help='list all reports')
arg_group.add_argument('-d', '--delete', metavar='ID', type=int, help='delete report by ID')
arg_group.add_argument('-D', '--delete-all', action='store_true', help='delete all reports')

args = arg_parser.parse_args()

load_dotenv()
AUTH_TOKEN = os.getenv('YM_AUTH_TOKEN')
if not AUTH_TOKEN:
    print('Environment variable `YM_AUTH_TOKEN` is missing', file=sys.stderr)
    exit(1)

ym = LogsAPI(auth_token=AUTH_TOKEN, counter_id=args.counter_id)

if args.list:
    print('Getting a list of reports…')
    reports = ym.get_all_reports_info()
    reports = reports['requests']
    reports_len = len(reports)

    print(f'Reports found: {reports_len}\n')
    if reports_len == 0:
        exit(0)

    table = []
    total_size = 0

    for report in reports:
        table.append({
            'ID': report['request_id'],
            'Start date': report['date1'],
            'End date': report['date2'],
            'Attrib': report['attribution'],
            '# fields': len(report['fields']),
            '# parts': len(report['parts']),
            'Size': naturalsize(report['size'], binary=True),
            'Status': report['status']
        })
        total_size += report['size']

    print(tabulate(table, headers='keys', tablefmt='pipe'))
    print()
    print('Total size:', naturalsize(total_size, binary=True))

if args.delete:
    report_id = args.delete
    result: ReportDeleteResult = ym.delete_report(report_id)
    if result.success:
        print(f'Report #{report_id} has been successfully deleted')
    else:
        print(f"Can't delete the report. Error:\n\n{result.error}\n")

if args.delete_all:
    print('Getting a list of reports…')
    reports = ym.get_all_reports_info()
    reports = reports['requests']
    reports_len = len(reports)

    print(f'Reports found: {reports_len}\n')
    if reports_len == 0:
        exit(0)

    for report in reports:
        report_id = report['request_id']
        print(f'Deleting report #{report_id}… ', end='', flush=True)
        result: ReportDeleteResult = ym.delete_report(report_id)
        if result.success:
            print('DONE\n')
        else:
            print(f'FAILED. Error:\n{result.error}\n')