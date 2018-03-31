#!/usr/bin/env python

# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import argparse
import gzip
import numpy as np
import json
import os
import sys
from dateutil.parser import parse as parsedate
from collections import defaultdict
from operator import itemgetter
import logging
from datetime import datetime


LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO


config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "LOGGING_FILE": None,
    "TS_FILE": '/var/tmp/log_analyzer.ts'
}


def createParser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='config.json')
    return parser


def get_config(module_config: dict) -> dict:
    parser = createParser()
    namespace = parser.parse_args()
    config_file = namespace.config
    if not os.path.exists(config_file):
        raise(FileNotFoundError(f'Config File {config_file} not found'))
    logging.info(f"Config file: {config_file}")
    with open(config_file) as f:
        conf_string = f.read()
    config_from_file = json.loads(conf_string) if conf_string else dict()  # TODO здесь будет исключение
    return merge_config(module_config, config_from_file) if config_from_file else module_config


def merge_config(module_config: dict, config_from_file: dict) -> dict:
    config = module_config
    for k, v in config_from_file.items():
        config[k] = v
    return config


log_format = ('$remote_addr', '$remote_user', '$http_x_real_ip', '$time_local', '$request',
              '$status', '$body_bytes_sent', '$http_referer', '$http_user_agent', "$http_x_forwarded_for",
              '$http_X_REQUEST_ID', '$http_X_RB_USER', '$request_time')

config = get_config(config)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL,
                            filename=config['LOGGING_FILE'])

def get_date_from_logname(logname: str) -> datetime:
    logname = os.path.splitext(logname)[0]
    logname = logname.split('-')
    date = logname[-1]
    date = parsedate(date)
    return date


def get_reportfile_name(logname: str, prefix='report') -> str:
    date = get_date_from_logname(logname)
    datestr = date.strftime('%Y.%m.%d')
    return f"{prefix}-{datestr}.html"


def get_item(line: list, item='') -> str:
    """получаем элементы по одному, сшиваем значения между "" и []"""
    item = ' '.join([item, line.pop(0)]) if item else line.pop(0)
    if ('[' in item) and (']' not in item):
        item = get_item(line, item)
    elif item.count('"') == 1:
        item = get_item(line, item)
    return item


def format_parsed(parsed_line: list) -> dict:
    return dict((i for i in zip(log_format, parsed_line)))


def parse(line: bytes) -> dict:
    line = line.decode(encoding="utf=8").strip()
    parsed = []
    line = line.split()
    while line:
        parsed.append(get_item(line).strip('"'))
    return format_parsed(parsed)


def get_url_time(parsed: dict) -> tuple:
    if parsed['$request'] == '0':
        return 'unparsed', 0.
    try:
        method, url, http = parsed['$request'].split()
        request_time = float(parsed['$request_time'])
    except Exception:
        return 'unparsed', 0
    return url, request_time


def parse_file(filename: str) -> tuple:
    f = gzip.open(filename, 'rb') if filename.endswith('.gz') else open(filename, 'rb')
    urls_counts, urls_times = parse_lines(f)
    f.close()
    return urls_counts, urls_times


def parse_lines(f) -> tuple:
    """словарь url: количество запросов
    словарь url: список request time"""
    urls_counts = defaultdict(lambda: 0)
    urls_times = defaultdict(list)
    logging.info("Parsing lines... ")
    for line in f:
        record = parse(line)
        url, request_time = get_url_time(record)
        urls_counts[url] += 1
        urls_times[url].append(request_time)
    return urls_counts, urls_times


def get_filename(log_dir: str, report_dir: str) -> str:
    logs = [e.name for e in os.scandir(log_dir) if e.is_file() and 'nginx-access-ui' in e.name]
    if not logs: return ''
    reports = [e.name for e in os.scandir(report_dir) if e.is_file() and not e.name.startswith('.')]
    reports_dates = [get_date_from_logname(report) for report in reports]
    logs.sort(key=get_date_from_logname)
    logname = logs[-1]
    is_report_generated = get_date_from_logname(logname) in reports_dates
    return '' if is_report_generated else logname


def get_statistic(url_counts:dict, url_times:dict, report_size:int) -> list:
    logging.info('Count url sum time')
    url_sum_time = {url: sum(times) for url, times in url_times.items()}
    main_statistic = dict()
    logging.info('Count all requests count')
    main_statistic['all_requests_count'] = sum(url_counts.values())
    unparsed_count = url_counts['unparsed']
    logging.info(f"Unparsed: {url_counts['unparsed']} items.")
    unparsed_perc =  unparsed_count / main_statistic['all_requests_count']
    logging.info(f"Unparsed part: {unparsed_perc}")
    if unparsed_perc > 0.5:
        logging.warning('Unparsed part > 0.5, exiting.')
        sys.exit(1)
    logging.info('Count all sum time')
    main_statistic['sum_time'] = sum(url_sum_time.values())
    statistic = []
    logging.info('Count statistic')
    for url in url_counts:
        # count ‐ сколько раз встречается URL, абсолютное значение
        count = url_counts[url]
        # count_perc ‐ сколько раз встречается URL, в процентах относительно общего числа запросов
        count_perc = (count / main_statistic['all_requests_count']) * 100
        # time_sum ‐ суммарный ```$request_time``` для данного URL'а, абсолютное значение
        time_sum = sum(url_times[url])
        # time_perc ‐ суммарный ```$request_time``` для данного URL'а, в процентах относительно общего $request_time всех
        # запросов
        time_perc = (url_sum_time[url] / main_statistic['sum_time']) * 100
        # time_avg ‐ средний ```$request_time``` для данного URL'а
        time_avg = np.mean(url_times[url])
        # time_max ‐ максимальный ```$request_time``` для данного URL'а
        time_max = max(url_times[url])
        # time_med ‐ медиана ```$request_time``` для данного URL'а
        time_med = np.median(url_times[url])
        statistic.append({
            'url': url,
            'count': count,
            'count_perc': round(count_perc, 3),
            'time_sum': round(time_sum, 3),
            'time_perc': round(time_perc, 3),
            'time_avg': round(time_avg, 3),
            'time_max': round(time_max, 3),
            'time_med': round(time_med, 3),
        })
    logging.info(f'len statistics: {len(statistic)}')
    logging.info('sorting...')
    statistic.sort(key=itemgetter('time_sum'), reverse=True)
    statistic = statistic[:report_size]
    return statistic


def render_report(statistic: list, report_file: str) -> None:
    with open('report.html') as f:
        report = f.read()
    statistic_json = json.dumps(statistic)
    statistic_str = statistic_json.replace('}, {', '},\n {')
    report = report.replace('$table_json', statistic_str)

    with open(report_file, 'w') as f:
        f.write(report)


def create_ts_file(ts_file: str) -> None:
    with open(ts_file, 'w') as f:
        ts = str(datetime.now().timestamp())
        f.write(ts)


def main() -> None:
    logging.info('Get filename')
    filename = get_filename(config["LOG_DIR"], config["REPORT_DIR"])
    if not filename:
        logging.info('Last report is already generated, exiting')
        return
    logging.info('Count urls')
    urls_counts, urls_times = parse_file(os.path.join(config["LOG_DIR"], filename))
    logging.info('Count stats')
    statistic = get_statistic(urls_counts, urls_times, config['REPORT_SIZE'])
    logging.info('Rendering')
    report_file = os.path.join(config["REPORT_DIR"], get_reportfile_name(filename))
    logging.info(f"Save to: {report_file}")
    render_report(statistic, report_file)
    create_ts_file(config["TS_FILE"])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e)
