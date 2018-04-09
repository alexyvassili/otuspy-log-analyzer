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


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='config.json')
    return parser


def get_config(module_config: dict) -> dict:
    parser = create_parser()
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


def get_file_date(entry: os.DirEntry) -> datetime.date:
    return datetime.fromtimestamp(entry.stat().st_mtime).date()


def get_reportfile_name(file_date: datetime.date, prefix='report') -> str:
    datestr = file_date.strftime('%Y.%m.%d')
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
    open_func = gzip.open if filename.endswith('.gz') else open
    with open_func(filename, 'rb') as f:
        urls_counts, urls_times = parse_lines(f)
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


def _get_timestamp(entry: os.DirEntry) -> datetime:
    return datetime.fromtimestamp(entry.stat().st_mtime)


def _get_date_from_mtime(entry: os.DirEntry) -> datetime.date:
    return _get_timestamp(entry).date()


def get_filename(log_dir: str, report_dir: str) -> os.DirEntry:
    logs = [e for e in os.scandir(log_dir) if e.is_file() and 'nginx-access-ui' in e.name]
    if not logs: return None
    reports = [e for e in os.scandir(report_dir) if e.is_file() and e.name.endswith('.html')]
    reports_dates = [get_file_date(report) for report in reports]
    logs.sort(key=_get_timestamp, reverse=True)
    log_entry = logs[0]
    is_report_generated = get_file_date(log_entry) in reports_dates
    return None if is_report_generated else log_entry


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


def main(config) -> None:
    logging.info('Get filename')
    file_entry = get_filename(config["LOG_DIR"], config["REPORT_DIR"])
    if not file_entry:
        logging.info('Last report is already generated, exiting')
        return
    filename = file_entry.name
    file_date = get_file_date(file_entry)
    logging.info('Count urls')
    urls_counts, urls_times = parse_file(os.path.join(config["LOG_DIR"], filename))
    logging.info('Count stats')
    statistic = get_statistic(urls_counts, urls_times, config['REPORT_SIZE'])
    logging.info('Rendering')
    report_file = os.path.join(config["REPORT_DIR"], get_reportfile_name(file_date))
    logging.info(f"Save to: {report_file}")
    render_report(statistic, report_file)
    create_ts_file(config["TS_FILE"])


if __name__ == "__main__":
    try:
        config = get_config(config)
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL,
                            filename=config['LOGGING_FILE'])
        main(config)
    except Exception as e:
        logging.exception(e)
