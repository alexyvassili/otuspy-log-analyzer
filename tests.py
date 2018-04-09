import unittest
from log_analyzer import *


class LogAnalyzerTest(unittest.TestCase):

    def test_merge_config(self):
        module_config = {
            "REPORT_SIZE": 1000,
            "REPORT_DIR": "./reports",
            "LOG_DIR": "./log",
            "LOGGING_FILE": None,
            "TS_FILE": '/var/tmp/log_analyzer.ts',
        }
        file_config = {
            "REPORT_SIZE": 3000,
            "LOG_DIR": "./logs",
            "LOGGING_FILE": None,
        }
        config = {
            "REPORT_SIZE": 3000,
            "REPORT_DIR": "./reports",
            "LOG_DIR": "./logs",
            "LOGGING_FILE": None,
            "TS_FILE": '/var/tmp/log_analyzer.ts',
        }
        self.assertEqual(config, merge_config(module_config, file_config))

    def test_get_date_from_logname(self):
        pass

    def test_get_reportfile_name(self):
        pass

    def test_get_item_simple(self):
        self.assertEqual('this', get_item(['this', 'is', 'sample', 'item']))

    def test_get_item_simple_quotes(self):
        self.assertEqual('"this is sample"', get_item(['"this', 'is', 'sample"', 'item']))

    def test_get_item_simple_brackets(self):
        self.assertEqual('[this is sample item]', get_item(['[this is', 'sample', 'item]']))

    def test_format_parsed(self):
        pass

    def test_parse(self):
        line = b'1.196.116.32 -  - [29/Jun/2017:03:50:22 +0300] "GET /api/v2/banner/25019354 HTTP/1.1" 200 927 "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" "1498697422-2190034393-4708-9752759" "dc7161be3" 0.390\n'
        parsed = {'$remote_addr': '1.196.116.32', '$remote_user': '-', '$http_x_real_ip': '-',
         '$time_local': '[29/Jun/2017:03:50:22 +0300]', '$request': 'GET /api/v2/banner/25019354 HTTP/1.1',
         '$status': '200', '$body_bytes_sent': '927', '$http_referer': '-',
         '$http_user_agent': 'Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5', '$http_x_forwarded_for': '-',
         '$http_X_REQUEST_ID': '1498697422-2190034393-4708-9752759', '$http_X_RB_USER': 'dc7161be3',
         '$request_time': '0.390',}
        self.assertEqual(parsed, parse(line))

    def test_get_url_time(self):
        parsed = {'$remote_addr': '1.196.116.32', '$remote_user': '-', '$http_x_real_ip': '-',
                  '$time_local': '[29/Jun/2017:03:50:22 +0300]', '$request': 'GET /api/v2/banner/25019354 HTTP/1.1',
                  '$status': '200', '$body_bytes_sent': '927', '$http_referer': '-',
                  '$http_user_agent': 'Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5',
                  '$http_x_forwarded_for': '-',
                  '$http_X_REQUEST_ID': '1498697422-2190034393-4708-9752759', '$http_X_RB_USER': 'dc7161be3',
                  '$request_time': '0.390', }
        self.assertEqual(('/api/v2/banner/25019354', 0.39), get_url_time(parsed))

    def test_parse_lines(self):
        pass

    def test_get_statistic(self):
        pass


if __name__ == '__main__' :
    unittest.main()