#!/usr/bin/env python
import configargparse
import io
import logging
import socketserver
import psycopg2
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from threading import Lock


_opts = configargparse.ArgParser()
_opts.add('--address', dest="address", action="store", default="localhost",
          help='the IP address for metrics exposure')
_opts.add('--port', dest="port", action="store", default=10000, type=int,
          help='the port for metrics exposure')
_opts.add('--db_url', required=True, dest="db_url", action="store", env_var="DB_URL",
          help='url for the db connection')
_opts.add('--verbose',
          action="store_const", const=logging.DEBUG, default=logging.INFO,
          help='verbose logging',
          dest="logging_level")

_args = _opts.parse_args()

logging.basicConfig(level=_args.logging_level,
                    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
                    datefmt="%Y-%m-%dT%H:%M:%S")

_logger = logging.getLogger("prometheus")

class NextcloudSqlStatsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with _conn.cursor() as cur:
            try:
                cur.execute("select count(*) from oc_filecache")
                count, = cur.fetchone()
                cur.execute("select sum(size) from oc_filecache where parent = -1")
                size, = cur.fetchone()

                with io.StringIO() as out:
                    out.write("# TYPE nextcloud_file_size_total gauge\n")
                    out.write("nextcloud_file_size_total %d\n" % size)

                    out.write("# TYPE nextcloud_file_count gauge\n")
                    out.write("nextcloud_file_count %d\n" % count)

                    self.send_response(HTTPStatus.OK)
                    body = out.getvalue().encode("utf-8")

                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()

                    self.wfile.write(body)
            except:
                _logger.warning("unexpected error.", exc_info=True)
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header('Content-Length', 0)
                self.end_headers()
                raise


    def log_message(self, format, *args):
        pass

_conn = psycopg2.connect(_args.db_url)

try:
    with socketserver.ThreadingTCPServer((_args.address, _args.port), NextcloudSqlStatsHandler) as httpd:
        _logger.info("listening on {}".format(httpd.server_address))
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
finally:
    _conn.close()
