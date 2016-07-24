from contextlib import closing
from requests import RequestException
import os
import requests
import subprocess
import sys
import yaml

import arrow
import json
import uuid

from datetime import datetime, date, time
from functools import partial


class ExtendedJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime) or isinstance(obj, date) or isinstance(obj, time) or isinstance(obj, arrow.arrow.Arrow):
            return arrow.get(obj).isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


dumps = partial(json.dumps, cls=ExtendedJsonEncoder)
loads = json.loads


class Session(object):
    def __init__(self, conf_file=None):
        if conf_file is None:
            if os.environ.get('API_CONF_FILE', None):
                self.conf_file = os.environ.get('API_CONF_FILE')
            else:
                self.conf_file = os.path.expanduser("~/.config/valk/config.yaml")
        else:
            self.conf_file = conf_file

        if self.conf_file is False:
            self.ephemeral_config = {}
        else:
            if not os.path.isfile(self.conf_file):
                conf_dir = os.path.dirname(self.conf_file)
                if not os.path.isdir(conf_dir):
                    os.makedirs(conf_dir)
                with open(self.conf_file, "w") as f:
                    yaml.dump({}, f)

    def get_conf(self):
        if self.conf_file is False:
            return self.ephemeral_config
        else:
            with open(self.conf_file, "r") as f:
                return yaml.load(f) or {}

    def set_conf(self, value):
        if self.conf_file is False:
            self.ephemeral_config = value
        else:
            with open(self.conf_file, "w") as f:
                yaml.dump(value, f)
            return value

    @property
    def authorization(self):
        return self.get_conf().get('headers', {}).get('Authorization', None)

    @authorization.setter
    def authorization(self, value):
        conf = self.get_conf()
        if conf.get('headers', None) is None:
            conf['headers'] = {}
        conf['headers']['Authorization'] = value
        self.set_conf(conf)
        return value

    @property
    def api_url(self):
        # env var API_URL over rules config setting
        return os.environ.get('API_URL', self.get_conf().get('url', None))

    @api_url.setter
    def api_url(self, value):
        value = value.rstrip("/")
        self.set_conf(dict(self.get_conf(), url=value))
        return value

    def req(self, method, url, data, expected_status='*', out=sys.stderr, fh=sys.stderr):
        try:
            with closing(self.request(method,
                                      url,
                                      data=data)) as r:
                status_ok, payload = self.pprint_response(r,
                                                          expected_status=expected_status,
                                                          dump_headers=True,
                                                          jq=True,
                                                          out=out,
                                                          fh=fh)
        except RequestException as e:
            sys.stderr.write("%s %s\n" % (method, url))
            sys.stderr.write(repr(e))
            sys.exit(2)

        if not status_ok:
            sys.exit(1)
        else:
            return r, payload

    def request(self, method, url, headers=None, **kwargs):
        method = method.upper()
        if headers is None:
            headers = {}
        if not headers.get('Authorization'):
            headers['Authorization'] = self.authorization
        if not headers.get('Accept'):
            headers['Accept'] = 'application/json'
        if not headers.get('Content-Type'):
            headers['Content-Type'] = 'application/json'
        data = kwargs.get('data', None)
        if data is not None:
            if not (isinstance(data, str) or isinstance(data, unicode)):
                kwargs['data'] = valkjson.dumps(data)
        url = url.lstrip("/")
        full_url = "{}/{}".format(self.api_url, url)
        req = requests.request(method, full_url, headers=headers, stream=True, **kwargs)
        response_headers = req.headers
        if response_headers.get('Authorization'):
            self.authorization = response_headers.get('Authorization')
        return req

    def pprint_response(self, r, expected_status='*', dump_headers=False, jq=False, out=sys.stdout, fh=sys.stdout):
        status_ok = expected_status == '*' or (r.status_code == int(expected_status))

        if dump_headers or not status_ok:
            out.write("SND %s %s\n" % (r.request.method, r.request.url))

        if dump_headers:
            for name in sorted(r.request.headers.keys()):
                out.write("SND %s: %s\n" % (name, r.request.headers[name]))
            if r.request.body:
                if jq:
                    with JQ(stdout=out) as j:
                        j.write(r.request.body)
                else:
                    out.write(r.request.body)

        out.write("\n")

        if dump_headers or not status_ok:
            out.write("RCV %s %s" % (r.status_code, r.reason))
            if not status_ok:
                out.write(" EXPECTED %s" % expected_status)
            out.write("\n")

        if dump_headers:
            for name in sorted(r.headers.keys()):
                out.write("RCV %s: %s\n" % (name, r.headers[name]))
            out.write("\n")

        data = []

        if r.headers.get('Content-Length', False):
            have_content = 0 < int(r.headers.get('Content-Length'))
        else:
            have_content = True

        if have_content:
            if jq and r.headers.get('Content-Type', '') == 'application/json':
                with JQ(stdout=out) as j:
                    for chunk in r.iter_content(64):
                        data += chunk
                        j.write(chunk)
            else:
                for chunk in r.iter_content(64):
                    data += chunk
                    fh.write(chunk)

            out.write("\n")

            if r.headers.get('Content-Type', '') == 'application/json':
                data = valkjson.loads(''.join(data))

        return status_ok, data


class JQ(object):
    def __init__(self, stdout=sys.stdout):
        self.stdout = stdout
        self.proc = None

    def __enter__(self):
        try:
            self.proc = subprocess.Popen(["jq", "."], stdin=subprocess.PIPE, stdout=self.stdout)
        except:
            self.proc = None
            return self.stdout
        return self.proc.stdin

    def __exit__(self, e_type, e, traceback):
        if self.proc:
            self.proc.stdin.close()
            self.proc.wait()
        return True
