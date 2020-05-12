# ╻ ╻╺┳╸╺┳╸┏━┓   ┏━╸╻ ╻╺┳┓ ┏━┓╻ ╻
# ┣━┫ ┃  ┃ ┣━┛   ┣╸ ┃╻┃ ┃┃ ┣━┛┗┳┛
# ╹ ╹ ╹  ╹ ╹  ╺━╸╹  ┗┻┛╺┻┛╹╹   ╹

# MIT License

# Copyright (c) 2020 Shane R. Spencer <spencersr@gmail.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# SPDX-License-Identifier: MIT

# Author: Shane R. Spencer <spencersr@gmail.com>

"""Multicorn based HTTP client PostgreSQL foreign data wrapper"""

import json

from datetime import datetime
from traceback import extract_stack
from urllib.parse import quote
from urllib.parse import quote_plus
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse
from urllib.parse import parse_qsl
from urllib.request import Request
from urllib.request import urlopen

from multicorn import ColumnDefinition
from multicorn import ForeignDataWrapper
from multicorn import TableDefinition
from multicorn.utils import DEBUG
from multicorn.utils import ERROR
from multicorn.utils import WARNING
from multicorn.utils import log_to_postgres


def json_isoformat_hack(o):
    if isinstance(o, datetime):
        return o.isoformat()


def true_or_false(bool_str):
    if bool_str == "true":
        return True
    return False


def post(url, body, body_type, encoding):

    headers = {}

    if body_type == "form":
        data = urlencode(body).encode(encoding)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body_type == "json":
        data = json.dumps(body, default=json_isoformat_hack).encode(encoding)
        headers["Content-Type"] = "application/json"

    request = Request(url=url, data=data, headers=headers, method="POST")

    response = urlopen(request)


def get(url):
    request = Request(url=url, method="GET")
    response = urlopen(request)


def url_append_data_as_query(url, query):
    parsed_url = urlparse(url)
    qsl = parse_qsl(parsed_url.query)
    qsl.extend((key, val) for (key, val) in query.items())
    parsed_url = parsed_url._replace(query=urlencode(qsl))
    return urlunparse(parsed_url)


def mapping_urlencode_variants(obj, encoding="utf-8"):
    mapping_safe = {quote(f"{key}"): quote_plus(str(val)) for (key, val) in obj.items()}
    mapping_unsafe = {
        quote(f"{key}_unsafe"): quote(str(val)) for (key, val) in obj.items()
    }
    return {**mapping_safe, **mapping_unsafe}


class HttpFDW(ForeignDataWrapper):
    def __init__(self, fdw_options, fdw_columns):
        super(HttpFDW, self).__init__(fdw_options, fdw_columns)

        self.url_initial = fdw_options["url"]
        self.execute_url_initial = fdw_options.get("execute_url", self.url_initial)
        self.insert_url_initial = fdw_options.get("insert_url", self.url_initial)

        self.url_prerendered = self.url_initial.format(**vars(self))
        self.execute_url_prerendered = self.execute_url_initial.format(**vars(self))
        self.insert_url_prerendered = self.insert_url_initial.format(**vars(self))

        self.method = fdw_options.get("method", "POST")
        self.execute_method = fdw_options.get("execute_method", self.method)
        self.insert_method = fdw_options.get("insert_method", self.method)

        self.body_type = fdw_options.get("body_type", "form")

        self.encoding = fdw_options.get("encoding", "utf-8")

        data_as_query = fdw_options.get("data_as_query", "false")
        self.data_as_query = true_or_false(data_as_query)

        self.fdw_options = fdw_options
        self.fdw_columns = fdw_columns

    @property
    def rowid_column(self):
        return "fdw_rowid"

    def execute(self, quals, columns):
        execute_url_rendered = self.execute_url_prerendered.format(
            http_fdw_function="execute",
            http_fdw_method=self.execute_method,
            **mapping_urlencode_variants(quals),
        )

        if self.data_as_query:
            insert_url_rendered = url_append_data_as_query(insert_url_rendered, quals)

        if self.method == "POST":
            post(insert_url_rendered, quals, self.body_type, self.encoding)
        elif self.method == "GET":
            get(insert_url_rendered)

    def insert(self, values):
        insert_url_rendered = self.insert_url_prerendered.format(
            http_fdw_function="insert",
            http_fdw_method=self.insert_method,
            **mapping_urlencode_variants(values),
        )

        if self.data_as_query:
            insert_url_rendered = url_append_data_as_query(insert_url_rendered, values)

        if self.method == "POST":
            post(insert_url_rendered, values, self.body_type, self.encoding)
        elif self.method == "GET":
            get(insert_url_rendered)
