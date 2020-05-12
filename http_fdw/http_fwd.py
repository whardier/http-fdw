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

from urllib.request import urlopen

from multicorn import ColumnDefinition
from multicorn import ForeignDataWrapper
from multicorn import TableDefinition
from multicorn.utils import DEBUG
from multicorn.utils import ERROR
from multicorn.utils import WARNING
from multicorn.utils import log_to_postgres


# ╻ ╻╺┳╸╺┳╸┏━┓┏━╸╺┳┓╻ ╻
# ┣━┫ ┃  ┃ ┣━┛┣╸  ┃┃┃╻┃
# ╹ ╹ ╹  ╹ ╹  ╹  ╺┻┛┗┻┛
class HttpFDW(ForeignDataWrapper):
    def __init__(self, fdw_options, fdw_columns):
        super(HttpFDW, self).__init__(fdw_options, fdw_columns)

        self.url_template = fdw_options["url"]
        # log_to_postgres(repr((fdw_options, fdw_columns)), level=DEBUG)

    def execute(self, quals, columns):
        log_to_postgres(repr((quals, columns)), level=DEBUG)

    def insert(self, values):
        log_to_postgres(repr((values,)), level=DEBUG)

    @property
    def rowid_column(self):
        return "fdw_rowid"
