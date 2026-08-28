# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Milvus-style stderr logging shared by the supervisor and its workers."""

import json
import logging
from datetime import datetime


class MilvusFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).astimezone()
        offset = timestamp.strftime("%z")
        date = timestamp.strftime("%Y/%m/%d %H:%M:%S")
        date += f".{timestamp.microsecond // 1000:03d} {offset[:3]}:{offset[3:]}"
        level = "WARN" if record.levelno == logging.WARNING else record.levelname
        message = json.dumps(record.getMessage(), ensure_ascii=False)
        line = f"[{date}] [{level}] [PyUDF] [{record.filename}:{record.lineno}] [{message}] [pid={record.process}]"
        if record.exc_info:
            line += " [stack=" + json.dumps(self.formatException(record.exc_info), ensure_ascii=False) + "]"
        if record.stack_info:
            line += " [stackInfo=" + json.dumps(record.stack_info, ensure_ascii=False) + "]"
        return line


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(MilvusFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
