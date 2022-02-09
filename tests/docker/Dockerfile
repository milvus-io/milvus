# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

FROM python:3.6.8-jessie

RUN apt-get update && apt-get install -y jq

COPY ./tests/python_client/requirements.txt /requirements.txt

RUN python3 -m pip install --no-cache-dir -r /requirements.txt

CMD ["tail", "-f", "/dev/null"]
