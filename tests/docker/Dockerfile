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

FROM python:3.8-buster

RUN apt-get update && apt-get install -y jq

COPY ./tests/python_client/requirements.txt /requirements.txt

RUN python3 -m pip install --upgrade setuptools \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install --no-cache-dir  -r /requirements.txt --timeout 30 --retries 6

COPY ./tests/compression/python_client/requirements.txt /requirements.txt
RUN python3 -m pip install pytest-benchmark \
    && python3 -m pip install --no-cache-dir  -r /requirements.txt --timeout 30 --retries 6

# Install Go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV GO111MODULE on
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.20.7.linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
    mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
    go clean --modcache && \
    chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

CMD ["tail", "-f", "/dev/null"]
