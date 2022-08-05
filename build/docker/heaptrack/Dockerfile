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

FROM milvusdb/openblas:ubuntu18.04-20210428 AS build

RUN apt-get update && apt-get install -y --no-install-recommends wget curl ca-certificates gnupg2 && \
  wget -qO- "https://cmake.org/files/v3.18/cmake-3.18.6-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
  apt-get update && apt-get install -y --no-install-recommends \
  g++ gcc make libssl-dev zlib1g-dev \
  libboost-regex-dev libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-serialization-dev libboost-iostreams-dev \
  libunwind-dev && \
  apt-get remove --purge -y && \
  rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/heaptrack/ && wget -qO- "https://github.com/KDE/heaptrack/archive/refs/tags/v1.3.0.tar.gz" | tar --strip-components=1 -xz -C /opt/heaptrack

RUN cd /opt/heaptrack && \
  mkdir build && \
  cd build && \
  cmake -DCMAKE_BUILD_TYPE=Release .. && \
  make -j$(nproc)

RUN cd /opt/heaptrack/build && \
  make install

RUN rm -rf /opt/heaptrack

FROM ubuntu:bionic-20200921

COPY --from=build /usr/local/bin/heaptrack /opt/heaptrack/bin/heaptrack
COPY --from=build /usr/local/lib/heaptrack /opt/heaptrack/lib/heaptrack
COPY --from=build /usr/lib/x86_64-linux-gnu/libunwind* /opt/heaptrack/lib/

CMD ["tail", "-f", "/dev/null"]

