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

FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg2 apt-transport-https && \
    wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list' && \
    wget -qO- "https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    apt-get update && apt-get install -y --no-install-recommends \
    g++ git gfortran lsb-core ccache \
    libboost-serialization-dev libboost-filesystem-dev libboost-system-dev libboost-regex-dev \
    curl libtool automake libssl-dev pkg-config libcurl4-openssl-dev python3-pip \
    clang-format-6.0 clang-tidy-6.0 \
    lcov mysql-client libmysqlclient-dev intel-mkl-gnu-2019.5-281 intel-mkl-core-2019.5-281 libopenblas-dev liblapack3 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so \
  /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

RUN sh -c 'echo export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2019.5.281/linux/mkl/lib/intel64:\$LD_LIBRARY_PATH > /etc/profile.d/mkl.sh'

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

WORKDIR /root

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "start" ]
