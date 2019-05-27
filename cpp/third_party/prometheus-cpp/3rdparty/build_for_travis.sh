#!/bin/bash

set -euo pipefail

THIRDPARTY_ROOT=$(cd $(dirname "${BASH_SOURCE[0]}") && /bin/pwd -P)
INSTALL_PREFIX="${TRAVIS_BUILD_DIR:?}/_opt"

mkdir "${THIRDPARTY_ROOT}/civetweb/_build"
cd "${THIRDPARTY_ROOT}/civetweb/_build"
cmake  .. -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" -DCIVETWEB_ENABLE_CXX=ON -DCIVETWEB_ENABLE_SSL=OFF -DCIVETWEB_BUILD_TESTING=OFF
make -j4
make install

mkdir "${THIRDPARTY_ROOT}/googletest/_build"
cd "${THIRDPARTY_ROOT}/googletest/_build"
cmake  .. -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"
make -j4
make install
