#!/usr/bin/env bash
set -x
set -e

function installswig() {
    # Need SWIG >= 3.0.8
    cd /tmp/ &&
        wget https://github.com/swig/swig/archive/rel-3.0.12.tar.gz &&
        tar zxf rel-3.0.12.tar.gz && cd swig-rel-3.0.12 &&
        ./autogen.sh && ./configure --prefix "${HOME}" 1>/dev/null &&
        make >/dev/null &&
        make install >/dev/null
}

if [ "${TRAVIS_OS_NAME}" == linux ]; then
    installswig
fi
