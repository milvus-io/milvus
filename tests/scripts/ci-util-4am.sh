#!/bin/bash

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

# Exit immediately for non zero status
set -e
# Check unset variables
set -u
# Print commands
set -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"


# Install pytest requirements
function install_pytest_requirements(){
 echo "Install pytest requirements"
 cd ${ROOT}/tests/python_client

# MIRROR_HOST="${MIRROR_HOST:-nexus-nexus-repository-manager.nexus}"


export PIP_TRUSTED_HOST="nexus-ci.zilliz.cc"
export PIP_INDEX_URL="https://nexus-ci.zilliz.cc/repository/pypi-all/simple"
export PIP_INDEX="https://nexus-ci.zilliz.cc/repository/pypi-all/pypi"
export PIP_FIND_LINKS="https://nexus-ci.zilliz.cc/repository/pypi-all/pypi"
 python3 -m pip install --no-cache-dir -r requirements.txt --timeout 30 --retries 6
 pip uninstall pytest-tags -y || true
}

# Login in ci docker registry
function docker_login_ci_registry(){

    if [[ -z "${CI_REGISTRY_USERNAME:-}" || -z "${CI_REGISTRY_PASSWORD:-}" ]]; then 
       echo "Please setup docker credential for ci registry-${HUB}"
    else
        echo "docker login ci registry"
        docker login  -u ${CI_REGISTRY_USERNAME} -p ${CI_REGISTRY_PASSWORD} ${HUB}
    fi
}

