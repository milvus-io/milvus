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
 python3 -m pip install --no-cache-dir -r requirements.txt
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

# Check if requirement file changed or not in PR
function check_requirement_file(){
     URL="https://api.github.com/repos/milvus-io/milvus/pulls/$CHANGE_ID/files"

     CHANGED_FILES=( $(curl -s --max-time 10 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET -G ${URL} \
     | jq -r  '.[] | select (.filename | endswith("requirements.txt") ) | .filename') )

      for file in ${CHANGED_FILES[@]}
      do
          if [[ "${file}" == "tests/python_client/requirements.txt" ]]; then 
            REQUIREMENT_CHANGED=true 
          fi 
      done 
}