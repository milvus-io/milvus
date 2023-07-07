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
function milvus_ci_release_name(){
    # Rules for helm release name 
    local name="m"
    if [[ "${MILVUS_SERVER_TYPE:-}" == "distributed-pulsar" ]]; then
        # Distributed pulsar mode
       name+="dp"
    elif [[ "${MILVUS_SERVER_TYPE:-}" == "distributed-kafka" ]]; then
        # Distributed kafka mode
       name+="dk"
    elif [[ "${MILVUS_SERVER_TYPE:-}" == "standalone-kafka" ]]; then
        # Standalone kafka mode
        name+="sk"
    elif [[ "${MILVUS_SERVER_TYPE:-}" == "distributed" ]]; then
        # Distributed mode
       name+="d"
    elif [[ "${MILVUS_SERVER_TYPE:-}" == "standalone-authentication" ]]; then
        # Standalone authentication mode
       name+="a"
    else
       # Standalone mode
        name+="s"

    fi 
    # Add pr number into release name 
    if [[ -n ${CHANGE_ID:-} ]]; then 
        name+="-${CHANGE_ID:-}"
    fi 

    
    # Add Jenkins BUILD_ID into Name
    if [[ -n ${JENKINS_BUILD_ID:-} ]]; then 
            name+="-${JENKINS_BUILD_ID}"
    fi 


    if [[ "${CI_MODE:-}" == "nightly" ]]; then
        # Nightly CI
       name+="-n"
    else 
       # Pull Request CI    
        name+="-pr"

    fi 

    if [[ "${MODE:-}" == "gpu" ]]; then
        name+="-gpu"
    fi

    export MILVUS_HELM_RELEASE_NAME=${name}
    echo ${name}
}
milvus_ci_release_name