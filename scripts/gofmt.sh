#!/usr/bin/env bash

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

## green to echo
function green(){
    echo -e "\033[32m $1 \033[0m"
}

## Error
function bred(){
    echo -e "\033[31m\033[01m $1 \033[0m"
}

files=()
files_need_gofmt=()

if [[ $# -ne 0 ]]; then
    for arg in "$@"; do
        if [ -f "$arg" ];then
            files+=("$arg")
        fi
        if [ -d "$arg" ];then
            for file in `find $arg -type f | grep "\.go$"`; do
                files+=("$file")
            done
        fi
    done
fi

# Check for files that fail gofmt.
if [[ "${#files[@]}" -ne 0 ]]; then
    for file in "${files[@]}"; do
        diff="$(gofmt -s -d ${file} 2>&1)"
        if [[ -n "$diff" ]]; then
            files_need_gofmt+=("${file}")
        fi
    done
fi

if [[ "${#files_need_gofmt[@]}" -ne 0 ]]; then
    bred "ERROR!"
    for file in "${files_need_gofmt[@]}"; do
        gofmt -s -d ${file} 2>&1
    done
    echo ""
    bred "Some files have not been gofmt'd. To fix these errors, "
    bred "copy and paste the following:"
    for file in "${files_need_gofmt[@]}"; do
        bred "  gofmt -s -w ${file}"
    done
    exit 1
else
    green "OK"
    exit 0
fi
