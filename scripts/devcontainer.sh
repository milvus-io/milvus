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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

export OS_NAME="${OS_NAME:-ubuntu18.04}"

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     machine=Linux;;
    Darwin*)    machine=Mac;;
    CYGWIN*)    machine=Cygwin;;
    MINGW*)     machine=MinGw;;
    *)          machine="UNKNOWN:${unameOut}"
esac

# Attempt to run in the container with the same UID/GID as we have on the host,
# as this results in the correct permissions on files created in the shared
# volumes. This isn't always possible, however, as IDs less than 100 are
# reserved by Debian, and IDs in the low 100s are dynamically assigned to
# various system users and groups. To be safe, if we see a UID/GID less than
# 500, promote it to 501. This is notably necessary on macOS Lion and later,
# where administrator accounts are created with a GID of 20. This solution is
# not foolproof, but it works well in practice.
uid=$(id -u)
gid=$(id -g)
[ "$uid" -lt 500 ] && uid=501
[ "$gid" -lt 500 ] && gid=$uid

if [ "${1-}" = "build" ];then
   CHECK_BUILDER=1
fi

if [ "${CHECK_BUILDER:-}" == "1" ];then
    awk 'c&&c--{sub(/^/,"#")} /# Command/{c=3} 1' $ROOT_DIR/docker-compose.yml > $ROOT_DIR/docker-compose-devcontainer.yml
else
    awk 'c&&c--{sub(/^/,"#")} /# Build devcontainer/{c=5} 1' $ROOT_DIR/docker-compose.yml > $ROOT_DIR/docker-compose-devcontainer.yml.tmp
    awk 'c&&c--{sub(/^/,"#")} /# Command/{c=3} 1' $ROOT_DIR/docker-compose-devcontainer.yml.tmp > $ROOT_DIR/docker-compose-devcontainer.yml
    rm $ROOT_DIR/docker-compose-devcontainer.yml.tmp
fi

if [ "${machine}" == "Mac" ];then
    sed -i '' "s/# user: {{ CURRENT_ID }}/user: \"$uid:$gid\"/g" $ROOT_DIR/docker-compose-devcontainer.yml
else
    sed -i "s/# user: {{ CURRENT_ID }}/user: \"$uid:$gid\"/g" $ROOT_DIR/docker-compose-devcontainer.yml
fi

pushd "$ROOT_DIR"

mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-ccache"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-go-mod"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/thirdparty"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-vscode-extensions"
chmod -R 777 "${DOCKER_VOLUME_DIRECTORY:-.docker}"

if [ "${1-}" = "build" ];then
   docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml pull --ignore-pull-failures builder
   docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml build builder
fi

if [ "${1-}" = "up" ]; then
    docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml up -d
fi

if [ "${1-}" = "down" ]; then
    docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml down
fi

popd
