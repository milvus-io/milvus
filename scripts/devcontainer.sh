#!/usr/bin/env bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

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

awk 'c&&c--{sub(/^/,"#")} /# Build devcontainer/{c=5} 1' $ROOT_DIR/docker-compose.yml > $ROOT_DIR/docker-compose-devcontainer.yml.tmp

awk 'c&&c--{sub(/^/,"#")} /# Command/{c=3} 1' $ROOT_DIR/docker-compose-devcontainer.yml.tmp > $ROOT_DIR/docker-compose-devcontainer.yml

rm $ROOT_DIR/docker-compose-devcontainer.yml.tmp

if [ "${machine}" == "Mac" ];then
    sed -i '' "s/# user: {{ CURRENT_ID }}/user: \"$uid:$gid\"/g" $ROOT_DIR/docker-compose-devcontainer.yml
else
    sed -i "s/# user: {{ CURRENT_ID }}/user: \"$uid:$gid\"/g" $ROOT_DIR/docker-compose-devcontainer.yml
fi

if [ "${1-}" = "up" ]; then
    docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml up -d
fi

if [ "${1-}" = "down" ]; then
    docker-compose -f $ROOT_DIR/docker-compose-devcontainer.yml down
fi
