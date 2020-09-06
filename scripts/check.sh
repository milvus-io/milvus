#!/usr/bin/env bash

check_protoc_version() {
    version=$(protoc --version)
    major=$(echo ${version} | sed -n -e 's/.*\([0-9]\{1,\}\)\.[0-9]\{1,\}\.[0-9]\{1,\}.*/\1/p')
    minor=$(echo ${version} | sed -n -e 's/.*[0-9]\{1,\}\.\([0-9]\{1,\}\)\.[0-9]\{1,\}.*/\1/p')
    if [ "$major" -eq 3 ] && [ "$minor" -eq 8 ]; then
	    return 0
    fi
    echo "protoc version not match, version 3.8.x is needed, current version: ${version}"
    return 0
}

if ! check_protoc_version; then
	exit 1
fi
