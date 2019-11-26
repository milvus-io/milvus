#!/bin/bash

OS_NAME="linux"
CODE_NAME=$(lsb_release -sc)
OS_MD5=$(lsb_release -s | md5sum | cut -d " " -f 1)
BRANCH_NAMES=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's=[a-zA-Z]*\/==g' | awk -F", " '{$1=""; print $0}')
ARTIFACTORY_URL=""
CCACHE_DIRECTORY="${HOME}/.ccache"

while getopts "l:d:h" arg
do
        case $arg in
             l)
                ARTIFACTORY_URL=$OPTARG
                ;;
             d)
                CCACHE_DIRECTORY=$OPTARG
                ;;
             h) # help
                echo "

parameter:
-l: artifactory url
-d: ccache directory
-h: help

usage:
./build.sh -l \${ARTIFACTORY_URL} -d \${CCACHE_DIRECTORY} [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

if [[ -z "${ARTIFACTORY_URL}" || "${ARTIFACTORY_URL}" == "" ]];then
    echo "you have not input ARTIFACTORY_URL !"
    exit 1
fi

for BRANCH_NAME in ${BRANCH_NAMES}
do
    echo "fetching ${BRANCH_NAME}/ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz"
    wget -q --method HEAD "${ARTIFACTORY_URL}/${BRANCH_NAME}/ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz"
    if [[ $? == 0 ]];then
        wget "${ARTIFACTORY_URL}/${BRANCH_NAME}/ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz" && \
        mkdir -p ${CCACHE_DIRECTORY} && \
        tar zxf ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz -C ${CCACHE_DIRECTORY} && \
        rm ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz
        if [[ $? == 0 ]];then
            echo "found cache"
            exit 0
        fi
    fi
done

echo "could not download cache" && exit 1

