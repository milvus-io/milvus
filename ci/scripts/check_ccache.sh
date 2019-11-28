#!/bin/bash

OS_NAME="linux"
CODE_NAME=$(lsb_release -sc)
BUILD_ENV_DOCKER_IMAGE_ID="${BUILD_ENV_IMAGE_ID}"
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

check_ccache() {
    BRANCH=$1
    echo "fetching ${BRANCH}/ccache-${OS_NAME}-${CODE_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz"
    wget -q --method HEAD "${ARTIFACTORY_URL}/${BRANCH}/ccache-${OS_NAME}-${CODE_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz"
    if [[ $? == 0 ]];then
        wget -q "${ARTIFACTORY_URL}/${BRANCH}/ccache-${OS_NAME}-${CODE_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz" && \
        mkdir -p ${CCACHE_DIRECTORY} && \
        tar zxf ccache-${OS_NAME}-${CODE_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz -C ${CCACHE_DIRECTORY} && \
        rm ccache-${OS_NAME}-${CODE_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz
        if [[ $? == 0 ]];then
            echo "found cache"
            exit 0
        fi
    fi
}

for BRANCH_NAME in ${BRANCH_NAMES}
do
    if [[ "${BRANCH_NAME}" != "HEAD" ]];then
        check_ccache ${BRANCH_NAME}
    fi
done

if [[ -n "${CHANGE_BRANCH}" && "${BRANCH_NAME}" =~ "PR-" ]];then
    check_ccache ${CHANGE_BRANCH}
    check_ccache ${BRANCH_NAME}
fi

echo "could not download cache" && exit 1

