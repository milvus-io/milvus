#!/bin/bash

OS_NAME="${OS_NAME}"
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


PACKAGE_FILE="ccache-${OS_NAME}-${BUILD_ENV_DOCKER_IMAGE_ID}.tar.gz"

function check_ccache() {
    BRANCH=$1
    echo "fetching ${BRANCH}/${PACKAGE_FILE}"
    wget -q --spider "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}"
    return $?
}

function download_file() {
    BRANCH=$1
    wget -q "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}" && \
    mkdir -p ${CCACHE_DIRECTORY} && \
    tar zxf ${PACKAGE_FILE} -C ${CCACHE_DIRECTORY} && \
    rm ${PACKAGE_FILE}
    return $?
}

if [[ -n "${CHANGE_TARGET}" && "${BRANCH_NAME}" =~ "PR-" ]];then
    check_ccache ${CHANGE_TARGET}
    if [[ $? == 0 ]];then
        download_file ${CHANGE_TARGET}
        if [[ $? == 0 ]];then
            echo "found cache"
            exit 0
        fi
    fi

    check_ccache ${BRANCH_NAME}
    if [[ $? == 0 ]];then
        download_file ${BRANCH_NAME}
        if [[ $? == 0 ]];then
            echo "found cache"
            exit 0
        fi
    fi
fi

for CURRENT_BRANCH in ${BRANCH_NAMES}
do
    if [[ "${CURRENT_BRANCH}" != "HEAD" ]];then
        check_ccache ${CURRENT_BRANCH}
        if [[ $? == 0 ]];then
            download_file ${CURRENT_BRANCH}
            if [[ $? == 0 ]];then
                echo "found cache"
                exit 0
            fi
        fi
    fi
done

echo "could not download cache" && exit 1

