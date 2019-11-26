#!/bin/bash

OS_NAME="linux"
CODE_NAME=$(lsb_release -sc)
OS_MD5=$(lsb_release -s | md5sum | cut -d " " -f 1)
BRANCH_NAME=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's/.*, //' | sed 's=[a-zA-Z]*\/==g')
ARTIFACTORY_URL=""
ARTIFACTORY_USER=""
ARTIFACTORY_PASSWORD=""
CCACHE_DIRECTORY="${HOME}/.ccache"

while getopts "l:u:p:d:h" arg
do
        case $arg in
             l)
                ARTIFACTORY_URL=$OPTARG
                ;;
             u)
                ARTIFACTORY_USER=$OPTARG
                ;;
             p)
                ARTIFACTORY_PASSWORD=$OPTARG
                ;;
             d)
                CCACHE_DIRECTORY=$OPTARG
                ;;
             h) # help
                echo "

parameter:
-l: artifactory url
-u: artifactory user
-p: artifactory password
-d: ccache directory
-h: help

usage:
./build.sh -l \${ARTIFACTORY_URL} -u \${ARTIFACTORY_USER} -p \${ARTIFACTORY_PASSWORD} -d \${CCACHE_DIRECTORY} [-h]
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

PACKAGE_FILE="ccache-${OS_NAME}-${CODE_NAME}-${OS_MD5}.tar.gz"
REMOTE_PACKAGE_PATH="${ARTIFACTORY_URL}/${BRANCH_NAME}"

echo "Updating ccache package file: ${PACKAGE_FILE}"
tar zcf ./${PACKAGE_FILE} -C ${HOME}/.ccache .
echo "Uploading ccache package file ${PACKAGE_FILE} to ${REMOTE_PACKAGE_PATH}"
curl -u${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD} -T ${PACKAGE_FILE} ${REMOTE_PACKAGE_PATH}/${PACKAGE_FILE}
if [[ $? == 0 ]];then
    echo "Uploading ccache package file success !"
    exit 0
else
    echo "Uploading ccache package file fault !"
    exit 1
fi
