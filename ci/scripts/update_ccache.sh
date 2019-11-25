#!/bin/bash

OS_NAME="linux"
CODE_NAME=$(lsb_release -sc)
OS_MD5=$(lsb_release -s | md5sum | cut -d " " -f 1)
BRANCH_NAMES=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's=[a-zA-Z]*\/==g' | awk -F", " '{$1=""; print $0}')
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

tar zcf ./ccache.tar.gz -C ${HOME}/.ccache .

