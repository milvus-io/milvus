#!/bin/bash

HELP="
Usage:
  $0 [flags] [Arguments]

    -l [ARTIFACTORY_URL]          Artifactory URL
    --cache_dir=[CCACHE_DIR]      Ccache directory
    -f [FILE] or --file=[FILE]    Ccache compress package file
    -u [USERNAME]                 Artifactory Username
    -p [PASSWORD]                 Artifactory Password
    -h or --help                  Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=$(getopt -o "l:f:u:p:h" -l "cache_dir::,file::,help" -n "$0" -- "$@")

eval set -- "${ARGS}"

while true ; do
        case "$1" in
                -l)
                        # o has an optional argument. As we are in quoted mode,
                        # an empty parameter will be generated if its optional
                        # argument is not found.
                        case "$2" in
                                "") echo "Option Artifactory URL, no argument"; exit 1 ;;
                                *)  ARTIFACTORY_URL=$2 ; shift 2 ;;
                        esac ;;
                --cache_dir)
                        case "$2" in
                                "") echo "Option cache_dir, no argument"; exit 1 ;;
                                *)  CCACHE_DIR=$2 ; shift 2 ;;
                        esac ;;
                -u)
                        case "$2" in
                                "") echo "Option Username, no argument"; exit 1 ;;
                                *)  USERNAME=$2 ; shift 2 ;;
                        esac ;;
                -p)
                        case "$2" in
                                "") echo "Option Password, no argument"; exit 1 ;;
                                *)  PASSWORD=$2 ; shift 2 ;;
                        esac ;;
                -f|--file)
                        case "$2" in
                                "") echo "Option file, no argument"; exit 1 ;;
                                *)  PACKAGE_FILE=$2 ; shift 2 ;;
                        esac ;;
                -h|--help) echo -e "${HELP}" ; exit 0 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

# Set defaults for vars modified by flags to this script
CCACHE_DIR=${CCACHE_DIR:="${HOME}/.ccache"}
PACKAGE_FILE=${PACKAGE_FILE:="ccache-${OS_NAME}-${BUILD_ENV_IMAGE_ID}.tar.gz"}
BRANCH_NAME=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's/.*, //' | sed 's=[a-zA-Z]*\/==g')

if [[ -z "${ARTIFACTORY_URL}" || "${ARTIFACTORY_URL}" == "" ]];then
    echo "You have not input ARTIFACTORY_URL !"
    exit 1
fi

if [[ ! -d "${CCACHE_DIR}" ]]; then
    echo "\"${CCACHE_DIR}\" directory does not exist !"
    exit 1
fi

function check_ccache() {
    BRANCH=$1
    wget -q --spider "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}"
    return $?
}

echo -e "===\n=== ccache statistics after build\n==="
ccache --show-stats

if [[ -n "${CHANGE_TARGET}" && "${BRANCH_NAME}" =~ "PR-" ]]; then
    check_ccache ${CHANGE_TARGET}
    if [[ $? == 0 ]];then
        echo "Skip Update ccache package ..." && exit 0
    fi
fi

if [[ "${BRANCH_NAME}" != "HEAD" ]];then
    REMOTE_PACKAGE_PATH="${ARTIFACTORY_URL}/${BRANCH_NAME}"
    echo "Updating ccache package file: ${PACKAGE_FILE}"
    tar zcf ./"${PACKAGE_FILE}" -C "${CCACHE_DIR}" .
    echo "Uploading ccache package file ${PACKAGE_FILE} to ${REMOTE_PACKAGE_PATH}"
    curl -u"${USERNAME}":"${PASSWORD}" -T "${PACKAGE_FILE}" "${REMOTE_PACKAGE_PATH}"/"${PACKAGE_FILE}"
    if [[ $? == 0 ]];then
        echo "Uploading ccache package file success !"
        exit 0
    else
        echo "Uploading ccache package file fault !"
        exit 1
    fi
fi

echo "Skip Update ccache package ..."
