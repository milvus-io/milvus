#!/bin/bash

HELP="
Usage:
  $0 [flags] [Arguments]

    -l [ARTIFACTORY_URL]          Artifactory URL
    --cache_dir=[CCACHE_DIR]      Ccache directory
    -f [FILE] or --file=[FILE]    Ccache compress package file
    -h or --help                  Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=$(getopt -o "l:f:h" -l "cache_dir::,file::,help" -n "$0" -- "$@")

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
BRANCH_NAMES=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's=[a-zA-Z]*\/==g' | awk -F", " '{$1=""; print $0}')

if [[ -z "${ARTIFACTORY_URL}" || "${ARTIFACTORY_URL}" == "" ]];then
    echo "You have not input ARTIFACTORY_URL !"
    exit 1
fi

function check_ccache() {
    BRANCH=$1
    echo "fetching ${BRANCH}/${PACKAGE_FILE}"
    wget -q --spider "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}"
    return $?
}

function download_file() {
    BRANCH=$1
    wget -q "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}" && \
    mkdir -p "${CCACHE_DIR}" && \
    tar zxf "${PACKAGE_FILE}" -C "${CCACHE_DIR}" && \
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
