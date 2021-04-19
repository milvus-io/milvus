#!/bin/bash

HELP="
Usage:
  $0 [flags] [Arguments]

    -l [ARTIFACTORY_URL]          Artifactory URL
    --cache_dir=[CACHE_DIR]       Cache directory
    -f [FILE] or --file=[FILE]    Cache compress package file
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
                                *)  CACHE_DIR=$2 ; shift 2 ;;
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
BRANCH_NAMES=$(git log --decorate | head -n 1 | sed 's/.*(\(.*\))/\1/' | sed 's=[a-zA-Z]*\/==g' | awk -F", " '{$1=""; print $0}')

if [[ -z "${ARTIFACTORY_URL}" || "${ARTIFACTORY_URL}" == "" ]];then
    echo "You have not input ARTIFACTORY_URL !"
    exit 1
fi

if [[ -z "${CACHE_DIR}" ]]; then
    echo "You have not input CACHE_DIR !"
    exit 1
fi

if [[ -z "${PACKAGE_FILE}" ]]; then
    echo "You have not input PACKAGE_FILE !"
    exit 1
fi

function check_cache() {
    BRANCH=$1
    echo "fetching ${BRANCH}/${PACKAGE_FILE}"
    wget -q --spider "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}"
    return $?
}

function download_file() {
    BRANCH=$1
    wget -q "${ARTIFACTORY_URL}/${BRANCH}/${PACKAGE_FILE}" && \
    mkdir -p "${CACHE_DIR}" && \
    tar zxf "${PACKAGE_FILE}" -C "${CACHE_DIR}" && \
    rm ${PACKAGE_FILE}
    return $?
}

if [[ -n "${CHANGE_TARGET}" && "${BRANCH_NAME}" =~ "PR-" ]];then
    check_cache ${CHANGE_TARGET}
    if [[ $? == 0 ]];then
        download_file ${CHANGE_TARGET}
        if [[ $? == 0 ]];then
            echo "found cache"
            exit 0
        fi
    fi

    check_cache ${BRANCH_NAME}
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
        check_cache ${CURRENT_BRANCH}
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
