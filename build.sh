#!/bin/bash

BOLD=`tput bold`
NORMAL=`tput sgr0`
YELLOW='\033[1;33m'
ENDC='\033[0m'

function build_image() {
    dockerfile=$1
    remote_registry=$2
    tagged=$2
    buildcmd="docker build -t ${tagged} -f ${dockerfile} ."
    echo -e "${BOLD}$buildcmd${NORMAL}"
    $buildcmd
    pushcmd="docker push ${remote_registry}"
    echo -e "${BOLD}$pushcmd${NORMAL}"
    $pushcmd
    echo -e "${YELLOW}${BOLD}Image: ${remote_registry}${NORMAL}${ENDC}"
}

case "$1" in

all)
    [[ -z $MISHARDS_REGISTRY ]] && {
        echo -e "${YELLOW}Error: Please set docker registry first:${ENDC}\n\t${BOLD}export MISHARDS_REGISTRY=xxxx\n${ENDC}"
        exit 1
    }

    version=""
    [[ ! -z $2 ]] && version=":${2}"
    build_image "Dockerfile" "${MISHARDS_REGISTRY}${version}" "${MISHARDS_REGISTRY}"
    ;;
*)
    echo "Usage: [option...] {base | apps}"
    echo "all,      Usage: build.sh all [tagname|] => {docker_registry}:\${tagname}"
    ;;
esac
