#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

HELP="
Usage:
  $0 [flags] [Arguments]

    -i [INSTALL_PREFIX] or --install_prefix=[INSTALL_PREFIX]
                              Install directory used by install.
    -h or --help                      Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=`getopt -o "i:h" -l "install_prefix::,help" -n "$0" -- "$@"`

eval set -- "${ARGS}"

while true ; do
        case "$1" in
                -i|--install_prefix)
                        # o has an optional argument. As we are in quoted mode,
                        # an empty parameter will be generated if its optional
                        # argument is not found.
                        case "$2" in
                                "") echo "Option install_prefix, no argument"; exit 1 ;;
                                *)  INSTALL_PREFIX=$2 ; shift 2 ;;
                        esac ;;
                -h|--help) echo -e "${HELP}" ; exit 0 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

# Set defaults for vars modified by flags to this script
INSTALL_PREFIX=${INSTALL_PREFIX:="/var/lib/milvus"}
DIR_UNITTEST="${INSTALL_PREFIX}/unittest"

if [ -d ${INSTALL_PREFIX}/lib ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALL_PREFIX}/lib
fi

if [ ! -d ${DIR_UNITTEST} ]; then
	echo "The unittest folder does not exist!"
    exit 1
fi

pushd ${SCRIPTS_DIR}

for test in `ls ${DIR_UNITTEST}`; do
    echo $test
    # run unittest
    ${DIR_UNITTEST}/${test}
    if [ $? -ne 0 ]; then
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit 1
    fi
done

popd
