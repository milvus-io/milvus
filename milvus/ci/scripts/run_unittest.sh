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
    --mysql_user=[MYSQL_USER_NAME]    MySQL User Name
    --mysql_password=[MYSQL_PASSWORD]
                                      MySQL Password
    --mysql_host=[MYSQL_HOST]         MySQL Host
    -h or --help                      Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=`getopt -o "i:h" -l "install_prefix::,mysql_user::,mysql_password::,mysql_host::,help" -n "$0" -- "$@"`

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
                --mysql_user)
                        case "$2" in
                                "") echo "Option mysql_user, no argument"; exit 1 ;;
                                *)  MYSQL_USER_NAME=$2 ; shift 2 ;;
                        esac ;;
                --mysql_password)
                        case "$2" in
                                "") echo "Option mysql_password, no argument"; exit 1 ;;
                                *)  MYSQL_PASSWORD=$2 ; shift 2 ;;
                        esac ;;
                --mysql_host)
                        case "$2" in
                                "") echo "Option mysql_host, no argument"; exit 1 ;;
                                *)  MYSQL_HOST=$2 ; shift 2 ;;
                        esac ;;
                -h|--help) echo -e "${HELP}" ; exit 0 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

# Set defaults for vars modified by flags to this script
INSTALL_PREFIX=${INSTALL_PREFIX:="/var/lib/milvus"}
MYSQL_USER_NAME=${MYSQL_USER_NAME:="root"}
MYSQL_PASSWORD=${MYSQL_PASSWORD:="123456"}
MYSQL_HOST=${MYSQL_HOST:="127.0.0.1"}
MYSQL_PORT=${MYSQL_PORT:="3306"}
DIR_UNITTEST="${INSTALL_PREFIX}/unittest"

if [ -d ${INSTALL_PREFIX}/lib ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALL_PREFIX}/lib
fi

if [ ! -d ${DIR_UNITTEST} ]; then
	echo "The unittest folder does not exist!"
    exit 1
fi

pushd ${SCRIPTS_DIR}

MYSQL_DB_NAME=milvus_`date +%s%N`

function mysql_exc()
{
    cmd=$1
    mysql -h${MYSQL_HOST} -u${MYSQL_USER_NAME} -p${MYSQL_PASSWORD} -e "${cmd}"
    if [ $? -ne 0 ]; then
        echo "mysql $cmd run failed"
    fi
}

mysql_exc "CREATE DATABASE IF NOT EXISTS ${MYSQL_DB_NAME};"
mysql_exc "GRANT ALL PRIVILEGES ON ${MYSQL_DB_NAME}.* TO '${MYSQL_USER_NAME}'@'%';"
mysql_exc "FLUSH PRIVILEGES;"
mysql_exc "USE ${MYSQL_DB_NAME};"

for test in `ls ${DIR_UNITTEST}`; do
    echo $test
    case ${test} in
        test_db)
            # set run args for test_db
            args="mysql://${MYSQL_USER_NAME}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB_NAME}"
            ;;
        *test_*)
            args=""
            ;;
    esac
    # run unittest
    ${DIR_UNITTEST}/${test} "${args}"
    if [ $? -ne 0 ]; then
        echo ${args}
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit 1
    fi
done

mysql_exc "DROP DATABASE IF EXISTS ${MYSQL_DB_NAME};"

popd