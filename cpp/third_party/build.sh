#!/bin/bash

BUILD_TYPE="Release"

while getopts "p:t:d:h" arg
do
        case $arg in
             p)
                HOME_PATH=$OPTARG # PG_HOME
                ;;
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             h) # help
                echo "

parameter:
-p: postgresql install path.
-t: build type

usage:
./build.sh -p \${PG_HOME} -t \${BUILD_TYPE}
                "
                exit 0
                ;;
             ?)
                echo "unknown argument"
        exit 1
        ;;
        esac
done

if [ -n ${HOME_PATH} ];then
  export PG_HOME=${HOME_PATH}
fi

while IFS='' read -r line || [[ -n "$line" ]]; do
    cd $line
	./build.sh #pwd is used in the script. Thus you have to run it in its directory
        if [ $? -ne 0 ];then
           exit 1
        fi
	cd ../
done < project.conf

