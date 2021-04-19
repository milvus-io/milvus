#!/bin/bash

BUILD_TYPE="Release"

while getopts "p:t:d:h" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             h) # help
                echo "

parameter:
-p: postgresql install path.
-t: build type

usage:
./build.sh -t \${BUILD_TYPE}
                "
                exit 0
                ;;
             ?)
                echo "unknown argument"
        exit 1
        ;;
        esac
done

if [[ -d build ]]; then
	rm ./build -r
fi

while IFS='' read -r line || [[ -n "$line" ]]; do
    cd $line
	./build.sh -t ${BUILD_TYPE}
        if [ $? -ne 0 ];then
           exit 1
        fi
	cd ../
done < project.conf

