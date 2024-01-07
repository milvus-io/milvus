#!/usr/bin/env bash
BUILD_UNITTEST="OFF"
while getopts "p:s:t:h" arg; do
  case $arg in
  p)
    INSTALL_PREFIX=$OPTARG
    ;;
  s)
    SOURCE_DIR=$OPTARG
    ;;
  t)
    BUILD_UNITTEST=$OPTARG
    ;;
  h) # help
    echo "
parameter:
-p: install prefix
-s: source directory
-t: ON means build with unittest
-h: help

usage:
./azure_build.sh -p \${INSTALL_PREFIX} -s \${SOURCE_DIR} [-t \${BUILD_UNITTEST}] [-h]
"
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done

ARCHITECTURE=$(uname -m)
if [[ ${ARCHITECTURE} == "aarch64" ]]; then
  export VCPKG_FORCE_SYSTEM_BINARIES="arm"
fi

AZURE_CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
${SOURCE_DIR}"

if [[ -f "/opt/vcpkg/scripts/buildsystems/vcpkg.cmake" ]]; then
  AZURE_CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
  -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
  ${SOURCE_DIR}"
fi

echo ${AZURE_CMAKE_CMD}
${AZURE_CMAKE_CMD}

make install