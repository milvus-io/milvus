cmake_minimum_required(VERSION 3.12)

include(ExternalProject)

ExternalProject_Add(GoogleTest
                    GIT_REPOSITORY    https://github.com/google/googletest.git
                    GIT_TAG           release-1.8.0
                    SOURCE_DIR        "${GTEST_ROOT}/googletest"
                    BINARY_DIR        "${GTEST_ROOT}/build"
                    INSTALL_DIR		  "${GTEST_ROOT}/install"
                    CMAKE_ARGS        ${GTEST_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${GTEST_ROOT}/install)

