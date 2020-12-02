#=============================================================================
# Copyright (c) 2018-2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#=============================================================================
cmake_minimum_required(VERSION 3.14...3.17 FATAL_ERROR)

project(wrapper-Arrow)

include(ExternalProject)

ExternalProject_Add(Arrow
    GIT_REPOSITORY    https://github.com/apache/arrow.git
    GIT_TAG           apache-arrow-2.0.0
    GIT_SHALLOW       true
    SOURCE_DIR        "${ARROW_ROOT}/arrow"
    SOURCE_SUBDIR     "cpp"
    BINARY_DIR        "${ARROW_ROOT}/build"
    INSTALL_DIR       "${ARROW_ROOT}/install"
    CMAKE_ARGS        ${ARROW_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${ARROW_ROOT}/install)
