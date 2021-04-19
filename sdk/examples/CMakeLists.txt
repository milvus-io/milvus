#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/utils UTIL_SRC_FILES)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/common COMMON_SRC_FILES)

file( GLOB APP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp )
foreach( sourcefile ${APP_SOURCES} )
    file(RELATIVE_PATH filename ${CMAKE_CURRENT_SOURCE_DIR} ${sourcefile})     
    string( REPLACE ".cpp" "" program ${filename} )
    add_executable( ${program} ${sourcefile} ${COMMON_SRC_FILES} ${UTIL_SRC_FILES})
    target_link_libraries( ${program} milvus_sdk pthread )
    install(TARGETS ${program} DESTINATION bin)
endforeach( sourcefile ${APP_SOURCES} )
