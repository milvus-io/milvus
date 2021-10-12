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

cmake_minimum_required( VERSION 3.18 )
project(dablooms)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(dablooms STATIC dablooms.cpp murmur.cpp)
target_include_directories(dablooms
    PUBLIC 
        ${PROJECT_SOURCE_DIR}
)

target_sources(dablooms PUBLIC dablooms.cpp murmur.cpp

    )
set_target_properties( dablooms PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR} )
# target_link_libraries(dablooms PUBLIC dablooms)

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()

install(TARGETS dablooms DESTINATION ${CMAKE_INSTALL_PREFIX})
