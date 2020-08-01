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

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

set(FOUND_OPENBLAS "unknown")

add_subdirectory(index)
set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
foreach (DIR ${INDEX_INCLUDE_DIRS})
    include_directories(${DIR})
endforeach ()

aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler            SCHEDULER_MAIN_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/action     SCHEDULER_ACTION_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/event      SCHEDULER_EVENT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/job        SCHEDULER_JOB_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/selector   SCHEDULER_SELECTOR_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/resource   SCHEDULER_RESOURCE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/task       SCHEDULER_TASK_FILES )

set( SCHEDULER_FILES  ${SCHEDULER_MAIN_FILES}
                      ${SCHEDULER_ACTION_FILES}
                      ${SCHEDULER_EVENT_FILES}
                      ${SCHEDULER_JOB_FILES}
                      ${SCHEDULER_SELECTOR_FILES}
                      ${SCHEDULER_RESOURCE_FILES}
                      ${SCHEDULER_TASK_FILES}
                      )

add_subdirectory( utils )
add_subdirectory( metrics )
add_subdirectory( config )
add_subdirectory( tracing )
add_subdirectory( query )
add_subdirectory( db ) # target milvus_engine
add_subdirectory( log )
add_subdirectory( server )

target_link_libraries( server
                PUBLIC milvus_engine
                       config
                       metrics
                       tracing
                       log
                       oatpp
                       query
                       utils
                       # dependency prometheus
                       prometheus-cpp-push
                       prometheus-cpp-pull
                       prometheus-cpp-core
                       curl
                       )

# **************************** Get&Print Include Directories ****************************
get_property( dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES )

foreach ( dir ${dirs} )
    message( STATUS "Current Include DIRS: "${dir} )
endforeach ()

set( SERVER_LIBS server )


add_executable( milvus_server   ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
                                ${SCHEDULER_FILES}
                                )

target_link_libraries( milvus_server PRIVATE ${SERVER_LIBS} )
install( TARGETS milvus_server DESTINATION bin )

install( FILES
         ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
         ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
         ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
         ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}
         ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}.0
         ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}.1.00
         DESTINATION lib )

if ( FOUND_OPENBLAS STREQUAL "false" )
    install( FILES
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}.0
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}.0.3
        DESTINATION lib )
endif()
