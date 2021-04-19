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

#include_directories(${MILVUS_ENGINE_SRC}/grpc)
set(FOUND_OPENBLAS "unknown")
add_subdirectory(index)
set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
foreach (DIR ${INDEX_INCLUDE_DIRS})
    include_directories(${DIR})
endforeach ()


add_subdirectory( utils )
add_subdirectory(dog_segment)
add_subdirectory( cache )
#add_subdirectory( query )
add_subdirectory( db ) # target milvus_engine
add_subdirectory( server )

set(link_lib
	milvus_engine
	dog_segment
	#query
        utils
        curl
        )


set( BOOST_LIB  libboost_system.a
                libboost_filesystem.a
                libboost_serialization.a
                )

set( THIRD_PARTY_LIBS   yaml-cpp 
                        )


target_link_libraries( server
PUBLIC ${link_lib}
${THIRD_PARTY_LIBS}
${BOOST_LIB}
       )

# **************************** Get&Print Include Directories ****************************
get_property( dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES )

foreach ( dir ${dirs} )
    message( STATUS "Current Include DIRS: ")
endforeach ()

set( SERVER_LIBS server )


add_executable( milvus_server   ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
                                )
#target_include_directories(db PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar-client-cpp/pulsar-client-cpp-src/pulsar-client-cpp/include)


target_link_libraries( milvus_server PRIVATE ${SERVER_LIBS} )
install( TARGETS milvus_server DESTINATION bin )
