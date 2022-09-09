# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


option( EMBEDDED_MILVUS "Enable embedded Milvus" OFF )

if ( EMBEDDED_MILVUS )
    add_compile_definitions( EMBEDDED_MILVUS )
endif()

milvus_add_pkg_config("milvus_storage")

set(STORAGE_FILES
        parquet_c.cpp
        PayloadStream.cpp
        DataCodec.cpp
        Util.cpp
        PayloadReader.cpp
        PayloadWriter.cpp
        FieldData.cpp
        IndexData.cpp
        InsertData.cpp
        Event.cpp
        )
add_library(milvus_storage SHARED ${STORAGE_FILES})
#target_link_libraries( milvus_storage PUBLIC milvus_common boost_system boost_filesystem aws-cpp-sdk-s3 pthread)
target_link_libraries( milvus_storage PUBLIC milvus_common pthread)

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()

install(TARGETS  milvus_storage DESTINATION "${CMAKE_INSTALL_LIBDIR}")
