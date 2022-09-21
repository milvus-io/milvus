# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

set(INDEX_FILES
        StringIndexMarisa.cpp
        Utils.cpp
        VectorMemIndex.cpp
        IndexFactory.cpp
        )

if ( BUILD_DISK_ANN STREQUAL "ON" )
    set(INDEX_FILES
            ${INDEX_FILES}
            VectorDiskIndex.cpp
            )
endif ()

milvus_add_pkg_config("milvus_index")
add_library(milvus_index SHARED ${INDEX_FILES})

set(PLATFORM_LIBS )
if ( LINUX OR APPLE )
    set(PLATFORM_LIBS marisa)
endif()
if (MSYS)
    set(PLATFORM_LIBS -Wl,--allow-multiple-definition)
endif ()

target_link_libraries(milvus_index
        milvus_storage
        ${PLATFORM_LIBS}
        )

install(TARGETS milvus_index DESTINATION "${CMAKE_INSTALL_LIBDIR}")
