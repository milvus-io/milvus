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

option( EMBEDDED_MILVUS "Enable embedded Milvus" OFF )

if ( EMBEDDED_MILVUS )
    add_compile_definitions( EMBEDDED_MILVUS )
endif()

milvus_add_pkg_config("milvus_segcore")

set(SEGCORE_FILES
        Collection.cpp
        collection_c.cpp
        segment_c.cpp
        SegmentGrowingImpl.cpp
        SegmentSealedImpl.cpp
        FieldIndexing.cpp
        InsertRecord.cpp
        Reduce.cpp
        metrics_c.cpp
        plan_c.cpp
        reduce_c.cpp
        load_index_c.cpp
        load_field_data_c.cpp
        SegmentInterface.cpp
        SegcoreConfig.cpp
        IndexConfigGenerator.cpp
        segcore_init_c.cpp
        ScalarIndex.cpp
        TimestampIndex.cpp
        Utils.cpp
        ConcurrentVector.cpp)
add_library(milvus_segcore SHARED ${SEGCORE_FILES})

find_library(TBB NAMES tbb)
set(PLATFORM_LIBS dl)

if (MSYS)
  set(PLATFORM_LIBS )
endif()

target_link_libraries(milvus_segcore
        milvus_query
        ${PLATFORM_LIBS}
        ${TBB}
        ${OpenMP_CXX_FLAGS}
        )

install(TARGETS milvus_segcore DESTINATION "${CMAKE_INSTALL_LIBDIR}")
