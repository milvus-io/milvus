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


set(SEGCORE_FILES
        Collection.cpp
        collection_c.cpp
        segment_c.cpp
        SegmentGrowing.cpp
        SegmentGrowingImpl.cpp
        SegmentSealedImpl.cpp
        FieldIndexing.cpp
        InsertRecord.cpp
        Reduce.cpp
        plan_c.cpp
        reduce_c.cpp
        load_index_c.cpp
        SealedIndexingRecord.cpp
        SegmentInterface.cpp
        SegcoreConfig.cpp
        segcore_init_c.cpp
        ScalarIndex.cpp
        TimestampIndex.cpp
        )
add_library(milvus_segcore SHARED
        ${SEGCORE_FILES}
        )

target_link_libraries(milvus_segcore
        tbb milvus_utils pthread knowhere log milvus_proto ${OpenMP_CXX_FLAGS}
        dl
        milvus_common
        milvus_query
        milvus_utils
        milvus_storage
        milvus_config
        )

