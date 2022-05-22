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

set(COMMON_SRC
        Schema.cpp
        Types.cpp
        SystemProperty.cpp
        vector_index_c.cpp
        )

add_library(milvus_common SHARED
        ${COMMON_SRC}
        )

if ( MSYS )
    target_link_libraries(milvus_common
            milvus_utils
            milvus_config
            milvus_log
            knowhere
            milvus_proto
            yaml-cpp
            boost_bitset_ext
            arrow
            parquet
            )
else()
    target_link_libraries(milvus_common
            milvus_utils
            milvus_config
            milvus_log
            knowhere
            milvus_proto
            yaml-cpp
            boost_bitset_ext
            arrow
            parquet
            arrow_bundled
            )
endif()

install(TARGETS milvus_common DESTINATION lib)
