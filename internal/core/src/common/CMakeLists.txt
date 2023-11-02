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

milvus_add_pkg_config("milvus_common")

find_package(glog REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(TBB REQUIRED)
find_package(fmt REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(opentelemetry-cpp REQUIRED)

set(COMMON_SRC
        Schema.cpp
        SystemProperty.cpp
        Slice.cpp
        binary_set_c.cpp
        init_c.cpp
        Common.cpp
        RangeSearchHelper.cpp
        Tracer.cpp
        IndexMeta.cpp
        EasyAssert.cpp
        ConfigKnowhere.cpp
        Log.cpp
)

add_library(milvus_common SHARED ${COMMON_SRC})

target_link_libraries(milvus_common PUBLIC
        milvus_proto
        boost_bitset_ext
        simdjson
        opendal
        knowhere

        glog::glog
        yaml-cpp
        TBB::tbb
        fmt::fmt-header-only
        nlohmann_json::nlohmann_json
        opentelemetry-cpp::opentelemetry-cpp
)

install(TARGETS milvus_common DESTINATION "${CMAKE_INSTALL_LIBDIR}")
