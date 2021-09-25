// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <iostream>

#include "segcore/segcore_init_c.h"
#include "segcore/SegcoreConfig.h"
#include "utils/Log.h"
#include "config/ConfigKnowhere.h"

namespace milvus::segcore {
extern "C" void
SegcoreInit() {
    milvus::config::KnowhereInitImpl();
}

extern "C" void
SegcoreSetChunkRows(const int64_t value) {
    milvus::segcore::SegcoreConfig& config = milvus::segcore::SegcoreConfig::default_config();
    config.set_chunk_rows(value);
    LOG_SEGCORE_DEBUG_ << "set config chunk_size: " << config.get_chunk_rows();
}

extern "C" void
SegcoreSetSimdType(const char* value) {
    milvus::config::KnowhereSetSimdType(value);
    LOG_SEGCORE_DEBUG_ << "set config simd_type: " << value;
}

}  // namespace milvus::segcore
