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

#include "config/ConfigKnowhere.h"
#include "log/Log.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/segcore_init_c.h"
#include "segcore/SingletonConfig.h"

namespace milvus::segcore {
extern "C" void
SegcoreInit(const char* conf_file) {
    milvus::config::KnowhereInitImpl(conf_file);
}

// TODO merge small index config into one config map, including enable/disable small_index
extern "C" void
SegcoreSetChunkRows(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_chunk_rows(value);
}

extern "C" void
SegcoreSetEnableGrowingSegmentIndex(const bool value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_enable_growing_segment_index(value);
}

extern "C" void
SegcoreSetNlist(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_nlist(value);
}

extern "C" void
SegcoreSetNprobe(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_nprobe(value);
}

extern "C" void
SegcoreSetKnowhereThreadPoolNum(const uint32_t num_threads) {
    milvus::config::KnowhereInitThreadPool(num_threads);
}

// return value must be freed by the caller
extern "C" char*
SegcoreSetSimdType(const char* value) {
    LOG_SEGCORE_INFO_ << "set config simd_type: " << value;
    auto real_type = milvus::config::KnowhereSetSimdType(value);
    char* ret = reinterpret_cast<char*>(malloc(real_type.length() + 1));
    memcpy(ret, real_type.c_str(), real_type.length());
    ret[real_type.length()] = 0;
    return ret;
}

extern "C" void
SegcoreSetEnableParallelReduce(bool flag) {
    LOG_SEGCORE_INFO_ << "set enable parallel reduce: " << flag;
    milvus::segcore::SingletonConfig::GetInstance().set_enable_parallel_reduce(
        flag);
}

extern "C" void
SegcoreSetNqThresholdToEnableParallelReduce(int64_t nq) {
    LOG_SEGCORE_INFO_ << "set nq threshold to enable parallel reduce: " << nq;
    milvus::segcore::SingletonConfig::GetInstance()
        .set_nq_threshold_to_enable_parallel_reduce(nq);
}

extern "C" void
SegcoreSetKThresholdToEnableParallelReduce(int64_t k) {
    LOG_SEGCORE_INFO_ << "set topk threshold to enable parallel reduce: " << k;
    milvus::segcore::SingletonConfig::GetInstance()
        .set_k_threshold_to_enable_parallel_reduce(k);
}

}  // namespace milvus::segcore
