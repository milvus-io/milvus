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

#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/Types.h"
#include "common/Utils.h"
#include "exceptions/EasyAssert.h"
#include "utils/Json.h"

namespace milvus::segcore {
struct SmallIndexConf {
    std::string index_type;
    nlohmann::json build_params;
    nlohmann::json search_params;
};

class SegcoreConfig {
 private:
    SegcoreConfig() {
        // hard code configurations for small index
        SmallIndexConf sub_conf;
        sub_conf.build_params["nlist"] = nlist_;
        sub_conf.search_params["nprobe"] = nprobe_;
        sub_conf.index_type = "IVF";
        table_[MetricType::METRIC_L2] = sub_conf;
        table_[MetricType::METRIC_INNER_PRODUCT] = sub_conf;
    }

 public:
    static SegcoreConfig&
    default_config() {
        // TODO: remove this when go side is ready
        static SegcoreConfig config;
        return config;
    }

    void
    parse_from(const std::string& string_path);

    const SmallIndexConf&
    at(MetricType metric_type) const {
        Assert(table_.count(metric_type));
        return table_.at(metric_type);
    }

    int64_t
    get_chunk_rows() const {
        return chunk_rows_;
    }

    void
    set_chunk_rows(int64_t chunk_rows) {
        chunk_rows_ = chunk_rows;
    }

    int64_t
    get_index_chunk_rows() const {
        return index_chunk_rows_;
    }

    void
    set_index_chunk_rows(int64_t index_chunk_rows) {
        AssertInfo(index_chunk_rows % chunk_rows_ == 0, "index_chunk_rows must be divisible by b");
        index_chunk_rows_ = index_chunk_rows;
    }

    void
    set_nlist(int64_t nlist) {
        nlist_ = nlist;
    }

    void
    set_nprobe(int64_t nprobe) {
        nprobe_ = nprobe;
    }

    void
    set_small_index_config(MetricType metric_type, const SmallIndexConf& small_index_conf) {
        table_[metric_type] = small_index_conf;
    }

    int64_t
    index_to_num_data_chunks() const {
        return lower_div(index_chunk_rows_, chunk_rows_);
    }

    std::vector<int64_t>
    index_to_data_chunks(int64_t index_chunk_id) const {
        std::vector<int64_t> ret;
        auto num_data_chunks = index_to_num_data_chunks();
        auto data_chunk_beg = num_data_chunks * index_chunk_id;
        for (int64_t i = 0; i < num_data_chunks; i++) {
            ret.push_back(data_chunk_beg + i);
        }
        return ret;
    }

    int64_t
    at_least_data_chunks(int64_t index_chunk_id) const {
        auto num_data_chunks = index_to_num_data_chunks();
        return (index_chunk_id + 1) * num_data_chunks;
    }

 private:
    int64_t chunk_rows_ = 32 * 1024;
    int64_t index_chunk_rows_ = 32 * 1024;
    int64_t nlist_ = 100;
    int64_t nprobe_ = 4;
    std::map<MetricType, SmallIndexConf> table_;
};

}  // namespace milvus::segcore
