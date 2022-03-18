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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/common/BinarySet.h"
#include "indexbuilder/IndexCreatorBase.h"

namespace milvus::indexbuilder {

// TODO: better to distinguish binary vec & float vec.
class VecIndexCreator : public IndexCreatorBase {
 public:
    explicit VecIndexCreator(const char* serialized_type_params, const char* serialized_index_params);

    void
    Build(const knowhere::DatasetPtr& dataset) override {
        BuildWithoutIds(dataset);
    }

    knowhere::BinarySet
    Serialize() override;

    void
    Load(const knowhere::BinarySet& binary_set) override;

    int64_t
    dim();

 public:
    // used for tests
    struct QueryResult {
        std::vector<milvus::knowhere::IDType> ids;
        std::vector<float> distances;
        int64_t nq;
        int64_t topk;
    };

    std::unique_ptr<QueryResult>
    Query(const knowhere::DatasetPtr& dataset);

    std::unique_ptr<QueryResult>
    QueryWithParam(const knowhere::DatasetPtr& dataset, const char* serialized_search_params);

 private:
    void
    parse();

    std::string
    get_index_type();

    std::string
    get_metric_type();

    knowhere::IndexMode
    get_index_mode();

    int64_t
    get_index_file_slice_size();

    template <typename T>
    std::optional<T>
    get_config_by_name(std::string name);

    void
    StoreRawData(const knowhere::DatasetPtr& dataset);

    void
    LoadRawData();

    template <typename T>
    void
    check_parameter(knowhere::Config& conf,
                    const std::string& key,
                    std::function<T(std::string)> fn,
                    std::optional<T> default_v = std::nullopt);

    template <typename ParamsT>
    void
    parse_impl(const std::string& serialized_params_str, knowhere::Config& conf);

    std::unique_ptr<QueryResult>
    QueryImpl(const knowhere::DatasetPtr& dataset, const knowhere::Config& conf);

 public:
    void
    BuildWithIds(const knowhere::DatasetPtr& dataset);

    void
    BuildWithoutIds(const knowhere::DatasetPtr& dataset);

 private:
    knowhere::VecIndexPtr index_ = nullptr;
    std::string type_params_;
    std::string index_params_;
    milvus::json type_config_;
    milvus::json index_config_;
    knowhere::Config config_;
    std::vector<uint8_t> raw_data_;
    std::once_flag raw_data_loaded_;
};

}  // namespace milvus::indexbuilder
