// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <mutex>

#include "hnswlib/hnswlib.h"

#include "knowhere/index/vector_index/VectorIndex.h"

namespace knowhere {

class IndexHNSW : public VectorIndex {
 public:
    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& index_binary) override;

    DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) override;

    //    void
    //    set_preprocessor(PreprocessorPtr preprocessor) override;
    //
    //    void
    //    set_index_model(IndexModelPtr model) override;
    //
    //    PreprocessorPtr
    //    BuildPreprocessor(const DatasetPtr& dataset, const Config& config) override;

    IndexModelPtr
    Train(const DatasetPtr& dataset, const Config& config) override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    void
    Seal() override;

    int64_t
    Count() override;

    int64_t
    Dimension() override;

 private:
    std::mutex mutex_;
    std::shared_ptr<hnswlib::HierarchicalNSW<float>> index_;
};

}  // namespace knowhere
