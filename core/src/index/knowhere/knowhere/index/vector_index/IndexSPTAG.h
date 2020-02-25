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

#include <SPTAG/AnnService/inc/Core/VectorIndex.h>

#include <cstdint>
#include <memory>
#include <string>

#include "VectorIndex.h"
#include "knowhere/index/IndexModel.h"

namespace knowhere {

class CPUSPTAGRNG : public VectorIndex {
 public:
    explicit CPUSPTAGRNG(const std::string& IndexType);

 public:
    BinarySet
    Serialize() override;

    //    VectorIndexPtr
    //    Clone() override;

    void
    Load(const BinarySet& index_array) override;

 public:
    // PreprocessorPtr
    // BuildPreprocessor(const DatasetPtr &dataset, const Config &config) override;

    int64_t
    Count() override;

    int64_t
    Dimension() override;

    IndexModelPtr
    Train(const DatasetPtr& dataset, const Config& config) override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) override;

    void
    Seal() override;

 private:
    void
    SetParameters(const Config& config);

 private:
    PreprocessorPtr preprocessor_;
    std::shared_ptr<SPTAG::VectorIndex> index_ptr_;
    SPTAG::IndexAlgoType index_type_;
};

using CPUSPTAGRNGPtr = std::shared_ptr<CPUSPTAGRNG>;

class CPUSPTAGRNGIndexModel : public IndexModel {
 public:
    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& binary) override;

 private:
    std::shared_ptr<SPTAG::VectorIndex> index_;
};

using CPUSPTAGRNGIndexModelPtr = std::shared_ptr<CPUSPTAGRNGIndexModel>;

}  // namespace knowhere
