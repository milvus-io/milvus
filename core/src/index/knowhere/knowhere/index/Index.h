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

#include "IndexModel.h"
#include "IndexType.h"
#include "knowhere/common/BinarySet.h"
#include "knowhere/common/Dataset.h"
#include "knowhere/index/preprocessor/Preprocessor.h"

namespace knowhere {

class Index {
 public:
    virtual BinarySet
    Serialize() = 0;

    virtual void
    Load(const BinarySet& index_binary) = 0;

    // @throw
    virtual DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) = 0;

 public:
    IndexType
    idx_type() const {
        return idx_type_;
    }

    void
    set_idx_type(IndexType idx_type) {
        idx_type_ = idx_type;
    }

    virtual void
    set_preprocessor(PreprocessorPtr preprocessor) {
    }

    virtual void
    set_index_model(IndexModelPtr model) {
    }

 private:
    IndexType idx_type_;
};

using IndexPtr = std::shared_ptr<Index>;

}  // namespace knowhere
