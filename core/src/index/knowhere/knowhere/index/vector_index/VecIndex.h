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

#include <faiss/utils/ConcurrentBitset.h>
#include <memory>
#include <utility>
#include <vector>

#include "knowhere/common/Dataset.h"
#include "knowhere/common/Typedef.h"
#include "knowhere/index/Index.h"
#include "knowhere/index/vector_index/IndexType.h"
#include "segment/Types.h"

namespace milvus {
namespace knowhere {

class VecIndex : public Index {
 public:
    virtual void
    BuildAll(const DatasetPtr& dataset_ptr, const Config& config) {
        Train(dataset_ptr, config);
        Add(dataset_ptr, config);
    }

    virtual void
    Train(const DatasetPtr& dataset, const Config& config) = 0;

    virtual void
    Add(const DatasetPtr& dataset, const Config& config) = 0;

    virtual void
    AddWithoutIds(const DatasetPtr& dataset, const Config& config) = 0;

    virtual DatasetPtr
    Query(const DatasetPtr& dataset, const Config& config) = 0;

    virtual DatasetPtr
    QueryById(const DatasetPtr& dataset, const Config& config) {
        return nullptr;
    }

    // virtual DatasetPtr
    // QueryByRange(const DatasetPtr&, const Config&) = 0;
    //
    // virtual MetricType
    // metric_type() = 0;

    virtual int64_t
    Dim() = 0;

    virtual int64_t
    Count() = 0;

    virtual IndexType
    index_type() const {
        return index_type_;
    }

    virtual IndexMode
    index_mode() const {
        return index_mode_;
    }

    virtual DatasetPtr
    GetVectorById(const DatasetPtr& dataset, const Config& config) {
        return nullptr;
    }

    virtual void
    GetBlacklist(faiss::ConcurrentBitsetPtr& bitset_ptr) {
        bitset_ptr = bitset_;
    }

    virtual void
    SetBlacklist(faiss::ConcurrentBitsetPtr bitset_ptr) {
        bitset_ = std::move(bitset_ptr);
    }

    virtual const std::vector<milvus::segment::doc_id_t>&
    GetUids() const {
        return uids_;
    }

    virtual void
    SetUids(std::vector<milvus::segment::doc_id_t>& uids) {
        uids_.clear();
        uids_.swap(uids);
    }

    int64_t
    Size() override {
        if (size_ != -1) {
            return size_;
        }
        return Count() * Dim() * sizeof(FloatType);
    }

 protected:
    IndexType index_type_ = "";
    IndexMode index_mode_ = IndexMode::MODE_CPU;
    faiss::ConcurrentBitsetPtr bitset_ = nullptr;

 private:
    std::vector<milvus::segment::doc_id_t> uids_;
};

using VecIndexPtr = std::shared_ptr<VecIndex>;

}  // namespace knowhere
}  // namespace milvus
