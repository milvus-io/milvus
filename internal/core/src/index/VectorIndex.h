// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <boost/dynamic_bitset.hpp>

#include "Utils.h"
#include "knowhere/index/index_factory.h"
#include "index/Index.h"
#include "common/Types.h"
#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "knowhere/version.h"

namespace milvus::index {

class VectorIndex : public IndexBase {
 public:
    explicit VectorIndex(const IndexType& index_type,
                         const MetricType& metric_type)
        : IndexBase(index_type), metric_type_(metric_type) {
    }

 public:
    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override {
        PanicInfo(Unsupported,
                  "vector index don't support build index with raw data");
    };

    virtual void
    AddWithDataset(const DatasetPtr& dataset, const Config& config) {
        PanicInfo(Unsupported, "vector index don't support add with dataset");
    }

    virtual void
    Query(const DatasetPtr dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset,
          SearchResult& search_result) const = 0;

    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    VectorIterators(const DatasetPtr dataset,
                    const knowhere::Json& json,
                    const BitsetView& bitset) const {
        PanicInfo(NotImplemented,
                  "VectorIndex:" + this->GetIndexType() +
                      " didn't implement VectorIterator interface, "
                      "there must be sth wrong in the code");
    }

    virtual const bool
    HasRawData() const = 0;

    virtual std::vector<uint8_t>
    GetVector(const DatasetPtr dataset) const = 0;

    virtual std::unique_ptr<const knowhere::sparse::SparseRow<float>[]>
    GetSparseVector(const DatasetPtr dataset) const = 0;

    IndexType
    GetIndexType() const {
        return index_type_;
    }

    MetricType
    GetMetricType() const {
        return metric_type_;
    }

    int64_t
    GetDim() const {
        return dim_;
    }

    void
    SetDim(int64_t dim) {
        dim_ = dim;
    }

    virtual void
    CleanLocalData() {
    }

    virtual void
    CheckCompatible(const IndexVersion& version) {
        std::string err_msg =
            "version not support : " + std::to_string(version) +
            " , knowhere current version " +
            std::to_string(
                knowhere::Version::GetCurrentVersion().VersionNumber());
        AssertInfo(
            knowhere::Version::VersionSupport(knowhere::Version(version)),
            err_msg);
    }

    virtual bool
    IsMmapSupported() const {
        return knowhere::IndexFactory::Instance().FeatureCheck(
            index_type_, knowhere::feature::MMAP);
    }

    knowhere::Json
    PrepareSearchParams(const SearchInfo& search_info) const {
        knowhere::Json search_cfg = search_info.search_params_;

        search_cfg[knowhere::meta::METRIC_TYPE] = search_info.metric_type_;
        search_cfg[knowhere::meta::TOPK] = search_info.topk_;

        // save trace context into search conf
        if (search_info.trace_ctx_.traceID != nullptr &&
            search_info.trace_ctx_.spanID != nullptr) {
            search_cfg[knowhere::meta::TRACE_ID] =
                tracer::GetTraceIDAsHexStr(&search_info.trace_ctx_);
            search_cfg[knowhere::meta::SPAN_ID] =
                tracer::GetSpanIDAsHexStr(&search_info.trace_ctx_);
            search_cfg[knowhere::meta::TRACE_FLAGS] =
                search_info.trace_ctx_.traceFlags;
        }

        return search_cfg;
    }

 private:
    MetricType metric_type_;
    int64_t dim_;
};

}  // namespace milvus::index
