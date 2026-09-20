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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/TypeTraits.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"

namespace milvus {
struct OpContext;
}  // namespace milvus

// Unified vector query contract. Knowhere types stay in vector interfaces and
// implementations, not shared or scalar contracts. Construction, persistence,
// and loading use separate objects.

namespace milvus::index {

// Vector search inputs projected from the execution layer: search parameters,
// metric, topk, and trace context. Array offsets, grouping, visibility, and
// result folding belong to the consumer, not the index reader.
struct VectorSearchParams {
    knowhere::Json search_params_;
    MetricType metric_type_;
    int64_t topk_{0};
    tracer::TraceContext trace_ctx_;
};

class IVectorReader {
 public:
    virtual ~IVectorReader() = default;

    virtual void
    Search(const DatasetPtr& dataset,
           const VectorSearchParams& params,
           const BitsetView& bitset,
           milvus::OpContext* op_ctx,
           SearchResult& result) const = 0;

    // Returned iterators do not carry a reader pin. Consumers may retain them in
    // SearchResult::vector_iterators_, and knowhere iterators borrow the index's
    // IdMap to map their result ids back to logical rows. The consumer must keep
    // all borrowed state alive for iterator use. TODO: audit and wire that
    // lifetime anchor across deferred consumption; this return type alone does
    // not establish it.
    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr& dataset,
              const knowhere::Json& json,
              const BitsetView& bitset,
              milvus::OpContext* op_ctx) const = 0;

    virtual bool
    RefineEnabled() const = 0;

    // Whether the original vectors can be recovered from the index at all.
    // GetVector and GetSparseVector return owning results. The combined
    // dense/sparse surface still exposes unsupported combinations in some
    // implementations.
    virtual bool
    HasRawData() const = 0;

    virtual std::vector<uint8_t>
    GetVector(const DatasetPtr& dataset) const = 0;

    virtual std::unique_ptr<
        const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr& dataset) const = 0;

    // Vector metadata and iterator configuration. Iterator callers need
    // PrepareSearchParams because Iterators accepts prepared JSON.
    virtual MetricType
    Metric() const = 0;

    virtual IndexType
    KnowhereIndexType() const = 0;

    virtual int64_t
    Dim() const = 0;

    virtual knowhere::Json
    PrepareSearchParams(const VectorSearchParams& params) const = 0;

    // Nullable vectors omit null rows from the engine's physical coordinate
    // space, but that mapping is owned by knowhere's IdMap (#50524): bitsets,
    // requested ids and result ids all stay in the segment's logical row
    // space, so the only nullable surface here is logical-row metadata.
    virtual bool
    HasValidData() const = 0;

    virtual int64_t
    ValidCount() const = 0;

    virtual bool
    IsRowValid(int64_t logical_offset) const = 0;

    // Runtime/backend capability checks remain authoritative: exposing the
    // unified vector contract does not make every operation available for
    // every physical type or backend.
    virtual knowhere::expected<knowhere::DataSetPtr>
    CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                  const BitsetView& bitset,
                  const int64_t* labels,
                  size_t labels_len,
                  bool is_cosine,
                  milvus::OpContext* op_ctx) const = 0;

    // VECTOR_ARRAY retrieval returns concatenated vectors and one terminal
    // offset after the requested lists.
    virtual std::pair<std::vector<uint8_t>, std::vector<size_t>>
    GetEmbListByIds(const DatasetPtr& dataset,
                    const std::string& metric_type) const = 0;
};

// IGrowingIndex provides common publication and GrowingIndexSnapshotPin; IAppendable<Batch>
// provides the typed input interface. Both are declared in IGrowingIndex.h.

}  // namespace milvus::index
