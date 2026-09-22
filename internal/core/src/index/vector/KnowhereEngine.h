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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/vector/VectorTypeUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/index/index.h"
#include "knowhere/index/index_node.h"
#include "knowhere/object.h"
#include "knowhere/version.h"

// Shared knowhere handle and metadata, held by composition in readers and build
// artifacts rather than inherited through a concrete index implementation.
// The nullable row mapping lives inside the native index's knowhere IdMap
// (#50524): builders and loaders publish the public-row validity bitmap into
// it, and knowhere derives both mapping directions.

namespace milvus::index {

class KnowhereEngine {
 public:
    // Create an in-memory engine. physical_type selects Create<T> after type
    // erasure; elem_type is NONE for ordinary vectors and the physical element type
    // for VECTOR_ARRAY. Storage IO is injected into Loader/Artifact, not retained
    // through a FileManagerContext on query interfaces.
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   bool use_knowhere_build_pool = true);

    // The knowhere DataView index used by the interim (build-in-place and
    // growing) path: it reads the caller's memory through `view_data` instead of
    // owning a copy, and has no file manager at all. Re-homed from
    // `VectorMemIndex<T>`'s second ctor (`index/VectorMemIndex.cpp:203-233`).
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   knowhere::ViewDataOp view_data,
                   bool use_knowhere_build_pool = true);

    // Disk-engine creation. `engine_object` is borrowed only for the
    // synchronous IndexFactory::Create<T> call. `backing_owner` retains every
    // file-manager and local-file dependency used by the native node.
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   std::shared_ptr<const void> backing_owner,
                   const knowhere::Pack<std::shared_ptr<milvus::FileManager>>&
                       engine_object,
                   bool use_knowhere_build_pool = true);

    // Copying retains the backing owner, intrusive knowhere handle, and shared
    // immutable embedding-list offsets. A consuming artifact conversion can
    // transfer built state without copying index data or its O(rows) offset
    // array.
    // A reader sharing a mutable growing handle must separately freeze its
    // logical row prefix and physical count; the handle -- and the append-only
    // IdMap inside it -- is not a physically immutable ANN snapshot.
    KnowhereEngine(const KnowhereEngine&) = default;
    KnowhereEngine&
    operator=(const KnowhereEngine&) = delete;
    KnowhereEngine(KnowhereEngine&& other) = default;
    KnowhereEngine&
    operator=(KnowhereEngine&& other);

    // The owner precedes the native handle so destruction releases the node
    // before any file-manager or mmap dependency it may still reference.
    std::shared_ptr<const void> backing_owner;
    knowhere::Index<knowhere::IndexNode> native_index;

    // --- self-description ---------------------------------------------------

    IndexType
    KnowhereIndexType() const {
        return index_type_;
    }

    MetricType
    Metric() const {
        return metric_type_;
    }

    int64_t
    Dim() const {
        return dim_;
    }

    void
    SetDim(int64_t dim) {
        dim_ = dim;
    }

    bool
    IsEmbeddingList() const {
        return embedding_list_;
    }

    DataType
    PhysicalType() const {
        return physical_type_;
    }

    bool
    UseBuildPool() const {
        return use_knowhere_build_pool_;
    }

    // Embedding-list bookkeeping, re-homed from the `empty_emb_list_offsets_`
    // member shared by both of today's index classes
    // (`VectorMemIndex.h:159`, `VectorDiskIndex.h:301`).
    bool
    IsEmptyEmbListIndex() const {
        return embedding_list_ && !EmptyEmbListOffsets().empty();
    }

    void
    SetEmptyEmbListOffsets(std::vector<size_t> offsets) {
        empty_emb_list_offsets_ =
            std::make_shared<const std::vector<size_t>>(std::move(offsets));
    }

    void
    SetEmptyEmbListOffsets(std::shared_ptr<const std::vector<size_t>> offsets) {
        empty_emb_list_offsets_ = std::move(offsets);
    }

    const std::vector<size_t>&
    EmptyEmbListOffsets() const {
        static const std::vector<size_t> empty;
        return empty_emb_list_offsets_ == nullptr ? empty
                                                  : *empty_emb_list_offsets_;
    }

 private:
    IndexType index_type_;
    MetricType metric_type_;
    int64_t dim_{0};
    DataType physical_type_{DataType::NONE};
    bool embedding_list_{false};
    bool use_knowhere_build_pool_{true};
    // One immutable generation shared by artifacts/readers that copy this
    // engine handle. A setter publishes a fresh vector and cannot mutate a
    // generation already observed by a reader.
    std::shared_ptr<const std::vector<size_t>> empty_emb_list_offsets_;
};

knowhere::Json
PrepareVectorSearchParams(const VectorSearchParams& params);

bool
KnowhereMmapSupported(const IndexType& index_type);

// Decode owning raw-vector results from knowhere datasets. These helpers need
// no reader state and are shared across vector query implementations.
template <typename T>
std::vector<uint8_t>
DecodeVectorByIdsResult(const knowhere::DataSetPtr& result);

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult(const knowhere::DataSetPtr& result);

template <>
std::vector<uint8_t>
DecodeVectorByIdsResult<sparse_u32_f32>(const knowhere::DataSetPtr& result) =
    delete;

template <>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<sparse_u32_f32>(const knowhere::DataSetPtr& result) =
    delete;

}  // namespace milvus::index
