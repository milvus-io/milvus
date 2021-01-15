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
#include "common/Types.h"
#include "common/Schema.h"
#include "query/Plan.h"
#include "common/Span.h"

namespace milvus::segcore {

class SegmentInterface {
 public:
    virtual void
    FillTargetEntry(const query::Plan* Plan, QueryResult& results) const = 0;

    virtual QueryResult
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_groups[],
           const Timestamp timestamps[],
           int64_t num_groups) const = 0;

    virtual int64_t
    GetMemoryUsageInBytes() const = 0;

    virtual int64_t
    get_row_count() const = 0;

    virtual ~SegmentInterface() = default;
};

// internal API for DSL calculation
class SegmentInternalInterface : public SegmentInterface {
 public:
    virtual const Schema&
    get_schema() const = 0;

    virtual int64_t
    get_safe_num_chunk() const = 0;

    template <typename T>
    Span<T>
    chunk_data(FieldOffset field_offset, int64_t chunk_id) const {
        return static_cast<Span<T>>(chunk_data_impl(field_offset, chunk_id));
    }

 protected:
    // blob and row_count
    virtual SpanBase
    chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const = 0;
};

}  // namespace milvus::segcore
