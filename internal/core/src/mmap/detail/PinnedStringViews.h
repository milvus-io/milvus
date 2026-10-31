// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace milvus {
class StringChunk;

namespace detail::storage {

// Storage-only batch adapter over the existing StringChunk getter. Each chunk
// is already resolved and pinned; row_offsets are physical rows in that
// getter's int domain. The caller keeps its existing backing leases alive
// through both this call and the consumer of output's borrowed views.
//
// There is no owner acquisition, null/skip decision or Expr operation here.
// Input and output arrays contain count lanes in the caller's original order.
// Zero count dereferences none and permits null arrays; count has no batch cap.
// Non-empty null chunks and rows outside the getter's int domain are rejected
// before dereference or narrowing; rows inside that domain retain the original
// getter's range errors. No output is consumed if loading any lane throws.
// Optional state outputs are a pair: the caller has already established that
// every lane is valid/unskipped. Fill them alongside values, without separate
// tiny-buffer passes. This adapter never decides NULL/SkipIndex semantics.
void
GatherPinnedStringViews(const StringChunk* const* chunks,
                        const std::size_t* row_offsets,
                        std::int64_t count,
                        std::string_view* output,
                        bool* validity = nullptr,
                        bool* skipped = nullptr);

}  // namespace detail::storage
}  // namespace milvus
