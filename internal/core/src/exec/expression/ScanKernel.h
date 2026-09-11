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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common/Types.h"
#include "common/ValidityView.h"
#include "index/SkipIndex.h"

namespace milvus {
namespace exec {

enum class FilterType { sequential = 0, random = 1 };

// One sub-batch of values handed to a kernel. Position i of the batch maps to
// position i of the TriStateOut passed in the same call.
template <typename T>
struct CandidateBatch {
    // Never nullptr when a kernel is called. A NULL row may point at a
    // placeholder value; validity is authoritative.
    const T* data = nullptr;
    // NULL rows of this sub-batch. KernelAdapter folds them after Eval. A
    // kernel whose evaluation can throw on a placeholder must skip them.
    ValidityView validity{};
    // bitmap_input sliced to this sub-batch. Empty means every row is a
    // candidate.
    TargetBitmapView candidates{};
    // Global segment offsets of the rows. Non-null only for kernels that
    // declare kNeedsSegmentOffsets and only on readers that provide them.
    const int32_t* segment_offsets = nullptr;
    size_t size = 0;

    bool
    IsCandidate(size_t i) const {
        return candidates.size() == 0 || candidates[i];
    }
};

// Three-valued output of one sub-batch.
//   TRUE    = match 1, known 1
//   FALSE   = match 0, known 1
//   UNKNOWN = match 0, known 0
// Every row starts as FALSE.
struct TriStateOut {
    TargetBitmapView match;
    TargetBitmapView known;

    void
    SetTrue(size_t i) {
        match[i] = true;
        known[i] = true;
    }

    void
    SetFalse(size_t i) {
        match[i] = false;
        known[i] = true;
    }

    void
    SetUnknown(size_t i) {
        match[i] = false;
        known[i] = false;
    }

    TriStateOut
    Slice(size_t offset, size_t size) {
        return TriStateOut{match.view(offset, size), known.view(offset, size)};
    }
};

// A kernel evaluates one predicate over a CandidateBatch. Sequential batches
// are contiguous rows or elements; random batches come from offset input,
// Take, or index reverse lookup.
template <typename K, typename T>
concept ScanKernel =
    requires(K& kernel, const CandidateBatch<T>& batch, TriStateOut out) {
        kernel.template Eval<FilterType::sequential>(batch, out);
        kernel.template Eval<FilterType::random>(batch, out);
    };

// Optional: prune a whole data chunk by SkipIndex statistics. EvalKernel
// copies the kernel into the reader's skip function on every call, and the
// Scan and Take readers keep the copy from the first call for the rest of the
// expression. Everything CanSkip reads must be held by value or owned by the
// expression, never by a per-batch local.
template <typename K>
concept KernelCanSkip = requires(const K& kernel,
                                 const SkipIndex& skip_index,
                                 FieldId field_id,
                                 int64_t chunk_id) {
    {
        kernel.CanSkip(skip_index, field_id, chunk_id)
    } -> std::convertible_to<bool>;
};

// Optional: the predicate is FALSE for every non-NULL row of the batch.
template <typename K>
concept KernelAlwaysFalse = requires(const K& kernel) {
    { kernel.AlwaysFalse() } -> std::convertible_to<bool>;
};

// Optional: the predicate is TRUE for every non-NULL row of the batch.
template <typename K>
concept KernelAlwaysTrue = requires(const K& kernel) {
    { kernel.AlwaysTrue() } -> std::convertible_to<bool>;
};

// Optional: the kernel reads CandidateBatch::segment_offsets.
template <typename K>
inline constexpr bool kKernelNeedsSegmentOffsets =
    requires { requires std::remove_cvref_t<K>::kNeedsSegmentOffsets; };

// Optional: NULL rows are FALSE and known instead of UNKNOWN (EXISTS). Only
// rows handed to the kernel and constant batches honour it: SkipIndex-pruned
// chunks and index reverse-lookup misses stay UNKNOWN, so EvalKernel rejects
// it together with CanSkip or an index-only non-constant scan.
template <typename K>
inline constexpr bool kKernelNullRowsKnownFalse =
    requires { requires std::remove_cvref_t<K>::kNullRowsKnownFalse; };

}  // namespace exec
}  // namespace milvus
