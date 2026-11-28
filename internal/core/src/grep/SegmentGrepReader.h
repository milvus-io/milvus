// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "common/RegexQuery.h"
#include "grep/Grep.h"
#include "index/contracts/query/INgramReader.h"
#include "segcore/SegmentInterface.h"

// Segment-side building blocks shared by the grep filter expression
// (exec/expression/GrepFilterExpr.cpp) and the GREP_MATCHES output column
// (grep/GrepMatchesColumn.cpp): reading text for a list of row offsets and
// selecting an NGRAM reader for a compiled plan.
namespace milvus::grep {

// Keeps either borrowed chunk views pinned or bulk-read strings owned until
// matching and result copying for this read batch have finished.
struct GrepInputBatch {
    using Views =
        PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>;
    int64_t count = 0;
    std::optional<Views> pinned;
    std::unique_ptr<DataArray> owned;
    std::vector<int64_t> owned_positions;
    std::vector<Views> text_pins;
    std::vector<std::optional<std::string_view>> inline_text;

    // nullopt for a database NULL.
    std::optional<std::string_view>
    Text(int64_t index) const;
};

// Number of leading offsets that one ReadGrepBatch call may read together:
// at most max_rows, and for view reads only offsets of the first offset's chunk.
int64_t
GrepReadCount(const segcore::SegmentInternalInterface* segment,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t max_rows,
              int64_t rows,
              bool use_views,
              const ChunkedColumnInterface* view_column);

// Reads `count` values at `offsets`, where count comes from GrepReadCount.
GrepInputBatch
ReadGrepBatch(
    const segcore::SegmentInternalInterface* segment,
    const std::shared_ptr<const segcore::SegmentReadSnapshot>& snapshot,
    milvus::OpContext* op_ctx,
    const FieldMeta& meta,
    const int64_t* offsets,
    int64_t count,
    bool use_views,
    const ChunkedColumnInterface* view_column);

// A request-scoped NGRAM reader together with the necessary-condition tree it
// accepted. Preparation does not enumerate postings.
struct GrepIndex {
    index::IIndexReaderBasePtr owner;
    const index::INgramReader* reader;
    RegexLiteralCondition condition;
};

// Selects a sealed, whole-field NGRAM reader with complete row coverage that
// can handle the plan's condition tree. `size` is the number of input rows; no
// reader is prepared for empty input. Loading errors propagate.
std::optional<GrepIndex>
PrepareGrepIndex(const segcore::SegmentInternalInterface* segment,
                 milvus::OpContext* op_ctx,
                 const FieldMeta& meta,
                 const Plan& plan,
                 int64_t size,
                 int64_t rows);

}  // namespace milvus::grep
