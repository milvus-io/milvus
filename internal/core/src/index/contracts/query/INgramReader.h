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

#include <string_view>

#include "common/Types.h"
#include "index/contracts/query/IPatternMatchReader.h"

// Ngram candidate generation, exposed as a pure query mixin. Results are a
// superset (caps.ngram_candidates = true, caps.exact = false); the consumer
// fetches original values and verifies the predicate. The index neither calls
// the executor nor performs that refinement. PatternOp names the operation;
// implementing INgramReader does not imply exact IPatternMatchReader support.

namespace milvus::index {

class INgramReader {
 public:
    virtual ~INgramReader() = default;

    // Whether this literal/operator can use the index, for example whether the
    // literal meets min_gram. This is a per-call check after pinning, not a cached
    // capability bit or an exception-based capability probe.
    virtual bool
    CanHandle(std::string_view literal, PatternOp op) const = 0;

    // Phase 1: candidate generation. The result is AND-merged into `candidates`
    // (the caller initializes it non-empty). Semantically a superset.
    // Requires CanHandle(literal, op) == true.
    virtual void
    Candidates(std::string_view literal,
               PatternOp op,
               TargetBitmap& candidates) const = 0;
};

}  // namespace milvus::index
