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
#include <string_view>

#include "common/Types.h"

// Tokenized full-text queries, exposed as a pure mixin. Text readers compose an
// engine snapshot rather than inheriting an inverted-index implementation.
// Builder/Loader handle sealed construction; growing input and publication use
// IAppendable<TextBatch> and IGrowingIndex separately.

namespace milvus::index {

class ITextMatchReader {
 public:
    virtual ~ITextMatchReader() = default;

    // 1 = hit; output has Count() bits in the reader's coordinate domain.
    virtual TargetBitmap
    MatchQuery(std::string_view query, uint32_t min_should_match) const = 0;

    virtual TargetBitmap
    PhraseMatchQuery(std::string_view query, uint32_t slop) const = 0;

    virtual TargetBitmap
    FuzzyMatchQuery(std::string_view query,
                    uint32_t max_edit_distance) const = 0;
};

}  // namespace milvus::index
