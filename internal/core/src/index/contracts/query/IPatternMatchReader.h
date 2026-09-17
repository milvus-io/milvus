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

// Exact string-pattern predicates. Pure mixin, independent of IIndexReaderBase
// and point/range queries; implementations expose only supported interfaces.

namespace milvus::index {

// Native pattern operations. Translate plan enums at the consumer boundary;
// query contracts do not depend on protobuf operator types.
enum class PatternOp {
    Match,         // LIKE
    PrefixMatch,   // startsWith
    PostfixMatch,  // endsWith
    InnerMatch,    // substring, "%value%"
    RegexMatch,    // regex substring match, `=~ "pattern"`
};

class IPatternMatchReader {
 public:
    virtual ~IPatternMatchReader() = default;

    // Per-call routing guard. FM-index uses this to reject degenerate
    // literals; other families can use the operation whenever their static
    // ReaderCaps advertises pattern matching.
    virtual bool
    ShouldUseForOp(PatternOp op, std::string_view pattern) const {
        return true;
    }

    // Encoding is operation-specific: Match takes raw SQL LIKE syntax, RegexMatch
    // takes a raw regular expression, and prefix/postfix/inner operations take a
    // literal (including any percent or underscore characters). The implementation
    // performs engine-specific conversion. Output bits are in the reader's domain
    // and the bitmap size equals Count().
    virtual TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const = 0;
};

// String-only CRTP bridge for typed scalar readers. Numeric instantiations
// inherit an empty primary, so they do not expose IPatternMatchReader.
template <typename Derived,
          typename T,
          bool DelegateShouldUseForOp = false>
class PatternMatchReaderAdapter {
};

template <typename Derived, bool DelegateShouldUseForOp>
class PatternMatchReaderAdapter<Derived,
                                std::string_view,
                                DelegateShouldUseForOp>
    : public IPatternMatchReader {
 public:
    bool
    ShouldUseForOp(PatternOp op, std::string_view pattern) const override {
        if constexpr (DelegateShouldUseForOp) {
            return static_cast<const Derived*>(this)->ShouldUseForOpImpl(
                op, pattern);
        }
        return IPatternMatchReader::ShouldUseForOp(op, pattern);
    }

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override {
        return static_cast<const Derived*>(this)->PatternMatchImpl(pattern, op);
    }
};

}  // namespace milvus::index
