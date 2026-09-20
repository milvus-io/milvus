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

// String-pattern predicates. Pure mixin, independent of IIndexReaderBase
// and point/range queries; implementations expose only supported interfaces.
//
// Results are EXACT by default. One documented exception exists: a family may
// declare a specific operation as candidates-only through
// PatternMatchIsExact(op) == false, in which case PatternMatch returns a
// SUPERSET of the answer and the consumer must recheck each candidate against
// the raw column. FM-index uses that for general LIKE (PatternOp::Match).

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

    // Whether PatternMatch(op) answers EXACTLY.
    //
    // The default is true: a family that advertises pattern matching returns
    // the exact result set, and a consumer may emit it as a final answer. An
    // implementation returns false for one operation to declare that it
    // answers with a CANDIDATE SUPERSET (every true row is included; some
    // false rows may be too). A consumer that receives a non-exact answer MUST
    // recheck each candidate against the raw column before emitting a result;
    // serving it directly would return rows the predicate does not match.
    //
    // This is a per-operation property, not a per-call one: it does not depend
    // on the pattern, so a consumer can decide its execution path before
    // running the query. The only current non-exact case is FM-index's
    // PatternOp::Match (see FmIndexReader::PatternMatch and
    // PhyUnaryRangeFilterExpr::ExecFMMatch).
    virtual bool
    PatternMatchIsExact(PatternOp op) const {
        return true;
    }

    // Encoding is operation-specific: Match takes raw SQL LIKE syntax, RegexMatch
    // takes a raw regular expression, and prefix/postfix/inner operations take a
    // literal (including any percent or underscore characters). The implementation
    // performs engine-specific conversion. Output bits are in the reader's domain
    // and the bitmap size equals Count(). Exact unless
    // PatternMatchIsExact(op) is false, in which case the bitmap is a
    // candidate superset.
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
