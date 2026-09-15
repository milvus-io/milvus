// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// Shared encoding for TupleTermFilterExpr's tuple set (`[a, b] in
// [[v1,w1], ...]`). See design doc
// docs/design-docs/design_docs/20260901-tuple-term-membership-expression.md.
//
// The encode functions below are the single source of truth for the
// canonical tuple-key byte encoding, called from two places that must never
// diverge: query::ProtoParser::ParseTupleTermFilterExprs (PlanProto.cpp)
// calls them once per configured tuple, from parsed proto::plan::GenericValue
// elements, to build the TupleMembership hash set at plan-parse time;
// exec::PhyTupleTermFilterExpr (exec/expression/TupleTermExpr.cpp) calls them
// once per row per column, from runtime column values, to build the probe
// key. A runtime key equals a parse-time key iff the underlying values are
// equal, and that equivalence holds only as long as both call sites keep
// using exactly these functions.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>

#include "pb/plan.pb.h"

namespace milvus {

// Tags the canonical byte encoding of one tuple element so that values of
// different declared C++ types can never alias each other's byte image --
// defense in depth on top of column position already disambiguating types;
// see BloomFilterExpr's "Value domains" note (common/BloomFilterEnvelope.h)
// for the sibling class of bug this guards against. Every element
// additionally carries an explicit 8-byte length prefix, even the
// fixed-width kinds, so concatenating several encoded elements can never let
// a boundary inside one element's payload be misread as the start of the
// next -- e.g. two VARCHAR columns ("ab","c") and ("a","bc") must encode to
// different keys.
enum class TupleElementTag : uint8_t {
    kInt64 = 1,
    kDouble = 2,
    kBool = 3,
    kBytes = 4,
};

void
EncodeTupleElementInt64(int64_t v, std::string& out);

void
EncodeTupleElementDouble(double v, std::string& out);

void
EncodeTupleElementBool(bool v, std::string& out);

void
EncodeTupleElementBytes(const char* data, size_t len, std::string& out);

// Appends val's canonical encoding to key. Only the value kinds
// TupleTermExpr columns can carry after Go's castValue reach here: bool,
// int64 (also carries every narrower declared integer width, widened),
// float/double (the proto has one float_val field for both FLOAT and DOUBLE
// columns), and string. Throws SegcoreError{ExprInvalid} on any other kind
// (e.g. array_val/bytes_val), which would mean an inconsistent plan --
// TupleTermExpr.columns are documented as top-level scalar fields only, so
// the Go parser never emits a nested-array or blob tuple element.
void
EncodeGenericValue(const proto::plan::GenericValue& val, std::string& key);

// TupleMembership: an immutable set of canonically-encoded tuples, built
// once from TupleTermExpr.tuples (query::ProtoParser::ParseTupleTermFilterExprs)
// and shared by every per-segment physical expression compiled from the same
// plan node -- mirrors RoaringMembership's "decode once, share the decoded
// form" contract: rebuilding a hash set with many tuples per segment would
// be as wasteful as redecoding a Roaring bitmap per segment.
//
// Each member key is the concatenation of every column's canonically-encoded
// value, in column order. Encoding is unambiguous across positions because
// every element is length-prefixed (see EncodeTupleElementBytes et al.
// above), and it never has to unify values of different declared types:
// TupleTermExpr.tuples elements are already cast against their column's type
// at parse time (Go: castValue), so column i's key bytes always come from
// the one C++ type that column's DataType maps to.
class TupleMembership {
 public:
    explicit TupleMembership(std::unordered_set<std::string> keys)
        : keys_(std::move(keys)) {
    }

    bool
    Contains(const std::string& key) const {
        return keys_.find(key) != keys_.end();
    }

    size_t
    size() const {
        return keys_.size();
    }

 private:
    const std::unordered_set<std::string> keys_;
};

}  // namespace milvus
