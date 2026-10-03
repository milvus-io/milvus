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

#include "common/TupleMembership.h"

#include <cstring>

#include "common/EasyAssert.h"

namespace milvus {

namespace {

// Appends an 8-byte little-endian length prefix, then the type tag is
// expected to have already been appended by the caller -- kept as a private
// helper so every EncodeTupleElement* function writes the same
// tag-then-length-then-payload shape without repeating the loop.
void
AppendLength(uint64_t len, std::string& out) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((len >> (8 * i)) & 0xff));
    }
}

}  // namespace

void
EncodeTupleElementInt64(int64_t v, std::string& out) {
    out.push_back(static_cast<char>(TupleElementTag::kInt64));
    AppendLength(8, out);
    uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((u >> (8 * i)) & 0xff));
    }
}

void
EncodeTupleElementDouble(double v, std::string& out) {
    out.push_back(static_cast<char>(TupleElementTag::kDouble));
    AppendLength(8, out);
    uint64_t bits;
    static_assert(sizeof(bits) == sizeof(v),
                  "double must be 8 bytes for this encoding");
    std::memcpy(&bits, &v, sizeof(bits));
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((bits >> (8 * i)) & 0xff));
    }
}

void
EncodeTupleElementBool(bool v, std::string& out) {
    out.push_back(static_cast<char>(TupleElementTag::kBool));
    AppendLength(1, out);
    out.push_back(v ? 1 : 0);
}

void
EncodeTupleElementBytes(const char* data, size_t len, std::string& out) {
    out.push_back(static_cast<char>(TupleElementTag::kBytes));
    AppendLength(len, out);
    out.append(data, len);
}

void
EncodeGenericValue(const proto::plan::GenericValue& val, std::string& key) {
    using ValCase = proto::plan::GenericValue::ValCase;
    switch (val.val_case()) {
        case ValCase::kBoolVal:
            EncodeTupleElementBool(val.bool_val(), key);
            return;
        case ValCase::kInt64Val:
            EncodeTupleElementInt64(val.int64_val(), key);
            return;
        case ValCase::kFloatVal:
            EncodeTupleElementDouble(val.float_val(), key);
            return;
        case ValCase::kStringVal:
            EncodeTupleElementBytes(
                val.string_val().data(), val.string_val().size(), key);
            return;
        default:
            ThrowInfo(ExprInvalid,
                      "tuple 'in' value has an unsupported kind for a "
                      "top-level scalar tuple column (val_case={})",
                      static_cast<int>(val.val_case()));
    }
}

}  // namespace milvus
