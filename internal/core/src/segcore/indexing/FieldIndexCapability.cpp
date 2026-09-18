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

#include "segcore/indexing/FieldIndexCapability.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::segcore {
namespace {

bool
EntryBefore(const IndexCapabilityEntry& lhs, const IndexCapabilityEntry& rhs) {
    return lhs.key.identity < rhs.key.identity;
}

size_t
CombineHash(size_t lhs, size_t rhs) {
    return lhs ^ (rhs + 0x9e3779b9U + (lhs << 6U) + (lhs >> 2U));
}

}  // namespace

IndexIdentity::IndexIdentity(std::variant<int64_t, uint64_t> value)
    : value_(std::move(value)) {
}

IndexIdentity
IndexIdentity::PreBuiltIndex(int64_t index_id) {
    return IndexIdentity(index_id);
}

IndexIdentity
IndexIdentity::SegmentLocal(uint64_t registration_id) {
    return IndexIdentity(registration_id);
}

IndexIdentity::Kind
IndexIdentity::kind() const {
    return value_.index() == 0 ? Kind::PreBuiltIndex : Kind::SegmentLocal;
}

bool
IndexIdentity::operator==(const IndexIdentity& other) const {
    return value_ == other.value_;
}

bool
IndexIdentity::operator<(const IndexIdentity& other) const {
    if (value_.index() != other.value_.index()) {
        return value_.index() < other.value_.index();
    }
    if (value_.index() == 0) {
        return std::get<int64_t>(value_) < std::get<int64_t>(other.value_);
    }
    return std::get<uint64_t>(value_) < std::get<uint64_t>(other.value_);
}

size_t
IndexKeyHash::operator()(const IndexKey& key) const {
    size_t identity_hash;
    if (key.identity.kind() == IndexIdentity::Kind::PreBuiltIndex) {
        identity_hash =
            std::hash<int64_t>()(std::get<int64_t>(key.identity.value_));
    } else {
        identity_hash =
            std::hash<uint64_t>()(std::get<uint64_t>(key.identity.value_));
    }
    identity_hash = CombineHash(
        std::hash<uint8_t>()(static_cast<uint8_t>(key.identity.kind())),
        identity_hash);
    return CombineHash(std::hash<int64_t>()(key.field_id.get()), identity_hash);
}

FieldIndexCapability::FieldIndexCapability(
    FieldId field_id, std::vector<IndexCapabilityEntry> entries)
    : field_id_(field_id) {
    for (const auto& entry : entries) {
        AssertInfo(entry.key.field_id == field_id_,
                   "field index capability for field {} contains field {}",
                   field_id_.get(),
                   entry.key.field_id.get());
    }

    std::sort(entries.begin(), entries.end(), EntryBefore);
    const auto duplicate = std::adjacent_find(
        entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.key == rhs.key;
        });
    AssertInfo(duplicate == entries.end(),
               "field index capability for field {} contains duplicate key",
               field_id_.get());

    if (entries.empty()) {
        static const auto empty =
            std::make_shared<const std::vector<IndexCapabilityEntry>>();
        entries_ = empty;
    } else {
        entries_ = std::make_shared<const std::vector<IndexCapabilityEntry>>(
            std::move(entries));
    }
}

const IndexCapabilityEntry*
FieldIndexCapability::Find(const IndexKey& key) const {
    if (key.field_id != field_id_) {
        return nullptr;
    }
    const auto& entries = *entries_;
    const auto it = std::lower_bound(
        entries.begin(),
        entries.end(),
        key.identity,
        [](const IndexCapabilityEntry& entry, const IndexIdentity& identity) {
            return entry.key.identity < identity;
        });
    return it != entries.end() && it->key == key ? &*it : nullptr;
}

bool
SameCaps(const index::ReaderCaps& a, const index::ReaderCaps& b) {
    return a.predicate == b.predicate && a.pattern_match == b.pattern_match &&
           a.text_match == b.text_match &&
           a.ngram_candidates == b.ngram_candidates && a.spatial == b.spatial &&
           a.nested == b.nested && a.value_lookup == b.value_lookup &&
           a.cheap_value_lookup == b.cheap_value_lookup &&
           a.json_paths == b.json_paths && a.exact == b.exact;
}

}  // namespace milvus::segcore
