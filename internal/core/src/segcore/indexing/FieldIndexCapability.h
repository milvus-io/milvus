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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/query/ReaderCaps.h"

// Segment-level collection of index capabilities, readable without pinning.
// Keep a list of per-entry descriptors rather than OR-ing all caps: exactness
// and query interfaces must describe the same index the consumer will select.

namespace milvus::segcore {

// Distinguishes indexes identified before segment construction from
// segment-local registrations without borrowing one integer namespace for
// both. The variant preserves the complete signed pre-built index id and
// unsigned segment-local registration id ranges.
class IndexIdentity {
 public:
    enum class Kind : uint8_t {
        PreBuiltIndex,
        SegmentLocal,
    };

    static IndexIdentity
    PreBuiltIndex(int64_t index_id);

    static IndexIdentity
    SegmentLocal(uint64_t registration_id);

    Kind
    kind() const;

    bool
    operator==(const IndexIdentity& other) const;

    bool
    operator<(const IndexIdentity& other) const;

 private:
    explicit IndexIdentity(std::variant<int64_t, uint64_t> value);

    std::variant<int64_t, uint64_t> value_;

    friend struct IndexKeyHash;
};

// Addresses one index inside the segment's inventory. JSON path is capability
// metadata, not identity: an ordinary catalog index keeps its index id, a
// file-backed TextMatch index uses its build id, and a segment-built/interim
// index receives a segment-local registration id.
struct IndexKey {
    FieldId field_id;
    IndexIdentity identity;

    bool
    operator==(const IndexKey& other) const {
        return field_id == other.field_id && identity == other.identity;
    }
};

struct IndexKeyHash {
    size_t
    operator()(const IndexKey& key) const;
};

// One inventory entry's capability record. Built at LOAD time from load
// metadata (family + build parameters) via the registered
// `index::LoaderEntry::derive_caps` function — never by touching the index
// object.
struct IndexCapabilityEntry {
    IndexKey key;

    // Empty for whole-field indexes. For a per-path JSON cast index this is
    // the selected path; it never substitutes for the stable inventory key.
    std::string json_path;

    // Normalized JSON_CAST_TYPE. This distinguishes, for example, DOUBLE from
    // ARRAY_DOUBLE even though both readers expose DOUBLE as their value type.
    // Non-JSON entries keep UNKNOWN.
    JsonCastType json_cast_type{JsonCastType::UNKNOWN};

    // "inverted" / "bitmap" / "sort" / "marisa" / "fmindex" / "text" /
    // "ngram" / "rtree" / "json_flat" ... — the canonical registry key.
    std::string family;

    // The value type indexed by the reader. ARRAY uses its element type; a JSON
    // per-path index uses its cast target, not the parent field type.
    DataType value_type{DataType::NONE};

    // Metadata-derived caps must equal the opened reader's Caps(); the inventory
    // checks that invariant after pinning.
    index::ReaderCaps caps;
};

class FieldIndexCapability {
 public:
    explicit FieldIndexCapability(
        FieldId field_id, std::vector<IndexCapabilityEntry> entries = {});

    FieldId
    field_id() const {
        return field_id_;
    }

    bool
    empty() const {
        return entries_->empty();
    }

    const std::vector<IndexCapabilityEntry>&
    entries() const {
        return *entries_;
    }

    // Returns the exact entry, or null when `key` names another field or is not
    // registered. Copies of this capability share one immutable entry snapshot,
    // so the pointer remains valid while any copy retaining that snapshot lives.
    const IndexCapabilityEntry*
    Find(const IndexKey& key) const;

 private:
    FieldId field_id_;
    std::shared_ptr<const std::vector<IndexCapabilityEntry>> entries_;
};

// Capability equality for the inventory's post-pin consistency check.
bool
SameCaps(const index::ReaderCaps& a, const index::ReaderCaps& b);

}  // namespace milvus::segcore
