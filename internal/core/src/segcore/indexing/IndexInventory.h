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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "segcore/indexing/FieldIndexCapability.h"
#include "segcore/indexing/IndexPin.h"

// Segment index inventory: metadata, cache entries, and the sealed query pin.
// Capability(field) reads metadata only. Once execution chooses an entry, its
// pin exit opens the payload and checks Caps(). Keep the root pin alive while
// borrowing any query mixin from its IIndexReaderBase.
//
namespace milvus::segcore {

class IndexInventory {
 public:
    using RootSlot = std::shared_ptr<
        milvus::cachinglayer::CacheSlot<index::IIndexReaderBase>>;

    struct Entry {
        // Pure data, readable without a pin.
        IndexCapabilityEntry meta;

        // Type-erased slot shared across index families. The cache cell uniquely
        // owns its IIndexReaderBase; query consumers only borrow it through pins.
        RootSlot slot;
    };

    // ---- Build side (load) --------------------------------------------------
    // Called by segcore's load-planning path once per index. The concrete family
    // and caps are derived from selector/runtime metadata before payload open;
    // registration installs that metadata with a cold slot whose translator
    // opens the reader only when the slot is pinned.
    // Returns the replaced slot, if this exact key already existed. The caller
    // retires it only after publishing the new runtime generation.
    RootSlot
    Register(Entry entry);

    // Returns the removed slot for post-publication retirement.
    RootSlot
    Drop(const IndexKey& key);

    // Metadata-only capability lookup; must not pin payloads. The returned
    // value is a cheap handle to the immutable per-field snapshot rebuilt only
    // when this inventory generation is mutated.
    // Expression consumers select one exact entry from this value before
    // pinning. Legacy availability helpers may project the same metadata but
    // must not open a payload to answer capability questions.
    FieldIndexCapability
    Capability(FieldId field_id) const;

    // Metadata-only snapshot used while normalizing one unpublished runtime
    // generation. Slots and readers are not exposed.
    std::vector<IndexCapabilityEntry>
    Entries() const;

    // Internal lifecycle snapshot for post-publication warmup retirement.
    // Query consumers never receive slots through this API.
    std::vector<RootSlot>
    Slots() const;

    // Erased sealed-segment exit. Callers select one exact metadata entry before
    // pinning, then borrow the required query mixin while retaining this pin.
    // A missing key returns empty; cache loading failures and metadata/reader
    // mismatches propagate as failures.
    IndexPin
    PinIndex(milvus::OpContext* op_ctx, const IndexKey& key) const;

 private:
    void
    RefreshCapability(FieldId field_id);

    std::unordered_map<IndexKey, Entry, IndexKeyHash> entries_;
    std::unordered_map<FieldId, FieldIndexCapability> capabilities_;
};

// Related ownership boundaries:
//   - Element-to-row offsets are column-derived runtime state, shared by struct
//     fields and replaced with the column. Readers must not retain stale copies.
//   - Readers report owned resources through IIndexReaderBase; translators
//     handle cache admission/accounting. Remote artifact estimates are not
//     memory usage.
//   - GrowingIndexSet uniquely owns growing publishers; GrowingIndexSnapshotPin retains one
//     published reader generation and its contiguous row coverage together.

}  // namespace milvus::segcore
