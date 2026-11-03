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

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/synchronization/DistributedMutex.h>

#include "common/OpContext.h"
#include "common/Types.h"
#include "index/contracts/IIndex.h"
#include "segcore/indexing/FieldIndexCapability.h"
#include "index/contracts/query/IIndexReaderBase.h"

// Sealed index registrations, resolved owners, and request readers.
// Keep each request reader alive while using interfaces borrowed from it.
namespace milvus::segcore {

class IndexInventory {
 public:
    struct ResolvedData {
        std::string family;
        DataType value_type;
        index::ReaderCaps caps;
        index::IndexPtr index;
    };

    using Factory = std::function<ResolvedData(milvus::OpContext*)>;

    // Runtime snapshots share entries and their initialization results.
    class Entry {
     public:
        Entry(IndexCapabilityEntry meta, index::IndexPtr index);
        Entry(IndexRegistration registration, Factory factory);

        const IndexRegistration&
        registration() const {
            return registration_;
        }

        const ResolvedData&
        Resolve(milvus::OpContext* op_ctx) const;

        index::IndexPtr
        Peek() const;

        // Retirement is remembered even when lazy initialization has not finished.
        void
        CancelWarmup() const;

     private:
        // Registration is immutable; resolved data is immutable once ready.
        IndexRegistration registration_;
        mutable std::optional<ResolvedData> resolved_;
        mutable Factory factory_;
        mutable folly::DistributedMutex init_mutex_;
        // Publishes resolved_ to Resolve() and Peek() without taking the lock.
        mutable std::atomic<bool> ready_{false};
        mutable std::atomic<bool> retired_{false};
    };

    // ---- Build side (load) --------------------------------------------------
    // Installs a shared entry without resolving it. Returns the replaced entry, if any; retire it after publishing the new runtime.
    std::shared_ptr<Entry>
    Register(std::shared_ptr<Entry> entry);

    // Returns the removed entry for post-publication retirement.
    std::shared_ptr<Entry>
    Drop(const IndexKey& key);

    // Resolves this field's entries and assembles a fresh capability list.
    // Lazy resolution may read artifact metadata but does not load readers.
    // All entries must resolve; successful entries are reused after failures.
    FieldIndexCapability
    ResolveCapability(milvus::OpContext* op_ctx, FieldId field_id) const;

    // Copies registrations without resolving entries or exposing slots/readers.
    std::vector<IndexRegistration>
    Entries() const;

    std::vector<IndexRegistration>
    Entries(FieldId field_id) const;

    // Retain all entries without resolving them, for post-publication retirement.
    std::vector<std::shared_ptr<Entry>>
    SnapshotEntries() const;

    // Resolves the selected key and retains its request reader.
    // Missing keys return nullptr; resolution, load, and caps errors propagate.
    index::IIndexReaderBasePtr
    CreateIndexReader(milvus::OpContext* op_ctx, const IndexKey& key) const;

 private:
    using FieldEntries =
        std::unordered_map<IndexKey, std::shared_ptr<Entry>, IndexKeyHash>;
    std::unordered_map<FieldId, FieldEntries> entries_;
};

// Related ownership boundaries:
//   - Element-to-row offsets are column-derived runtime state, shared by struct
//     fields and replaced with the column. Readers must not retain stale copies.
//   - Concrete storage cells report cache usage; remote artifact estimates
//     describe admission requirements rather than live memory usage.
//   - GrowingIndexSet uniquely owns growing publishers; GrowingIndexSnapshotPin retains one
//     published reader generation and its contiguous row coverage together.

}  // namespace milvus::segcore
