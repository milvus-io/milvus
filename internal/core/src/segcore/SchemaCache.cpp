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

#include "segcore/SchemaCache.h"

#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::segcore {
namespace {

struct SchemaKey {
    int64_t collection_id;
    uint64_t version;

    bool
    operator==(const SchemaKey&) const = default;
};

struct SchemaKeyHash {
    size_t
    operator()(const SchemaKey& key) const noexcept {
        const auto collection_hash = std::hash<int64_t>{}(key.collection_id);
        const auto version_hash = std::hash<uint64_t>{}(key.version);
        return collection_hash ^
               (version_hash + 0x9e3779b9 + (collection_hash << 6) +
                (collection_hash >> 2));
    }
};

struct Entry {
    explicit Entry(SchemaPtr schema) : schema(std::move(schema)) {
    }

    SchemaPtr schema;
    size_t acquisitions = 0;
};

}  // namespace

struct SchemaCache::State {
    void
    Release(const SchemaKey& key, Entry* entry) noexcept {
        std::lock_guard lock(mutex);
        auto it = entries.find(key);
        if (it == entries.end() || it->second.get() != entry ||
            entry->acquisitions == 0) {
            return;
        }

        --entry->acquisitions;
        if (entry->acquisitions == 0) {
            entries.erase(it);
        }
    }

    mutable std::mutex mutex;
    std::unordered_map<SchemaKey, std::unique_ptr<Entry>, SchemaKeyHash>
        entries;
};

SchemaCache::SchemaCache() : state_(std::make_shared<State>()) {
}

SchemaCache::~SchemaCache() = default;

SchemaPtr
SchemaCache::GetOrCreate(
    int64_t collection_id,
    const milvus::proto::schema::CollectionSchema& schema_proto) {
    AssertInfo(collection_id > 0,
               "collection id must be positive, got {}",
               collection_id);
    AssertInfo(schema_proto.version() >= 0,
               "schema version must be non-negative, got {}",
               schema_proto.version());

    const SchemaKey key{collection_id,
                        static_cast<uint64_t>(schema_proto.version())};
    auto state = state_;
    Entry* entry = nullptr;
    {
        std::lock_guard lock(state->mutex);
        if (auto it = state->entries.find(key); it != state->entries.end()) {
            entry = it->second.get();
        } else {
            auto schema = Schema::ParseFrom(schema_proto);
            schema->set_schema_version(schema_proto.version());
            auto inserted = std::make_unique<Entry>(std::move(schema));
            entry = inserted.get();
            state->entries.emplace(key, std::move(inserted));
        }
        ++entry->acquisitions;
    }

    // Build the external control block after dropping the cache mutex. If its
    // allocation throws, shared_ptr invokes the deleter and rolls back the
    // acquisition without trying to lock the mutex recursively.
    return SchemaPtr(entry->schema.get(), [state, key, entry](Schema*) {
        state->Release(key, entry);
    });
}

size_t
SchemaCache::EntryCountForTest() const {
    std::lock_guard lock(state_->mutex);
    return state_->entries.size();
}

SchemaCache&
GetGlobalSchemaCache() {
    static SchemaCache cache;
    return cache;
}

}  // namespace milvus::segcore
