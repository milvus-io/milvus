// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "segcore/SchemaCache.h"

#include <mutex>
#include <unordered_map>
#include <utility>

namespace milvus::segcore {
namespace {

struct SchemaKey {
    int64_t collection_id;
    int32_t version;

    bool
    operator==(const SchemaKey&) const = default;
};

struct SchemaKeyHash {
    size_t
    operator()(const SchemaKey& key) const noexcept {
        const auto first = std::hash<int64_t>{}(key.collection_id);
        const auto second = std::hash<int32_t>{}(key.version);
        return first ^ (second + 0x9e3779b9 + (first << 6) + (first >> 2));
    }
};

struct Entry {
    explicit Entry(SchemaPtr value) : value(std::move(value)) {
    }

    SchemaPtr value;
    size_t leases = 0;
};

}  // namespace

struct SchemaCache::State {
    void
    Release(const SchemaKey& key, Entry* entry) noexcept {
        std::lock_guard lock(mutex);
        auto it = entries.find(key);
        if (it == entries.end() || it->second.get() != entry ||
            entry->leases == 0) {
            return;
        }
        if (--entry->leases == 0) {
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
    const SchemaKey key{collection_id, schema_proto.version()};
    auto state = state_;
    Entry* entry = nullptr;
    {
        std::lock_guard lock(state->mutex);
        if (auto it = state->entries.find(key); it != state->entries.end()) {
            entry = it->second.get();
        } else {
            auto inserted =
                std::make_unique<Entry>(Schema::ParseFrom(schema_proto));
            entry = inserted.get();
            state->entries.emplace(key, std::move(inserted));
        }
        ++entry->leases;
    }
    // Construct the returned shared_ptr after releasing the mutex. If control
    // block allocation throws, its deleter re-enters State::Release.
    return SchemaPtr(entry->value.get(), [state, key, entry](Schema*) {
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
