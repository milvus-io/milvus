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

#include "segcore/indexing/IndexInventory.h"

#include <chrono>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::segcore {

// Eager: metadata and owner already exist; storage may still be unloaded.
IndexInventory::Entry::Entry(IndexCapabilityEntry meta, index::IndexPtr index)
    : registration_(RegistrationFromCapability(meta)),
      resolved_(ResolvedData{std::move(meta.family),
                             meta.value_type,
                             meta.caps,
                             std::move(index)}),
      ready_(true) {
    AssertInfo(resolved_->index != nullptr,
               "eager index entry for field {} has no index owner",
               registration_.key.field_id.get());
}

// Lazy: keep the factory for metadata resolution and index creation on first use.
IndexInventory::Entry::Entry(IndexRegistration registration, Factory factory)
    : registration_(std::move(registration)), factory_(std::move(factory)) {
    AssertInfo(factory_ != nullptr,
               "cannot create a lazy index without a factory");
}

const IndexInventory::ResolvedData&
IndexInventory::Entry::Resolve(milvus::OpContext* op_ctx) const {
    if (!ready_.load(std::memory_order_acquire)) {
        const auto check_cancelled = [op_ctx] {
            if (op_ctx &&
                op_ctx->cancellation_token.isCancellationRequested()) {
                throw SegcoreError(ErrorCode::FollyCancel,
                                   "lazy index initialization cancelled");
            }
        };
        std::unique_lock lock(init_mutex_, std::defer_lock);
        // Bound each wait so this caller can cancel independently of the factory.
        do {
            check_cancelled();
        } while (!lock.try_lock_for(std::chrono::milliseconds(10)));
        check_cancelled();

        if (!ready_.load(std::memory_order_acquire)) {
            auto result = factory_(op_ctx);
            AssertInfo(result.index != nullptr,
                       "lazy index factory returned null owner");
            resolved_ = std::move(result);
            // Release the captured load metadata before publishing the result.
            factory_ = nullptr;
            // Pair with CancelWarmup's sequentially consistent publication check.
            ready_.store(true);
            if (retired_.load()) {
                resolved_->index->CancelWarmup();
            }
        }
    }
    return *resolved_;
}

index::IndexPtr
IndexInventory::Entry::Peek() const {
    return ready_.load(std::memory_order_acquire) ? resolved_->index : nullptr;
}

void
IndexInventory::Entry::CancelWarmup() const {
    retired_.store(true);
    if (ready_.load()) {
        resolved_->index->CancelWarmup();
    }
}

std::shared_ptr<IndexInventory::Entry>
IndexInventory::Register(std::shared_ptr<Entry> entry) {
    auto key = entry->registration().key;
    auto& field_entries = entries_[key.field_id];
    auto it = field_entries.find(key);
    if (it == field_entries.end()) {
        field_entries.emplace(std::move(key), std::move(entry));
        return nullptr;
    }
    auto retired = it->second;
    it->second = std::move(entry);
    return retired;
}

std::shared_ptr<IndexInventory::Entry>
IndexInventory::Drop(const IndexKey& key) {
    auto field_it = entries_.find(key.field_id);
    if (field_it == entries_.end()) {
        return nullptr;
    }
    auto& field_entries = field_it->second;
    auto it = field_entries.find(key);
    if (it == field_entries.end()) {
        return nullptr;
    }
    auto retired = it->second;
    field_entries.erase(it);
    if (field_entries.empty()) {
        entries_.erase(field_it);
    }
    return retired;
}

FieldIndexCapability
IndexInventory::ResolveCapability(milvus::OpContext* op_ctx,
                                  FieldId field_id) const {
    const auto field_it = entries_.find(field_id);
    if (field_it == entries_.end()) {
        return FieldIndexCapability(field_id);
    }
    const auto& field_entries = field_it->second;
    std::vector<IndexCapabilityEntry> entries;
    entries.reserve(field_entries.size());
    for (const auto& [key, entry] : field_entries) {
        const auto& resolved = entry->Resolve(op_ctx);
        const auto& registration = entry->registration();
        entries.push_back(IndexCapabilityEntry{key,
                                               registration.json_path,
                                               registration.json_cast_type,
                                               resolved.family,
                                               resolved.value_type,
                                               resolved.caps});
    }
    return FieldIndexCapability(field_id, std::move(entries));
}

std::vector<IndexRegistration>
IndexInventory::Entries() const {
    size_t count = 0;
    for (const auto& [_, field_entries] : entries_) {
        count += field_entries.size();
    }
    std::vector<IndexRegistration> result;
    result.reserve(count);
    for (const auto& [_, field_entries] : entries_) {
        for (const auto& [key, entry] : field_entries) {
            result.push_back(entry->registration());
        }
    }
    return result;
}

std::vector<IndexRegistration>
IndexInventory::Entries(FieldId field_id) const {
    const auto field_it = entries_.find(field_id);
    if (field_it == entries_.end()) {
        return {};
    }
    const auto& field_entries = field_it->second;
    std::vector<IndexRegistration> registrations;
    registrations.reserve(field_entries.size());
    for (const auto& [key, entry] : field_entries) {
        registrations.push_back(entry->registration());
    }
    return registrations;
}

std::vector<std::shared_ptr<IndexInventory::Entry>>
IndexInventory::SnapshotEntries() const {
    std::vector<std::shared_ptr<Entry>> result;
    size_t count = 0;
    for (const auto& [_, entries] : entries_) {
        count += entries.size();
    }
    result.reserve(count);
    for (const auto& [_, entries] : entries_) {
        for (const auto& [key, entry] : entries) {
            result.push_back(entry);
        }
    }
    return result;
}

index::IIndexReaderBasePtr
IndexInventory::CreateIndexReader(milvus::OpContext* op_ctx,
                                  const IndexKey& key) const {
    const auto field_it = entries_.find(key.field_id);
    if (field_it == entries_.end()) {
        return {};
    }
    const auto& field_entries = field_it->second;
    const auto it = field_entries.find(key);
    if (it == field_entries.end()) {
        return {};
    }

    const auto& entry = it->second;
    const auto& resolved = entry->Resolve(op_ctx);
    auto reader = resolved.index->CreateReader(op_ctx);
    AssertInfo(reader != nullptr, "index returned a null request reader");
    AssertInfo(SameCaps(resolved.caps, reader->Caps()),
               "index capability metadata disagrees with request reader");
    return reader;
}

}  // namespace milvus::segcore
